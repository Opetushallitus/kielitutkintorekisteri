package fi.oph.kitu.yki

import fi.oph.kitu.ilmoittautumisjarjestelma.IlmoittautumisjarjestelmaService
import fi.oph.kitu.tiedontuontischema.Henkilosuoritus
import fi.oph.kitu.tiedontuontischema.TiedonsiirtoFailure
import fi.oph.kitu.tiedontuontischema.TiedonsiirtoSuccess
import fi.oph.kitu.tiedontuontischema.YkiSuoritus
import fi.oph.kitu.util.validation.ValidationService
import fi.oph.kitu.util.validation.getOrThrow
import fi.oph.kitu.webmvc.csvAttachmentResponse
import fi.oph.kitu.yki.Arviointitila.ARVIOITU
import fi.oph.kitu.yki.arvioijat.ArvioijarekisteriAsetukset
import fi.oph.kitu.yki.arvioijat.Tallennuslahde
import fi.oph.kitu.yki.arvioijat.YkiArvioija
import fi.oph.kitu.yki.arvioijat.YkiArvioijaColumn
import fi.oph.kitu.yki.arvioijat.YkiArvioijaParams
import fi.oph.kitu.yki.arvioijat.YkiArvioijaRepository
import fi.oph.kitu.yki.arvioijat.YkiArvioijaService
import fi.oph.kitu.yki.historia.YkiHistoriaSiirtymatonColumn
import fi.oph.kitu.yki.historia.YkiHistoriaSiirtymatonParams
import fi.oph.kitu.yki.historia.YkiHistoriaSiirtymatonRepository
import fi.oph.kitu.yki.suoritukset.YkiSuoritusColumn
import fi.oph.kitu.yki.suoritukset.YkiSuoritusEntity
import fi.oph.kitu.yki.suoritukset.YkiSuoritusRepository
import io.opentelemetry.api.trace.Span
import io.opentelemetry.api.trace.StatusCode
import io.swagger.v3.oas.annotations.Operation
import io.swagger.v3.oas.annotations.media.Content
import io.swagger.v3.oas.annotations.media.ExampleObject
import io.swagger.v3.oas.annotations.media.Schema
import io.swagger.v3.oas.annotations.responses.ApiResponse
import io.swagger.v3.oas.annotations.responses.ApiResponses
import io.swagger.v3.oas.annotations.tags.Tag
import jakarta.servlet.http.HttpSession
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.ModelAttribute
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody
import io.swagger.v3.oas.annotations.parameters.RequestBody as SwaggerRequestBody

@RestController
@RequestMapping("/yki/api")
@Tag(name = "Yleinen kielitutkinto")
class YkiApiController(
    private val service: YkiService,
    private val validationService: ValidationService,
    private val ykiArvioijaRepository: YkiArvioijaRepository,
    private val ykiSuoritusRepository: YkiSuoritusRepository,
    private val ilmoittautumisjarjestelma: IlmoittautumisjarjestelmaService,
    private val arvioijaService: YkiArvioijaService,
    private val asetukset: ArvioijarekisteriAsetukset,
    private val historiaSiirtymatonRepository: YkiHistoriaSiirtymatonRepository,
) {
    @GetMapping("/suoritukset", "/suoritus", produces = ["text/csv"])
    fun getSuorituksetAsCsv(
        @ModelAttribute params: YkiSuorituksetParams = YkiSuorituksetParams(),
        session: HttpSession? = null,
    ): ResponseEntity<StreamingResponseBody> =
        params.withRecalledSearch(session).let { withSearch ->
            csvAttachmentResponse<YkiSuoritusColumn, _>(
                filename = withSearch.csvFileName(),
                data =
                    service.allSuorituksetIncludingOpiskeluoikeusOid(
                        withSearch.versionHistory,
                        service.extendFilterWithLinkedOidsOrThrow(withSearch.toFilter()),
                    ),
                excludeTags = withSearch.excludeTags(),
            )
        }

    @GetMapping("/arvioijat", produces = ["text/csv"])
    fun getArvioijatCsv(
        @ModelAttribute params: YkiArvioijaParams = YkiArvioijaParams(),
    ): ResponseEntity<StreamingResponseBody> =
        csvAttachmentResponse<YkiArvioijaColumn, _>(
            filename = params.csvFileName(),
            data = arvioijaService.haeKaikki(params),
            excludeTags = params.excludeTags(),
        )

    @GetMapping("/historia-siirtymattomat", produces = ["text/csv"])
    fun getHistoriaSiirtymattomatCsv(
        @ModelAttribute params: YkiHistoriaSiirtymatonParams = YkiHistoriaSiirtymatonParams(),
    ): ResponseEntity<StreamingResponseBody> =
        csvAttachmentResponse<YkiHistoriaSiirtymatonColumn, _>(
            filename = params.csvFileName(),
            data = historiaSiirtymatonRepository.haeKaikki(params),
            excludeTags = params.excludeTags(),
        )

    @PostMapping("/suoritus")
    @Tag(name = "oauth2")
    @Operation(
        summary = "Yleisen kielitutkinnon suoritusten siirto Kielitutkintorekisteriin",
        requestBody =
            SwaggerRequestBody(
                "Yleisen kielitutkinnon suoritus",
                content = [
                    Content(
                        mediaType = "application/json",
                        schema = Schema(YkiSuoritus::class),
                        examples = [
                            ExampleObject(
                                name = "Yleisen kielitutkinnon suoritus",
                                externalValue = "/kielitutkinnot/schema-examples/yki-suoritus.json",
                            ),
                        ],
                    ),
                ],
            ),
    )
    @ApiResponses(
        value = [
            ApiResponse(
                responseCode = "200",
                description = "OK",
                content = [
                    Content(
                        mediaType = "application/json",
                        schema = Schema(TiedonsiirtoSuccess::class),
                        examples = [
                            ExampleObject(
                                name = "Onnistunut siirto",
                                externalValue = "/kielitutkinnot/schema-examples/tiedonsiirto-ok.json",
                            ),
                        ],
                    ),
                ],
            ),
            ApiResponse(
                responseCode = "400",
                description = "Virheellinen suorituksen rakenne",
                content = [
                    Content(
                        mediaType = "application/json",
                        schema = Schema(TiedonsiirtoFailure::class),
                        examples = [
                            ExampleObject(
                                name = "virheellinen henkilö-oid",
                                externalValue = "/kielitutkinnot/schema-examples/bad-request-invalid-oid.json",
                            ),
                        ],
                    ),
                ],
            ),
            ApiResponse(
                responseCode = "401",
                description = "Ei käyttöoikeuksia tai yritettiin siirtää väärän tyyppistä suoritusta",
                content = [
                    Content(
                        mediaType = "application/json",
                        schema = Schema(TiedonsiirtoFailure::class),
                        examples = [
                            ExampleObject(
                                name = "Ei oikeutta siirtää kyseistä suoritusta",
                                externalValue = "/kielitutkinnot/schema-examples/tiedonsiirto-forbidden-yki.json",
                            ),
                        ],
                    ),
                ],
            ),
        ],
    )
    fun postHenkilosuoritus(
        @RequestBody data: Henkilosuoritus<YkiSuoritus>,
    ): ResponseEntity<*> {
        val enrichedData = validationService.validateAndEnrich(data).getOrThrow()
        val entity =
            try {
                YkiSuoritusEntity.from(enrichedData)
            } catch (e: IllegalArgumentException) {
                return TiedonsiirtoFailure
                    .badRequest(e.message.toString())
                    .toResponseEntity()
                    .also {
                        val span = Span.current()
                        span.recordException(e)
                        span.setStatus(StatusCode.ERROR)
                    }
            }

        Span.current().setAttribute(
            "arvioitu",
            entity.arviointitila == ARVIOITU || entity.arviointitila == Arviointitila.TARKISTUSARVIOITU,
        )

        ykiSuoritusRepository.save(entity, false)
        ilmoittautumisjarjestelma.sendArvioinninTila(entity)
        return TiedonsiirtoSuccess().toResponseEntity()
    }

    @PostMapping("/arvioija")
    @Tag(name = "oauth2")
    @Operation(
        summary = "Yleisen kielitutkinnon arvioijan siirto Kielitutkintorekisteriin",
        requestBody =
            SwaggerRequestBody(
                "Yleisen kielitutkinnon arvioija",
                content = [
                    Content(
                        mediaType = "application/json",
                        schema = Schema(YkiArvioija::class),
                        examples = [
                            ExampleObject(
                                name = "Yleisen kielitutkinnon arvioija",
                                externalValue = "/kielitutkinnot/schema-examples/yki-arvioija.json",
                            ),
                        ],
                    ),
                ],
            ),
    )
    @ApiResponses(
        value = [
            ApiResponse(
                responseCode = "200",
                description = "OK",
                content = [
                    Content(
                        mediaType = "application/json",
                        schema = Schema(TiedonsiirtoSuccess::class),
                        examples = [
                            ExampleObject(
                                name = "Onnistunut siirto",
                                externalValue = "/kielitutkinnot/schema-examples/tiedonsiirto-ok.json",
                            ),
                        ],
                    ),
                ],
            ),
            ApiResponse(
                responseCode = "400",
                description = "Virheellinen arvioijan rakenne",
                content = [
                    Content(
                        mediaType = "application/json",
                        schema = Schema(TiedonsiirtoFailure::class),
                        examples = [
                            ExampleObject(
                                name = "Puuttuva kenttä",
                                externalValue =
                                    "/kielitutkinnot/schema-examples/bad-request-arvioija-missing-value.json",
                            ),
                        ],
                    ),
                ],
            ),
        ],
    )
    fun postArvioija(
        @RequestBody arvioija: YkiArvioija,
    ): ResponseEntity<*> {
        val validatedArvioija = validationService.validateAndEnrich(arvioija).getOrThrow()

        return if (asetukset.integraatioKaytossa) {
            paivitaYhteystiedot(validatedArvioija)
        } else {
            // Siirtymavaihe: kitu ei ole viela master, joten Solkin koko payload otetaan vastaan.
            // Osittainen push ei saa pyyhkia muita kielia eika Solkin omaa dataa lahetetaan
            // takaisin Solkiin (Tallennuslahde.SOLKI hoitaa molemmat).
            ykiArvioijaRepository.tallenna(validatedArvioija.toEntity(), lahde = Tallennuslahde.SOLKI)
            TiedonsiirtoSuccess().toResponseEntity()
        }
    }

    /**
     * Kavennettu sisaantulo (§4.2): kitun ollessa master Solki saa paivittaa vain yhteystiedot.
     * Nimet omistaa ONR ja arviointioikeudet kitu, joten muu payload ohitetaan. Tuntematonta
     * arvioijaa ei luoda: kitu paattaa kuka rekisterissa on.
     */
    private fun paivitaYhteystiedot(arvioija: YkiArvioija): ResponseEntity<*> {
        val olemassaoleva =
            ykiArvioijaRepository.findByArvioijaOid(arvioija.arvioijaOid)
                ?: return TiedonsiirtoFailure
                    .badRequest("Arvioijaa ${arvioija.arvioijaOid} ei ole rekisterissa")
                    .toResponseEntity()

        ykiArvioijaRepository.tallenna(
            olemassaoleva.copy(
                sahkopostiosoite = arvioija.sahkopostiosoite,
                katuosoite = arvioija.katuosoite,
                postinumero = arvioija.postinumero,
                postitoimipaikka = arvioija.postitoimipaikka,
            ),
            lahde = Tallennuslahde.SOLKI,
        )

        return TiedonsiirtoSuccess().toResponseEntity()
    }
}
