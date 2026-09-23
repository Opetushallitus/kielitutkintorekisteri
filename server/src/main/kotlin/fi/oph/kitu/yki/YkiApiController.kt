package fi.oph.kitu.yki

import fi.oph.kitu.ilmoittautumisjarjestelma.IlmoittautumisjarjestelmaService
import fi.oph.kitu.oid.Oid
import fi.oph.kitu.oppijanumero.OppijanumeroException
import fi.oph.kitu.oppijanumero.OppijanumeroHakuService
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
import org.springframework.http.HttpStatus
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
    private val oppijanumeroHaku: OppijanumeroHakuService,
    private val arvioijaService: YkiArvioijaService,
    private val asetukset: ArvioijarekisteriAsetukset,
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

    @PostMapping("/oppijanumero-haku")
    @Tag(name = "oauth2")
    @Operation(
        summary = "Oppijanumeron haku henkilötunnuksen ja nimien perusteella",
        description =
            "Palauttaa Oppijanumerorekisterin master-oppijanumeron henkilölle, joka tunnistetaan " +
                "henkilötunnuksen ja nimien perusteella. Tarkoitettu historiadatan migraatioon " +
                "riveille, joilta oppijanumero puuttuu.",
    )
    @ApiResponses(
        value = [
            ApiResponse(responseCode = "200", description = "OK"),
            ApiResponse(responseCode = "400", description = "Pakollinen kenttä puuttuu"),
            ApiResponse(responseCode = "404", description = "Oppijaa ei löytynyt Oppijanumerorekisteristä"),
            ApiResponse(
                responseCode = "502",
                description =
                    "Oppijanumerorekisterin haku epäonnistui. Virheteksti sisältää " +
                        "oppijanumerorekisterin oman HTTP-statuksen ja vastauksen, jotta " +
                        "esimerkiksi validointivirhe ja kuormanrajoitus erottuvat toisistaan.",
            ),
        ],
    )
    fun postOppijanumeroHaku(
        @RequestBody haku: OppijanumeroHakuRequest,
    ): ResponseEntity<*> {
        if (haku.hetu.isBlank() || haku.etunimet.isBlank() || haku.sukunimi.isBlank()) {
            return TiedonsiirtoFailure
                .badRequest("hetu, etunimet ja sukunimi ovat pakollisia")
                .toResponseEntity()
        }
        val oppija =
            oppijanumeroHaku.oppijaOf(
                hetu = haku.hetu,
                etunimet = haku.etunimet,
                sukunimi = haku.sukunimi,
                kutsumanimi = haku.kutsumanimi,
            )
        return oppijanumeroHaku.haeMasterOid(oppija).fold(
            ifLeft = { error ->
                when (error) {
                    is OppijanumeroException.OppijaNotIdentifiedException,
                    is OppijanumeroException.OppijaNotFoundException,
                    -> {
                        TiedonsiirtoFailure(
                            HttpStatus.NOT_FOUND,
                            listOf("Oppijaa ei löytynyt Oppijanumerorekisteristä"),
                        ).toResponseEntity()
                    }

                    else -> {
                        TiedonsiirtoFailure(
                            HttpStatus.BAD_GATEWAY,
                            listOf(
                                "Oppijanumeron haku epäonnistui (${error::class.simpleName}): " +
                                    oppijanumeroVirheenKuvaus(error),
                            ),
                        ).toResponseEntity()
                    }
                }
            },
            ifRight = { oid -> ResponseEntity.ok(OppijanumeroHakuResponse(oid)) },
        )
    }

    @PostMapping("/oppijanumero-haku-hetulista")
    @Tag(name = "oauth2")
    @Operation(
        summary = "Oppijanumeroiden haku pelkillä henkilötunnuksilla",
        description =
            "Palauttaa oppijanumerot annetuille henkilötunnuksille ilman nimivertailua. " +
                "Tarkoitettu historiadatan migraatioon, jossa nimet ovat usein vanhentuneita: " +
                "nimillä tehty haku hylkää tällaiset henkilöt vaikka he ovat rekisterissä. " +
                "Vastaus sisältää vain ratkenneet tunnukset; loput luetellaan kentässä puuttuvat.",
    )
    @ApiResponses(
        value = [
            ApiResponse(responseCode = "200", description = "OK"),
            ApiResponse(responseCode = "400", description = "Tyhjä tai liian suuri hetulista"),
            ApiResponse(responseCode = "502", description = "Oppijanumerorekisterin haku epäonnistui"),
        ],
    )
    fun postOppijanumeroHakuHetulista(
        @RequestBody haku: OppijanumeroHetulistaRequest,
    ): ResponseEntity<*> {
        val hetut =
            haku.hetut
                .map { it.trim() }
                .filter { it.isNotEmpty() }
                .distinct()
        if (hetut.isEmpty()) {
            return TiedonsiirtoFailure.badRequest("hetut ei saa olla tyhjä").toResponseEntity()
        }
        if (hetut.size > HETULISTAN_ENIMMAISKOKO) {
            return TiedonsiirtoFailure
                .badRequest("hetut: enintään $HETULISTAN_ENIMMAISKOKO kerralla, sai ${hetut.size}")
                .toResponseEntity()
        }

        return oppijanumeroHaku.haeOppijanumerotHetuilla(hetut).fold(
            ifLeft = { error ->
                TiedonsiirtoFailure(
                    HttpStatus.BAD_GATEWAY,
                    listOf(
                        "Oppijanumeroiden haku epäonnistui (${error::class.simpleName}): " +
                            oppijanumeroVirheenKuvaus(error),
                    ),
                ).toResponseEntity()
            },
            ifRight = { oppijanumerot ->
                ResponseEntity.ok(
                    OppijanumeroHetulistaResponse(
                        oppijanumerot = oppijanumerot.mapValues { (_, oid) -> oid.toString() },
                        puuttuvat = hetut.filterNot { oppijanumerot.containsKey(it) },
                    ),
                )
            },
        )
    }

    private fun oppijanumeroVirheenKuvaus(error: OppijanumeroException): String {
        val vastaus = (error as? OppijanumeroException.HasResponse)?.response
        return listOfNotNull(
            vastaus?.let { "oppijanumerorekisteri vastasi HTTP ${it.statusCode.value()}" },
            error.oppijanumeroServiceError
                ?.message
                ?.trim()
                ?.takeIf { it.isNotEmpty() },
            vastaus
                ?.body
                ?.trim()
                ?.takeIf { it.isNotEmpty() }
                ?.take(ONR_VIRHEEN_ENIMMAISPITUUS),
            error.message?.takeIf { vastaus == null },
        ).joinToString(", ").ifBlank { "ei lisätietoja" }
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

data class OppijanumeroHakuRequest(
    val hetu: String,
    val etunimet: String,
    val sukunimi: String,
    val kutsumanimi: String? = null,
)

data class OppijanumeroHakuResponse(
    val oid: Oid,
)

data class OppijanumeroHetulistaRequest(
    val hetut: List<String>,
)

data class OppijanumeroHetulistaResponse(
    val oppijanumerot: Map<String, String>,
    val puuttuvat: List<String>,
)

private const val ONR_VIRHEEN_ENIMMAISPITUUS = 300
private const val HETULISTAN_ENIMMAISKOKO = 1000
