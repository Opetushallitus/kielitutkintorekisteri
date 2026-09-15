package fi.oph.kitu.yki.arvioijat

import fi.oph.kitu.html.FormErrors
import fi.oph.kitu.html.Pagination
import fi.oph.kitu.html.ViewMessage
import fi.oph.kitu.html.table.httpParams
import fi.oph.kitu.i18n.UiText
import fi.oph.kitu.oid.Oid
import fi.oph.kitu.oppijanumero.OppijanumeroException
import fi.oph.kitu.security.CurrentUser
import fi.oph.kitu.util.TimeService
import fi.oph.kitu.util.validation.Validation.ValidationError
import fi.oph.kitu.webmvc.Links
import fi.oph.kitu.yki.arvioijat.solki.Lahetystulos
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.stereotype.Controller
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.ModelAttribute
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.ResponseStatus
import java.net.URI

@Controller
@RequestMapping("/yki/arvioijat")
class YkiArvioijaViewController(
    private val arvioijaService: YkiArvioijaService,
    private val kausiService: YkiArvioijaKausiService,
    private val asetukset: ArvioijarekisteriAsetukset,
    private val timeService: TimeService,
) {
    @GetMapping("", produces = ["text/html"])
    fun arvioijatView(
        @ModelAttribute params: YkiArvioijaParams,
    ): ResponseEntity<String> {
        val arvioijat = arvioijaService.haeSivullinen(params)
        val kokonaismaara = arvioijaService.laske(params)

        return ResponseEntity.ok(
            YkiArvioijaPage.render(
                arvioijat = arvioijat,
                params = params,
                pagination =
                    Pagination.valueOf(
                        currentPageNumber = params.page,
                        numberOfRows = kokonaismaara,
                        pageSize = params.limit,
                        url = { page ->
                            Links.Yki.arvioijat() + httpParams(params.toMap() + ("page" to page.toString()))
                        },
                    ),
                muokkausKaytossa = asetukset.muokkausKaytossa,
            ),
        )
    }

    @GetMapping("/uusi", produces = ["text/html"])
    fun uusiArvioijaView(
        @ModelAttribute form: ArvioijaHakuFormData,
    ): ResponseEntity<String> = ResponseEntity.ok(YkiArvioijaLomakePage.renderHaku(form, FormErrors.EMPTY))

    @PostMapping("/uusi/haku", produces = ["text/html"])
    fun arvioijaHaku(
        @ModelAttribute form: ArvioijaHakuFormData,
    ): ResponseEntity<String> =
        arvioijaService.haeHenkilotiedot(form.oppijanumero).fold(
            ifLeft = { virhe ->
                ResponseEntity.ok(YkiArvioijaLomakePage.renderHaku(form, lomakevirheet(virhe)))
            },
            ifRight = { esitaytto ->
                ResponseEntity.ok(
                    YkiArvioijaLomakePage.renderLomake(ArvioijaFormData.of(esitaytto), FormErrors.EMPTY),
                )
            },
        )

    @PostMapping("/uusi", produces = ["text/html"])
    fun luoArvioija(
        @ModelAttribute form: ArvioijaFormData,
        viewMessage: ViewMessage? = null,
    ): ResponseEntity<String> {
        val oid =
            Oid.parse(form.arvioijaOid).getOrNull()
                ?: return lomakeVirheella(
                    form,
                    "arvioijaOid",
                    UiText.Yki.Arvioija.oppijanumero
                        .toString(),
                )
        val alkupaiva =
            form.kaudenAlkupaiva
                ?: return lomakeVirheella(
                    form,
                    "kaudenAlkupaiva",
                    "${UiText.Yki.Arvioija.kaudenAlkupaiva} on pakollinen tieto",
                )

        return arvioijaService.luoArvioija(form.toCommand(oid, alkupaiva), CurrentUser.oid(), form.muokattu).fold(
            ifLeft = { virhe ->
                ResponseEntity.ok(YkiArvioijaLomakePage.renderLomake(form, lomakevirheet(virhe)))
            },
            ifRight = { arvioija ->
                viewMessage?.showSuccess(
                    UiText.Yki.Arvioija.tallennettu
                        .toString(),
                )
                ResponseEntity
                    .status(HttpStatus.SEE_OTHER)
                    .location(URI.create(Links.Yki.arvioija(arvioija.id!!.toInt())))
                    .build()
            },
        )
    }

    @GetMapping("/{id}/muokkaa", produces = ["text/html"])
    fun muokkaaArvioijaaView(
        @PathVariable id: Int,
    ): ResponseEntity<String> {
        val arvioija =
            arvioijaService.haeArvioija(id)
                ?: throw YkiArvioijaNotFoundError()

        val turvakielto = arvioijaService.haeTurvakielto(arvioija.arvioijaOid)

        return ResponseEntity.ok(muokkausLomake(id, ArvioijaFormData.of(arvioija, turvakielto), FormErrors.EMPTY))
    }

    @PostMapping("/{id}", produces = ["text/html"])
    fun tallennaMuutokset(
        @PathVariable id: Int,
        @ModelAttribute form: ArvioijaFormData,
        viewMessage: ViewMessage? = null,
    ): ResponseEntity<String> {
        if (!arvioijaService.onOlemassa(id)) {
            throw YkiArvioijaNotFoundError()
        }

        val lomakkeenOid =
            Oid.parse(form.arvioijaOid).getOrNull()
                ?: return ResponseEntity.ok(
                    muokkausLomake(
                        id,
                        form,
                        yleinen(
                            UiText.Yki.Arvioija.eiLoydy
                                .toString(),
                        ),
                    ),
                )

        return arvioijaService
            .paivitaArvioija(id, form.toPaivitys(lomakkeenOid), CurrentUser.oid(), form.muokattu)
            .fold(
                ifLeft = { virhe ->
                    if (virhe == YkiArvioijaError.ArvioijaaEiLoydy) {
                        throw YkiArvioijaNotFoundError()
                    } else {
                        ResponseEntity.ok(muokkausLomake(id, form, lomakevirheet(virhe)))
                    }
                },
                ifRight = {
                    viewMessage?.showSuccess(
                        UiText.Yki.Arvioija.muutoksetTallennettu
                            .toString(),
                    )
                    ResponseEntity
                        .status(HttpStatus.SEE_OTHER)
                        .location(URI.create(Links.Yki.arvioija(id)))
                        .build()
                },
            )
    }

    private fun muokkausLomake(
        id: Int,
        form: ArvioijaFormData,
        errors: FormErrors,
    ): String =
        YkiArvioijaLomakePage.renderLomake(
            form = form,
            errors = errors,
            otsikko = UiText.Yki.Arvioija.muokkaaArvioijaa,
            naytaKausi = false,
            action = Links.Yki.arvioija(id),
            tallennaTeksti = UiText.Yki.Arvioija.tallennaMuutokset,
            peruutusLinkki = Links.Yki.arvioija(id),
        )

    @PostMapping("/{id}/passivoi", produces = ["text/html"])
    fun passivoiArvioija(
        @PathVariable id: Int,
        viewMessage: ViewMessage? = null,
    ): ResponseEntity<String> =
        arvioijaService.passivoiArvioija(id, CurrentUser.oid()).fold(
            ifLeft = { throw YkiArvioijaNotFoundError() },
            ifRight = {
                viewMessage?.showSuccess(
                    UiText.Yki.Arvioija.passivoitu
                        .toString(),
                )
                ResponseEntity
                    .status(HttpStatus.SEE_OTHER)
                    .location(URI.create(Links.Yki.arvioija(id)))
                    .build()
            },
        )

    @PostMapping("/{id}/laheta", produces = ["text/html"])
    fun lahetaArvioijaSolkiin(
        @PathVariable id: Int,
        viewMessage: ViewMessage? = null,
    ): ResponseEntity<String> =
        arvioijaService.lahetaUudelleen(id).fold(
            ifLeft = { throw YkiArvioijaNotFoundError() },
            ifRight = { tulos ->
                // Lahetys on jo tehty synkronisesti, joten viesti kertoo lopputuloksen eika
                // pelkkaa kaynnistysta: muuten kytkin kiinni tai 500 nayttaisi onnistumiselta.
                when (tulos) {
                    Lahetystulos.LAHETETTY -> {
                        viewMessage?.showSuccess(
                            UiText.Yki.Arvioija.lahetysOnnistui
                                .toString(),
                        )
                    }

                    Lahetystulos.VIRHE -> {
                        viewMessage?.showError(
                            UiText.Yki.Arvioija.lahetysEpaonnistui
                                .toString(),
                        )
                    }

                    Lahetystulos.EI_KAYTOSSA -> {
                        viewMessage?.showInfo(
                            UiText.Yki.Arvioija.lahetysEiKaytossa
                                .toString(),
                        )
                    }
                }
                ResponseEntity
                    .status(HttpStatus.SEE_OTHER)
                    .location(URI.create(Links.Yki.arvioija(id)))
                    .build()
            },
        )

    @GetMapping("/{id}", produces = ["text/html"])
    fun arvioijaView(
        @PathVariable id: Int,
        viewMessage: ViewMessage? = null,
    ): ResponseEntity<String> {
        val arvioija =
            arvioijaService.haeArvioija(id)
                ?: throw YkiArvioijaNotFoundError()

        return ResponseEntity.ok(
            YkiArvioijaTiedotPage.render(
                arvioija = arvioija,
                kaudet = kausiService.haeKaudet(id),
                muutosloki = kausiService.haeMuutosloki(id),
                turvakielto = arvioijaService.haeTurvakielto(arvioija.arvioijaOid),
                flash = viewMessage?.consume(),
                muokkausKaytossa = asetukset.muokkausKaytossa,
                integraatioKaytossa = asetukset.integraatioKaytossa,
                tanaan = timeService.today(),
            ),
        )
    }

    private fun lomakeVirheella(
        form: ArvioijaFormData,
        kentta: String,
        viesti: String,
    ): ResponseEntity<String> =
        ResponseEntity.ok(
            YkiArvioijaLomakePage.renderLomake(
                form,
                FormErrors.of(listOf(ValidationError(listOf(kentta), viesti))),
            ),
        )
}

@ResponseStatus(value = HttpStatus.NOT_FOUND, reason = "YKI-arvioijaa ei löytynyt")
class YkiArvioijaNotFoundError : RuntimeException()
