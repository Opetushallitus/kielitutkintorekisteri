package fi.oph.kitu.yki.arvioijat

import arrow.core.Either
import fi.oph.kitu.html.FormErrors
import fi.oph.kitu.html.ViewMessage
import fi.oph.kitu.i18n.LocalizedString
import fi.oph.kitu.i18n.UiText
import fi.oph.kitu.security.CurrentUser
import fi.oph.kitu.util.validation.Validation.ValidationError
import fi.oph.kitu.webmvc.Links
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.stereotype.Controller
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.ModelAttribute
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestMapping
import java.net.URI
import java.time.LocalDate

@Controller
@RequestMapping("/yki/arvioijat/{id}/kaudet")
class YkiArvioijaKausiViewController(
    private val arvioijaService: YkiArvioijaService,
    private val kausiService: YkiArvioijaKausiService,
) {
    @GetMapping("/uusi", produces = ["text/html"])
    fun uusiKausiView(
        @PathVariable id: Int,
        @ModelAttribute form: KausiFormData,
    ): ResponseEntity<String> {
        val arvioija = arvioijaService.haeArvioija(id) ?: return eiLoydy()
        return ResponseEntity.ok(
            lomake(arvioija, form, FormErrors.EMPTY, Links.Yki.kaudet(id), UiText.Yki.Arvioija.Kausi.uusi),
        )
    }

    @PostMapping("", produces = ["text/html"])
    fun luoKausi(
        @PathVariable id: Int,
        @ModelAttribute form: KausiFormData,
        viewMessage: ViewMessage? = null,
    ): ResponseEntity<String> {
        val arvioija = arvioijaService.haeArvioija(id) ?: return eiLoydy()
        val alkupaiva =
            form.alkupaiva
                ?: return puuttuvaAlkupaiva(arvioija, form, Links.Yki.kaudet(id), UiText.Yki.Arvioija.Kausi.uusi)

        return kausiService
            .lisaaKausi(id, komento(id, null, alkupaiva, form), CurrentUser.oid())
            .vastaus(
                arvioija,
                form,
                Links.Yki.kaudet(id),
                UiText.Yki.Arvioija.Kausi.uusi,
                viewMessage,
                UiText.Yki.Arvioija.Kausi.lisatty,
            )
    }

    @GetMapping("/{kausiId}/muokkaa", produces = ["text/html"])
    fun muokkaaKausiView(
        @PathVariable id: Int,
        @PathVariable kausiId: Int,
    ): ResponseEntity<String> {
        val arvioija = arvioijaService.haeArvioija(id) ?: return eiLoydy()
        val kausi = kausiService.haeKausi(id, kausiId) ?: return eiLoydy()

        return ResponseEntity.ok(
            lomake(
                arvioija,
                KausiFormData.of(kausi),
                FormErrors.EMPTY,
                Links.Yki.kausi(id, kausiId),
                UiText.Yki.Arvioija.Kausi.muokkaaOtsikko,
            ),
        )
    }

    @PostMapping("/{kausiId}", produces = ["text/html"])
    fun paivitaKausi(
        @PathVariable id: Int,
        @PathVariable kausiId: Int,
        @ModelAttribute form: KausiFormData,
        viewMessage: ViewMessage? = null,
    ): ResponseEntity<String> {
        val arvioija = arvioijaService.haeArvioija(id) ?: return eiLoydy()
        val action = Links.Yki.kausi(id, kausiId)
        val alkupaiva =
            form.alkupaiva ?: return puuttuvaAlkupaiva(arvioija, form, action, UiText.Yki.Arvioija.Kausi.muokkaaOtsikko)

        return kausiService
            .paivitaKausi(id, kausiId, komento(id, kausiId, alkupaiva, form), CurrentUser.oid())
            .vastaus(
                arvioija,
                form,
                action,
                UiText.Yki.Arvioija.Kausi.muokkaaOtsikko,
                viewMessage,
                UiText.Yki.Arvioija.Kausi.paivitetty,
            )
    }

    @PostMapping("/{kausiId}/passivoi", produces = ["text/html"])
    fun passivoiKausi(
        @PathVariable id: Int,
        @PathVariable kausiId: Int,
        viewMessage: ViewMessage? = null,
    ): ResponseEntity<String> =
        kausiService
            .passivoiKausi(id, kausiId, CurrentUser.oid())
            .flash(id, viewMessage, UiText.Yki.Arvioija.Kausi.passivoitu)

    @PostMapping("/{kausiId}/poista", produces = ["text/html"])
    fun poistaKausi(
        @PathVariable id: Int,
        @PathVariable kausiId: Int,
        viewMessage: ViewMessage? = null,
    ): ResponseEntity<String> =
        kausiService
            .poistaKausi(id, kausiId, CurrentUser.oid())
            .flash(id, viewMessage, UiText.Yki.Arvioija.Kausi.poistettu)

    private fun komento(
        arvioijaId: Int,
        kausiId: Int?,
        alkupaiva: LocalDate,
        form: KausiFormData,
    ) = TallennaKausi(
        arvioijaId = arvioijaId,
        kausiId = kausiId,
        alkupaiva = alkupaiva,
        arviointioikeudet = form.arviointioikeudet(),
        ashaNumero = form.ashaNumeroTrimmattuna(),
    )

    /** Lomakkeeton toiminto ei voi nayttaa kenttavirhetta, joten virhe kerrotaan flash-viestina. */
    private fun Either<YkiArvioijaError, Unit>.flash(
        arvioijaId: Int,
        viewMessage: ViewMessage?,
        onnistui: LocalizedString,
    ): ResponseEntity<String> =
        fold(
            ifLeft = { virhe ->
                if (virhe == YkiArvioijaError.ArvioijaaEiLoydy || virhe == YkiArvioijaError.KauttaEiLoydy) {
                    eiLoydy()
                } else {
                    viewMessage?.showError(lomakevirheet(virhe).yleiset.joinToString(" "))
                    takaisin(arvioijaId)
                }
            },
            ifRight = {
                viewMessage?.showSuccess(onnistui.toString())
                takaisin(arvioijaId)
            },
        )

    private fun Either<YkiArvioijaError, Unit>.vastaus(
        arvioija: YkiArvioijaEntity,
        form: KausiFormData,
        action: String,
        otsikko: LocalizedString,
        viewMessage: ViewMessage?,
        onnistui: LocalizedString,
    ): ResponseEntity<String> =
        fold(
            ifLeft = { virhe ->
                if (virhe == YkiArvioijaError.ArvioijaaEiLoydy || virhe == YkiArvioijaError.KauttaEiLoydy) {
                    eiLoydy()
                } else {
                    ResponseEntity.ok(lomake(arvioija, form, lomakevirheet(virhe), action, otsikko))
                }
            },
            ifRight = {
                viewMessage?.showSuccess(onnistui.toString())
                takaisin(arvioija.id!!.toInt())
            },
        )

    private fun puuttuvaAlkupaiva(
        arvioija: YkiArvioijaEntity,
        form: KausiFormData,
        action: String,
        otsikko: LocalizedString,
    ): ResponseEntity<String> =
        ResponseEntity.ok(
            lomake(
                arvioija,
                form,
                FormErrors.of(
                    listOf(
                        ValidationError(
                            listOf("alkupaiva"),
                            "${UiText.Yki.Arvioija.kaudenAlkupaiva} on pakollinen tieto",
                        ),
                    ),
                ),
                action,
                otsikko,
            ),
        )

    private fun lomake(
        arvioija: YkiArvioijaEntity,
        form: KausiFormData,
        errors: FormErrors,
        action: String,
        otsikko: LocalizedString,
    ): String = YkiKausiLomakePage.render(arvioija, form, errors, action, otsikko)

    private fun takaisin(arvioijaId: Int): ResponseEntity<String> =
        ResponseEntity
            .status(HttpStatus.SEE_OTHER)
            .location(URI.create(Links.Yki.arvioija(arvioijaId)))
            .build()

    private fun eiLoydy(): Nothing = throw YkiArvioijaNotFoundError()
}
