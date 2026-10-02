package fi.oph.kitu.yki.arvioijat

import fi.oph.kitu.html.FormErrors
import fi.oph.kitu.html.Page
import fi.oph.kitu.html.card
import fi.oph.kitu.html.cardContent
import fi.oph.kitu.html.dateInput
import fi.oph.kitu.html.formErrorSummary
import fi.oph.kitu.html.formField
import fi.oph.kitu.html.formPost
import fi.oph.kitu.html.input
import fi.oph.kitu.html.testId
import fi.oph.kitu.i18n.LocalizedString
import fi.oph.kitu.i18n.UiText
import fi.oph.kitu.i18n.unaryPlus
import fi.oph.kitu.webmvc.Links
import kotlinx.html.ButtonType
import kotlinx.html.InputType
import kotlinx.html.a
import kotlinx.html.button
import kotlinx.html.footer
import kotlinx.html.h1
import kotlinx.html.h2
import kotlinx.html.label
import kotlinx.html.p
import kotlinx.html.small

object YkiKausiLomakePage {
    fun render(
        arvioija: YkiArvioijaEntity,
        form: KausiFormData,
        errors: FormErrors,
        action: String,
        otsikko: LocalizedString,
    ): String =
        Page.renderHtml(ohje = UiText.Ohje.Yki.kausiLomake) {
            h1 { +otsikko }
            p { +"${arvioija.etunimet} ${arvioija.sukunimi}" }

            formErrorSummary(errors)

            formPost(action, testId = "kausiLomake") {
                card {
                    cardContent {
                        h2 { +UiText.Yki.Arvioija.kausihistoria }

                        formField(
                            label = UiText.Yki.Arvioija.kaudenAlkupaiva,
                            name = "alkupaiva",
                            errors = errors,
                            testId = "alkupaiva",
                        ) { invalid ->
                            dateInput("alkupaiva", form.alkupaiva, "alkupaiva-input", invalid)
                        }

                        label {
                            +UiText.Yki.Arvioija.kaudenPaattymispaiva
                            input(
                                type = InputType.date,
                                name = "paattymispaivaEsikatselu",
                                value = form.laskettuPaattymispaiva()?.toString().orEmpty(),
                            ) {
                                testId("paattymispaiva")
                                disabled = true
                            }
                            small { +UiText.Yki.Arvioija.kaudenPaattymispaivaOhje }
                        }

                        tekstikentta(UiText.Yki.Arvioija.ashaNumero, "ashaNumero", form.ashaNumero, errors)
                    }
                }

                card {
                    cardContent {
                        h2 { +UiText.Yki.Arvioija.arviointioikeudet }
                        p { +UiText.Yki.Arvioija.arviointioikeudetOhje }
                        arviointioikeusMatriisi(form.arviointioikeus.orEmpty(), errors)
                    }

                    footer {
                        button(type = ButtonType.submit) {
                            testId("tallennaKausi")
                            +UiText.Yki.Arvioija.Kausi.tallenna
                        }
                        a(href = Links.Yki.arvioija(arvioija.id!!.toInt())) {
                            testId("peruutaKausi")
                            +UiText.Yki.Arvioija.Kausi.peruuta
                        }
                    }
                }
            }
        }
}
