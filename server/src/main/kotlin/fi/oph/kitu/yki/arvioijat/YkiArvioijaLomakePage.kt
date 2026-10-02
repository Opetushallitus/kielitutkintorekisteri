package fi.oph.kitu.yki.arvioijat

import fi.oph.kitu.html.FormErrors
import fi.oph.kitu.html.Page
import fi.oph.kitu.html.ariaInvalid
import fi.oph.kitu.html.card
import fi.oph.kitu.html.cardContent
import fi.oph.kitu.html.dateInput
import fi.oph.kitu.html.formErrorSummary
import fi.oph.kitu.html.formField
import fi.oph.kitu.html.formPost
import fi.oph.kitu.html.hiddenValue
import fi.oph.kitu.html.input
import fi.oph.kitu.html.testId
import fi.oph.kitu.html.warningMessage
import fi.oph.kitu.i18n.LocalizedString
import fi.oph.kitu.i18n.UiText
import fi.oph.kitu.i18n.unaryPlus
import fi.oph.kitu.webmvc.Links
import fi.oph.kitu.yki.Tutkintokieli
import fi.oph.kitu.yki.Tutkintotaso
import kotlinx.html.ButtonType
import kotlinx.html.FlowContent
import kotlinx.html.InputType
import kotlinx.html.a
import kotlinx.html.button
import kotlinx.html.fieldSet
import kotlinx.html.footer
import kotlinx.html.h1
import kotlinx.html.h2
import kotlinx.html.label
import kotlinx.html.p
import kotlinx.html.small
import kotlinx.html.table
import kotlinx.html.tbody
import kotlinx.html.td
import kotlinx.html.th
import kotlinx.html.thead
import kotlinx.html.tr

object YkiArvioijaLomakePage {
    fun renderHaku(
        form: ArvioijaHakuFormData,
        errors: FormErrors,
    ): String =
        Page.renderHtml(ohje = UiText.Ohje.Yki.arvioijaLomake) {
            h1 { +UiText.Yki.Arvioija.uusiArvioija }

            formErrorSummary(errors)

            card {
                cardContent {
                    oppijanumeroHakuLomake(form, errors)
                }
            }

            p {
                a(href = Links.Yki.arvioijat()) { +UiText.Yki.Arvioija.takaisinListaan }
            }
        }

    fun renderLomake(
        form: ArvioijaFormData,
        errors: FormErrors,
        otsikko: LocalizedString = UiText.Yki.Arvioija.uusiArvioija,
        naytaKausi: Boolean = true,
        action: String = Links.Yki.uusiArvioija(),
        tallennaTeksti: LocalizedString = UiText.Yki.Arvioija.tallenna,
        peruutusLinkki: String? = null,
    ): String =
        Page.renderHtml(ohje = UiText.Ohje.Yki.arvioijaLomake) {
            h1 { +otsikko }

            if (form.onOlemassa) {
                warningMessage(UiText.Yki.Arvioija.jorekisterissa)
            }

            form.turvakielto.varoitus?.let { warningMessage(it) }

            formErrorSummary(errors, piilokentat = listOf("arvioijaOid"))

            formPost(action) {
                hiddenValue("arvioijaOid", form.arvioijaOid.orEmpty())
                hiddenValue("turvakielto", form.turvakielto.name)
                hiddenValue("onOlemassa", form.onOlemassa.toString())
                form.muokattu?.let { hiddenValue("muokattu", it.toString()) }

                card {
                    cardContent {
                        h2 { +UiText.Yki.Arvioija.yhteystiedot }
                        tekstikentta(UiText.Yki.Arvioija.sukunimi, "sukunimi", form.sukunimi, errors)
                        tekstikentta(UiText.Yki.Arvioija.etunimet, "etunimet", form.etunimet, errors)
                        tekstikentta(
                            UiText.Yki.Arvioija.sahkopostiosoite,
                            "sahkopostiosoite",
                            form.sahkopostiosoite,
                            errors,
                        )
                        tekstikentta(UiText.Yki.Arvioija.katuosoite, "katuosoite", form.katuosoite, errors)
                        tekstikentta(UiText.Yki.Arvioija.postinumero, "postinumero", form.postinumero, errors)
                        tekstikentta(
                            UiText.Yki.Arvioija.postitoimipaikka,
                            "postitoimipaikka",
                            form.postitoimipaikka,
                            errors,
                        )
                    }
                }

                // Muokkauslomakkeella kausi ja sen hallintopaatos hallitaan tietosivun
                // kausitaulukosta, jottei alkupaivan muutos taalla loisi uutta kautta vanhan rinnalle.
                if (naytaKausi) {
                    card {
                        cardContent {
                            h2 { +UiText.Yki.Arvioija.rekisterimerkinta }

                            formField(
                                label = UiText.Yki.Arvioija.kaudenAlkupaiva,
                                name = "kaudenAlkupaiva",
                                errors = errors,
                                testId = "kaudenAlkupaiva",
                            ) { invalid ->
                                dateInput("kaudenAlkupaiva", form.kaudenAlkupaiva, "kaudenAlkupaiva-input", invalid)
                            }

                            label {
                                +UiText.Yki.Arvioija.kaudenPaattymispaiva
                                input(
                                    type = InputType.date,
                                    name = "kaudenPaattymispaivaEsikatselu",
                                    value = form.laskettuPaattymispaiva()?.toString().orEmpty(),
                                ) {
                                    testId("kaudenPaattymispaiva")
                                    disabled = true
                                }
                                small { +UiText.Yki.Arvioija.kaudenPaattymispaivaOhje }
                            }

                            tekstikentta(UiText.Yki.Arvioija.ashaNumero, "ashaNumero", form.ashaNumero, errors)
                        }
                    }
                }

                card {
                    if (naytaKausi) {
                        cardContent {
                            h2 { +UiText.Yki.Arvioija.arviointioikeudet }
                            p { +UiText.Yki.Arvioija.arviointioikeudetOhje }
                            arviointioikeusMatriisi(form.arviointioikeus.orEmpty(), errors)
                        }
                    }

                    footer {
                        button(type = ButtonType.submit) {
                            testId("tallennaArvioija")
                            +tallennaTeksti
                        }
                        peruutusLinkki?.let { peruuta ->
                            a(href = peruuta) {
                                testId("peruutaMuokkaus")
                                +UiText.Yki.Arvioija.peruuta
                            }
                        }
                    }
                }
            }
        }
}

private fun FlowContent.oppijanumeroHakuLomake(
    form: ArvioijaHakuFormData,
    errors: FormErrors,
) {
    p { +UiText.Yki.Arvioija.hakuOhjeOppijanumero }

    formPost(Links.Yki.arvioijaHaku(), testId = "oppijanumeroHakuLomake") {
        tekstikentta(UiText.Yki.Arvioija.oppijanumero, "oppijanumero", form.oppijanumero, errors)

        footer {
            button(type = ButtonType.submit) {
                testId("haeHenkilonTiedot")
                +UiText.Yki.Arvioija.haeHenkilonTiedot
            }
        }
    }
}

fun FlowContent.tekstikentta(
    label: LocalizedString,
    name: String,
    arvo: String?,
    errors: FormErrors,
) {
    formField(label = label, name = name, errors = errors, testId = name) { invalid ->
        input(type = InputType.text, name = name, value = arvo.orEmpty()) {
            testId("$name-input")
            ariaInvalid(invalid)
        }
    }
}

fun FlowContent.arviointioikeusMatriisi(
    valitut: List<String>,
    errors: FormErrors,
) {
    val virheet = errors["arviointioikeus"]

    fieldSet {
        if (virheet.isNotEmpty()) ariaInvalid(true)

        table(classes = "compact striped") {
            testId("arviointioikeusMatriisi")
            thead {
                tr {
                    th { +UiText.Yki.Arvioija.tutkintokieli }
                    Tutkintotaso.entries.forEach { taso -> th { +taso.nimi } }
                }
            }
            tbody {
                val legacyOikeudet =
                    Tutkintokieli.entries.filter { kieli ->
                        kieli.isLegacy() && Tutkintotaso.entries.any { ArvioijaFormData.valinta(kieli, it) in valitut }
                    }

                (Tutkintokieli.entries.filterNot { it.isLegacy() } + legacyOikeudet)
                    .forEach { kieli ->
                        tr {
                            th { +kieli.nimi }
                            Tutkintotaso.entries.forEach { taso ->
                                td {
                                    val arvo = ArvioijaFormData.valinta(kieli, taso)
                                    input(type = InputType.checkBox, name = "arviointioikeus", value = arvo) {
                                        testId("arviointioikeus-$arvo")
                                        checked = arvo in valitut
                                        // Vanhentunutta tutkintokielta ei voi enaa myontaa eika perua.
                                        disabled = kieli.isLegacy()
                                    }
                                }
                            }
                        }
                    }
            }
        }

        if (virheet.isNotEmpty()) {
            small {
                testId("arviointioikeus-error")
                +virheet.joinToString(" ")
            }
        }
    }
}
