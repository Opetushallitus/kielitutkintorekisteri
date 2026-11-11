package fi.oph.kitu.yki.arvioijat.hakemus

import fi.oph.kitu.html.Page
import fi.oph.kitu.html.ViewMessageData
import fi.oph.kitu.html.buttonLink
import fi.oph.kitu.html.card
import fi.oph.kitu.html.copyable
import fi.oph.kitu.html.errorsArticle
import fi.oph.kitu.html.formPost
import fi.oph.kitu.html.testId
import fi.oph.kitu.html.viewMessage
import fi.oph.kitu.i18n.UiText
import fi.oph.kitu.i18n.finnishDateTime
import fi.oph.kitu.i18n.unaryPlus
import fi.oph.kitu.webmvc.Links
import fi.oph.kitu.yki.arvioijat.TallennaArvioija
import kotlinx.html.ButtonType
import kotlinx.html.FlowContent
import kotlinx.html.a
import kotlinx.html.br
import kotlinx.html.button
import kotlinx.html.h1
import kotlinx.html.h2
import kotlinx.html.h3
import kotlinx.html.p
import kotlinx.html.table
import kotlinx.html.tbody
import kotlinx.html.td
import kotlinx.html.th
import kotlinx.html.thead
import kotlinx.html.tr

object ArvioijahakemusPage {
    fun render(
        tarkistettavat: List<TarkistettavaHakemus>,
        keskeneraiset: List<ArvioijahakemusEntity>,
        virheita: Long,
        muokkausKaytossa: Boolean,
        flash: ViewMessageData?,
    ): String =
        Page.renderHtml(
            wideContent = true,
            ohje = UiText.Ohje.Yki.arvioijahakemukset,
        ) {
            h1 { +UiText.Nav.yki }
            h2 { +UiText.Yki.Arvioijahakemus.otsikko }
            viewMessage(flash)
            p { +UiText.Yki.Arvioijahakemus.tarkistettavienKuvaus }
            errorsArticle(virheita, Links.Yki.arvioijahakemusVirheet())

            formPost(Links.Yki.haeArvioijahakemukset()) {
                button(type = ButtonType.submit, classes = "secondary") {
                    testId("haeArvioijahakemukset")
                    disabled = !muokkausKaytossa
                    +UiText.Yki.Arvioijahakemus.haeUudet
                }
            }

            if (tarkistettavat.isEmpty()) {
                p {
                    testId("eiTarkistettavia")
                    +UiText.Yki.Arvioijahakemus.eiTarkistettavia
                }
            } else {
                tarkistettavatTaulukko(tarkistettavat, muokkausKaytossa)
            }

            if (keskeneraiset.isNotEmpty()) {
                h3 { +UiText.Yki.Arvioijahakemus.keskeneraisetOtsikko }
                p { +UiText.Yki.Arvioijahakemus.keskeneraistenKuvaus }
                keskeneraisetTaulukko(keskeneraiset)
            }
        }

    private fun FlowContent.tarkistettavatTaulukko(
        rivit: List<TarkistettavaHakemus>,
        muokkausKaytossa: Boolean,
    ) {
        card(overflowAuto = true, compact = true) {
            table {
                testId("tarkistettavat-arvioijahakemukset")
                thead {
                    tr {
                        th { +UiText.Yki.Arvioijahakemus.nimi }
                        th { +UiText.Yki.Arvioijahakemus.henkilo }
                        th { +UiText.Yki.Arvioija.arviointioikeudet }
                        th { +UiText.Yki.Arvioijahakemus.rekisterissa }
                        th { +UiText.Yki.Arvioijahakemus.Maksu.otsikko }
                        th { +UiText.Yki.Arvioijahakemus.hakemus }
                        th { +UiText.Yki.Arvioijahakemus.haettu }
                        th {}
                    }
                }
                tbody {
                    rivit.forEach { rivi ->
                        tr {
                            testId(rivi.hakemusOid)
                            td { +"${rivi.sukunimi}, ${rivi.etunimet}" }
                            td { copyable(rivi.arvioijaOid) }
                            td { arviointioikeudet(rivi.hakemus.arviointioikeudet) }
                            td {
                                rivi.rekisterinArvioijaId?.let { id ->
                                    a(href = Links.Yki.arvioija(id)) { +UiText.Yki.Arvioijahakemus.onRekisterissa }
                                }
                            }
                            td { maksunTila(rivi.maksu) }
                            td { copyable(rivi.hakemusOid) }
                            td { finnishDateTime(rivi.ensimmainenHaku.toInstant()) }
                            td {
                                buttonLink(
                                    href = Links.Yki.tarkistaArvioijahakemus(rivi.hakemusOid),
                                    enabled = muokkausKaytossa,
                                    testId = "tarkista",
                                    disabledTooltip = UiText.Yki.Arvioija.kirjoitusEiKaytossa,
                                ) {
                                    +UiText.Yki.Arvioijahakemus.tarkista
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    private fun FlowContent.arviointioikeudet(oikeudet: List<TallennaArvioija.Arviointioikeus>) {
        oikeudet.forEachIndexed { indeksi, oikeus ->
            if (indeksi > 0) br
            +"${oikeus.kieli.nimi}: ${oikeus.tasot.sorted().joinToString { it.name }}"
        }
    }

    private fun FlowContent.keskeneraisetTaulukko(rivit: List<ArvioijahakemusEntity>) {
        card(overflowAuto = true, compact = true) {
            table {
                testId("arvioijahakemukset")
                thead {
                    tr {
                        th { +UiText.Yki.Arvioijahakemus.hakemus }
                        th { +UiText.Yki.Arvioijahakemus.henkilo }
                        th { +UiText.Yki.Arvioijahakemus.tila }
                        th { +UiText.Yki.Arvioijahakemus.syy }
                        th { +UiText.Yki.Arvioijahakemus.kasitelty }
                    }
                }
                tbody {
                    rivit.forEach { rivi ->
                        tr {
                            testId(rivi.hakemusOid)
                            td { copyable(rivi.hakemusOid) }
                            td { rivi.henkiloOid?.let { copyable(it) } }
                            td { +rivi.tila.nimi }
                            td { +rivi.syy.orEmpty() }
                            td { finnishDateTime(rivi.kasitelty.toInstant()) }
                        }
                    }
                }
            }
        }
    }
}
