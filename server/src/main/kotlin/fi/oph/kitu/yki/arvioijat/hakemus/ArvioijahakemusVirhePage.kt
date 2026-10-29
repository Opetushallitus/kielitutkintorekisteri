package fi.oph.kitu.yki.arvioijat.hakemus

import fi.oph.kitu.html.Page
import fi.oph.kitu.html.card
import fi.oph.kitu.html.copyable
import fi.oph.kitu.html.testId
import fi.oph.kitu.i18n.UiText
import fi.oph.kitu.i18n.finnishDateTime
import fi.oph.kitu.i18n.unaryPlus
import kotlinx.html.code
import kotlinx.html.h1
import kotlinx.html.h2
import kotlinx.html.li
import kotlinx.html.p
import kotlinx.html.table
import kotlinx.html.tbody
import kotlinx.html.td
import kotlinx.html.th
import kotlinx.html.thead
import kotlinx.html.tr
import kotlinx.html.ul

object ArvioijahakemusVirhePage {
    fun render(rivit: List<ArvioijahakemusVirheEntity>): String =
        Page.renderHtml(
            wideContent = true,
            ohje = UiText.Ohje.Yki.arvioijahakemusVirheet,
        ) {
            h1 { +UiText.Nav.yki }
            h2 { +UiText.Yki.Arvioijahakemus.Virheet.otsikko }
            p { +UiText.Yki.Arvioijahakemus.Virheet.kuvaus }

            if (rivit.isEmpty()) {
                p { +UiText.Yki.Arvioijahakemus.Virheet.eiRiveja }
            } else {
                card(overflowAuto = true, compact = true) {
                    table {
                        testId("arvioijahakemusten-virheet")
                        thead {
                            tr {
                                th { +UiText.Yki.Arvioijahakemus.hakemus }
                                th { +UiText.Yki.Arvioijahakemus.henkilo }
                                th { +UiText.Yki.Arvioijahakemus.Virheet.virheet }
                                th { +UiText.Yki.Arvioijahakemus.Virheet.ensimmainenYritys }
                                th { +UiText.Yki.Arvioijahakemus.Virheet.viimeisinYritys }
                                th { +UiText.Yki.Arvioijahakemus.Virheet.yrityksia }
                            }
                        }
                        tbody {
                            rivit.forEach { rivi ->
                                tr {
                                    testId(rivi.hakemusOid)
                                    td { copyable(rivi.hakemusOid) }
                                    td { rivi.henkiloOid?.let { copyable(it) } }
                                    td {
                                        ul {
                                            rivi.virheet.forEach { virhe ->
                                                li {
                                                    +virhe.tyyppi.nimi
                                                    virhe.kentta?.let {
                                                        +" "
                                                        code { +it }
                                                    }
                                                    virhe.arvo?.let {
                                                        +": "
                                                        code { +it.toString() }
                                                    }
                                                    virhe.viesti?.let { +": $it" }
                                                }
                                            }
                                        }
                                    }
                                    td { finnishDateTime(rivi.ensimmainenYritys.toInstant()) }
                                    td { finnishDateTime(rivi.viimeisinYritys.toInstant()) }
                                    td { +rivi.yrityksia.toString() }
                                }
                            }
                        }
                    }
                }
            }
        }
}
