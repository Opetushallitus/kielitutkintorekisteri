package fi.oph.kitu.yki.arvioijat.hakemus

import fi.oph.kitu.html.Page
import fi.oph.kitu.html.card
import fi.oph.kitu.html.testId
import fi.oph.kitu.i18n.UiText
import fi.oph.kitu.i18n.finnishDateTime
import fi.oph.kitu.i18n.unaryPlus
import kotlinx.html.h1
import kotlinx.html.h2
import kotlinx.html.p
import kotlinx.html.table
import kotlinx.html.tbody
import kotlinx.html.td
import kotlinx.html.th
import kotlinx.html.thead
import kotlinx.html.tr

object ArvioijahakemusPage {
    fun render(rivit: List<ArvioijahakemusEntity>): String =
        Page.renderHtml(
            wideContent = true,
            ohje = UiText.Ohje.Yki.arvioijahakemukset,
        ) {
            h1 { +UiText.Nav.yki }
            h2 { +UiText.Yki.Arvioijahakemus.otsikko }
            p { +UiText.Yki.Arvioijahakemus.kuvaus }

            if (rivit.isEmpty()) {
                p { +UiText.Yki.Arvioijahakemus.eiRiveja }
            } else {
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
                                    td { +rivi.hakemusOid }
                                    td { +rivi.henkiloOid.orEmpty() }
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
}
