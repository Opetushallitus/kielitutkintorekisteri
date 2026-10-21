package fi.oph.kitu.vkt.uusintaoikeus

import fi.oph.kitu.html.Page
import fi.oph.kitu.html.csvDownloadButton
import fi.oph.kitu.html.table.ColumnTag
import fi.oph.kitu.html.table.DisplayTableColumn
import fi.oph.kitu.html.table.displayTable
import fi.oph.kitu.html.table.httpParams
import fi.oph.kitu.html.testId
import fi.oph.kitu.i18n.LocalizedString
import fi.oph.kitu.i18n.UiText
import fi.oph.kitu.i18n.finnishDate
import fi.oph.kitu.i18n.unaryPlus
import fi.oph.kitu.webmvc.Links
import kotlinx.html.FlowContent
import kotlinx.html.TBODY
import kotlinx.html.article
import kotlinx.html.h1
import kotlinx.html.h2
import kotlinx.html.header
import kotlinx.html.li
import kotlinx.html.nav
import kotlinx.html.p
import kotlinx.html.span
import kotlinx.html.table
import kotlinx.html.tbody
import kotlinx.html.td
import kotlinx.html.th
import kotlinx.html.thead
import kotlinx.html.tr
import kotlinx.html.ul

object VktUusintaoikeudetPage {
    fun render(
        raportti: VktUusintaoikeusraportti,
        params: VktUusintaoikeusParams,
    ): String =
        Page.renderHtml(
            wideContent = true,
            ohje = UiText.Ohje.Vkt.uusintaoikeudet,
        ) {
            h1 { +UiText.Nav.vkt }
            h2 { +UiText.Nav.vktUusintaoikeudet }
            p { +UiText.Vkt.Uusintaoikeus.kuvaus }
            p {
                +UiText.Vkt.Uusintaoikeus.tarkastelupaiva
                +": "
                span {
                    testId("tarkastelupaiva")
                    +raportti.tarkastelupaiva.finnishDate()
                }
            }

            yhteenveto(raportti.yhteenveto)

            article(classes = "overflow-auto") {
                header {
                    nav {
                        ul {
                            li {
                                +UiText.Vkt.Uusintaoikeus.uusittaviaOsakokeita
                                +": "
                                span {
                                    testId("numberOfRows")
                                    +raportti.rivit.size.toString()
                                }
                            }
                            li { csvDownloadButton(Links.Vkt.uusintaoikeudetCsv() + httpParams(params.toMap())) }
                        }
                    }
                }

                if (raportti.rivit.isEmpty()) {
                    p { +UiText.Vkt.Uusintaoikeus.eiOikeutettuja }
                } else {
                    displayTable(
                        rows = raportti.rivit,
                        columns =
                            DisplayTableColumn.of<VktUusintaoikeusColumn, VktUusintaoikeus>(setOf(ColumnTag.LIST_VIEW)),
                        sortedBy = params.sortColumn,
                        sortDirection = params.sortDirection,
                        testId = "uusintaoikeudet",
                        rowTestId = {
                            "${it.suorittajanOid}-${it.tutkintokieli}-${it.taitotaso.koodiarvo}-${it.osakoe.koodiarvo}"
                        },
                        urlParams = params.toMap(),
                    )
                }
            }
        }

    private fun FlowContent.yhteenveto(yhteenveto: VktUusintaoikeusyhteenveto) {
        table {
            testId("yhteenveto")
            thead {
                tr {
                    th { +UiText.Vkt.Sarake.taitotaso }
                    th { +UiText.Vkt.Uusintaoikeus.henkiloita }
                    th { +UiText.Vkt.Uusintaoikeus.uusittaviaOsakokeita }
                }
            }
            tbody {
                yhteenveto.taitotasoittain.forEach { (taso, maarat) ->
                    yhteenvetorivi(taso.koodiarvo, taso.nimi, maarat)
                }
                yhteenvetorivi("yhteensa", UiText.Vkt.yhteensa, yhteenveto.yhteensa)
            }
        }
    }

    private fun TBODY.yhteenvetorivi(
        rivinTestId: String,
        otsikko: LocalizedString,
        maarat: VktUusintaoikeusyhteenveto.Maarat,
    ) {
        tr {
            testId("yhteenveto-$rivinTestId")
            th { +otsikko }
            td {
                testId("henkiloita")
                +maarat.henkiloita.toString()
            }
            td {
                testId("osakokeita")
                +maarat.osakokeita.toString()
            }
        }
    }
}
