package fi.oph.kitu.yki.suoritukset

import fi.oph.kitu.html.Page
import fi.oph.kitu.html.ViewMessageData
import fi.oph.kitu.html.filterDescriptionList
import fi.oph.kitu.html.listSearchForm
import fi.oph.kitu.html.listViewActions
import fi.oph.kitu.html.table.ColumnTag
import fi.oph.kitu.html.table.DisplayTableColumn
import fi.oph.kitu.html.table.displayTableBody
import fi.oph.kitu.html.table.displayTableHeader
import fi.oph.kitu.html.table.enumFilter
import fi.oph.kitu.html.table.httpParams
import fi.oph.kitu.html.testId
import fi.oph.kitu.html.viewMessage
import fi.oph.kitu.i18n.UiText
import fi.oph.kitu.i18n.unaryPlus
import fi.oph.kitu.webmvc.Links
import fi.oph.kitu.webmvc.toUrlParam
import kotlinx.html.a
import kotlinx.html.article
import kotlinx.html.fieldSet
import kotlinx.html.h1
import kotlinx.html.h2
import kotlinx.html.header
import kotlinx.html.li
import kotlinx.html.p
import kotlinx.html.table

object YkiTarkistusarviointiTilastotPage {
    fun render(
        rivit: List<YkiTarkistusarviointiTilastoRivi>,
        params: YkiTarkistusarvioinnitParams,
        valinnat: YkiTarkistusarviointiTilastoValinnat,
        warning: ViewMessageData? = null,
    ): String =
        Page.renderHtml(wideContent = true, ohje = UiText.Ohje.Yki.tarkistusarvioinnit) {
            h1 { +UiText.Yki.tarkistusarvioinnit }
            h2 { +UiText.Yki.Tilastot.otsikko }
            viewMessage(warning)

            listSearchForm(
                value = params.search,
                placeholder = UiText.Yki.hakusanaTarkistusarviointi,
                buttonText = UiText.Yki.suodata,
                params = params.toCsvMap() + valinnat.toMap(),
            )

            article(classes = "overflow-auto") {
                header {
                    listViewActions(
                        countLabel = UiText.Yki.tarkistusarviointejaYhteensa,
                        numberOfItems =
                            rivit
                                .filter { it.kohde == YkiTarkistusarviointiTilastoKohde.Tarkistusarvioinnit }
                                .sumOf { it.yhteensa },
                        csvHref =
                            Links.Yki.tarkistusarviointiTilastotCsv() +
                                httpParams(params.toCsvMap() + valinnat.toMap()),
                        extraActions = {
                            li {
                                a(href = takaisinTarkistusarviointeihin(params, valinnat)) {
                                    testId("takaisin-tarkistusarviointeihin")
                                    +UiText.Yki.Tilastot.takaisinTarkistusarviointeihin
                                }
                            }
                        },
                    ) {
                        tarkistusarviointiFilterButton(
                            params = params,
                            naytaHyvaksymispaiva = true,
                            sailytettavatParametrit = valinnat.toMap() - "tila",
                        ) {
                            fieldSet {
                                enumFilter<Tarkistusarvioinnintila>(
                                    "tila",
                                    UiText.Yki.Tilastot.tila
                                        .toString(),
                                    valinnat.tila,
                                )
                            }
                        }
                    }
                    filterDescriptionList(valinnat.filterDescriptions() + params.filterDescriptions())
                }

                ryhmittelyLomake(
                    piilotetut = params.toCsvMap() + ("tila" to valinnat.tila.toUrlParam()),
                    aikaryhmittely = valinnat.valittuAikaryhmittely(),
                    valinnat =
                        YkiTarkistusarviointiTilastoColumn.muutRyhmittelyt.map {
                            RyhmittelyValinta(it.urlParam, it.uiHeaderValue, it in valinnat.valittuRyhmittely())
                        },
                )

                if (rivit.isEmpty()) {
                    p { +UiText.Yki.Tilastot.eiTarkistusarviointeja }
                } else {
                    table {
                        val naytettavat =
                            YkiTarkistusarviointiTilastoColumn
                                .sarakkeet(
                                    valinnat.valittuRyhmittely(),
                                ).map { it.urlParam }
                        val columns =
                            DisplayTableColumn
                                .of<YkiTarkistusarviointiTilastoColumn, YkiTarkistusarviointiTilastoRivi>(
                                    setOf(ColumnTag.LIST_VIEW),
                                ).filter { it.sortKey in naytettavat }

                        displayTableHeader(
                            columns = columns,
                            preserveSortDirection = false,
                            selectableRows = false,
                            tableId = "tilastot-table",
                        )

                        displayTableBody(
                            rows = rivit,
                            columns = columns,
                            rowClasses = "tilastorivi",
                        )
                    }
                    p {
                        testId("tilastot-huomautus")
                        +UiText.Yki.Tilastot.riviEivatSummaudu
                    }
                }
            }
        }

    private fun takaisinTarkistusarviointeihin(
        params: YkiTarkistusarvioinnitParams,
        valinnat: YkiTarkistusarviointiTilastoValinnat,
    ): String =
        (
            if (valinnat.tila == listOf(Tarkistusarvioinnintila.Hyvaksytty)) {
                Links.Yki.hyvaksytytTarkistusArvioinnit()
            } else {
                Links.Yki.tarkistusArvioinnit()
            }
        ) + httpParams(params.toCsvMap())
}
