package fi.oph.kitu.yki.suoritukset

import fi.oph.kitu.html.Page
import fi.oph.kitu.html.ViewMessageData
import fi.oph.kitu.html.filterDescriptionList
import fi.oph.kitu.html.listViewActions
import fi.oph.kitu.html.table.ColumnTag
import fi.oph.kitu.html.table.DisplayTableColumn
import fi.oph.kitu.html.table.displayTableBody
import fi.oph.kitu.html.table.displayTableHeader
import fi.oph.kitu.html.table.httpParams
import fi.oph.kitu.html.testId
import fi.oph.kitu.html.viewMessage
import fi.oph.kitu.i18n.UiText
import fi.oph.kitu.i18n.unaryPlus
import fi.oph.kitu.webmvc.Links
import fi.oph.kitu.yki.YkiSuorituksetParams
import kotlinx.html.a
import kotlinx.html.article
import kotlinx.html.h1
import kotlinx.html.h2
import kotlinx.html.header
import kotlinx.html.li
import kotlinx.html.p
import kotlinx.html.table
import org.springframework.security.web.csrf.CsrfToken

object YkiSuoritusTilastotPage {
    fun render(
        tilastot: List<YkiSuoritusTilastoRivi>,
        filterParams: YkiSuorituksetParams,
        csrfToken: CsrfToken?,
        warning: ViewMessageData? = null,
    ): String =
        Page.renderHtml(
            wideContent = true,
            ohje = UiText.Ohje.Yki.suoritukset,
        ) {
            h1 { +UiText.Nav.yki }
            h2 { +UiText.Yki.Tilastot.otsikko }
            viewMessage(warning)

            ykiSuoritusHakulomake(filterParams, csrfToken)

            article(classes = "overflow-auto") {
                header {
                    listViewActions(
                        countLabel = UiText.Yki.suorituksiaYhteensa,
                        numberOfItems = tilastot.sumOf { it.lukumaara },
                        csvHref = Links.Yki.suorituksetTilastotCsv() + httpParams(filterParams.toMap()),
                        extraActions = {
                            li {
                                a(href = Links.Yki.suoritukset() + httpParams(filterParams.toMap())) {
                                    testId("takaisin-suorituksiin")
                                    +UiText.Yki.Tilastot.takaisinSuorituksiin
                                }
                            }
                        },
                    ) {
                        ykiSuoritusFilterButton(
                            filterParams,
                            sailytettavatParametrit =
                                mapOf(
                                    "tilastoSortColumn" to filterParams.tilastoSortColumn.urlParam,
                                    "tilastoSortDirection" to filterParams.tilastoSortDirection.name,
                                ),
                        )
                    }
                    filterDescriptionList(filterParams.filterDescriptions())
                }

                if (tilastot.isEmpty()) {
                    p { +UiText.Yki.Tilastot.eiSuorituksia }
                } else {
                    table {
                        val columns =
                            DisplayTableColumn.of<YkiSuoritusTilastoColumn, YkiSuoritusTilastoRivi>(
                                setOf(ColumnTag.LIST_VIEW),
                            )

                        displayTableHeader(
                            columns = columns,
                            sortedBy = filterParams.tilastoSortColumn,
                            sortDirection = filterParams.tilastoSortDirection,
                            urlParams = filterParams.toMap(),
                            preserveSortDirection = false,
                            selectableRows = false,
                            tableId = "tilastot-table",
                            sortColumnParam = "tilastoSortColumn",
                            sortDirectionParam = "tilastoSortDirection",
                        )

                        displayTableBody(
                            rows = tilastot,
                            columns = columns,
                            rowClasses = "tilastorivi",
                        )
                    }
                }
            }
        }
}
