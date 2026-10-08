package fi.oph.kitu.yki.suoritukset

import fi.oph.kitu.html.Page
import fi.oph.kitu.html.ViewMessageData
import fi.oph.kitu.html.filterDescriptionList
import fi.oph.kitu.html.input
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
import kotlinx.html.ButtonType
import kotlinx.html.FlowContent
import kotlinx.html.FormMethod
import kotlinx.html.InputType
import kotlinx.html.a
import kotlinx.html.article
import kotlinx.html.button
import kotlinx.html.fieldSet
import kotlinx.html.form
import kotlinx.html.h1
import kotlinx.html.h2
import kotlinx.html.header
import kotlinx.html.label
import kotlinx.html.legend
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
                                    "ryhmittely" to filterParams.toMap()["ryhmittely"],
                                ),
                        )
                    }
                    filterDescriptionList(filterParams.filterDescriptions())
                }

                ryhmittelyLomake(filterParams)

                if (tilastot.isEmpty()) {
                    p { +UiText.Yki.Tilastot.eiSuorituksia }
                } else {
                    table {
                        val naytettavat =
                            YkiSuoritusTilastoColumn.sarakkeet(filterParams.valittuRyhmittely()).map { it.urlParam }
                        val columns =
                            DisplayTableColumn
                                .of<YkiSuoritusTilastoColumn, YkiSuoritusTilastoRivi>(setOf(ColumnTag.LIST_VIEW))
                                .filter { it.sortKey in naytettavat }
                        val (sarake, suunta) = filterParams.tilastoJarjestys()

                        displayTableHeader(
                            columns = columns,
                            sortedBy = sarake,
                            sortDirection = suunta,
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

private fun FlowContent.ryhmittelyLomake(params: YkiSuorituksetParams) {
    val valitut = params.valittuRyhmittely()
    form(action = "", method = FormMethod.get) {
        testId("ryhmittely-lomake")
        input(type = InputType.hidden, name = "recallSearch", value = "true")
        params
            .toMap()
            .filterKeys { it != "ryhmittely" && it != "page" && it != "recallSearch" }
            .forEach { (name, value) -> value?.let { input(type = InputType.hidden, name = name, value = it) } }
        fieldSet(classes = "grid") {
            legend { +UiText.Yki.Tilastot.ryhmittely }
            YkiSuoritusTilastoColumn.ryhmittelyt.forEach { sarake ->
                label {
                    input(
                        type = InputType.checkBox,
                        name = "ryhmittely",
                        value = sarake.urlParam,
                        checked = sarake in valitut,
                    ) {
                        testId("ryhmittely-${sarake.urlParam}")
                    }
                    +sarake.uiHeaderValue
                }
            }
            button(type = ButtonType.submit) { +UiText.Yki.Tilastot.paivita }
        }
    }
}
