package fi.oph.kitu.kotoutumiskoulutus.suoritukset
import fi.oph.kitu.html.Page
import fi.oph.kitu.html.Pagination
import fi.oph.kitu.html.csvDownloadButton
import fi.oph.kitu.html.errorsArticle
import fi.oph.kitu.html.filterDescriptionList
import fi.oph.kitu.html.hiddenValue
import fi.oph.kitu.html.hiddenValues
import fi.oph.kitu.html.input
import fi.oph.kitu.html.pagination
import fi.oph.kitu.html.table.ColumnTag
import fi.oph.kitu.html.table.DisplayTableColumn
import fi.oph.kitu.html.table.dateFilter
import fi.oph.kitu.html.table.displayTableBody
import fi.oph.kitu.html.table.displayTableHeader
import fi.oph.kitu.html.table.enumFilter
import fi.oph.kitu.html.table.httpParams
import fi.oph.kitu.html.table.tableFilterDialog
import fi.oph.kitu.html.table.toggleFilter
import fi.oph.kitu.html.testId
import fi.oph.kitu.i18n.UiText
import fi.oph.kitu.i18n.unaryPlus
import fi.oph.kitu.webmvc.Links
import kotlinx.html.ButtonType
import kotlinx.html.FlowContent
import kotlinx.html.FormMethod
import kotlinx.html.InputType
import kotlinx.html.article
import kotlinx.html.button
import kotlinx.html.fieldSet
import kotlinx.html.form
import kotlinx.html.h1
import kotlinx.html.h2
import kotlinx.html.header
import kotlinx.html.id
import kotlinx.html.li
import kotlinx.html.nav
import kotlinx.html.table
import kotlinx.html.ul

object KielitestiSuorituksetPage {
    fun render(
        suoritukset: List<KielitestiSuoritus>,
        errorsCount: Long,
        filterParams: KielitestiSuorituksetParams,
        numberOfSuoritukset: Int,
        pagination: Pagination,
    ): String =
        Page.renderHtml(
            wideContent = true,
        ) {
            h1 { +UiText.Nav.kotoutumiskoulutuksenPaattotesti }
            h2 { +UiText.Nav.suoritukset }
            errorsArticle(errorsCount, Links.Kielitesti.virheet())

            form(action = "", method = FormMethod.get, classes = "grid center-vertically") {
                hiddenValues(filterParams.toMap().filterKeys { it != "search" })
                fieldSet {
                    attributes["role"] = "search"
                    input {
                        testId("search")
                        id = "search"
                        type = InputType.search
                        name = "search"
                        value = filterParams.search
                        placeholder = UiText.Koto.hakusana.toString()
                    }
                    button {
                        testId("search-button")
                        type = ButtonType.submit
                        +UiText.Koto.suodata
                    }
                }
            }

            article(classes = "overflow-auto") {
                header {
                    nav {
                        ul {
                            li {
                                +UiText.Koto.suorituksiaYhteensa
                                +": $numberOfSuoritukset"
                            }
                            li {
                                csvDownloadButton(
                                    Links.Kielitesti.suorituksetCsv() + httpParams(filterParams.toMap()),
                                )
                            }
                            li { kielitestiSuoritusFilterButton(filterParams) }
                        }
                    }
                    filterDescriptionList(filterParams.filterDescriptions())
                }

                table {
                    val columns =
                        DisplayTableColumn.of<KielitestiSuoritusColumn, KielitestiSuoritus>(
                            setOf(ColumnTag.LIST_VIEW),
                            filterParams.excludeTags(),
                        )
                    displayTableHeader(
                        columns = columns,
                        sortedBy = filterParams.sortColumn,
                        sortDirection = filterParams.sortDirection,
                        preserveSortDirection = false,
                        selectableRows = false,
                        tableId = "kielitesti-suoritukset-table",
                        urlParams = filterParams.toMap(),
                    )
                    displayTableBody(
                        rows = suoritukset,
                        columns = columns,
                        rowClasses = "suoritus",
                        rowTestId = { "suoritus-summary-row" },
                        isFaded = { a -> !a.completed },
                    )
                }
            }
            pagination(pagination)
        }
}

fun FlowContent.kielitestiSuoritusFilterButton(params: KielitestiSuorituksetParams) {
    tableFilterDialog("suoritukset") {
        hiddenValue("search", params.search)
        fieldSet(classes = "grid") {
            dateFilter("suoritusalku", UiText.Koto.suoritusaikaAlkaen.toString(), params.suoritusalku)
            dateFilter("suoritusloppu", UiText.Koto.suoritusaikaPaattyen.toString(), params.suoritusloppu)
        }
        fieldSet {
            enumFilter<Testikieli>(
                "testikieli",
                UiText.Koto.Sarake.testikieli
                    .toString(),
                params.testikieli,
            )
        }
        fieldSet {
            toggleFilter(
                "piilotaHenkilotiedot",
                UiText.Filter.piilotaHenkilotiedot.toString(),
                params.piilotaHenkilotiedot,
            )
        }
        fieldSet {
            enumFilter<NaytettavatSuoritukset>(
                "naytettavatSuoritukset",
                UiText.Filter.naytettavatSuoritukset.toString(),
                params.naytettavatSuoritukset,
                kaikkiOption = false,
            )
        }
    }
}
