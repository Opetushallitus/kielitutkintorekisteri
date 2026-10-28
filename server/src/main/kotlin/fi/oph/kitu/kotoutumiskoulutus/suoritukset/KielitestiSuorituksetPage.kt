package fi.oph.kitu.kotoutumiskoulutus.suoritukset
import fi.oph.kitu.html.Page
import fi.oph.kitu.html.Pagination
import fi.oph.kitu.html.errorsArticle
import fi.oph.kitu.html.filterDescriptionList
import fi.oph.kitu.html.hiddenValue
import fi.oph.kitu.html.listSearchForm
import fi.oph.kitu.html.listViewActions
import fi.oph.kitu.html.pagination
import fi.oph.kitu.html.table.ColumnTag
import fi.oph.kitu.html.table.DisplayTableColumn
import fi.oph.kitu.html.table.dateFilter
import fi.oph.kitu.html.table.displayTableBody
import fi.oph.kitu.html.table.displayTableHeader
import fi.oph.kitu.html.table.enumFilter
import fi.oph.kitu.html.table.enumRadioFilter
import fi.oph.kitu.html.table.httpParams
import fi.oph.kitu.html.table.piilotaHenkilotiedotFilter
import fi.oph.kitu.html.table.tableFilterDialog
import fi.oph.kitu.html.varhaisinPaiva
import fi.oph.kitu.i18n.UiText
import fi.oph.kitu.i18n.unaryPlus
import fi.oph.kitu.webmvc.Links
import kotlinx.html.FlowContent
import kotlinx.html.article
import kotlinx.html.fieldSet
import kotlinx.html.h1
import kotlinx.html.h2
import kotlinx.html.header
import kotlinx.html.table
import java.time.LocalDate

object KielitestiSuorituksetPage {
    fun render(
        suoritukset: List<KielitestiSuoritus>,
        errorsCount: Long,
        filterParams: KielitestiSuorituksetParams,
        numberOfSuoritukset: Int,
        pagination: Pagination,
        varhaisinSuorituspaiva: LocalDate?,
    ): String =
        Page.renderHtml(
            wideContent = true,
            ohje = UiText.Ohje.Koto.suoritukset,
        ) {
            h1 { +UiText.Nav.kotoutumiskoulutuksenPaattotesti }
            h2 { +UiText.Nav.suoritukset }
            errorsArticle(errorsCount, Links.Kielitesti.virheet())
            varhaisinPaiva(
                UiText.Koto.varhaisinSuorituspaiva,
                listOfNotNull(varhaisinSuorituspaiva?.let { null to it }),
            )

            listSearchForm(
                value = filterParams.search,
                placeholder = UiText.Koto.hakusana,
                buttonText = UiText.Koto.suodata,
                params = filterParams.toMap(),
            )

            article(classes = "overflow-auto") {
                header {
                    listViewActions(
                        countLabel = UiText.Koto.suorituksiaYhteensa,
                        numberOfItems = numberOfSuoritukset,
                        csvHref = Links.Kielitesti.suorituksetCsv() + httpParams(filterParams.toMap()),
                    ) { kielitestiSuoritusFilterButton(filterParams) }
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
            piilotaHenkilotiedotFilter(params.piilotaHenkilotiedot)
        }
        fieldSet {
            enumRadioFilter(
                "naytettavatSuoritukset",
                UiText.Filter.naytettavatSuoritukset.toString(),
                params.naytettavatSuoritukset,
            )
        }
    }
}
