package fi.oph.kitu.yki.suoritukset
import fi.oph.kitu.html.Page
import fi.oph.kitu.html.Pagination
import fi.oph.kitu.html.ViewMessageData
import fi.oph.kitu.html.errorsArticle
import fi.oph.kitu.html.filterDescriptionList
import fi.oph.kitu.html.formPost
import fi.oph.kitu.html.input
import fi.oph.kitu.html.koskiErrorsArticle
import fi.oph.kitu.html.listViewActions
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
import fi.oph.kitu.html.viewMessage
import fi.oph.kitu.i18n.UiText
import fi.oph.kitu.i18n.unaryPlus
import fi.oph.kitu.webmvc.Links
import fi.oph.kitu.yki.Arviointitila
import fi.oph.kitu.yki.Tutkintokieli
import fi.oph.kitu.yki.Tutkintotaso
import fi.oph.kitu.yki.YkiSuorituksetParams
import kotlinx.html.ButtonType
import kotlinx.html.FlowContent
import kotlinx.html.InputType
import kotlinx.html.article
import kotlinx.html.button
import kotlinx.html.fieldSet
import kotlinx.html.footer
import kotlinx.html.h1
import kotlinx.html.h2
import kotlinx.html.header
import kotlinx.html.section
import kotlinx.html.table
import org.springframework.security.web.csrf.CsrfToken

object YkiSuorituksetPage {
    fun render(
        suoritukset: List<YkiSuoritusEntity>,
        totalSuoritukset: Long,
        filterParams: YkiSuorituksetParams,
        pagination: Pagination,
        errorsCount: Long,
        koskiErrorsCount: Long,
        csrfToken: CsrfToken?,
        warning: ViewMessageData? = null,
    ): String =
        Page.renderHtml(
            wideContent = true,
            ohje = UiText.Ohje.Yki.suoritukset,
        ) {
            val latestVersions = if (filterParams.versionHistory) suoritukset.latestVersions().map { it.id } else null

            h1 { +UiText.Nav.yki }
            h2 { +UiText.Nav.suoritukset }
            errorsArticle(errorsCount, Links.Yki.suorituksetVirheet())
            koskiErrorsArticle(koskiErrorsCount, Links.Yki.koskiVirheet())
            viewMessage(warning)

            section(classes = "grid center-vertically") {
                formPost(action = "", csrfToken = csrfToken) {
                    fieldSet {
                        attributes["role"] = "search"
                        input(
                            id = "search",
                            type = InputType.text,
                            name = "search",
                            value = filterParams.search,
                            placeholder = UiText.Yki.hakusana.toString(),
                        ) {
                            button(type = ButtonType.submit) {
                                +UiText.Yki.suodata
                            }
                        }
                    }
                }
            }

            article(classes = "overflow-auto") {
                header {
                    listViewActions(
                        countLabel = UiText.Yki.suorituksiaYhteensa,
                        numberOfItems = totalSuoritukset,
                        csvHref = Links.Yki.suorituksetCsv() + httpParams(filterParams.toMap()),
                    ) { ykiSuoritusFilterButton(filterParams) }
                    filterDescriptionList(filterParams.filterDescriptions())
                }

                table {
                    val columns =
                        DisplayTableColumn.of<YkiSuoritusColumn, YkiSuoritusEntity>(
                            setOf(ColumnTag.LIST_VIEW),
                            filterParams.excludeTags(),
                        )

                    displayTableHeader(
                        columns = columns,
                        sortedBy = filterParams.sortColumn,
                        sortDirection = filterParams.sortDirection,
                        urlParams = filterParams.toMap().plus("page" to pagination.currentPageNumber.toString()),
                        preserveSortDirection = false,
                        selectableRows = false,
                        tableId = "suoritukset-table",
                    )

                    displayTableBody(
                        rows = suoritukset,
                        columns = columns,
                        rowClasses = "suoritus",
                        isFaded = latestVersions?.let { { row -> !latestVersions.contains(row.id) } },
                    )
                }
            }

            footer {
                pagination(pagination)
            }
        }
}

fun FlowContent.ykiSuoritusFilterButton(params: YkiSuorituksetParams) {
    tableFilterDialog("suoritukset") {
        input(type = InputType.hidden, name = "recallSearch", value = "true")
        fieldSet(classes = "grid") {
            dateFilter("tutkintoalku", UiText.Yki.tutkintopaivaAlkaen.toString(), params.tutkintoalku)
            dateFilter("tutkintoloppu", UiText.Yki.tutkintopaivaPaattyen.toString(), params.tutkintoloppu)
        }
        fieldSet {
            enumFilter<Tutkintokieli>(
                "tutkintokieli",
                UiText.Yki.Sarake.tutkintokieli
                    .toString(),
                params.tutkintokieli,
            )
        }
        fieldSet {
            enumFilter<Tutkintotaso>(
                "tutkintotaso",
                UiText.Yki.Sarake.tutkintotaso
                    .toString(),
                params.tutkintotaso,
            )
        }
        fieldSet {
            enumFilter<Arviointitila>(
                "arviointitila",
                UiText.Yki.Sarake.arviointitila
                    .toString(),
                params.arviointitila,
            )
        }
        fieldSet {
            toggleFilter("versionHistory", UiText.Yki.naytaVersiohistoria.toString(), params.versionHistory)
        }
        fieldSet {
            toggleFilter(
                "piilotaHenkilotiedot",
                UiText.Filter.piilotaHenkilotiedot.toString(),
                params.piilotaHenkilotiedot,
            )
        }
        fieldSet {
            toggleFilter(
                "piilotaVanhentuneetTiedot",
                UiText.Yki.piilotaVanhentuneet.toString(),
                params.piilotaVanhentuneetTiedot,
            )
        }
    }
}
