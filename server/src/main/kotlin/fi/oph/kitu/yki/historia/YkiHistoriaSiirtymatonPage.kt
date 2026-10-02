package fi.oph.kitu.yki.historia

import fi.oph.kitu.html.Page
import fi.oph.kitu.html.Pagination
import fi.oph.kitu.html.card
import fi.oph.kitu.html.hiddenValue
import fi.oph.kitu.html.hiddenValues
import fi.oph.kitu.html.input
import fi.oph.kitu.html.listViewHeader
import fi.oph.kitu.html.pagination
import fi.oph.kitu.html.table.ColumnTag
import fi.oph.kitu.html.table.DisplayTableColumn
import fi.oph.kitu.html.table.displayTable
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
import kotlinx.html.button
import kotlinx.html.fieldSet
import kotlinx.html.form
import kotlinx.html.h1
import kotlinx.html.h2
import kotlinx.html.p
import kotlinx.html.section

object YkiHistoriaSiirtymatonPage {
    fun render(
        rivit: List<YkiHistoriaSiirtymatonEntity>,
        params: YkiHistoriaSiirtymatonParams,
        pagination: Pagination,
    ): String =
        Page.renderHtml(
            wideContent = true,
            ohje = UiText.Ohje.Yki.historiaSiirtymattomat,
        ) {
            h1 { +UiText.Nav.yki }
            h2 { +UiText.Nav.historiaSiirtymattomat }
            p { +UiText.Yki.Historia.kuvaus }

            siirtymatonSearch(params)

            listViewHeader(
                countLabel = UiText.Yki.Historia.rivejaYhteensa,
                numberOfItems = pagination.numberOfItems,
                csvHref = Links.Yki.historiaSiirtymattomatCsv() + httpParams(params.toMap()),
                filterDescriptions = params.filterDescriptions(),
            ) { siirtymatonFilterButton(params) }

            if (rivit.isEmpty()) {
                p { +UiText.Yki.Historia.eiRiveja }
            } else {
                siirtymatonTable(rivit, params, pagination)
            }
        }
}

fun FlowContent.siirtymatonTable(
    rivit: List<YkiHistoriaSiirtymatonEntity>,
    params: YkiHistoriaSiirtymatonParams,
    pagination: Pagination,
) {
    card(overflowAuto = true, compact = true) {
        val columns =
            DisplayTableColumn.of<YkiHistoriaSiirtymatonColumn, YkiHistoriaSiirtymatonEntity>(
                setOf(ColumnTag.LIST_VIEW),
                params.excludeTags(),
            )

        displayTable(
            rivit,
            columns,
            sortedBy = params.sortColumn,
            sortDirection = params.sortDirection,
            testId = "siirtymattomat",
            rowTestId = { it.solkiId ?: it.id.toString() },
            urlParams = params.toMap(),
        )
    }

    pagination(pagination)
}

fun FlowContent.siirtymatonFilterButton(params: YkiHistoriaSiirtymatonParams) {
    tableFilterDialog("") {
        params.search.takeIf { it.isNotEmpty() }?.let { hiddenValue("search", it) }
        fieldSet {
            enumFilter(
                "syyluokka",
                UiText.Yki.Historia.syyluokka
                    .toString(),
                params.syyluokka,
            )
        }
        fieldSet {
            toggleFilter(
                "piilotaHenkilotiedot",
                UiText.Filter.piilotaHenkilotiedot.toString(),
                params.piilotaHenkilotiedot,
            )
        }
    }
}

/**
 * Haku kulkee GET-lomakkeena samoin kuin arvioijalistassa: suodattimet, jarjestys ja sivutus
 * ovat jo URL-parametreja, joten lajittelulinkin klikkaus ei saa pyyhkia hakusanaa.
 */
fun FlowContent.siirtymatonSearch(params: YkiHistoriaSiirtymatonParams) {
    section(classes = "grid center-vertically") {
        form(action = Links.Yki.historiaSiirtymattomat(), method = FormMethod.get) {
            hiddenValues(params.toMap() - "search" - "page")
            fieldSet {
                attributes["role"] = "search"
                input(
                    id = "search",
                    type = InputType.text,
                    name = "search",
                    value = params.search,
                    placeholder =
                        UiText.Yki.Historia.hakusana
                            .toString(),
                ) {
                    testId("siirtymatonSearch")
                    button(type = ButtonType.submit) { +UiText.Yki.suodata }
                }
            }
        }
    }
}
