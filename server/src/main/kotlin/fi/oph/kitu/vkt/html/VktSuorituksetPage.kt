package fi.oph.kitu.vkt.html
import fi.oph.kitu.html.Page
import fi.oph.kitu.html.Pagination
import fi.oph.kitu.html.ViewMessageData
import fi.oph.kitu.html.card
import fi.oph.kitu.html.hiddenValue
import fi.oph.kitu.html.listViewHeader
import fi.oph.kitu.html.pagination
import fi.oph.kitu.html.table.ColumnTag
import fi.oph.kitu.html.table.DisplayTableColumn
import fi.oph.kitu.html.table.dateFilter
import fi.oph.kitu.html.table.displayTable
import fi.oph.kitu.html.table.enumFilter
import fi.oph.kitu.html.table.httpParams
import fi.oph.kitu.html.table.tableFilterDialog
import fi.oph.kitu.html.table.toggleFilter
import fi.oph.kitu.html.table.trueFalseOrAllFilter
import fi.oph.kitu.html.testId
import fi.oph.kitu.html.viewMessage
import fi.oph.kitu.i18n.Translations
import fi.oph.kitu.i18n.UiText
import fi.oph.kitu.i18n.unaryPlus
import fi.oph.kitu.vkt.VktSuoritusColumn
import fi.oph.kitu.vkt.VktSuoritusFilter
import fi.oph.kitu.vkt.VktSuoritusFlat
import fi.oph.kitu.vkt.VktSuoritusOrder
import fi.oph.kitu.webmvc.Links
import kotlinx.html.FlowContent
import kotlinx.html.fieldSet
import kotlinx.html.h1
import kotlinx.html.h2

object VktSuorituksetPage {
    fun render(
        suoritukset: Iterable<VktSuoritusFlat>,
        filter: VktSuoritusFilter,
        order: VktSuoritusOrder,
        pagination: Pagination,
        translations: Translations,
        messages: List<ViewMessageData>,
    ): String =
        Page.renderHtml(
            wideContent = true,
            ohje = UiText.Ohje.Vkt.suoritukset,
        ) {
            h1 { +UiText.Nav.vkt }
            h2 { +UiText.Nav.kaikkiSuoritukset }
            messages.forEach { viewMessage(it) }
            vktSearch(filter)
            listViewHeader(
                countLabel = UiText.Vkt.yhteensa,
                numberOfItems = pagination.numberOfItems,
                csvHref = Links.Vkt.suorituksetCsv() + httpParams(filter.toMap()),
                filterDescriptions = filter.filterDescriptions(),
            ) { vktSuoritusFilterButton(filter) }
            vktKaikkiSuorituksetTable(suoritukset, filter, order, pagination, translations)
        }
}

fun FlowContent.vktKaikkiSuorituksetTable(
    suoritukset: Iterable<VktSuoritusFlat>,
    filter: VktSuoritusFilter,
    order: VktSuoritusOrder,
    pagination: Pagination,
    t: Translations,
) {
    card(overflowAuto = true, compact = true) {
        val columns =
            DisplayTableColumn.of<VktSuoritusColumn, VktSuoritusFlat>(
                setOf(ColumnTag.LIST_VIEW),
                filter.excludeTags(),
            )

        displayTable(
            suoritukset.toList(),
            columns,
            sortedBy = order.sortColumn,
            sortDirection = order.sortDirection,
            testId = "suoritukset",
            rowTestId = { "${it.suorittajanOid}-${it.tutkintokieli}" },
            urlParams = filter.toMap(),
        )
    }

    pagination(pagination)
}

fun FlowContent.vktSuoritusFilterButton(filter: VktSuoritusFilter) {
    tableFilterDialog("") {
        filter.search?.let { hiddenValue("search", filter.search) }
        fieldSet(classes = "grid") {
            dateFilter("alkupaiva", UiText.Vkt.alkaen.toString(), filter.alkupaiva)
            dateFilter("loppupaiva", UiText.Vkt.paattyen.toString(), filter.loppupaiva)
        }
        fieldSet {
            enumFilter(
                "tutkintokieli",
                UiText.Vkt.Sarake.tutkintokieli
                    .toString(),
                filter.tutkintokieli,
            )
        }
        fieldSet {
            enumFilter(
                "taitotaso",
                UiText.Vkt.Sarake.taitotaso
                    .toString(),
                filter.taitotaso,
            )
        }
        fieldSet {
            enumFilter(
                "arvioitu",
                UiText.Vkt.erinomaisenArvioinninTila.toString(),
                filter.arvioitu,
            )
        }
        fieldSet {
            trueFalseOrAllFilter(
                "merkittyPoistettavaksi",
                UiText.Vkt.poistettavaksiMerkitty.toString(),
                filter.merkittyPoistettavaksi,
                Triple(
                    UiText.Vkt.naytaKaikki.toString(),
                    UiText.Vkt.naytaVainPoistettavat.toString(),
                    UiText.Vkt.piilotaPoistettavat.toString(),
                ),
            )
        }
        fieldSet {
            toggleFilter(
                "piilotaHenkilotiedot",
                UiText.Filter.piilotaHenkilotiedot.toString(),
                filter.piilotaHenkilotiedot,
            )
        }
    }
}
