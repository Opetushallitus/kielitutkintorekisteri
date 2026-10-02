package fi.oph.kitu.html

import fi.oph.kitu.html.table.RenderableDisplayTableEnum
import fi.oph.kitu.html.table.displayTable
import fi.oph.kitu.i18n.LocalizedString
import fi.oph.kitu.jdbc.SortDirection
import kotlinx.html.h1
import kotlinx.html.h2

inline fun <reified C, T> errorTablePage(
    title: String,
    subtitle: String,
    sortColumn: C,
    sortDirection: SortDirection,
    rows: List<T>,
    ohje: LocalizedString? = null,
): String where C : Enum<C>, C : RenderableDisplayTableEnum<T> =
    Page.renderHtml(
        wideContent = true,
        ohje = ohje,
    ) {
        h1 { +title }
        h2 { +subtitle }
        displayTable(
            rows = rows,
            columns = enumValues<C>().map { it.withValue(it.getValue) },
            sortedBy = sortColumn,
            sortDirection = sortDirection,
            rowClasses = "virheet",
            rowTestId = { "error-row" },
        )
    }
