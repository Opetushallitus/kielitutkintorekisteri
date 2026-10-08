package fi.oph.kitu.html.table

import fi.oph.kitu.html.debugTrace
import fi.oph.kitu.html.input
import fi.oph.kitu.html.javascript
import fi.oph.kitu.html.testId
import fi.oph.kitu.i18n.CurrentLanguage
import fi.oph.kitu.jdbc.SortDirection
import fi.oph.kitu.jdbc.reverse
import fi.oph.kitu.jdbc.toSymbol
import kotlinx.html.FlowContent
import kotlinx.html.InputType
import kotlinx.html.TABLE
import kotlinx.html.TBODY
import kotlinx.html.a
import kotlinx.html.style
import kotlinx.html.table
import kotlinx.html.tbody
import kotlinx.html.td
import kotlinx.html.th
import kotlinx.html.thead
import kotlinx.html.tr
import org.apereo.cas.client.util.CommonUtils.urlEncode
import java.util.UUID

data class DisplayTableColumn<T>(
    val label: String,
    val sortKey: String? = null,
    val width: String? = null,
    val testId: String? = null,
    val getValue: (T) -> String = { "Not implemented" },
    val renderHtml: (FlowContent.(T) -> Unit)?,
) {
    companion object {
        inline fun <reified C : Enum<C>, T> of(
            includeTags: Set<ColumnTag>,
            excludeTags: Set<ColumnTag> = emptySet(),
        ): List<DisplayTableColumn<T>> =
            RenderableDisplayTableEnum
                .getByTags<C, T>(includeTags, excludeTags)
                .map {
                    DisplayTableColumn(
                        label = it.uiHeaderValue.toString(),
                        sortKey = it.urlParam,
                        testId = it.name,
                        getValue = it.getValue,
                        renderHtml = it.renderHtml,
                    )
                }
    }
}

fun <T> TABLE.displayTableHeader(
    columns: List<DisplayTableColumn<T>>,
    sortedBy: DisplayTableEnum? = null,
    sortDirection: SortDirection? = null,
    urlParams: Map<String, String?> = emptyMap(),
    preserveSortDirection: Boolean,
    selectableRows: Boolean,
    tableId: String,
    sortColumnParam: String = "sortColumn",
    sortDirectionParam: String = "sortDirection",
) {
    val sortedByKey = sortedBy?.urlParam
    thead {
        tr {
            if (selectableRows) {
                td {
                    input(type = InputType.checkBox)
                }
            }
            columns.forEach {
                th {
                    attributes["id"] = it.sortKey ?: ""
                    testId(it.testId)
                    if (it.width != null) {
                        style = "width: ${it.width};"
                    }
                    if (it.sortKey != null && sortedBy != null && sortDirection != null) {
                        val isSortedColumn = it.sortKey == sortedByKey
                        a(
                            href =
                                httpParams(
                                    urlParams +
                                        mapOf(
                                            sortColumnParam to it.sortKey,
                                            sortDirectionParam to
                                                if (isSortedColumn) {
                                                    sortDirection.reverse().name
                                                } else if (preserveSortDirection) {
                                                    SortDirection.ASC.name
                                                } else {
                                                    sortDirection.name
                                                },
                                        ),
                                ),
                        ) {
                            +it.label
                            if (isSortedColumn) {
                                +" ${sortDirection.toSymbol()}"
                            }
                        }
                    } else {
                        +it.label
                    }
                }
            }
        }
    }
}

fun <T> TABLE.displayTableBody(
    rows: List<T>,
    columns: List<DisplayTableColumn<T>>,
    rowTestId: ((T) -> String)? = null,
    tbodyClasses: String? = null,
    rowClasses: String? = null,
    selectableRow: ((T) -> CheckboxKey?)? = null,
    isFaded: ((T) -> Boolean)? = null,
    afterRow: TBODY.(T) -> Unit = {},
) {
    tbody(tbodyClasses) {
        rows.forEach { row ->
            val classes =
                listOfNotNull(
                    rowClasses,
                    "faded".takeIf { isFaded?.invoke(row) == true },
                ).joinToString(" ").ifEmpty { null }
            tr(classes = classes) {
                testId(rowTestId?.let { it(row) })
                selectableRow?.let {
                    td {
                        selectableRow(row)?.let { key ->
                            input(type = InputType.checkBox, name = key.name, value = key.value)
                        }
                    }
                }
                columns.forEach { column ->
                    td {
                        testId(column.testId)
                        column.renderHtml?.invoke(this, row) ?: +column.getValue(row)
                    }
                }
            }
            afterRow(this, row)
        }
    }
}

fun <T> FlowContent.displayTable(
    rows: List<T>,
    columns: List<DisplayTableColumn<T>>,
    sortedBy: DisplayTableEnum? = null,
    sortDirection: SortDirection? = null,
    testId: String? = null,
    rowTestId: ((T) -> String)? = null,
    rowClasses: String? = null,
    urlParams: Map<String, String?> = emptyMap(),
    selectableRowName: ((T) -> CheckboxKey?)? = null,
    isFaded: ((T) -> Boolean)? = null,
) {
    val tableId = "table-${UUID.randomUUID()}"

    table(classes = "striped") {
        attributes["id"] = tableId
        testId(testId)
        debugTrace()
        displayTableHeader(
            columns = columns,
            sortedBy = sortedBy,
            sortDirection = sortDirection,
            urlParams = urlParams,
            preserveSortDirection = true,
            selectableRows = selectableRowName != null,
            tableId = tableId,
        )

        displayTableBody(
            rows = rows,
            columns = columns,
            rowTestId = rowTestId,
            rowClasses = rowClasses,
            selectableRow = selectableRowName,
            isFaded = isFaded,
        )
    }

    selectableRowName?.let {
        javascript(
            """
            const toggleAllCb = document
              .getElementById("$tableId")
              .querySelector('thead input[type="checkbox"]');
              
            const cbs = [
              ...document
                .getElementById("$tableId")
                .querySelectorAll('tbody input[type="checkbox"]'),
            ];
            
            toggleAllCb.addEventListener("change", () => {
              const toggleOn = cbs.some((cb) => !cb.checked);
              cbs.forEach((cb) => (cb.checked = toggleOn));
              toggleAllCb.checked = toggleOn;
            });
            
            cbs.forEach(cb => cb.addEventListener("change", () => {
              const toggleOn = cbs.some((cb) => !cb.checked);
              toggleAllCb.checked = !toggleOn;
            }));
            """.trimIndent(),
        )
    }
}

fun <K, V> httpParams(params: Map<K, V?>): String =
    "?${params
        .filter { (_, value) -> value != null }
        .map { (key, value) -> "$key=${urlEncode(value?.toString().orEmpty())}" }
        .joinToString("&")}"

data class CheckboxKey(
    val name: String,
    val value: String,
)
