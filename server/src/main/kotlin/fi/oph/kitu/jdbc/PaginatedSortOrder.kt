package fi.oph.kitu.jdbc

import fi.oph.kitu.html.table.DisplayTableEnum

const val PAGINATED_DEFAULT_PAGE_SIZE = 50

interface PaginatedSortOrder<C : DisplayTableEnum> {
    val sortColumn: C
    val sortDirection: SortDirection
    val pageNumber: Int?
    val pageSize: Int
}

fun PaginatedSortOrder<*>.orderSql(): String = "${sortColumn.entityName} $sortDirection"

fun PaginatedSortOrder<*>.pageSql(): String? =
    pageNumber?.let {
        val koko = pageSize.coerceAtLeast(1)
        "LIMIT $koko OFFSET ${koko * it.coerceAtLeast(0)}"
    }

fun PaginatedSortOrder<*>.toMap(): Map<String, String> =
    mapOf(
        "sortColumn" to sortColumn.name,
        "sortDirection" to sortDirection.name,
        "pageNumber" to pageNumber.toString(),
    )
