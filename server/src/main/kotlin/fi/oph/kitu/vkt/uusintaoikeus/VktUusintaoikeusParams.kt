package fi.oph.kitu.vkt.uusintaoikeus

import fi.oph.kitu.jdbc.SortDirection

data class VktUusintaoikeusParams(
    val sortColumn: VktUusintaoikeusColumn = VktUusintaoikeusColumn.Sukunimi,
    val sortDirection: SortDirection = SortDirection.ASC,
) {
    fun toMap(): Map<String, String?> =
        mapOf(
            "sortColumn" to sortColumn.urlParam,
            "sortDirection" to sortDirection.name,
        )
}
