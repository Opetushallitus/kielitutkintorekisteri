package fi.oph.kitu.html.table

import fi.oph.kitu.html.Pagination
import fi.oph.kitu.i18n.LocalizedString

interface ListViewParams {
    val piilotaHenkilotiedot: Boolean

    fun excludeTags(): Set<ColumnTag> = setOfNotNull(ColumnTag.PERSONAL_DATA.takeIf { piilotaHenkilotiedot })
}

interface PaginatedListViewParams : ListViewParams {
    val page: Int
    val limit: Int

    fun toMap(): Map<String, String?>
}

fun PaginatedListViewParams.pagination(
    numberOfRows: Int,
    baseUrl: String = "",
): Pagination =
    Pagination.valueOf(
        currentPageNumber = page,
        numberOfRows = numberOfRows,
        pageSize = limit,
        url = { pageNumber -> baseUrl + httpParams(toMap() + ("page" to pageNumber.toString())) },
    )

fun <E : Enum<E>> valintaDescription(
    otsikko: LocalizedString,
    valitut: List<E>,
    nimi: (E) -> String = { it.toString() },
): String? =
    if (valitut.isEmpty()) null else valitut.sorted().joinToString(", ", prefix = "$otsikko: ", transform = nimi)
