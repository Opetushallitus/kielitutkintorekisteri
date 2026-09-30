package fi.oph.kitu.yki.historia

import fi.oph.kitu.html.table.ColumnTag
import fi.oph.kitu.i18n.UiText
import fi.oph.kitu.jdbc.PAGINATED_DEFAULT_PAGE_SIZE
import fi.oph.kitu.jdbc.PaginatedSortOrder
import fi.oph.kitu.jdbc.SortDirection
import fi.oph.kitu.jdbc.SqlFilterBuilder
import fi.oph.kitu.webmvc.buildCsvFilename

data class YkiHistoriaSiirtymatonParams(
    var search: String = "",
    var syyluokka: YkiHistoriaSiirtymattomyydenSyy? = null,
    var piilotaHenkilotiedot: Boolean = false,
    var sortColumn: YkiHistoriaSiirtymatonColumn = YkiHistoriaSiirtymatonColumn.Tutkintopaiva,
    var sortDirection: SortDirection = SortDirection.DESC,
    var page: Int = 1,
    var limit: Int = PAGINATED_DEFAULT_PAGE_SIZE,
) {
    fun toMap(): Map<String, String?> =
        mapOf(
            "search" to search.ifEmpty { null },
            "syyluokka" to syyluokka?.name,
            "piilotaHenkilotiedot" to if (piilotaHenkilotiedot) "true" else null,
            "page" to page.toString(),
            "sortColumn" to sortColumn.urlParam,
            "sortDirection" to sortDirection.name,
        )

    fun toOrder() =
        YkiHistoriaSiirtymatonOrder(
            sortColumn = sortColumn,
            sortDirection = sortDirection,
            pageNumber = (page - 1).coerceAtLeast(0),
            pageSize = limit,
        )

    /** CSV-vienti ei sivuta: pageNumber null jattaa LIMITin kokonaan pois. */
    fun toCsvOrder() = toOrder().copy(pageNumber = null)

    fun excludeTags(): Set<ColumnTag> = setOfNotNull(if (piilotaHenkilotiedot) ColumnTag.PERSONAL_DATA else null)

    fun csvFileName() =
        buildCsvFilename(
            "yki_historia_siirtymattomat",
            piilotaHenkilotiedot,
            syyluokka?.name,
        )

    fun filterDescriptions(): List<String> =
        listOfNotNull(
            search.trim().takeIf { it.isNotEmpty() }?.let { "${UiText.Yki.Historia.hakusana}: $it" },
            syyluokka?.let { "${UiText.Yki.Historia.syyluokka}: ${it.nimi}" },
            if (piilotaHenkilotiedot) UiText.Filter.henkilotiedotPiilotettu.toString() else null,
        )

    fun whereSql(): String? = sqlSpec.whereClauseOrNull()

    fun sqlParams(): Map<String, Any?> = sqlSpec.params()

    private fun hakusanat(): List<String> = search.trim().split(Regex("\\s+")).filter { it.isNotEmpty() }

    /**
     * Rakennetaan kerran per instanssi: whereSql ja params tarvitsevat molemmat saman
     * suodattimen, ja erillisina funktiokutsuina SqlFilterBuilder rakennettiin joka
     * kyselylla kahdesti.
     */
    private val sqlSpec by lazy {
        SqlFilterBuilder().apply {
            hakusanat().forEachIndexed { i, term ->
                val param = "hakusana_$i"
                add(
                    """
                    solki_id ILIKE :$param
                    OR sukunimi ILIKE :$param
                    OR etunimet ILIKE :$param
                    OR hetu ILIKE :$param
                    OR jarjestajan_nimi ILIKE :$param
                    """.trimIndent(),
                    param to "%$term%",
                )
            }
            syyluokka?.let {
                add("syyluokka = :syyluokka::yki_historia_siirtymattomyyden_syy", "syyluokka" to it.name)
            }
        }
    }
}

data class YkiHistoriaSiirtymatonOrder(
    override val sortColumn: YkiHistoriaSiirtymatonColumn = YkiHistoriaSiirtymatonColumn.Tutkintopaiva,
    override val sortDirection: SortDirection = SortDirection.DESC,
    override val pageNumber: Int? = 0,
    override val pageSize: Int = PAGINATED_DEFAULT_PAGE_SIZE,
) : PaginatedSortOrder<YkiHistoriaSiirtymatonColumn>
