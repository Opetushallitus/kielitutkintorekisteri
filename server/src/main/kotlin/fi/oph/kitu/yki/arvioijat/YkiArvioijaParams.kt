package fi.oph.kitu.yki.arvioijat

import fi.oph.kitu.html.table.ColumnTag
import fi.oph.kitu.i18n.UiText
import fi.oph.kitu.jdbc.PAGINATED_DEFAULT_PAGE_SIZE
import fi.oph.kitu.jdbc.PaginatedSortOrder
import fi.oph.kitu.jdbc.SortDirection
import fi.oph.kitu.jdbc.SqlFilter
import fi.oph.kitu.jdbc.SqlFilterBuilder
import fi.oph.kitu.jdbc.addSearchTerms
import fi.oph.kitu.util.searchTerms
import fi.oph.kitu.webmvc.buildCsvFilename
import fi.oph.kitu.yki.Tutkintokieli
import fi.oph.kitu.yki.Tutkintotaso
import org.springframework.format.annotation.DateTimeFormat
import java.time.LocalDate

data class YkiArvioijaParams(
    var search: String = "",
    var tila: Rekisterointitila? = null,
    var kieli: Tutkintokieli? = null,
    var taso: Tutkintotaso? = null,
    @param:DateTimeFormat(iso = DateTimeFormat.ISO.DATE)
    var kausiPaattyyEnnen: LocalDate? = null,
    var vainSolkiVirheet: Boolean = false,
    var piilotaHenkilotiedot: Boolean = false,
    var sortColumn: YkiArvioijaColumn = YkiArvioijaColumn.Sukunimi,
    var sortDirection: SortDirection = SortDirection.ASC,
    var page: Int = 1,
    var limit: Int = PAGINATED_DEFAULT_PAGE_SIZE,
) : SqlFilter() {
    fun toMap(): Map<String, String?> =
        mapOf(
            "search" to search.ifEmpty { null },
            "tila" to tila?.name,
            "kieli" to kieli?.name,
            "taso" to taso?.name,
            "kausiPaattyyEnnen" to kausiPaattyyEnnen?.toString(),
            "vainSolkiVirheet" to if (vainSolkiVirheet) "true" else null,
            "piilotaHenkilotiedot" to if (piilotaHenkilotiedot) "true" else null,
            "page" to page.toString(),
            "sortColumn" to sortColumn.urlParam,
            "sortDirection" to sortDirection.name,
        )

    fun toOrder() =
        YkiArvioijaOrder(
            sortColumn = sortColumn,
            sortDirection = sortDirection,
            pageNumber = (page - 1).coerceAtLeast(0),
            pageSize = limit,
        )

    fun excludeTags(): Set<ColumnTag> = setOfNotNull(if (piilotaHenkilotiedot) ColumnTag.PERSONAL_DATA else null)

    fun csvFileName() =
        buildCsvFilename(
            "yki_arvioijat",
            piilotaHenkilotiedot,
            tila?.name,
            kieli?.name,
            taso?.name,
        )

    fun filterDescriptions(): List<String> =
        listOfNotNull(
            search.trim().takeIf { it.isNotEmpty() }?.let { "${UiText.Yki.hakusanaArvioija}: $it" },
            tila?.let { "${UiText.Yki.Sarake.tila}: $it" },
            kieli?.let { "${UiText.Yki.Sarake.kieli}: ${it.solkiCode}" },
            taso?.let { "${UiText.Yki.Sarake.taso}: $it" },
            kausiPaattyyEnnen?.let { "${UiText.Yki.Sarake.kaudenPaattymispaiva} < $it" },
            if (vainSolkiVirheet) UiText.Yki.solkiLahetystenVirheet.toString() else null,
            if (piilotaHenkilotiedot) UiText.Filter.henkilotiedotPiilotettu.toString() else null,
        )

    override fun SqlFilterBuilder.build() {
        addSearchTerms(
            searchTerms(search),
            listOf("sukunimi", "etunimet", "arvioija_oid", "sahkopostiosoite", "asha_numero"),
            "hakusana",
        )
        // Alikyselyn laskettu tekstisarake: ei enum-castia.
        tila?.let { add("tila = :tila", "tila" to it.name) }
        kieli?.let { add("kieli = :kieli::yki_tutkintokieli", "kieli" to it.name) }
        taso?.let { add("tasot @> ARRAY[:taso]::text[]", "taso" to it.name) }
        kausiPaattyyEnnen?.let {
            add("kauden_paattymispaiva IS NOT NULL AND kauden_paattymispaiva < :kausiLoppuu", "kausiLoppuu" to it)
        }
        if (vainSolkiVirheet) add("solki_lahetysvirhe IS NOT NULL")
    }
}

data class YkiArvioijaOrder(
    override val sortColumn: YkiArvioijaColumn = YkiArvioijaColumn.Sukunimi,
    override val sortDirection: SortDirection = SortDirection.ASC,
    override val pageNumber: Int? = 0,
    override val pageSize: Int = PAGINATED_DEFAULT_PAGE_SIZE,
) : PaginatedSortOrder<YkiArvioijaColumn>
