package fi.oph.kitu.yki.arvioijat

import fi.oph.kitu.csv.buildCsvFilename
import fi.oph.kitu.csv.toCsvFilenameSegment
import fi.oph.kitu.html.table.PaginatedListViewParams
import fi.oph.kitu.html.table.valintaDescription
import fi.oph.kitu.i18n.UiText
import fi.oph.kitu.jdbc.PAGINATED_DEFAULT_PAGE_SIZE
import fi.oph.kitu.jdbc.PaginatedSortOrder
import fi.oph.kitu.jdbc.SortDirection
import fi.oph.kitu.jdbc.SqlFilter
import fi.oph.kitu.jdbc.SqlFilterBuilder
import fi.oph.kitu.jdbc.addAnyOf
import fi.oph.kitu.jdbc.addSearchTerms
import fi.oph.kitu.util.searchTerms
import fi.oph.kitu.webmvc.toUrlParam
import fi.oph.kitu.yki.Tutkintokieli
import fi.oph.kitu.yki.Tutkintotaso
import org.springframework.format.annotation.DateTimeFormat
import java.time.LocalDate

data class YkiArvioijaParams(
    var search: String = "",
    var tila: List<Rekisterointitila> = emptyList(),
    var kieli: List<Tutkintokieli> = emptyList(),
    var taso: List<Tutkintotaso> = emptyList(),
    @param:DateTimeFormat(iso = DateTimeFormat.ISO.DATE)
    var kausiPaattyyEnnen: LocalDate? = null,
    var vainSolkiVirheet: Boolean = false,
    override var piilotaHenkilotiedot: Boolean = false,
    var sortColumn: YkiArvioijaColumn = YkiArvioijaColumn.Sukunimi,
    var sortDirection: SortDirection = SortDirection.ASC,
    override var page: Int = 1,
    override var limit: Int = PAGINATED_DEFAULT_PAGE_SIZE,
) : SqlFilter(),
    PaginatedListViewParams {
    override fun toMap(): Map<String, String?> =
        mapOf(
            "search" to search.ifEmpty { null },
            "tila" to tila.toUrlParam(),
            "kieli" to kieli.toUrlParam(),
            "taso" to taso.toUrlParam(),
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

    fun csvFileName() =
        buildCsvFilename(
            "yki_arvioijat",
            piilotaHenkilotiedot,
            tila.toCsvFilenameSegment(),
            kieli.toCsvFilenameSegment(),
            taso.toCsvFilenameSegment(),
        )

    fun filterDescriptions(): List<String> =
        listOfNotNull(
            search.trim().takeIf { it.isNotEmpty() }?.let { "${UiText.Yki.hakusanaArvioija}: $it" },
            valintaDescription(UiText.Yki.Sarake.tila, tila),
            valintaDescription(UiText.Yki.Sarake.kieli, kieli) { it.solkiCode },
            valintaDescription(UiText.Yki.Sarake.taso, taso),
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
        addAnyOf("tila", "tila", tila)
        addAnyOf("kieli", "kieli", kieli)
        if (taso.isNotEmpty()) add("tasot && ARRAY[:taso]::text[]", "taso" to taso.map { it.name })
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
