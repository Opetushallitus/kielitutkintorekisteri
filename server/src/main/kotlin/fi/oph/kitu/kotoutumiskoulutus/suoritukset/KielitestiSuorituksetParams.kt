package fi.oph.kitu.kotoutumiskoulutus.suoritukset
import fi.oph.kitu.csv.buildCsvFilename
import fi.oph.kitu.csv.toCsvFilenameSegment
import fi.oph.kitu.html.table.PaginatedListViewParams
import fi.oph.kitu.html.table.valintaDescription
import fi.oph.kitu.i18n.UiText
import fi.oph.kitu.i18n.aikarajausDescription
import fi.oph.kitu.jdbc.PAGINATED_DEFAULT_PAGE_SIZE
import fi.oph.kitu.jdbc.SortDirection
import fi.oph.kitu.webmvc.toTrueOrNull
import fi.oph.kitu.webmvc.toUrlParam
import org.springframework.format.annotation.DateTimeFormat
import java.time.LocalDate

data class KielitestiSuorituksetParams(
    override val limit: Int = PAGINATED_DEFAULT_PAGE_SIZE,
    override val page: Int = 1,
    val sortColumn: KielitestiSuoritusColumn = KielitestiSuoritusColumn.Suoritusaika,
    val sortDirection: SortDirection = SortDirection.DESC,
    val search: String = "",
    @param:DateTimeFormat(iso = DateTimeFormat.ISO.DATE)
    var suoritusalku: LocalDate? = null,
    @param:DateTimeFormat(iso = DateTimeFormat.ISO.DATE)
    var suoritusloppu: LocalDate? = null,
    override val piilotaHenkilotiedot: Boolean = false,
    val testikieli: List<Testikieli> = emptyList(),
    val naytettavatSuoritukset: NaytettavatSuoritukset = NaytettavatSuoritukset.VALMIIT,
) : PaginatedListViewParams {
    fun toFilter(): KielitestiSuoritusFilter =
        KielitestiSuoritusFilter(
            search = search,
            suoritusalku = suoritusalku,
            suoritusloppu = suoritusloppu,
            testikieli = testikieli,
            naytettavatSuoritukset = naytettavatSuoritukset,
        )

    fun toOrder(): KielitestiSuoritusOrder =
        KielitestiSuoritusOrder(
            sortColumn = sortColumn,
            sortDirection = sortDirection,
            pageNumber = page - 1,
        )

    override fun toMap(): Map<String, String?> =
        mapOf(
            "search" to search,
            "page" to page.toString(),
            "sortColumn" to sortColumn.urlParam,
            "sortDirection" to sortDirection.name,
            "suoritusalku" to suoritusalku?.toString(),
            "suoritusloppu" to suoritusloppu?.toString(),
            "testikieli" to testikieli.toUrlParam(),
            "piilotaHenkilotiedot" to piilotaHenkilotiedot.toTrueOrNull(),
            "naytettavatSuoritukset" to naytettavatSuoritukset.name,
        )

    fun csvFileName() =
        buildCsvFilename(
            "koto",
            piilotaHenkilotiedot,
            testikieli.toCsvFilenameSegment(),
            suoritusalku?.toString(),
            suoritusloppu?.toString(),
        )

    fun filterDescriptions(): List<String> =
        listOfNotNull(
            aikarajausDescription(suoritusalku, suoritusloppu),
            valintaDescription(UiText.Koto.Sarake.testikieli, testikieli),
            if (piilotaHenkilotiedot) UiText.Filter.henkilotiedotPiilotettu.toString() else null,
            naytettavatSuoritukset
                .takeIf { it != NaytettavatSuoritukset.VALMIIT }
                ?.let { "${UiText.Filter.naytettavatSuoritukset}: ${it.nimi}" },
        )
}
