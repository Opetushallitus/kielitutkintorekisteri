package fi.oph.kitu.yki
import fi.oph.kitu.html.table.ColumnTag
import fi.oph.kitu.i18n.UiText
import fi.oph.kitu.i18n.aikarajausDescription
import fi.oph.kitu.jdbc.SortDirection
import fi.oph.kitu.util.SearchTerms
import fi.oph.kitu.webmvc.buildCsvFilename
import fi.oph.kitu.yki.suoritukset.YkiSuoritusAikaryhmittely
import fi.oph.kitu.yki.suoritukset.YkiSuoritusColumn
import fi.oph.kitu.yki.suoritukset.YkiSuoritusFilter
import fi.oph.kitu.yki.suoritukset.YkiSuoritusOrder
import fi.oph.kitu.yki.suoritukset.YkiSuoritusTilastoColumn
import org.springframework.format.annotation.DateTimeFormat
import java.time.LocalDate

data class YkiSuorituksetParams(
    var versionHistory: Boolean = false,
    var limit: Int = 100,
    var page: Int = 1,
    var sortColumn: YkiSuoritusColumn = YkiSuoritusColumn.Tutkintopaiva,
    var sortDirection: SortDirection = SortDirection.DESC,
    var search: String = "",
    var recallSearch: Boolean = false,
    @param:DateTimeFormat(iso = DateTimeFormat.ISO.DATE)
    var tutkintoalku: LocalDate? = null,
    @param:DateTimeFormat(iso = DateTimeFormat.ISO.DATE)
    var tutkintoloppu: LocalDate? = null,
    var tutkintokieli: Tutkintokieli? = null,
    var tutkintotaso: Tutkintotaso? = null,
    var piilotaHenkilotiedot: Boolean = false,
    val piilotaVanhentuneetTiedot: Boolean = false,
    val arviointitila: Arviointitila? = null,
    @param:DateTimeFormat(iso = DateTimeFormat.ISO.DATE)
    var tuontialku: LocalDate? = null,
    @param:DateTimeFormat(iso = DateTimeFormat.ISO.DATE)
    var tuontiloppu: LocalDate? = null,
    var tilastoSortColumn: YkiSuoritusTilastoColumn = YkiSuoritusTilastoColumn.Tutkintopaiva,
    var tilastoSortDirection: SortDirection = SortDirection.DESC,
    var aikaryhmittely: YkiSuoritusAikaryhmittely? = null,
    var ryhmittely: List<YkiSuoritusTilastoColumn>? = null,
) {
    fun toMap(): Map<String, String?> =
        mapOf(
            "recallSearch" to search.isNotEmpty().toTrueOrNull(),
            "versionHistory" to versionHistory.toTrueOrNull(),
            "page" to page.toString(),
            "sortColumn" to sortColumn.urlParam,
            "sortDirection" to sortDirection.name,
            "tutkintoalku" to tutkintoalku?.toString(),
            "tutkintoloppu" to tutkintoloppu?.toString(),
            "tutkintokieli" to tutkintokieli?.toString(),
            "tutkintotaso" to tutkintotaso?.toString(),
            "piilotaHenkilotiedot" to piilotaHenkilotiedot.toTrueOrNull(),
            "piilotaVanhentuneetTiedot" to piilotaVanhentuneetTiedot.toTrueOrNull(),
            "arviointitila" to arviointitila?.toString(),
            "tuontialku" to tuontialku?.toString(),
            "tuontiloppu" to tuontiloppu?.toString(),
            "tilastoSortColumn" to
                tilastoSortColumn.takeIf { it != YkiSuoritusTilastoColumn.Tutkintopaiva }?.urlParam,
            "tilastoSortDirection" to tilastoSortDirection.takeIf { it != SortDirection.DESC }?.name,
            "aikaryhmittely" to valittuAikaryhmittely().name.takeUnless { onOletusryhmittely() },
            "ryhmittely" to
                valitutMuutRyhmittelyt()
                    .takeUnless { onOletusryhmittely() || it.isEmpty() }
                    ?.joinToString(",") { it.urlParam },
        )

    fun valittuAikaryhmittely(): YkiSuoritusAikaryhmittely = aikaryhmittely ?: YkiSuoritusAikaryhmittely.Tutkintopaiva

    fun valitutMuutRyhmittelyt(): List<YkiSuoritusTilastoColumn> =
        if (aikaryhmittely == null && ryhmittely == null) {
            YkiSuoritusTilastoColumn.muutRyhmittelyt
        } else {
            YkiSuoritusTilastoColumn.muutRyhmittelyt.filter { it in ryhmittely.orEmpty() }
        }

    fun valittuRyhmittely(): List<YkiSuoritusTilastoColumn> =
        listOfNotNull(valittuAikaryhmittely().sarake) + valitutMuutRyhmittelyt()

    private fun onOletusryhmittely() = valittuRyhmittely() == YkiSuoritusTilastoColumn.oletusryhmittely

    fun tilastoJarjestys(): Pair<YkiSuoritusTilastoColumn, SortDirection> {
        val ryhmat = valittuRyhmittely()
        val ensimmainen = ryhmat.firstOrNull()
        return when {
            tilastoSortColumn == YkiSuoritusTilastoColumn.Lukumaara || tilastoSortColumn in ryhmat -> {
                tilastoSortColumn to tilastoSortDirection
            }

            ensimmainen == null -> {
                YkiSuoritusTilastoColumn.Lukumaara to tilastoSortDirection
            }

            ensimmainen in YkiSuoritusTilastoColumn.aikasarakkeet -> {
                ensimmainen to SortDirection.DESC
            }

            else -> {
                ensimmainen to SortDirection.ASC
            }
        }
    }

    fun toFilter() =
        YkiSuoritusFilter(
            search = SearchTerms.from(search),
            alkupaiva = tutkintoalku,
            loppupaiva = tutkintoloppu,
            tutkintokieli = tutkintokieli,
            tutkintotaso = tutkintotaso,
            arviointitila = arviointitila,
            tuontialku = tuontialku,
            tuontiloppu = tuontiloppu,
        )

    fun toOrder() =
        YkiSuoritusOrder(
            sortColumn = sortColumn,
            sortDirection = sortDirection,
        )

    fun excludeTags(): Set<ColumnTag> =
        setOfNotNull(
            if (piilotaHenkilotiedot) ColumnTag.PERSONAL_DATA else null,
            if (piilotaVanhentuneetTiedot) ColumnTag.OBSOLETE else null,
            if (versionHistory) null else ColumnTag.VERSION_HISTORY_ONLY,
        )

    fun csvFileName() =
        buildCsvFilename(
            "yki_suoritukset",
            piilotaHenkilotiedot,
            tutkintokieli?.toString(),
            tutkintotaso?.toString(),
            arviointitila?.toString(),
            tutkintoalku?.toString(),
            tutkintoloppu?.toString(),
            tuontialku?.let { "tuonti_$it" },
            tuontiloppu?.let { "tuonti_$it" },
        )

    fun tilastoCsvFileName() =
        buildCsvFilename(
            "yki_suoritustilastot",
            true,
            valittuRyhmittely()
                .takeUnless { onOletusryhmittely() }
                ?.let { ryhmat -> "ryhmittely_" + ryhmat.joinToString("-") { it.urlParam }.ifEmpty { "ei" } },
            tutkintokieli?.toString(),
            tutkintotaso?.toString(),
            arviointitila?.toString(),
            tutkintoalku?.toString(),
            tutkintoloppu?.toString(),
            tuontialku?.let { "tuonti_$it" },
            tuontiloppu?.let { "tuonti_$it" },
        )

    fun filterDescriptions(): List<String> =
        listOfNotNull(
            aikarajausDescription(tutkintoalku, tutkintoloppu),
            aikarajausDescription(tuontialku, tuontiloppu, prefix = UiText.Filter.rekisteriintuontiaikaPrefix),
            tutkintokieli?.let { "${UiText.Yki.Sarake.tutkintokieli}: $it" },
            tutkintotaso?.let { "${UiText.Yki.Sarake.tutkintotaso}: $it" },
            arviointitila?.let { "${UiText.Yki.Sarake.arviointitila}: ${it.viewText}" },
            if (piilotaHenkilotiedot) UiText.Filter.henkilotiedotPiilotettu.toString() else null,
            if (piilotaVanhentuneetTiedot) UiText.Yki.vanhentuneetPiilotettu.toString() else null,
            if (versionHistory) UiText.Yki.naytaVersiohistoria.toString() else null,
        )
}
