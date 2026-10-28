package fi.oph.kitu.vkt

import fi.oph.kitu.csv.buildCsvFilename
import fi.oph.kitu.csv.toCsvFilenameSegment
import fi.oph.kitu.html.table.ListViewParams
import fi.oph.kitu.html.table.Nimetty
import fi.oph.kitu.html.table.valintaDescription
import fi.oph.kitu.i18n.LocalizedString
import fi.oph.kitu.i18n.UiText
import fi.oph.kitu.i18n.aikarajausDescription
import fi.oph.kitu.jdbc.PAGINATED_DEFAULT_PAGE_SIZE
import fi.oph.kitu.jdbc.PaginatedSortOrder
import fi.oph.kitu.jdbc.SortDirection
import fi.oph.kitu.jdbc.SqlFilter
import fi.oph.kitu.jdbc.SqlFilterBuilder
import fi.oph.kitu.jdbc.addAnyOf
import fi.oph.kitu.jdbc.addSearchTerms
import fi.oph.kitu.jdbc.orderSql
import fi.oph.kitu.koodisto.Koodisto
import fi.oph.kitu.util.searchTerms
import fi.oph.kitu.webmvc.toTrueOrNull
import fi.oph.kitu.webmvc.toUrlParam
import java.time.LocalDate

data class VktSuoritusFilter(
    val search: String? = null,
    val alkupaiva: LocalDate? = null,
    val loppupaiva: LocalDate? = null,
    val tutkintokieli: List<Koodisto.Tutkintokieli> = emptyList(),
    val taitotaso: List<Koodisto.VktTaitotaso> = emptyList(),
    val arvioitu: List<VktArvioinninTila> = emptyList(),
    val merkittyPoistettavaksi: Boolean? = null,
    override val piilotaHenkilotiedot: Boolean = false,
) : SqlFilter(),
    ListViewParams {
    fun toMap(): Map<String, String?> =
        mapOf(
            "search" to search,
            "alkupaiva" to alkupaiva?.toString(),
            "loppupaiva" to loppupaiva?.toString(),
            "tutkintokieli" to tutkintokieli.toUrlParam(),
            "taitotaso" to taitotaso.toUrlParam(),
            "arvioitu" to arvioitu.toUrlParam(),
            "merkittyPoistettavaksi" to merkittyPoistettavaksi?.toString(),
            "piilotaHenkilotiedot" to piilotaHenkilotiedot.toTrueOrNull(),
        ).filterValues { it != null }

    fun filterDescriptions(): List<String> =
        listOfNotNull(
            aikarajausDescription(alkupaiva, loppupaiva),
            valintaDescription(UiText.Vkt.Sarake.tutkintokieli, tutkintokieli),
            valintaDescription(UiText.Vkt.Sarake.taitotaso, taitotaso),
            valintaDescription(UiText.Vkt.arvioinninTila, arvioitu),
            merkittyPoistettavaksi?.let {
                if (it) UiText.Vkt.vainPoistettavat.toString() else UiText.Vkt.vainEiPoistettavat.toString()
            },
            if (piilotaHenkilotiedot) UiText.Filter.henkilotiedotPiilotettu.toString() else null,
        )

    fun csvFileName() =
        buildCsvFilename(
            "vkt_suoritukset",
            piilotaHenkilotiedot,
            tutkintokieli.toCsvFilenameSegment(),
            taitotaso.toCsvFilenameSegment { it.koodiarvo },
            alkupaiva?.toString(),
            loppupaiva?.toString(),
            arvioitu.toCsvFilenameSegment { it.name.lowercase() },
            merkittyPoistettavaksi?.let { "merkitty_poistettavaksi" },
        )

    override fun SqlFilterBuilder.build() {
        addSearchTerms(
            searchTerms(search),
            listOf("vkt_suoritus.etunimet", "vkt_suoritus.sukunimi", "vkt_suoritus.suorittajan_oid"),
            "filter_search",
        )
        add(alkupaiva?.let { "osakokeet.tutkintopaiva >= :filter_alkupaiva" }, "filter_alkupaiva" to alkupaiva)
        add(loppupaiva?.let { "osakokeet.tutkintopaiva <= :filter_loppupaiva" }, "filter_loppupaiva" to loppupaiva)
        addAnyOf("tutkintokieli", "filter_kieli", tutkintokieli)
        addAnyOf("taitotaso", "filter_taso", taitotaso)
        add(arvioituQuery())
        add(merkittyPoistettavaksiQuery())
    }

    private fun arvioituQuery(): String? =
        arvioitu
            .sorted()
            .map { arvioituEhto(it) }
            .takeIf { it.isNotEmpty() }
            ?.joinToString(" OR ") { "($it)" }

    private fun arvioituEhto(tila: VktArvioinninTila): String =
        when (tila) {
            VktArvioinninTila.ArvioituOsittainTaiKokonaan -> {
                "osakokeet.arviointeja > 0"
            }

            // Rajataan vain ilmoittautumisen uusimpaan versioon (latest-id alikysely): muuten
            // vanha versio, jolta arvosanat puuttuivat, näkyisi vaikka päälle olisi tallennettu
            // uusi täydellinen versio. Lisäksi edellytetään, että tällä joinilla osallistuva
            // osakoerivi on null-arvosanainen — näin näytettävä tutkintopäivä on arvioimaton
            // päivä, ei toinen jo arvioitu päivä samasta suorituksesta.
            VktArvioinninTila.ArviointejaPuuttuu -> {
                """
                vkt_osakoe.arvosana IS NULL
                AND vkt_suoritus.id = (
                    SELECT id FROM vkt_suoritus latest
                    WHERE latest.ilmoittautumisen_id = vkt_suoritus.ilmoittautumisen_id
                    ORDER BY latest.created_at DESC
                    LIMIT 1
                )
                """.trimIndent()
            }
        }

    private fun merkittyPoistettavaksiQuery(): String? =
        merkittyPoistettavaksi?.let {
            if (merkittyPoistettavaksi) {
                "vkt_osakoe.merkitty_poistettavaksi is not null"
            } else {
                "vkt_osakoe.merkitty_poistettavaksi is null"
            }
        }

    companion object {
        val ERINOMAISEN_TASON_ILMOITTAUTUNEET =
            VktSuoritusFilter(
                taitotaso = listOf(Koodisto.VktTaitotaso.Erinomainen),
                arvioitu = listOf(VktArvioinninTila.ArviointejaPuuttuu),
            )

        val ERINOMAISEN_TASON_SUORITUKSET =
            VktSuoritusFilter(
                taitotaso = listOf(Koodisto.VktTaitotaso.Erinomainen),
                arvioitu = listOf(VktArvioinninTila.ArvioituOsittainTaiKokonaan),
            )

        val HYVAN_JA_TYYDYTTAVAN_TASON_SUORITUKSET =
            VktSuoritusFilter(
                taitotaso = listOf(Koodisto.VktTaitotaso.HyväJaTyydyttävä),
            )
    }
}

data class VktSuoritusOrder(
    override val sortColumn: VktSuoritusColumn = VktSuoritusColumn.Sukunimi,
    override val sortDirection: SortDirection = SortDirection.ASC,
    override val pageNumber: Int? = 0,
    override val pageSize: Int = PAGINATED_DEFAULT_PAGE_SIZE,
) : PaginatedSortOrder<VktSuoritusColumn> {
    override fun toString(): String = orderSql()
}

enum class VktArvioinninTila : Nimetty {
    ArvioituOsittainTaiKokonaan,
    ArviointejaPuuttuu,
    ;

    override val nimi: LocalizedString
        get() =
            when (this) {
                ArvioituOsittainTaiKokonaan -> UiText.Vkt.arvioituOsittain
                ArviointejaPuuttuu -> UiText.Vkt.arviointejaPuuttuu
            }
}
