package fi.oph.kitu.vkt

import fi.oph.kitu.html.table.ColumnTag
import fi.oph.kitu.html.table.Nimetty
import fi.oph.kitu.i18n.CurrentLanguage
import fi.oph.kitu.i18n.LocalizedString
import fi.oph.kitu.i18n.UiText
import fi.oph.kitu.i18n.aikarajausDescription
import fi.oph.kitu.jdbc.PAGINATED_DEFAULT_PAGE_SIZE
import fi.oph.kitu.jdbc.PaginatedSortOrder
import fi.oph.kitu.jdbc.SortDirection
import fi.oph.kitu.jdbc.SqlFilterBuilder
import fi.oph.kitu.jdbc.orderSql
import fi.oph.kitu.koodisto.Koodisto
import fi.oph.kitu.webmvc.buildCsvFilename
import fi.oph.kitu.yki.toTrueOrNull
import java.time.LocalDate

data class VktSuoritusFilter(
    val search: String? = null,
    val alkupaiva: LocalDate? = null,
    val loppupaiva: LocalDate? = null,
    val tutkintokieli: Koodisto.Tutkintokieli? = null,
    val taitotaso: Koodisto.VktTaitotaso? = null,
    val arvioitu: VktArvioinninTila? = null,
    val merkittyPoistettavaksi: Boolean? = null,
    val piilotaHenkilotiedot: Boolean = false,
) {
    fun toMap(): Map<String, String?> =
        mapOf(
            "search" to search,
            "alkupaiva" to alkupaiva?.toString(),
            "loppupaiva" to loppupaiva?.toString(),
            "tutkintokieli" to tutkintokieli?.name,
            "taitotaso" to taitotaso?.name,
            "arvioitu" to arvioitu?.name,
            "merkittyPoistettavaksi" to merkittyPoistettavaksi?.toString(),
            "piilotaHenkilotiedot" to piilotaHenkilotiedot.toTrueOrNull(),
        ).filterValues { it != null }

    fun filterDescriptions(): List<String> =
        listOfNotNull(
            aikarajausDescription(alkupaiva, loppupaiva),
            tutkintokieli?.let { "${UiText.Vkt.Sarake.tutkintokieli}: ${it.nimi}" },
            taitotaso?.let { "${UiText.Vkt.Sarake.taitotaso}: ${it.nimi}" },
            arvioitu?.let { "${UiText.Vkt.arvioinninTila}: ${it.nimi}" },
            merkittyPoistettavaksi?.let {
                if (it) UiText.Vkt.vainPoistettavat.toString() else UiText.Vkt.vainEiPoistettavat.toString()
            },
            if (piilotaHenkilotiedot) UiText.Filter.henkilotiedotPiilotettu.toString() else null,
        )

    fun csvFileName() =
        buildCsvFilename(
            "vkt_suoritukset",
            piilotaHenkilotiedot,
            tutkintokieli?.toString(),
            taitotaso?.toString(),
            alkupaiva?.toString(),
            loppupaiva?.toString(),
            arvioitu?.name?.lowercase(),
            merkittyPoistettavaksi?.let { "merkitty_poistettavaksi" },
        )

    fun excludeTags(): Set<ColumnTag> =
        setOfNotNull(
            if (piilotaHenkilotiedot) ColumnTag.PERSONAL_DATA else null,
        )

    fun whereSql(): String? = sqlSpec.whereClauseOrNull()

    fun params(): Map<String, Any?> = sqlSpec.params()

    /**
     * Rakennetaan kerran per instanssi: whereSql ja params tarvitsevat molemmat saman
     * suodattimen, ja erillisina funktiokutsuina SqlFilterBuilder rakennettiin joka
     * kyselylla kahdesti.
     */
    private val sqlSpec by lazy {
        SqlFilterBuilder().apply {
            add(searchQuery(), "filter_search" to "%${search.orEmpty()}%")
            add(alkupaiva?.let { "osakokeet.tutkintopaiva >= :filter_alkupaiva" }, "filter_alkupaiva" to alkupaiva)
            add(loppupaiva?.let { "osakokeet.tutkintopaiva <= :filter_loppupaiva" }, "filter_loppupaiva" to loppupaiva)
            add(tutkintokieli?.let { "tutkintokieli = :filter_kieli" }, "filter_kieli" to tutkintokieli?.name)
            add(taitotaso?.let { "taitotaso = :filter_taso" }, "filter_taso" to taitotaso?.name)
            add(arvioituQuery())
            add(merkittyPoistettavaksiQuery())
        }
    }

    private fun searchQuery(): String? =
        search?.takeIf { it.isNotEmpty() }?.let {
            """
            vkt_suoritus.etunimet ILIKE :filter_search
            OR vkt_suoritus.sukunimi ILIKE :filter_search
            OR vkt_suoritus.suorittajan_oid ILIKE :filter_search
            """.trimIndent()
        }

    private fun arvioituQuery(): String? =
        when (arvioitu) {
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

            else -> {
                null
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
                taitotaso = Koodisto.VktTaitotaso.Erinomainen,
                arvioitu = VktArvioinninTila.ArviointejaPuuttuu,
            )

        val ERINOMAISEN_TASON_SUORITUKSET =
            VktSuoritusFilter(
                taitotaso = Koodisto.VktTaitotaso.Erinomainen,
                arvioitu = VktArvioinninTila.ArvioituOsittainTaiKokonaan,
            )

        val HYVAN_JA_TYYDYTTAVAN_TASON_SUORITUKSET =
            VktSuoritusFilter(
                taitotaso = Koodisto.VktTaitotaso.HyväJaTyydyttävä,
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
