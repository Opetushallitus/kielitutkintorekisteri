package fi.oph.kitu.yki.suoritukset

import arrow.core.Either
import arrow.core.raise.either
import arrow.core.recover
import arrow.core.toNonEmptySetOrNull
import fi.oph.kitu.jdbc.SortDirection
import fi.oph.kitu.jdbc.SqlFilterBuilder
import fi.oph.kitu.oppijanumero.OppijanumeroException
import fi.oph.kitu.oppijanumero.OppijanumeroService
import fi.oph.kitu.util.SearchTerms
import fi.oph.kitu.util.TimeService
import fi.oph.kitu.yki.Arviointitila
import fi.oph.kitu.yki.Tutkintokieli
import fi.oph.kitu.yki.Tutkintotaso
import java.time.LocalDate

data class YkiSuoritusFilter(
    val search: SearchTerms? = null,
    val alkupaiva: LocalDate? = null,
    val loppupaiva: LocalDate? = null,
    val tutkintokieli: Tutkintokieli? = null,
    val tutkintotaso: Tutkintotaso? = null,
    val arviointitila: Arviointitila? = null,
    val tuontialku: LocalDate? = null,
    val tuontiloppu: LocalDate? = null,
) {
    fun whereSql(): String? = sqlSpec.whereClauseOrNull()

    fun params(): Map<String, Any?> = sqlSpec.params()

    fun requiresSubTables(): Boolean = tutkintokieli != null || tutkintotaso != null

    fun extendHenkiloOids(onr: OppijanumeroService): Either<OppijanumeroException, YkiSuoritusFilter> =
        either {
            val currentSearch = search ?: return@either this@YkiSuoritusFilter
            val oids = currentSearch.henkiloOids() ?: return@either this@YkiSuoritusFilter
            val expanded =
                oids
                    .toList()
                    .flatMap { term ->
                        val oid = onr.parseOid(term).bind()
                        onr
                            .getLinkedOids(oid)
                            .recover { error ->
                                if (error is OppijanumeroException.OppijaNotFoundException) setOf(oid) else raise(error)
                            }.bind()
                    }.map { it.toString() }
                    .toNonEmptySetOrNull() ?: oids
            copy(search = currentSearch.withHenkiloOids(expanded))
        }

    /**
     * Rakennetaan kerran per instanssi: whereSql ja params tarvitsevat molemmat saman
     * suodattimen, ja erillisina funktiokutsuina SqlFilterBuilder rakennettiin joka
     * kyselylla kahdesti.
     */
    private val sqlSpec by lazy {
        SqlFilterBuilder().apply {
            search?.texts()?.forEachIndexed { i, term ->
                val param = "filter_search_$i"
                add(searchTermClause(param), param to "%$term%")
            }
            search?.henkiloOids()?.let { oids ->
                add("suorittajan_oid IN (:henkilo_oids)", "henkilo_oids" to oids)
            }
            search?.orgOids()?.let { oids ->
                add("jarjestajan_tunnus_oid IN (:org_oids)", "org_oids" to oids)
            }
            search?.numbers()?.let { ids ->
                add("yki_suoritus.solki_id IN (:solki_ids)", "solki_ids" to ids)
            }
            add(alkupaiva?.let { "tutkintopaiva >= :filter_alkupaiva" }, "filter_alkupaiva" to alkupaiva)
            add(loppupaiva?.let { "tutkintopaiva <= :filter_loppupaiva" }, "filter_loppupaiva" to loppupaiva)
            add(tutkintokieli?.let { "tutkintokieli = :filter_kieli" }, "filter_kieli" to tutkintokieli?.name)
            add(tutkintotaso?.let { "tutkintotaso = :filter_taso" }, "filter_taso" to tutkintotaso?.name)
            add(
                arviointitila?.let { "arviointitila = :filter_arviointitila" },
                "filter_arviointitila" to arviointitila?.name,
            )
            val tuontiAlkuhetki = tuontialku?.atStartOfDay(TimeService.zoneId)?.toOffsetDateTime()
            add(tuontiAlkuhetki?.let { "received_at >= :filter_tuonti_alku" }, "filter_tuonti_alku" to tuontiAlkuhetki)
            val tuontiLoppuhetki = tuontiloppu?.plusDays(1)?.atStartOfDay(TimeService.zoneId)?.toOffsetDateTime()
            add(
                tuontiLoppuhetki?.let { "received_at < :filter_tuonti_loppu" },
                "filter_tuonti_loppu" to tuontiLoppuhetki,
            )
        }
    }

    private fun searchTermClause(param: String): String =
        """
        suorittajan_oid ILIKE :$param
        OR etunimet ILIKE :$param
        OR sukunimi ILIKE :$param
        OR email ILIKE :$param
        OR hetu ILIKE :$param
        OR jarjestajan_nimi ILIKE :$param
        """.trimIndent()

    companion object {
        fun from(search: String?) = YkiSuoritusFilter(search = SearchTerms.from(search))
    }
}

data class YkiSuoritusOrder(
    val sortColumn: YkiSuoritusColumn = YkiSuoritusColumn.Tutkintopaiva,
    val sortDirection: SortDirection = SortDirection.DESC,
) {
    override fun toString() =
        listOfNotNull(
            "${sortColumn.entityName} $sortDirection",
            when (sortColumn) {
                YkiSuoritusColumn.SolkiId -> "last_modified DESC"
                else -> null
            },
        ).joinToString(", ")
}
