package fi.oph.kitu.yki.suoritukset

import arrow.core.Either
import arrow.core.raise.either
import arrow.core.recover
import arrow.core.toNonEmptySetOrNull
import fi.oph.kitu.jdbc.SortDirection
import fi.oph.kitu.jdbc.SqlFilter
import fi.oph.kitu.jdbc.SqlFilterBuilder
import fi.oph.kitu.jdbc.addAnyOf
import fi.oph.kitu.jdbc.addSearchTerms
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
    val tutkintokieli: List<Tutkintokieli> = emptyList(),
    val tutkintotaso: List<Tutkintotaso> = emptyList(),
    val arviointitila: List<Arviointitila> = emptyList(),
    val tuontialku: LocalDate? = null,
    val tuontiloppu: LocalDate? = null,
) : SqlFilter() {
    fun requiresSubTables(): Boolean = tutkintokieli.isNotEmpty() || tutkintotaso.isNotEmpty()

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

    override fun SqlFilterBuilder.build() {
        addSearchTerms(search?.texts().orEmpty(), HAKUSARAKKEET, "filter_search")
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
        addAnyOf("tutkintokieli", "filter_kieli", tutkintokieli)
        addAnyOf("tutkintotaso", "filter_taso", tutkintotaso)
        addAnyOf("arviointitila", "filter_arviointitila", arviointitila)
        val tuontiAlkuhetki = tuontialku?.atStartOfDay(TimeService.zoneId)?.toOffsetDateTime()
        add(tuontiAlkuhetki?.let { "received_at >= :filter_tuonti_alku" }, "filter_tuonti_alku" to tuontiAlkuhetki)
        val tuontiLoppuhetki = tuontiloppu?.plusDays(1)?.atStartOfDay(TimeService.zoneId)?.toOffsetDateTime()
        add(
            tuontiLoppuhetki?.let { "received_at < :filter_tuonti_loppu" },
            "filter_tuonti_loppu" to tuontiLoppuhetki,
        )
    }

    companion object {
        private val HAKUSARAKKEET =
            listOf("suorittajan_oid", "etunimet", "sukunimi", "email", "hetu", "jarjestajan_nimi")

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
