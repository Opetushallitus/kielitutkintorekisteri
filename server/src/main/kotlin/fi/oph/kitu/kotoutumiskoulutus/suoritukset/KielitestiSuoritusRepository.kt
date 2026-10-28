package fi.oph.kitu.kotoutumiskoulutus.suoritukset

import fi.oph.kitu.jdbc.PAGINATED_DEFAULT_PAGE_SIZE
import fi.oph.kitu.jdbc.PaginatedSortOrder
import fi.oph.kitu.jdbc.SortDirection
import fi.oph.kitu.jdbc.SqlCondition
import fi.oph.kitu.jdbc.SqlFilter
import fi.oph.kitu.jdbc.SqlFilterBuilder
import fi.oph.kitu.jdbc.addAnyOf
import fi.oph.kitu.jdbc.orderSql
import fi.oph.kitu.jdbc.pageSql
import fi.oph.kitu.jdbc.searchTermsCondition
import fi.oph.kitu.oid.Oid
import fi.oph.kitu.oid.getOid
import fi.oph.kitu.organisaatiot.OrganisaatioService
import fi.oph.kitu.util.equalsIgnoringAnnotated
import fi.oph.kitu.util.searchTerms
import org.springframework.data.repository.CrudRepository
import org.springframework.data.repository.PagingAndSortingRepository
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.springframework.stereotype.Repository
import java.sql.Timestamp
import java.time.LocalDate

@Repository
interface KielitestiSuoritusRepository :
    CrudRepository<KielitestiSuoritus, Int>,
    PagingAndSortingRepository<KielitestiSuoritus, Int>

@Repository
class CustomKielitestiSuoritusRepository(
    private val organisaatioService: OrganisaatioService,
    private val jdbcNamedParameterTemplate: NamedParameterJdbcTemplate,
) {
    // Spring's Transient-annotation doesn't play well with Kotlin data classes, so it seemed easier to add
    // findById and findAll functions to the custom repository than to fight with the annotations
    fun findById(id: Int): KielitestiSuoritus? {
        val sql =
            """
            SELECT * FROM koto_suoritus WHERE id = :id
            """.trimMargin()
        return jdbcNamedParameterTemplate
            .query(sql, mapOf("id" to id), KielitestiSuoritus.fromRow)
            .firstOrNull()
    }

    fun findAll(): List<KielitestiSuoritus> {
        val sql =
            """
            SELECT * FROM koto_suoritus
            """.trimIndent()
        return jdbcNamedParameterTemplate.query(sql, KielitestiSuoritus.fromRow)
    }

    private fun KielitestiSuoritusFilter.withOrganisaatiohaku(): KielitestiSuoritusFilter =
        withOrgOids(
            search
                ?.let { organisaatioService.searchOrganisaatiot(it) }
                ?.nimet
                ?.keys
                ?.toList() ?: emptyList(),
        )

    fun findSuoritukset(
        filter: KielitestiSuoritusFilter = KielitestiSuoritusFilter(),
        order: KielitestiSuoritusOrder = KielitestiSuoritusOrder(),
    ): List<KielitestiSuoritus> {
        val haku = filter.withOrganisaatiohaku()

        val sql =
            """
            SELECT * from (
                SELECT DISTINCT ON (kurssi_id, oppijanumero, suoritusaika) * FROM koto_suoritus
                ORDER BY kurssi_id, oppijanumero, suoritusaika, last_modified DESC
                )
            ${haku.whereSql().orEmpty()}
            ORDER BY $order
            ${order.pageSql().orEmpty()}
            """.trimIndent()
        return jdbcNamedParameterTemplate.query(sql, haku.params(), KielitestiSuoritus.fromRow)
    }

    fun countSuoritukset(filter: KielitestiSuoritusFilter = KielitestiSuoritusFilter()): Int {
        val haku = filter.withOrganisaatiohaku()

        val sql =
            """
            SELECT count(*) from (
                SELECT DISTINCT ON (kurssi_id, oppijanumero, suoritusaika) * FROM koto_suoritus
                ORDER BY kurssi_id, oppijanumero, suoritusaika, last_modified DESC
                )
            ${haku.whereSql().orEmpty()}
            """.trimIndent()

        return jdbcNamedParameterTemplate.queryForObject(sql, haku.params(), Int::class.java) ?: 0
    }

    // Keskeneräiset poistetaan ja tallennetaan uudelleen jokaisella tuonnilla, joten niiden
    // last_modified kertoo ajon hetken eikä suorituksen saapumista.
    fun findLatestLastModified(): java.time.Instant? =
        jdbcNamedParameterTemplate
            .queryForObject(
                "SELECT MAX(last_modified) FROM koto_suoritus WHERE completed",
                emptyMap<String, Any>(),
                Timestamp::class.java,
            )?.toInstant()

    fun findVarhaisinSuoritusaika(): java.time.Instant? =
        jdbcNamedParameterTemplate
            .queryForObject(
                "SELECT MIN(suoritusaika) FROM koto_suoritus WHERE completed",
                emptyMap<String, Any>(),
                Timestamp::class.java,
            )?.toInstant()

    fun deleteAllKeskeneraiset(): Int =
        jdbcNamedParameterTemplate.update(
            "DELETE FROM koto_suoritus WHERE completed = false",
            emptyMap<String, Any>(),
        )

    fun exists(suoritus: KielitestiSuoritus): Boolean {
        val existing = findLatestSuoritusVersion(suoritus) ?: return false
        return existing.equalsIgnoringAnnotated(suoritus, "KOTO")
    }

    fun findOppijanumerot(
        kurssiId: Int,
        suoritusaika: java.time.Instant,
    ): Set<Oid> =
        jdbcNamedParameterTemplate
            .query(
                """
                SELECT DISTINCT oppijanumero FROM koto_suoritus
                WHERE completed AND oppijanumero IS NOT NULL
                AND kurssi_id = :kurssiId
                AND suoritusaika = :suoritusaika
                """.trimIndent(),
                mapOf("kurssiId" to kurssiId, "suoritusaika" to Timestamp.from(suoritusaika)),
            ) { rs, _ -> rs.getOid("oppijanumero") }
            .toSet()

    private fun findLatestSuoritusVersion(suoritus: KielitestiSuoritus): KielitestiSuoritus? {
        val sql =
            """
            SELECT DISTINCT ON (kurssi_id, oppijanumero, suoritusaika) * FROM koto_suoritus
            WHERE kurssi_id = :kurssiId
            AND oppijanumero = :oppijanumero
            AND suoritusaika = :suoritusaika
            ORDER BY kurssi_id, oppijanumero, suoritusaika, last_modified DESC
            """.trimIndent()
        val paramMap =
            mapOf(
                "kurssiId" to suoritus.kurssiId,
                "oppijanumero" to suoritus.oppijanumero?.toString(),
                "suoritusaika" to suoritus.suoritusaika?.let { Timestamp.from(it) },
            )
        return jdbcNamedParameterTemplate.query(sql, paramMap, KielitestiSuoritus.fromRow).firstOrNull()
    }
}

data class KielitestiSuoritusFilter(
    val search: String? = null,
    val suoritusalku: LocalDate? = null,
    val suoritusloppu: LocalDate? = null,
    val testikieli: List<Testikieli> = emptyList(),
    val orgOids: List<Oid> = emptyList(),
    val naytettavatSuoritukset: NaytettavatSuoritukset = NaytettavatSuoritukset.VALMIIT,
) : SqlFilter() {
    fun withOrgOids(oids: List<Oid>): KielitestiSuoritusFilter = copy(orgOids = oids)

    override fun SqlFilterBuilder.build() {
        val hakuehdot =
            listOfNotNull(
                searchTermsCondition(searchTerms(search), HAKUSARAKKEET, "search_str"),
                orgOidsCondition(),
            )
        if (hakuehdot.isNotEmpty()) {
            add(
                hakuehdot.joinToString(" OR ") { "(${it.sql})" },
                hakuehdot.fold(emptyMap()) { acc, it ->
                    acc +
                        it.params
                },
            )
        }
        addAnyOf("testikieli", "filter_kieli", testikieli)
        add(suoritusalku?.let { "suoritusaika >= :filter_alkupaiva" }, "filter_alkupaiva" to suoritusalku)
        add(
            suoritusloppu?.let { "suoritusaika <= :filter_loppupaiva" },
            "filter_loppupaiva" to suoritusloppu?.plusDays(1),
        )
        add(naytettavatSuoritukset.whereSql)
    }

    private fun orgOidsCondition(): SqlCondition? =
        orgOids.takeIf { it.isNotEmpty() && it.size < 10 }?.let {
            SqlCondition("oppilaitos_oid IN (:filter_org_oids)", mapOf("filter_org_oids" to it.map(Oid::toString)))
        }

    companion object {
        private val HAKUSARAKKEET =
            listOf(
                "oppijanumero",
                "etunimet",
                "sukunimi",
                "email",
                "kurssi",
                "kurssi_id::text",
                "opettajan_email",
                "oppilaitos_oid",
                "tehtavapaketti",
            )
    }
}

data class KielitestiSuoritusOrder(
    override val sortColumn: KielitestiSuoritusColumn = KielitestiSuoritusColumn.Suoritusaika,
    override val sortDirection: SortDirection = SortDirection.DESC,
    override val pageNumber: Int? = 0,
    override val pageSize: Int = PAGINATED_DEFAULT_PAGE_SIZE,
) : PaginatedSortOrder<KielitestiSuoritusColumn> {
    override fun toString(): String = orderSql()
}
