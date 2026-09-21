package fi.oph.kitu.kotoutumiskoulutus.suoritukset

import fi.oph.kitu.jdbc.PAGINATED_DEFAULT_PAGE_SIZE
import fi.oph.kitu.jdbc.PaginatedSortOrder
import fi.oph.kitu.jdbc.SortDirection
import fi.oph.kitu.jdbc.SqlFilterBuilder
import fi.oph.kitu.jdbc.orderSql
import fi.oph.kitu.jdbc.pageSql
import fi.oph.kitu.oid.Oid
import fi.oph.kitu.organisaatiot.OrganisaatioService
import fi.oph.kitu.util.equalsIgnoringAnnotated
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

    fun findSuoritukset(
        filter: KielitestiSuoritusFilter = KielitestiSuoritusFilter(),
        order: KielitestiSuoritusOrder = KielitestiSuoritusOrder(),
    ): List<KielitestiSuoritus> {
        val orgOids =
            filter.search
                ?.let { organisaatioService.searchOrganisaatiot(it) }
                ?.nimet
                ?.keys
                ?.toList() ?: emptyList()
        val searchQuery = filter.withOrgOids(orgOids).whereSql()

        val sql =
            """
            SELECT * from (
                SELECT DISTINCT ON (kurssi_id, oppijanumero, suoritusaika) * FROM koto_suoritus
                ORDER BY kurssi_id, oppijanumero, suoritusaika, last_modified DESC
                )
            ${searchQuery.orEmpty()}
            ORDER BY $order
            ${order.pageSql().orEmpty()}
            """.trimIndent()
        return jdbcNamedParameterTemplate.query(sql, filter.withOrgOids(orgOids).params(), KielitestiSuoritus.fromRow)
    }

    fun countSuoritukset(filter: KielitestiSuoritusFilter = KielitestiSuoritusFilter()): Int {
        val orgOids =
            filter.search
                ?.let { organisaatioService.searchOrganisaatiot(it) }
                ?.nimet
                ?.keys
                ?.toList() ?: emptyList()
        val searchQuery = filter.withOrgOids(orgOids).whereSql()

        val sql =
            """
            SELECT count(*) from (
                SELECT DISTINCT ON (kurssi_id, oppijanumero, suoritusaika) * FROM koto_suoritus
                ORDER BY kurssi_id, oppijanumero, suoritusaika, last_modified DESC
                )
            ${searchQuery.orEmpty()}
            """.trimIndent()

        return jdbcNamedParameterTemplate.queryForObject(
            sql,
            filter.withOrgOids(orgOids).params(),
            Int::class.java,
        )
            ?: 0
    }

    // Dashboard käyttää tätä "Viimeisin saapunut suoritus" -aikaleimana. Semantiikka on oikea koska
    // koto_suoritus on read-only virkailijalle: vain KoealustaService.importSuoritukset (ajastettu Koealusta-
    // polling) kirjoittaa ja exists()-tarkistus estää duplikaatit. Jos tulevaisuudessa lisätään esim.
    // virkailijan-UI:sta laukeava koto_suoritus-write, semantiikka rikkoutuu ja tämä metodi pitää korvata
    // erillisellä received_at-sarakkeella (vrt. YkiSuoritusRepository.findLatestReceivedAt).
    fun findLatestLastModified(): java.time.Instant? =
        jdbcNamedParameterTemplate
            .queryForObject(
                "SELECT MAX(last_modified) FROM koto_suoritus",
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
    val testikieli: Testikieli? = null,
    val orgOids: List<Oid> = emptyList(),
    val naytettavatSuoritukset: NaytettavatSuoritukset = NaytettavatSuoritukset.VALMIIT,
) {
    fun withOrgOids(oids: List<Oid>): KielitestiSuoritusFilter = copy(orgOids = oids)

    fun whereSql(): String? = toSql().whereClauseOrNull()

    fun params(): Map<String, Any?> = toSql().params()

    private fun toSql() =
        SqlFilterBuilder().apply {
            add(searchAndOrgQuery(), searchParams() + orgOidsParams())
            add(testikieli?.let { "testikieli = :filter_kieli" }, "filter_kieli" to testikieli?.name)
            add(suoritusalku?.let { "suoritusaika >= :filter_alkupaiva" }, "filter_alkupaiva" to suoritusalku)
            add(
                suoritusloppu?.let { "suoritusaika <= :filter_loppupaiva" },
                "filter_loppupaiva" to suoritusloppu?.plusDays(1),
            )
            add(naytettavatSuoritukset.whereSql)
        }

    private fun searchParams(): Map<String, String> =
        search
            ?.trim()
            ?.split(" ")
            ?.mapIndexed { index, term -> "search_str_$index" to "%$term%" }
            ?.toMap() ?: emptyMap()

    private fun searchAndOrgQuery(): String? =
        listOfNotNull(searchQuery(), orgOidsQuery())
            .takeIf { it.isNotEmpty() }
            ?.joinToString(" OR ")

    private fun searchQuery(): String? =
        searchParams()
            .takeIf { it.isNotEmpty() }
            ?.map { (key, _) ->
                """
                oppijanumero ILIKE :$key
                OR etunimet ILIKE :$key
                OR sukunimi ILIKE :$key
                OR email ILIKE :$key
                OR kurssi ILIKE :$key
                OR kurssi_id::text ILIKE :$key
                OR opettajan_email ILIKE :$key
                OR oppilaitos_oid ILIKE :$key
                OR tehtavapaketti ILIKE :$key
                """.trimIndent()
            }?.joinToString(" OR ")

    private fun orgOidsQuery(): String? =
        orgOids.takeIf { it.isNotEmpty() && it.size < 10 }?.let {
            "oppilaitos_oid IN (:filter_org_oids)"
        }

    private fun orgOidsParams(): Map<String, Any?> =
        orgOids.takeIf { it.isNotEmpty() && it.size < 10 }?.let {
            mapOf("filter_org_oids" to it.map(Oid::toString))
        } ?: emptyMap()
}

data class KielitestiSuoritusOrder(
    override val sortColumn: KielitestiSuoritusColumn = KielitestiSuoritusColumn.Suoritusaika,
    override val sortDirection: SortDirection = SortDirection.DESC,
    override val pageNumber: Int? = 0,
    override val pageSize: Int = PAGINATED_DEFAULT_PAGE_SIZE,
) : PaginatedSortOrder<KielitestiSuoritusColumn> {
    override fun toString(): String = orderSql()
}
