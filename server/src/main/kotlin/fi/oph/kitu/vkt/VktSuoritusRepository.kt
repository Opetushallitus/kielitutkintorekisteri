package fi.oph.kitu.vkt

import fi.oph.kitu.jdbc.getEnum
import fi.oph.kitu.jdbc.pageSql
import fi.oph.kitu.koodisto.Koodisto
import fi.oph.kitu.tiedontuontischema.VktHenkilosuoritus
import fi.oph.kitu.util.equalsIgnoringAnnotated
import io.opentelemetry.instrumentation.annotations.WithSpan
import org.springframework.data.repository.CrudRepository
import org.springframework.data.repository.PagingAndSortingRepository
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.core.RowMapper
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.springframework.stereotype.Repository
import java.time.OffsetDateTime
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors

interface VktSuoritusRepository :
    CrudRepository<VktSuoritusEntity, Int>,
    PagingAndSortingRepository<VktSuoritusEntity, Int> {
    fun findByIlmoittautumisenId(id: String): List<VktSuoritusEntity>
}

@Repository
class CustomVktSuoritusRepository(
    private val jdbcNamedParameterTemplate: NamedParameterJdbcTemplate,
    private val jdbcTemplate: JdbcTemplate,
    private val vktSuoritusRepository: VktSuoritusRepository,
) {
    @WithSpan
    fun save(entity: VktSuoritusEntity): VktSuoritusEntity? =
        if (!exists(entity)) {
            vktSuoritusRepository.save(entity)
        } else {
            null
        }

    @WithSpan
    fun exists(entity: VktSuoritusEntity): Boolean {
        val storedEntity =
            vktSuoritusRepository
                .findByIlmoittautumisenId(entity.ilmoittautumisenId)
                .maxByOrNull { it.createdAt ?: OffsetDateTime.MIN }
        return storedEntity?.let { entity.equalsIgnoringAnnotated(it, "VKT") } ?: false
    }

    @WithSpan
    fun find(
        filter: VktSuoritusFilter,
        order: VktSuoritusOrder,
    ): Iterable<VktSuoritusFlat> {
        val query =
            """
            WITH osakokeet AS (
                SELECT
                    vkt_suoritus.id as suoritus_id,
                    vkt_osakoe.tutkintopaiva,
                    count(*) filter (WHERE vkt_osakoe.arvosana is not null) AS arviointeja,
                    max(vkt_osakoe.arvosana) filter(WHERE vkt_osakoe.tyyppi = 'PuheenYmmärtäminen') AS puheen_ymmärtäminen,
                    max(vkt_osakoe.arvosana) filter(WHERE vkt_osakoe.tyyppi = 'Puhuminen') AS puhuminen,
                    max(vkt_osakoe.arvosana) filter(WHERE vkt_osakoe.tyyppi = 'TekstinYmmärtäminen') AS tekstin_ymmärtäminen,
                    max(vkt_osakoe.arvosana) filter(WHERE vkt_osakoe.tyyppi = 'Kirjoittaminen') AS kirjoittaminen
                FROM vkt_suoritus
                JOIN vkt_osakoe ON vkt_osakoe.suoritus_id = vkt_suoritus.id
                GROUP BY vkt_suoritus.id, vkt_osakoe.tutkintopaiva
            ),
            result AS (
                SELECT DISTINCT ON (vkt_suoritus.ilmoittautumisen_id)
                    vkt_suoritus.id as suoritus_id,
                    vkt_suoritus.ilmoittautumisen_id,
                    vkt_suoritus.suorittajan_oid,
                    vkt_suoritus.etunimet,
                    vkt_suoritus.sukunimi,
                    vkt_suoritus.tutkintokieli,
                    vkt_suoritus.taitotaso,
                    vkt_suoritus.suorituspaikkakunta,
                    vkt_suoritus.suorituksen_vastaanottaja,
                    osakokeet.tutkintopaiva,
                    max(osakokeet.puheen_ymmärtäminen) AS puheen_ymmärtäminen,
                    max(osakokeet.puhuminen) AS puhuminen,
                    max(osakokeet.tekstin_ymmärtäminen) AS tekstin_ymmärtäminen,
                    max(osakokeet.kirjoittaminen) AS kirjoittaminen
                FROM vkt_suoritus
                    JOIN vkt_osakoe ON vkt_osakoe.suoritus_id = vkt_suoritus.id
                    JOIN osakokeet ON osakokeet.suoritus_id = vkt_suoritus.id
            
                ${filter.whereSql().orEmpty()}
            
                GROUP BY vkt_suoritus.id, osakokeet.tutkintopaiva
                ORDER BY vkt_suoritus.ilmoittautumisen_id, vkt_suoritus.created_at DESC, osakokeet.tutkintopaiva
            )
            SELECT *
            FROM result
            ORDER BY $order, suoritus_id
            ${order.pageSql().orEmpty()}
            """.trimIndent()

        return jdbcNamedParameterTemplate.query(query, filter.params(), VktSuoritusFlat.fromRow)
    }

    fun getOppijanSuoritusIds(id: Tutkintoryhma): List<Int> {
        val query =
            """
            WITH suoritus AS (
                SELECT
                    *,
                    row_number() OVER (PARTITION BY ilmoittautumisen_id ORDER BY created_at DESC) rn
                FROM vkt_suoritus
                WHERE suorittajan_oid = :oppijanumero
                AND tutkintokieli = :tutkintokieli
                AND taitotaso = :taitotaso
            )
            SELECT s.id
            FROM suoritus s
            WHERE rn = 1
            """.trimIndent()

        val params = id.toSqlParams()

        return jdbcNamedParameterTemplate.queryForList(query, params, Int::class.java).filterNotNull()
    }

    // Dashboard käyttää tätä "Viimeisin saapunut suoritus" -aikaleimana. Semantiikka on tällä hetkellä
    // oikea koska vkt_suoritus.created_at asetetaan add_rekisteriintuontiaika-DB-triggerillä BEFORE INSERT
    // ja vain VktApiController.putHenkilosuoritus insertoi rivejä. Virkailija-UI päivittää vkt_osakoe-tauluun
    // eikä Koski-tilan päivitykset koske vkt_suoritus.created_at. Jos tulevaisuudessa lisätään
    // virkailija-UI:sta laukeava vkt_suoritus-insert, semantiikka rikkoutuu ja tämä metodi pitää korvata
    // erillisellä received_at-sarakkeella (vrt. YkiSuoritusRepository.findLatestReceivedAt).
    @WithSpan
    fun findLatestCreatedAt(): OffsetDateTime? =
        jdbcTemplate.queryForObject(
            "SELECT MAX(created_at) FROM vkt_suoritus",
            OffsetDateTime::class.java,
        )

    @WithSpan
    fun countSuorituksetByTaitotaso(): VktSuoritusCountsByTaitotaso {
        val futures =
            listOf(
                VktSuoritusFilter(),
                VktSuoritusFilter.ERINOMAISEN_TASON_ILMOITTAUTUNEET,
                VktSuoritusFilter.ERINOMAISEN_TASON_SUORITUKSET,
                VktSuoritusFilter.HYVAN_JA_TYYDYTTAVAN_TASON_SUORITUKSET,
            ).map { filter ->
                CompletableFuture.supplyAsync({ numberOfRowsForListView(filter) }, countExecutor)
            }
        try {
            CompletableFuture.allOf(*futures.toTypedArray()).join()
        } catch (t: Throwable) {
            futures.forEach { it.cancel(true) }
            throw t
        }
        return VktSuoritusCountsByTaitotaso(
            total = futures[0].get().toLong(),
            erinomaisenTasonIlmoittautuneet = futures[1].get().toLong(),
            erinomaisenTasonSuoritukset = futures[2].get().toLong(),
            hyvanJaTyydyttavanTasonSuoritukset = futures[3].get().toLong(),
        )
    }

    companion object {
        private val countExecutor =
            Executors.newFixedThreadPool(4) { runnable ->
                Thread(runnable, "vkt-dashboard-count").apply { isDaemon = true }
            }
    }

    @WithSpan
    fun numberOfRowsForListView(filter: VktSuoritusFilter): Int {
        val query =
            """
            WITH osakokeet AS (
                SELECT
                    vkt_suoritus.id as suoritus_id,
                    vkt_osakoe.tutkintopaiva,
                    count(*) filter (WHERE vkt_osakoe.arvosana is not null) AS arviointeja
                FROM vkt_suoritus
                    JOIN vkt_osakoe ON vkt_osakoe.suoritus_id = vkt_suoritus.id
                    GROUP BY vkt_suoritus.id, vkt_osakoe.tutkintopaiva
            ),
            result AS (
                SELECT DISTINCT ON (vkt_suoritus.ilmoittautumisen_id) 1
                FROM vkt_suoritus
                    JOIN vkt_osakoe ON vkt_osakoe.suoritus_id = vkt_suoritus.id
                    JOIN osakokeet ON osakokeet.suoritus_id = vkt_suoritus.id
            
                ${filter.whereSql().orEmpty()}
            
                GROUP BY vkt_suoritus.id, osakokeet.tutkintopaiva
                ORDER BY vkt_suoritus.ilmoittautumisen_id
            )
            SELECT COUNT(*) FROM result
            """.trimIndent()

        return jdbcNamedParameterTemplate.queryForObject(query, filter.params(), Int::class.java)!!
    }

    @WithSpan
    fun findOpiskeluoikeudetForKoskiTransfer(): Iterable<Tutkintoryhma> {
        val query =
            """
            SELECT
                suorittajan_oid oppijanumero,
                tutkintokieli,
                taitotaso
            FROM
                vkt_suoritus
            WHERE
                NOT koski_siirto_kasitelty
                AND NOT EXISTS (
                    SELECT 1
                    FROM vkt_osakoe
                    WHERE
                        vkt_osakoe.suoritus_id = vkt_suoritus.id
                        AND arvosana IS NULL)
            GROUP BY
                suorittajan_oid,
                tutkintokieli,
                taitotaso
            """.trimIndent()

        return jdbcTemplate.query(query, Tutkintoryhma.fromRow)
    }

    @WithSpan
    fun markSuoritusTransferredToKoski(
        id: Tutkintoryhma,
        koskiOpiskeluoikeusOid: String?,
    ) {
        val query =
            """
            UPDATE vkt_suoritus
            SET
                koski_siirto_kasitelty = true,
                koski_opiskeluoikeus = :koski_oid
            WHERE
                suorittajan_oid = :oppijanumero
                AND tutkintokieli = :tutkintokieli
                AND taitotaso = :taitotaso
            """.trimIndent()

        val params = id.toSqlParams() + mapOf("koski_oid" to koskiOpiskeluoikeusOid)

        jdbcNamedParameterTemplate.update(query, params)
    }

    @WithSpan
    fun requestTransferToKoski(id: Tutkintoryhma) {
        val query =
            """
            UPDATE vkt_suoritus
            SET
                koski_siirto_kasitelty = false
            WHERE
                suorittajan_oid = :oppijanumero
                AND tutkintokieli = :tutkintokieli
                AND taitotaso = :taitotaso
            """.trimIndent()

        val params = id.toSqlParams()

        jdbcNamedParameterTemplate.update(query, params)
    }

    @WithSpan
    fun cleanup() {
        val query =
            """
            DELETE FROM vkt_suoritus
            WHERE
            	NOT EXISTS (
            		SELECT 1
            		FROM vkt_osakoe
            		WHERE vkt_osakoe.suoritus_id = vkt_suoritus.id)

            """.trimIndent()

        jdbcTemplate.update(query)
    }

    data class VktSuoritusCountsByTaitotaso(
        val total: Long,
        val erinomaisenTasonIlmoittautuneet: Long,
        val erinomaisenTasonSuoritukset: Long,
        val hyvanJaTyydyttavanTasonSuoritukset: Long,
    )

    data class Tutkintoryhma(
        val oppijanumero: String,
        val tutkintokieli: Koodisto.Tutkintokieli,
        val taitotaso: Koodisto.VktTaitotaso,
    ) {
        fun toSqlParams() =
            mapOf(
                "oppijanumero" to oppijanumero,
                "tutkintokieli" to tutkintokieli.name,
                "taitotaso" to taitotaso.name,
            )

        companion object {
            fun from(suoritus: VktHenkilosuoritus) =
                Tutkintoryhma(
                    oppijanumero = suoritus.henkilo.oid.toString(),
                    tutkintokieli = suoritus.suoritus.kieli,
                    taitotaso = suoritus.suoritus.taitotaso,
                )

            val fromRow =
                RowMapper { rs, _ ->
                    Tutkintoryhma(
                        oppijanumero = rs.getString("oppijanumero"),
                        tutkintokieli = rs.getEnum<Koodisto.Tutkintokieli>("tutkintokieli"),
                        taitotaso = rs.getEnum<Koodisto.VktTaitotaso>("taitotaso"),
                    )
                }
        }
    }
}
