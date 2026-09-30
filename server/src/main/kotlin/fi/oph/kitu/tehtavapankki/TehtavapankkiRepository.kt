package fi.oph.kitu.tehtavapankki

import fi.oph.kitu.jdbc.getOffsetDateTimeOrNull
import fi.oph.kitu.util.defaultObjectMapper
import io.opentelemetry.instrumentation.annotations.WithSpan
import org.springframework.jdbc.core.SingleColumnRowMapper
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import tools.jackson.databind.JsonNode
import java.time.OffsetDateTime

@Service
class TehtavapankkiRepository(
    private val jdbc: NamedParameterJdbcTemplate,
) {
    @WithSpan
    fun insertPaketti(paketti: TehtavapakettiEntity): Int =
        jdbc
            .query(
                """
                INSERT INTO tehtavapaketti (
                    lahdejarjestelma, lahde_id, nimi, versio_hash, s3_avain, metadata,
                    lahde_filegenerated, lahde_published, lahde_version, lahde_language
                )
                VALUES (
                    :lahdejarjestelma, :lahde_id, :nimi, :versio_hash, :s3_avain, :metadata::jsonb,
                    :lahde_filegenerated, :lahde_published, :lahde_version, :lahde_language
                )
                RETURNING id
                """.trimIndent(),
                MapSqlParameterSource()
                    .addValue("lahdejarjestelma", paketti.lahdejarjestelma)
                    .addValue("lahde_id", paketti.lahdeId)
                    .addValue("nimi", paketti.nimi)
                    .addValue("versio_hash", paketti.versioHash)
                    .addValue("s3_avain", paketti.s3Avain)
                    .addValue("metadata", paketti.metadata.serialize())
                    .addValue("lahde_filegenerated", paketti.lahdeFilegenerated)
                    .addValue("lahde_published", paketti.lahdePublished)
                    .addValue("lahde_version", paketti.lahdeVersion)
                    .addValue("lahde_language", paketti.lahdeLanguage),
                SingleColumnRowMapper(Int::class.java),
            ).first()!!

    @WithSpan
    fun findLatestFilegeneratedBySource(
        lahdejarjestelma: String,
        lahdeId: String,
    ): OffsetDateTime? =
        jdbc
            .query(
                """
                SELECT lahde_filegenerated
                FROM tehtavapaketti
                WHERE lahdejarjestelma = :lahdejarjestelma AND lahde_id = :lahde_id
                ORDER BY luotu DESC
                LIMIT 1
                """.trimIndent(),
                mapOf("lahdejarjestelma" to lahdejarjestelma, "lahde_id" to lahdeId),
            ) { rs, _ -> rs.getOffsetDateTimeOrNull("lahde_filegenerated") }
            .firstOrNull()

    @WithSpan
    fun updateLahdeMetadata(
        id: Int,
        lahdeFilegenerated: OffsetDateTime?,
        lahdePublished: OffsetDateTime?,
        lahdeVersion: String?,
        lahdeLanguage: String?,
    ): Int =
        jdbc.update(
            """
            UPDATE tehtavapaketti
            SET lahde_filegenerated = :fg,
                lahde_published     = :pub,
                lahde_version       = :ver,
                lahde_language      = :lang
            WHERE id = :id
            """.trimIndent(),
            MapSqlParameterSource()
                .addValue("id", id)
                .addValue("fg", lahdeFilegenerated)
                .addValue("pub", lahdePublished)
                .addValue("ver", lahdeVersion)
                .addValue("lang", lahdeLanguage),
        )

    @WithSpan
    @Transactional
    fun insertRyhmat(ryhmat: List<TehtavaryhmaEntity>): List<Int> {
        if (ryhmat.isEmpty()) return emptyList()
        return ryhmat.map { ryhma ->
            jdbc
                .query(
                    """
                    INSERT INTO tehtavaryhma (paketti_id, nimi, jarjestys, metadata)
                    VALUES (:paketti_id, :nimi, :jarjestys, :metadata::jsonb)
                    RETURNING id
                    """.trimIndent(),
                    MapSqlParameterSource()
                        .addValue("paketti_id", ryhma.pakettiId)
                        .addValue("nimi", ryhma.nimi)
                        .addValue("jarjestys", ryhma.jarjestys)
                        .addValue("metadata", ryhma.metadata.serialize()),
                    SingleColumnRowMapper(Int::class.java),
                ).first()!!
        }
    }

    @WithSpan
    fun findRyhmatByPakettiId(pakettiId: Int): List<TehtavaryhmaEntity> =
        jdbc.query(
            "SELECT * FROM tehtavaryhma WHERE paketti_id = :paketti_id ORDER BY jarjestys",
            mapOf("paketti_id" to pakettiId),
            TehtavaryhmaEntity.fromRow,
        )

    @WithSpan
    @Transactional
    fun insertTehtavat(tehtavat: List<TehtavaEntity>): List<Int> {
        if (tehtavat.isEmpty()) return emptyList()
        return tehtavat.map { tehtava ->
            jdbc
                .query(
                    """
                    INSERT INTO tehtava (
                        paketti_id, ryhma_id, tyyppi, lahde_id, nimi, teksti, tekstin_formaatti,
                        jarjestys, metadata
                    )
                    VALUES (
                        :paketti_id, :ryhma_id, :tyyppi, :lahde_id, :nimi, :teksti, :tekstin_formaatti,
                        :jarjestys, :metadata::jsonb
                    )
                    RETURNING id
                    """.trimIndent(),
                    MapSqlParameterSource()
                        .addValue("paketti_id", tehtava.pakettiId)
                        .addValue("ryhma_id", tehtava.ryhmaId)
                        .addValue("tyyppi", tehtava.tyyppi)
                        .addValue("lahde_id", tehtava.lahdeId)
                        .addValue("nimi", tehtava.nimi)
                        .addValue("teksti", tehtava.teksti)
                        .addValue("tekstin_formaatti", tehtava.tekstinFormaatti)
                        .addValue("jarjestys", tehtava.jarjestys)
                        .addValue("metadata", tehtava.metadata.serialize()),
                    SingleColumnRowMapper(Int::class.java),
                ).first()!!
        }
    }

    @WithSpan
    @Transactional
    fun insertVastaukset(vastaukset: List<TehtavaVastausEntity>) {
        if (vastaukset.isEmpty()) return
        vastaukset.forEach { vastaus ->
            jdbc.update(
                """
                INSERT INTO tehtava_vastaus (
                    tehtava_id, jarjestys, teksti, tekstin_formaatti, metadata
                )
                VALUES (
                    :tehtava_id, :jarjestys, :teksti, :tekstin_formaatti, :metadata::jsonb
                )
                """.trimIndent(),
                MapSqlParameterSource()
                    .addValue("tehtava_id", vastaus.tehtavaId)
                    .addValue("jarjestys", vastaus.jarjestys)
                    .addValue("teksti", vastaus.teksti)
                    .addValue("tekstin_formaatti", vastaus.tekstinFormaatti)
                    .addValue("metadata", vastaus.metadata.serialize()),
            )
        }
    }

    @WithSpan
    fun findPakettiById(id: Int): TehtavapakettiEntity? =
        jdbc
            .query(
                "SELECT * FROM tehtavapaketti WHERE id = :id",
                mapOf("id" to id),
                TehtavapakettiEntity.fromRow,
            ).firstOrNull()

    @WithSpan
    fun findLatestPakettiBySource(
        lahdejarjestelma: String,
        lahdeId: String,
    ): TehtavapakettiEntity? =
        jdbc
            .query(
                """
                SELECT * FROM tehtavapaketti
                WHERE lahdejarjestelma = :lahdejarjestelma AND lahde_id = :lahde_id
                ORDER BY luotu DESC
                LIMIT 1
                """.trimIndent(),
                mapOf("lahdejarjestelma" to lahdejarjestelma, "lahde_id" to lahdeId),
                TehtavapakettiEntity.fromRow,
            ).firstOrNull()

    @WithSpan
    fun existsByVersionHash(
        lahdejarjestelma: String,
        lahdeId: String,
        versioHash: String,
    ): Boolean =
        jdbc.queryForObject(
            """
            SELECT EXISTS (
                SELECT 1 FROM tehtavapaketti
                WHERE lahdejarjestelma = :lahdejarjestelma
                  AND lahde_id = :lahde_id
                  AND versio_hash = :versio_hash
            )
            """.trimIndent(),
            mapOf(
                "lahdejarjestelma" to lahdejarjestelma,
                "lahde_id" to lahdeId,
                "versio_hash" to versioHash,
            ),
            Boolean::class.java,
        ) ?: false

    @WithSpan
    fun findTehtavatByPakettiId(pakettiId: Int): List<TehtavaEntity> =
        jdbc.query(
            "SELECT * FROM tehtava WHERE paketti_id = :paketti_id ORDER BY jarjestys",
            mapOf("paketti_id" to pakettiId),
            TehtavaEntity.fromRow,
        )

    @WithSpan
    @Transactional
    fun insertTiedostot(tiedostot: List<TehtavaTiedostoEntity>) {
        if (tiedostot.isEmpty()) return
        tiedostot.forEach { tiedosto ->
            jdbc.update(
                """
                INSERT INTO tehtava_tiedosto (tehtava_id, tiedostonimi, s3_avain)
                VALUES (:tehtava_id, :tiedostonimi, :s3_avain)
                """.trimIndent(),
                MapSqlParameterSource()
                    .addValue("tehtava_id", tiedosto.tehtavaId)
                    .addValue("tiedostonimi", tiedosto.tiedostonimi)
                    .addValue("s3_avain", tiedosto.s3Avain),
            )
        }
    }

    @WithSpan
    fun findTiedostotByTehtavaIds(tehtavaIds: List<Int>): Map<Int, List<TehtavaTiedostoEntity>> {
        if (tehtavaIds.isEmpty()) return emptyMap()
        val rows =
            jdbc.query(
                "SELECT * FROM tehtava_tiedosto WHERE tehtava_id IN (:ids) ORDER BY tehtava_id, id",
                mapOf("ids" to tehtavaIds),
                TehtavaTiedostoEntity.fromRow,
            )
        return rows.groupBy { it.tehtavaId }
    }

    @WithSpan
    fun findVastauksetByTehtavaIds(tehtavaIds: List<Int>): Map<Int, List<TehtavaVastausEntity>> {
        if (tehtavaIds.isEmpty()) return emptyMap()
        val rows =
            jdbc.query(
                "SELECT * FROM tehtava_vastaus WHERE tehtava_id IN (:ids) ORDER BY tehtava_id, jarjestys",
                mapOf("ids" to tehtavaIds),
                TehtavaVastausEntity.fromRow,
            )
        return rows.groupBy { it.tehtavaId }
    }

    /**
     * Hakee annettuja S3-avaimia vastaavat tehtäväpaketit. Palautuvat vain ne
     * avaimet, joille löytyy rivi tietokannasta.
     */
    @WithSpan
    fun findPaketitByS3Avaimet(s3Avaimet: Collection<String>): List<TehtavapakettiEntity> {
        if (s3Avaimet.isEmpty()) return emptyList()
        return jdbc.query(
            "SELECT * FROM tehtavapaketti WHERE s3_avain IN (:keys)",
            mapOf("keys" to s3Avaimet),
            TehtavapakettiEntity.fromRow,
        )
    }
}

private fun JsonNode.serialize(): String = defaultObjectMapper.writeValueAsString(this)
