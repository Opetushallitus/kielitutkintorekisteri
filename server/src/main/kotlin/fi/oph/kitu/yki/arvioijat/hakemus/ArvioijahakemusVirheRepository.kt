package fi.oph.kitu.yki.arvioijat.hakemus

import arrow.core.NonEmptyList
import fi.oph.kitu.util.defaultObjectMapper
import io.opentelemetry.instrumentation.annotations.WithSpan
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.springframework.stereotype.Repository
import java.time.OffsetDateTime

@Repository
class ArvioijahakemusVirheRepository(
    private val jdbc: NamedParameterJdbcTemplate,
) {
    @WithSpan
    fun tallenna(
        hakemusOid: String,
        henkiloOid: String?,
        virheet: NonEmptyList<ArvioijahakemuksenVirhe>,
        hetki: OffsetDateTime,
    ) {
        jdbc.update(
            """
            INSERT INTO yki_arvioijahakemus_virhe
                (hakemus_oid, henkilo_oid, virheet, ensimmainen_yritys, viimeisin_yritys, yrityksia)
            VALUES (:hakemusOid, :henkiloOid, :virheet::jsonb, :hetki, :hetki, 1)
            ON CONFLICT (hakemus_oid) DO UPDATE SET
                henkilo_oid = COALESCE(EXCLUDED.henkilo_oid, yki_arvioijahakemus_virhe.henkilo_oid),
                virheet = EXCLUDED.virheet,
                viimeisin_yritys = EXCLUDED.viimeisin_yritys,
                yrityksia = yki_arvioijahakemus_virhe.yrityksia + 1
            """.trimIndent(),
            MapSqlParameterSource()
                .addValue("hakemusOid", hakemusOid)
                .addValue("henkiloOid", henkiloOid)
                .addValue("virheet", defaultObjectMapper.writeValueAsString(virheet.map { it.tallennettava() }))
                .addValue("hetki", hetki),
        )
    }

    @WithSpan
    fun poista(hakemusOid: String) {
        jdbc.update(
            "DELETE FROM yki_arvioijahakemus_virhe WHERE hakemus_oid = :oid",
            MapSqlParameterSource("oid", hakemusOid),
        )
    }

    @WithSpan
    fun poistaKirjatut() {
        jdbc.update(
            """
            DELETE FROM yki_arvioijahakemus_virhe v
            WHERE EXISTS (SELECT 1 FROM yki_arvioijahakemus a WHERE a.hakemus_oid = v.hakemus_oid)
            """.trimIndent(),
            MapSqlParameterSource(),
        )
    }

    @WithSpan
    fun poistaVanhentuneet(listatut: Collection<String>) {
        if (listatut.isEmpty()) return
        jdbc.jdbcOperations.update({ connection ->
            connection
                .prepareStatement("DELETE FROM yki_arvioijahakemus_virhe WHERE hakemus_oid <> ALL (?)")
                .apply { setArray(1, connection.createArrayOf("text", listatut.toTypedArray())) }
        })
    }

    @WithSpan
    fun haeKaikki(): List<ArvioijahakemusVirheEntity> =
        jdbc.query(
            "SELECT * FROM yki_arvioijahakemus_virhe ORDER BY viimeisin_yritys DESC, hakemus_oid",
            ArvioijahakemusVirheEntity.fromRow,
        )

    @WithSpan
    fun count(): Long =
        jdbc.queryForObject(
            "SELECT count(*) FROM yki_arvioijahakemus_virhe",
            MapSqlParameterSource(),
            Long::class.java,
        ) ?: 0
}
