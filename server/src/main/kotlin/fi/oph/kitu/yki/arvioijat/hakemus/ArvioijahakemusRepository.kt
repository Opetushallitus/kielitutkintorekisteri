package fi.oph.kitu.yki.arvioijat.hakemus

import io.opentelemetry.instrumentation.annotations.WithSpan
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.springframework.stereotype.Repository
import java.time.LocalDate
import java.time.OffsetDateTime

@Repository
class ArvioijahakemusRepository(
    private val jdbc: NamedParameterJdbcTemplate,
) {
    @WithSpan
    fun kasitellyt(hakemusOidit: Collection<String>): Set<String> =
        if (hakemusOidit.isEmpty()) {
            emptySet()
        } else {
            jdbc
                .queryForList(
                    """
                    SELECT hakemus_oid FROM yki_arvioijahakemus
                    WHERE hakemus_oid IN (:oidit) AND tila <> 'ODOTTAA_YKSILOINTIA'
                    """.trimIndent(),
                    MapSqlParameterSource("oidit", hakemusOidit),
                    String::class.java,
                ).filterNotNull()
                .toSet()
        }

    @WithSpan
    fun odottaaYksilointia(
        hakemusOid: String,
        henkiloOid: String?,
        syy: String,
        hetki: OffsetDateTime,
    ) {
        jdbc.update(
            """
            INSERT INTO yki_arvioijahakemus (hakemus_oid, henkilo_oid, tila, syy, kasitelty)
            VALUES (:hakemusOid, :henkiloOid, 'ODOTTAA_YKSILOINTIA', :syy, :hetki)
            ON CONFLICT (hakemus_oid) DO UPDATE SET
                henkilo_oid = EXCLUDED.henkilo_oid,
                syy = EXCLUDED.syy,
                kasitelty = EXCLUDED.kasitelty
            WHERE yki_arvioijahakemus.tila = 'ODOTTAA_YKSILOINTIA'
            """.trimIndent(),
            MapSqlParameterSource()
                .addValue("hakemusOid", hakemusOid)
                .addValue("henkiloOid", henkiloOid)
                .addValue("syy", syy)
                .addValue("hetki", hetki),
        )
    }

    @WithSpan
    fun varaa(
        hakemusOid: String,
        henkiloOid: String,
        kaudenAlkupaiva: LocalDate,
        hetki: OffsetDateTime,
    ): Boolean =
        jdbc.update(
            """
            INSERT INTO yki_arvioijahakemus (hakemus_oid, henkilo_oid, tila, kauden_alkupaiva, kasitelty)
            VALUES (:hakemusOid, :henkiloOid, 'KASITTELYSSA', :kaudenAlkupaiva, :hetki)
            ON CONFLICT (hakemus_oid) DO NOTHING
            """.trimIndent(),
            MapSqlParameterSource()
                .addValue("hakemusOid", hakemusOid)
                .addValue("henkiloOid", henkiloOid)
                .addValue("kaudenAlkupaiva", kaudenAlkupaiva)
                .addValue("hetki", hetki),
        ) == 1

    @WithSpan
    fun merkitseKasitellyksi(
        hakemusOid: String,
        henkiloOid: String,
        arvioijaId: Int?,
        kaudenAlkupaiva: LocalDate,
        hetki: OffsetDateTime,
    ) {
        jdbc.update(
            """
            UPDATE yki_arvioijahakemus
            SET tila = 'KASITELTY',
                henkilo_oid = :henkiloOid,
                arvioija_id = :arvioijaId,
                kauden_alkupaiva = :kaudenAlkupaiva,
                kasitelty = :hetki
            WHERE hakemus_oid = :hakemusOid AND tila = 'KASITTELYSSA'
            """.trimIndent(),
            MapSqlParameterSource()
                .addValue("hakemusOid", hakemusOid)
                .addValue("henkiloOid", henkiloOid)
                .addValue("arvioijaId", arvioijaId)
                .addValue("kaudenAlkupaiva", kaudenAlkupaiva)
                .addValue("hetki", hetki),
        )
    }

    @WithSpan
    fun hae(hakemusOid: String): ArvioijahakemusEntity? =
        jdbc
            .query(
                "SELECT * FROM yki_arvioijahakemus WHERE hakemus_oid = :oid",
                MapSqlParameterSource("oid", hakemusOid),
                ArvioijahakemusEntity.fromRow,
            ).firstOrNull()

    @WithSpan
    fun haeKasittelemattomat(): List<ArvioijahakemusEntity> =
        jdbc.query(
            "SELECT * FROM yki_arvioijahakemus WHERE tila <> 'KASITELTY' ORDER BY kasitelty DESC, hakemus_oid",
            ArvioijahakemusEntity.fromRow,
        )

    @WithSpan
    fun haeKaikki(): List<ArvioijahakemusEntity> =
        jdbc.query(
            "SELECT * FROM yki_arvioijahakemus ORDER BY kasitelty DESC, hakemus_oid",
            ArvioijahakemusEntity.fromRow,
        )

    @WithSpan
    fun poista(
        hakemusOid: String,
        tila: ArvioijahakemuksenTila,
    ) {
        jdbc.update(
            "DELETE FROM yki_arvioijahakemus WHERE hakemus_oid = :oid AND tila = :tila",
            MapSqlParameterSource()
                .addValue("oid", hakemusOid)
                .addValue("tila", tila.name),
        )
    }

    @WithSpan
    fun poistaVanhentuneetYksiloimattomat(listatut: Collection<String>) {
        if (listatut.isEmpty()) return
        jdbc.jdbcOperations.update({ connection ->
            connection
                .prepareStatement(
                    "DELETE FROM yki_arvioijahakemus WHERE tila = 'ODOTTAA_YKSILOINTIA' AND hakemus_oid <> ALL (?)",
                ).apply { setArray(1, connection.createArrayOf("text", listatut.toTypedArray())) }
        })
    }
}
