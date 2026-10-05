package fi.oph.kitu.yki.arvioijat.hakemus

import io.opentelemetry.instrumentation.annotations.WithSpan
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.springframework.stereotype.Repository

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
    fun tallenna(hakemus: ArvioijahakemusEntity) {
        jdbc.update(
            """
            INSERT INTO yki_arvioijahakemus
                (hakemus_oid, henkilo_oid, tila, syy, arvioija_id, kauden_alkupaiva, kasitelty)
            VALUES (:hakemusOid, :henkiloOid, :tila, :syy, :arvioijaId, :kaudenAlkupaiva, :kasitelty)
            ON CONFLICT (hakemus_oid) DO UPDATE SET
                henkilo_oid = EXCLUDED.henkilo_oid,
                tila = EXCLUDED.tila,
                syy = EXCLUDED.syy,
                arvioija_id = EXCLUDED.arvioija_id,
                kauden_alkupaiva = EXCLUDED.kauden_alkupaiva,
                kasitelty = EXCLUDED.kasitelty
            """.trimIndent(),
            MapSqlParameterSource()
                .addValue("hakemusOid", hakemus.hakemusOid)
                .addValue("henkiloOid", hakemus.henkiloOid)
                .addValue("tila", hakemus.tila.name)
                .addValue("syy", hakemus.syy)
                .addValue("arvioijaId", hakemus.arvioijaId)
                .addValue("kaudenAlkupaiva", hakemus.kaudenAlkupaiva)
                .addValue("kasitelty", hakemus.kasitelty),
        )
    }

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
    fun poista(hakemusOid: String) {
        jdbc.update(
            "DELETE FROM yki_arvioijahakemus WHERE hakemus_oid = :oid",
            MapSqlParameterSource("oid", hakemusOid),
        )
    }

    fun deleteAll() {
        jdbc.update("DELETE FROM yki_arvioijahakemus", MapSqlParameterSource())
    }
}
