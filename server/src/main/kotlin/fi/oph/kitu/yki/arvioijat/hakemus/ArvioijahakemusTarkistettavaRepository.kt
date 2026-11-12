package fi.oph.kitu.yki.arvioijat.hakemus

import fi.oph.kitu.jdbc.getEnum
import fi.oph.kitu.jdbc.getLocalDateOrNull
import fi.oph.kitu.util.defaultObjectMapper
import fi.oph.kitu.yki.arvioijat.Arviointioikeusvalinta
import fi.oph.kitu.yki.arvioijat.TallennaArvioija
import io.opentelemetry.instrumentation.annotations.WithSpan
import org.springframework.jdbc.core.RowMapper
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.springframework.stereotype.Repository
import tools.jackson.module.kotlin.readValue
import java.time.OffsetDateTime

data class TarkistettavaHakemus(
    val hakemusOid: String,
    val henkiloOid: String,
    val arvioijaOid: String,
    val sukunimi: String,
    val etunimet: String,
    val hakemus: Arvioijahakemus,
    val ensimmainenHaku: OffsetDateTime,
    val viimeisinHaku: OffsetDateTime,
    val maksu: Maksutieto = Maksutieto.EI_TIEDOSSA,
) {
    companion object {
        val fromRow =
            RowMapper { rs, _ ->
                TarkistettavaHakemus(
                    hakemusOid = rs.getString("hakemus_oid"),
                    henkiloOid = rs.getString("henkilo_oid"),
                    arvioijaOid = rs.getString("arvioija_oid"),
                    sukunimi = rs.getString("sukunimi"),
                    etunimet = rs.getString("etunimet"),
                    hakemus =
                        Arvioijahakemus(
                            arviointioikeudet =
                                arviointioikeudet(defaultObjectMapper.readValue(rs.getString("arviointioikeudet"))),
                            sahkopostiosoite = rs.getString("sahkopostiosoite"),
                            katuosoite = rs.getString("katuosoite"),
                            postinumero = rs.getString("postinumero"),
                            postitoimipaikka = rs.getString("postitoimipaikka"),
                        ),
                    ensimmainenHaku = rs.getObject("ensimmainen_haku", OffsetDateTime::class.java),
                    viimeisinHaku = rs.getObject("viimeisin_haku", OffsetDateTime::class.java),
                    maksu =
                        Maksutieto(
                            tila = rs.getEnum("maksun_tila"),
                            maksettu = rs.getLocalDateOrNull("maksettu"),
                            erapaiva = rs.getLocalDateOrNull("maksun_erapaiva"),
                        ),
                )
            }

        fun valinnat(oikeudet: List<TallennaArvioija.Arviointioikeus>): List<String> =
            oikeudet.flatMap { oikeus ->
                oikeus.tasot.sorted().map { taso -> Arviointioikeusvalinta.koodaa(oikeus.kieli, taso) }
            }

        private fun arviointioikeudet(valinnat: List<String>): List<TallennaArvioija.Arviointioikeus> =
            valinnat
                .mapNotNull(Arviointioikeusvalinta::pura)
                .groupBy({ it.first }, { it.second })
                .map { (kieli, tasot) -> TallennaArvioija.Arviointioikeus(kieli, tasot.toSet()) }
    }
}

@Repository
class ArvioijahakemusTarkistettavaRepository(
    private val jdbc: NamedParameterJdbcTemplate,
) {
    @WithSpan
    fun tallenna(
        rivi: TarkistettavaHakemus,
        hetki: OffsetDateTime,
    ) {
        jdbc.update(
            """
            INSERT INTO yki_arvioijahakemus_tarkistettava
                (hakemus_oid, henkilo_oid, arvioija_oid, sukunimi, etunimet, arviointioikeudet,
                 sahkopostiosoite, katuosoite, postinumero, postitoimipaikka, ensimmainen_haku, viimeisin_haku,
                 maksun_tila, maksettu, maksun_erapaiva)
            SELECT :hakemusOid, :henkiloOid, :arvioijaOid, :sukunimi, :etunimet, :arviointioikeudet::jsonb,
                   :sahkopostiosoite, :katuosoite, :postinumero, :postitoimipaikka, :hetki, :hetki,
                   :maksunTila, :maksettu, :maksunErapaiva
            WHERE NOT EXISTS (
                SELECT 1 FROM yki_arvioijahakemus k
                WHERE k.hakemus_oid = :hakemusOid AND k.tila <> 'ODOTTAA_YKSILOINTIA'
            )
            ON CONFLICT (hakemus_oid) DO UPDATE SET
                henkilo_oid = EXCLUDED.henkilo_oid,
                arvioija_oid = EXCLUDED.arvioija_oid,
                sukunimi = EXCLUDED.sukunimi,
                etunimet = EXCLUDED.etunimet,
                arviointioikeudet = EXCLUDED.arviointioikeudet,
                sahkopostiosoite = EXCLUDED.sahkopostiosoite,
                katuosoite = EXCLUDED.katuosoite,
                postinumero = EXCLUDED.postinumero,
                postitoimipaikka = EXCLUDED.postitoimipaikka,
                viimeisin_haku = EXCLUDED.viimeisin_haku,
                maksun_tila = EXCLUDED.maksun_tila,
                maksettu = EXCLUDED.maksettu,
                maksun_erapaiva = EXCLUDED.maksun_erapaiva
            """.trimIndent(),
            MapSqlParameterSource()
                .addValue("hakemusOid", rivi.hakemusOid)
                .addValue("henkiloOid", rivi.henkiloOid)
                .addValue("arvioijaOid", rivi.arvioijaOid)
                .addValue("sukunimi", rivi.sukunimi)
                .addValue("etunimet", rivi.etunimet)
                .addValue(
                    "arviointioikeudet",
                    defaultObjectMapper.writeValueAsString(
                        TarkistettavaHakemus.valinnat(rivi.hakemus.arviointioikeudet),
                    ),
                ).addValue("sahkopostiosoite", rivi.hakemus.sahkopostiosoite)
                .addValue("katuosoite", rivi.hakemus.katuosoite)
                .addValue("postinumero", rivi.hakemus.postinumero)
                .addValue("postitoimipaikka", rivi.hakemus.postitoimipaikka)
                .addValue("maksunTila", rivi.maksu.tila.name)
                .addValue("maksettu", rivi.maksu.maksettu)
                .addValue("maksunErapaiva", rivi.maksu.erapaiva)
                .addValue("hetki", hetki),
        )
    }

    @WithSpan
    fun hae(hakemusOid: String): TarkistettavaHakemus? =
        jdbc
            .query(
                "$SELECT_NAKYVAT AND t.hakemus_oid = :oid",
                MapSqlParameterSource("oid", hakemusOid),
                TarkistettavaHakemus.fromRow,
            ).firstOrNull()

    @WithSpan
    fun haeKaikki(): List<TarkistettavaHakemus> =
        jdbc.query(
            "$SELECT_NAKYVAT ORDER BY t.ensimmainen_haku, t.hakemus_oid",
            TarkistettavaHakemus.fromRow,
        )

    @WithSpan
    fun count(): Long =
        jdbc.queryForObject(
            "SELECT count(*) FROM yki_arvioijahakemus_tarkistettava t WHERE $EI_KIRJANPITORIVIA",
            MapSqlParameterSource(),
            Long::class.java,
        ) ?: 0

    @WithSpan
    fun poista(hakemusOid: String) {
        jdbc.update(
            "DELETE FROM yki_arvioijahakemus_tarkistettava WHERE hakemus_oid = :oid",
            MapSqlParameterSource("oid", hakemusOid),
        )
    }

    @WithSpan
    fun poistaKirjatut() {
        jdbc.update(
            """
            DELETE FROM yki_arvioijahakemus_tarkistettava t
            WHERE EXISTS (SELECT 1 FROM yki_arvioijahakemus k WHERE k.hakemus_oid = t.hakemus_oid)
            """.trimIndent(),
            MapSqlParameterSource(),
        )
    }

    @WithSpan
    fun poistaVanhentuneet(listatut: Collection<String>) {
        if (listatut.isEmpty()) return
        jdbc.jdbcOperations.update({ connection ->
            connection
                .prepareStatement("DELETE FROM yki_arvioijahakemus_tarkistettava WHERE hakemus_oid <> ALL (?)")
                .apply { setArray(1, connection.createArrayOf("text", listatut.toTypedArray())) }
        })
    }

    companion object {
        private const val EI_KIRJANPITORIVIA =
            "NOT EXISTS (SELECT 1 FROM yki_arvioijahakemus k WHERE k.hakemus_oid = t.hakemus_oid)"

        private const val SELECT_NAKYVAT =
            """
            SELECT t.*
            FROM yki_arvioijahakemus_tarkistettava t
            WHERE $EI_KIRJANPITORIVIA
            """
    }
}
