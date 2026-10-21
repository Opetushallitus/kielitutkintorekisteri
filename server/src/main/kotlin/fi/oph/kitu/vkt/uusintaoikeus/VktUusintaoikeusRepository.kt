package fi.oph.kitu.vkt.uusintaoikeus

import io.opentelemetry.instrumentation.annotations.WithSpan
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.springframework.stereotype.Repository
import java.time.LocalDate

@Repository
class VktUusintaoikeusRepository(
    private val jdbcNamedParameterTemplate: NamedParameterJdbcTemplate,
) {
    @WithSpan
    fun findUusintaanOikeutetut(tarkastelupaiva: LocalDate): List<VktUusintaoikeus> {
        val query =
            """
            WITH
            -- KIOS lähettää muuttuneen ilmoittautumisen kokonaan uutena versiona: vain kunkin
            -- ilmoittautumisen viimeisin versio on voimassa.
            voimassa_olevat_suoritukset AS (
                SELECT DISTINCT ON (ilmoittautumisen_id) *
                FROM vkt_suoritus
                ORDER BY ilmoittautumisen_id, created_at DESC
            ),
            -- Vain arvioidut osakokeet: arvioimaton osakoe (esim. uusintaan ilmoittautuminen, jota ei
            -- ole vielä arvioitu) ei synnytä eikä kumoa uusintaoikeutta.
            osakokeet AS (
                SELECT
                    s.suorittajan_oid,
                    s.tutkintokieli,
                    s.taitotaso,
                    o.tyyppi,
                    o.tutkintopaiva,
                    o.arviointipaiva,
                    o.arvosana
                FROM voimassa_olevat_suoritukset s
                JOIN vkt_osakoe o ON o.suoritus_id = s.id
                WHERE o.arvosana IS NOT NULL
                  AND o.arviointipaiva IS NOT NULL
            ),
            -- Tutkinnot ja niiden osakoeparit (vrt. VktTutkinto.mahdollisetOsakokeidenTyypit).
            -- Ymmärtämisen osakokeet kuuluvat kahteen tutkintoon, joten sama osakoe voi olla
            -- uusittavissa kahden tutkinnon kautta.
            osakoeparit (tutkinto, uusittava, toinen) AS (
                VALUES
                    ('Kirjallinen'::vkt_tutkinnon_tyyppi, 'Kirjoittaminen'::vkt_osakokeen_tyyppi, 'TekstinYmmärtäminen'::vkt_osakokeen_tyyppi),
                    ('Kirjallinen', 'TekstinYmmärtäminen', 'Kirjoittaminen'),
                    ('Suullinen', 'Puhuminen', 'PuheenYmmärtäminen'),
                    ('Suullinen', 'PuheenYmmärtäminen', 'Puhuminen'),
                    ('Ymmärtäminen', 'TekstinYmmärtäminen', 'PuheenYmmärtäminen'),
                    ('Ymmärtäminen', 'PuheenYmmärtäminen', 'TekstinYmmärtäminen')
            ),
            -- Ehto 1: Tietyn tutkinnon toinen osakoe on hyväksytty.
            -- Ehto 6: Kieli ja taitotaso ovat molemmissa osakokeissa samat – tästä eteenpäin
            -- osakokeita verrataan vain saman henkilön, kielen ja taitotason sisällä.
            -- Toisen osakokeen viimeisin hyväksytty suoritus määrää tutkintokerran, johon oikeus liittyy.
            hyvaksytyt_toiset AS (
                SELECT DISTINCT ON (o.suorittajan_oid, o.tutkintokieli, o.taitotaso, p.tutkinto, p.uusittava)
                    o.suorittajan_oid,
                    o.tutkintokieli,
                    o.taitotaso,
                    p.tutkinto,
                    p.uusittava,
                    o.tutkintopaiva AS hyvaksytyn_tutkintopaiva
                FROM osakokeet o
                JOIN osakoeparit p ON p.toinen = o.tyyppi
                WHERE o.arvosana <> 'Hylätty'
                ORDER BY o.suorittajan_oid, o.tutkintokieli, o.taitotaso, p.tutkinto, p.uusittava, o.tutkintopaiva DESC
            ),
            -- Uusittavan osakokeen suoritus samalta tutkintokerralta: ajallisesti lähimpänä toisen
            -- osakokeen hyväksyttyä suoritusta. Myöhemmät suoritukset ovat uusintoja.
            saman_kerran_suoritukset AS (
                SELECT DISTINCT ON (h.suorittajan_oid, h.tutkintokieli, h.taitotaso, h.tutkinto, h.uusittava)
                    h.*,
                    o.tutkintopaiva,
                    o.arviointipaiva,
                    o.arvosana
                FROM hyvaksytyt_toiset h
                JOIN osakokeet o
                    ON o.suorittajan_oid = h.suorittajan_oid
                    AND o.tutkintokieli = h.tutkintokieli
                    AND o.taitotaso = h.taitotaso
                    AND o.tyyppi = h.uusittava
                ORDER BY
                    h.suorittajan_oid, h.tutkintokieli, h.taitotaso, h.tutkinto, h.uusittava,
                    abs(o.tutkintopaiva - h.hyvaksytyn_tutkintopaiva),
                    o.tutkintopaiva
            ),
            uusintaoikeudet AS (
                SELECT
                    k.*,
                    LEAST((k.arviointipaiva + interval '3 years')::date, :viimeinen_paiva) AS oikeus_paattyy
                FROM saman_kerran_suoritukset k
                WHERE
                    -- Ehto 2: Saman tutkinnon toinen osakoe on hylätty. Tämä ensimmäinen hylkäys on se
                    -- hylkäämispäätös, josta kolme vuotta lasketaan; hylätyt uusinnat eivät pidennä oikeutta.
                    k.arvosana = 'Hylätty'
                    -- Osakokeet ovat samaa tutkintoa vain, jos ne on suoritettu enintään kolmen vuoden
                    -- päässä toisistaan.
                    AND k.tutkintopaiva BETWEEN k.hyvaksytyn_tutkintopaiva - interval '3 years'
                                            AND k.hyvaksytyn_tutkintopaiva + interval '3 years'
                    -- Ehto 3: Hylättyä osakoetta ei ole tämän jälkeen suoritettu hyväksytysti.
                    AND NOT EXISTS (
                        SELECT 1
                        FROM osakokeet myohempi
                        WHERE myohempi.suorittajan_oid = k.suorittajan_oid
                          AND myohempi.tutkintokieli = k.tutkintokieli
                          AND myohempi.taitotaso = k.taitotaso
                          AND myohempi.tyyppi = k.uusittava
                          AND myohempi.arvosana <> 'Hylätty'
                          AND myohempi.tutkintopaiva > k.tutkintopaiva
                    )
                    -- Ehto 4: Hylkäämispäätöksestä ei ole tarkastelupäivänä kulunut yli kolmea vuotta.
                    AND k.arviointipaiva + interval '3 years' >= :tarkastelupaiva
                    -- Ehto 5: Tarkastelupäivä ei ole myöhempi kuin 31.12.2028.
                    AND :tarkastelupaiva <= :viimeinen_paiva
            ),
            -- Nimet henkilön viimeisimmästä suorituksesta.
            nimet AS (
                SELECT DISTINCT ON (suorittajan_oid, tutkintokieli, taitotaso)
                    suorittajan_oid,
                    tutkintokieli,
                    taitotaso,
                    sukunimi,
                    etunimet
                FROM vkt_suoritus
                ORDER BY suorittajan_oid, tutkintokieli, taitotaso, created_at DESC
            )
            -- Henkilöiden ja uusittavien osakokeiden määrä erotetaan: rivi on uusittava osakoe, ja sama
            -- osakoe on yksi rivi, vaikka se olisi uusittavissa kahden tutkinnon kautta. Silloin oikeus on
            -- voimassa myöhemmän hylkäämispäätöksen mukaan.
            SELECT
                u.suorittajan_oid,
                n.sukunimi,
                n.etunimet,
                u.tutkintokieli,
                u.taitotaso,
                u.uusittava AS osakoe,
                array_agg(u.tutkinto::text ORDER BY u.tutkinto) AS tutkinnot,
                max(u.arviointipaiva) AS hylkaamispaatos,
                max(u.oikeus_paattyy) AS oikeus_paattyy
            FROM uusintaoikeudet u
            JOIN nimet n
                ON n.suorittajan_oid = u.suorittajan_oid
                AND n.tutkintokieli = u.tutkintokieli
                AND n.taitotaso = u.taitotaso
            GROUP BY u.suorittajan_oid, n.sukunimi, n.etunimet, u.tutkintokieli, u.taitotaso, u.uusittava
            """.trimIndent()

        val params =
            mapOf(
                "tarkastelupaiva" to tarkastelupaiva,
                "viimeinen_paiva" to UUSINTAOIKEUDEN_VIIMEINEN_PAIVA,
            )

        return jdbcNamedParameterTemplate.query(query, params, VktUusintaoikeus.fromRow)
    }

    companion object {
        val UUSINTAOIKEUDEN_VIIMEINEN_PAIVA: LocalDate = LocalDate.of(2028, 12, 31)
    }
}
