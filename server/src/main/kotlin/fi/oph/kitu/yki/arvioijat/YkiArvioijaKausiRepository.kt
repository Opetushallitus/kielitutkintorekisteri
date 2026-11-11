package fi.oph.kitu.yki.arvioijat

import fi.oph.kitu.oid.Oid
import fi.oph.kitu.yki.Tutkintokieli
import fi.oph.kitu.yki.Tutkintotaso
import io.opentelemetry.instrumentation.annotations.WithSpan
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.springframework.stereotype.Repository
import org.springframework.transaction.annotation.Transactional
import java.sql.PreparedStatement
import java.time.LocalDate

data class Kausioikeus(
    val kieli: Tutkintokieli,
    val tasot: Set<Tutkintotaso>,
)

@Repository
class YkiArvioijaKausiRepository(
    private val jdbcTemplate: JdbcTemplate,
    private val namedJdbcTemplate: NamedParameterJdbcTemplate,
) {
    @WithSpan
    fun findKaudet(arvioijaId: Int): List<YkiArviointikausiEntity> {
        val kaudet =
            jdbcTemplate.query(
                """
                SELECT * FROM yki_arvioija_arviointikausi
                WHERE arvioija_id = ?
                ORDER BY alkupaiva DESC, id DESC
                """.trimIndent(),
                YkiArviointikausiEntity.fromRow,
                arvioijaId,
            )
        if (kaudet.isEmpty()) return kaudet

        val oikeudet = findOikeudet(kaudet.mapNotNull { it.id?.toInt() }).groupBy { it.kausiId?.toInt() }
        return kaudet.map { kausi -> kausi.copy(oikeudet = oikeudet[kausi.id?.toInt()].orEmpty()) }
    }

    @WithSpan
    fun findKausi(kausiId: Int): YkiArviointikausiEntity? =
        jdbcTemplate
            .query(
                "SELECT * FROM yki_arvioija_arviointikausi WHERE id = ?",
                YkiArviointikausiEntity.fromRow,
                kausiId,
            ).firstOrNull()
            ?.let { kausi -> kausi.copy(oikeudet = findOikeudet(listOf(kausiId))) }

    private fun findOikeudet(kausiIdt: List<Int>): List<YkiArviointikausiOikeusEntity> {
        if (kausiIdt.isEmpty()) return emptyList()
        return jdbcTemplate.query({ connection ->
            connection
                .prepareStatement(
                    """
                    SELECT * FROM yki_arvioija_arviointikausi_oikeus
                    WHERE kausi_id = ANY (?)
                    ORDER BY kieli
                    """.trimIndent(),
                ).apply { setArray(1, connection.createArrayOf("integer", kausiIdt.toTypedArray())) }
        }, YkiArviointikausiOikeusEntity.fromRow)
    }

    @WithSpan
    @Transactional
    fun lisaaKausi(
        arvioijaId: Int,
        alkupaiva: LocalDate,
        paattymispaiva: LocalDate?,
        oikeudet: List<Kausioikeus>,
        ashaNumero: String?,
        tekija: Oid?,
    ): Int {
        val kausiId =
            jdbcTemplate.queryForObject(
                """
                INSERT INTO yki_arvioija_arviointikausi
                    (arvioija_id, alkupaiva, paattymispaiva, asha_numero, luoja_oid, muokkaaja_oid)
                VALUES (?, ?, ?, ?, ?, ?)
                RETURNING id
                """.trimIndent(),
                Int::class.java,
                arvioijaId,
                alkupaiva,
                paattymispaiva,
                ashaNumero,
                tekija?.toString(),
                tekija?.toString(),
            )!!

        kirjoitaOikeudet(kausiId, oikeudet)
        return kausiId
    }

    @WithSpan
    @Transactional
    fun paivitaKausi(
        kausiId: Int,
        alkupaiva: LocalDate,
        paattymispaiva: LocalDate?,
        oikeudet: List<Kausioikeus>,
        ashaNumero: String?,
        tekija: Oid?,
    ) {
        jdbcTemplate.update(
            """
            UPDATE yki_arvioija_arviointikausi
            SET alkupaiva = ?, paattymispaiva = ?, asha_numero = ?, muokattu = now(), muokkaaja_oid = ?
            WHERE id = ?
            """.trimIndent(),
            alkupaiva,
            paattymispaiva,
            ashaNumero,
            tekija?.toString(),
            kausiId,
        )

        jdbcTemplate.update("DELETE FROM yki_arvioija_arviointikausi_oikeus WHERE kausi_id = ?", kausiId)
        kirjoitaOikeudet(kausiId, oikeudet)
    }

    /**
     * Passivointi vain kiristaa kautta: ilman LEASTia klikkaus pidentaisi jo paattynytta kautta.
     * Alkupaivaan ei kosketa, jotta katkaisu vastaa tasmalleen projektioon kirjoitettavaa —
     * muuten tallennan kausisynkronointi ei osu tahan riviin ja lisaisi haamukauden.
     */
    @WithSpan
    @Transactional
    fun passivoiKausi(
        kausiId: Int,
        tanaan: LocalDate,
        tekija: Oid?,
    ) {
        jdbcTemplate.update(
            """
            UPDATE yki_arvioija_arviointikausi
            SET paattymispaiva = LEAST(COALESCE(paattymispaiva, ?), ?),
                passivoitu = COALESCE(passivoitu, now()),
                passivoija_oid = COALESCE(passivoija_oid, ?),
                muokattu = now(),
                muokkaaja_oid = ?
            WHERE id = ?
            """.trimIndent(),
            tanaan,
            tanaan,
            tekija?.toString(),
            tekija?.toString(),
            kausiId,
        )
    }

    @WithSpan
    @Transactional
    fun poistaKausi(kausiId: Int) {
        jdbcTemplate.update("DELETE FROM yki_arvioija_arviointikausi WHERE id = ?", kausiId)
    }

    @WithSpan
    fun findMuutosloki(arvioijaId: Int): List<YkiArvioijaKausiEntity> =
        jdbcTemplate.query(
            "SELECT * FROM yki_arvioija_kausi WHERE arvioija_id = ? ORDER BY kirjattu DESC, id DESC",
            YkiArvioijaKausiEntity.fromRow,
            arvioijaId,
        )

    @WithSpan
    fun kirjaaMuutos(
        arvioijaId: Int,
        kausiId: Int?,
        toimenpide: Kausitoimenpide,
        kausi: YkiArviointikausiEntity,
        jatkorekisterointi: Boolean,
        tekija: Oid?,
    ) {
        if (kausi.oikeudet.isEmpty()) return
        jdbcTemplate.batchUpdate(
            """
            INSERT INTO yki_arvioija_kausi
                (arvioija_id, kieli, tasot, tila, kauden_alkupaiva, kauden_paattymispaiva,
                 jatkorekisterointi, asha_numero, toimenpide, kausi_id, kirjaaja_oid)
            VALUES (?, ?, ?, NULL, ?, ?, ?, ?, ?, ?, ?)
            """.trimIndent(),
            kausi.oikeudet,
            kausi.oikeudet.size,
        ) { ps: PreparedStatement, oikeus: YkiArviointikausiOikeusEntity ->
            ps.setInt(1, arvioijaId)
            ps.setString(2, oikeus.kieli.toString())
            ps.setArray(3, ps.connection.createArrayOf("text", oikeus.tasot.normalisoidutTasot()))
            ps.setObject(4, kausi.alkupaiva)
            ps.setObject(5, kausi.paattymispaiva)
            ps.setBoolean(6, jatkorekisterointi)
            ps.setString(7, kausi.ashaNumero)
            ps.setString(8, toimenpide.name)
            ps.setObject(9, kausiId)
            ps.setString(10, tekija?.toString())
        }
    }

    /**
     * Kirjoittaa arvioijan arviointioikeudet uusiksi kausista. Palauttaa `true` vain jos jokin
     * arvo tosiasiassa muuttui: muuten yollinen ajo leimaisi koko rekisterin muokatuksi ja
     * lahettaisi sen uudelleen Solkiin.
     *
     * Arvioija jolla ei ole yhtaan kautta jaa koskematta. Nain siirtymavaiheen Solki-data ja
     * historiadata jolta alkupaiva puuttuu sailyvat.
     */
    @WithSpan
    @Transactional
    fun paivitaProjektio(
        arvioijaId: Int,
        tanaan: LocalDate,
    ): Boolean {
        val kaudet = findKaudet(arvioijaId)
        if (kaudet.isEmpty()) return false

        val nykyiset = findArviointioikeudet(arvioijaId)
        val tavoite =
            Kausiprojektio.projisoi(kaudet, findEnsimmainenRekisterointipaiva(arvioijaId), nykyiset, tanaan)
        if (tavoite.isEmpty() || !Kausiprojektio.onMuuttunut(nykyiset, tavoite)) return false

        val sailytettavat = (tavoite + Kausiprojektio.sailytettavat(nykyiset, tavoite)).map { it.kieli }
        poistaYlimaaraiset(arvioijaId, sailytettavat)
        upsertProjektio(arvioijaId, tavoite)
        return true
    }

    @WithSpan
    fun findArviointioikeudet(arvioijaId: Int): List<YkiArviointioikeusEntity> =
        jdbcTemplate.query(
            "SELECT * FROM yki_arviointioikeus WHERE arvioija_id = ? ORDER BY kieli",
            YkiArviointioikeusEntity.fromRow,
            arvioijaId,
        )

    @WithSpan
    fun findArvioijaIdt(): List<Int> =
        jdbcTemplate
            .queryForList(
                "SELECT DISTINCT arvioija_id FROM yki_arvioija_arviointikausi ORDER BY arvioija_id",
                Int::class.java,
            ).filterNotNull()

    private fun findEnsimmainenRekisterointipaiva(arvioijaId: Int): LocalDate? =
        jdbcTemplate
            .queryForList(
                "SELECT arvioijan_ensimmainen_rekisterointipaiva FROM yki_arvioija WHERE id = ?",
                LocalDate::class.java,
                arvioijaId,
            ).firstOrNull()

    /** Siirtaa arvioijan ensimmaista rekisterointipaivaa vain aikaisemmaksi, jottei tuotu arvo katoa. */
    @WithSpan
    fun paivitaEnsimmainenRekisterointipaiva(arvioijaId: Int) {
        jdbcTemplate.update(
            """
            UPDATE yki_arvioija
            SET arvioijan_ensimmainen_rekisterointipaiva = vanhin.alkupaiva
            FROM (SELECT min(alkupaiva) AS alkupaiva
                  FROM yki_arvioija_arviointikausi
                  WHERE arvioija_id = ?) vanhin
            WHERE yki_arvioija.id = ?
              AND vanhin.alkupaiva IS NOT NULL
              AND (yki_arvioija.arvioijan_ensimmainen_rekisterointipaiva IS NULL
                   OR yki_arvioija.arvioijan_ensimmainen_rekisterointipaiva > vanhin.alkupaiva)
            """.trimIndent(),
            arvioijaId,
            arvioijaId,
        )
    }

    /**
     * Lisayslomake perustaa arvioijan ja hanen ensimmaisen kautensa yhdella tallennuksella, joten
     * hallintopaatoksen viite kirjataan kaudelle vasta sen synnyttya. Projektio rakennetaan aina,
     * myos ilman viitetta: tallenna ei kirjoita asha_numeroa, joten ilman tata jatkokausi jattaisi
     * projektioon edellisen kauden viitteen.
     */
    @WithSpan
    @Transactional
    fun viimeisteleKausi(
        arvioijaId: Int,
        alkupaiva: LocalDate,
        ashaNumero: String?,
        tanaan: LocalDate,
    ) {
        if (ashaNumero != null) {
            jdbcTemplate.update(
                """
                UPDATE yki_arvioija_arviointikausi
                SET asha_numero = ?, muokattu = muokattu
                WHERE arvioija_id = ? AND alkupaiva = ?
                """.trimIndent(),
                ashaNumero,
                arvioijaId,
                alkupaiva,
            )
        }
        paivitaProjektio(arvioijaId, tanaan)
    }

    /** Arvioijatason passivointi paattaa kaikki viela voimassa olevat kaudet, vain kiristaen. */
    @WithSpan
    @Transactional
    fun passivoiKaudet(
        arvioijaId: Int,
        tanaan: LocalDate,
        tekija: Oid?,
    ) {
        jdbcTemplate.update(
            """
            UPDATE yki_arvioija_arviointikausi
            SET paattymispaiva = LEAST(COALESCE(paattymispaiva, ?), ?),
                passivoitu = COALESCE(passivoitu, now()),
                passivoija_oid = COALESCE(passivoija_oid, ?),
                muokattu = now(),
                muokkaaja_oid = ?
            WHERE arvioija_id = ?
              AND (paattymispaiva IS NULL OR paattymispaiva >= ?)
            """.trimIndent(),
            tanaan,
            tanaan,
            tekija?.toString(),
            tekija?.toString(),
            arvioijaId,
            tanaan,
        )
    }

    /**
     * Sailytysajan alkuhetki nollataan kun merkinnalla on voimassa oleva tai tuleva kausi: uusi
     * kausi on uusi rekisterointi, joka kumoaa aiemman passivoinnin. Tulevakin kausi kumoaa, koska
     * muuten kauden paatyttya sailytysaika laskettaisiin vanhasta leimasta ja merkinta poistuisi heti.
     *
     * Ehto luetaan masterista ja ohittaa passivoidut kaudet. Tanaan katkaistu kausi on
     * paattymispaivan inklusiivisuuden takia viela tanaan AKTIIVINEN, joten projektiosta luettuna
     * mika tahansa saman paivan muokkaus pyyhkisi juuri asetetun leiman.
     */
    @WithSpan
    fun poistaPassivointileimaJosKausiVoimassaTaiTulossa(
        arvioijaId: Int,
        tanaan: LocalDate,
    ) {
        namedJdbcTemplate.update(
            """
            UPDATE yki_arvioija
            SET passivoitu = NULL
            WHERE id = :arvioijaId
              AND EXISTS (
                  SELECT 1 FROM yki_arvioija_arviointikausi kausi
                  WHERE kausi.arvioija_id = yki_arvioija.id
                    AND kausi.passivoitu IS NULL
                    AND (kausi.paattymispaiva IS NULL OR kausi.paattymispaiva >= :tanaan)
              )
            """.trimIndent(),
            mapOf("arvioijaId" to arvioijaId, "tanaan" to tanaan),
        )
    }

    /** Kausimuutos on lahetettava Solkiin, joten rivi palautetaan lahetysjonoon. */
    @WithSpan
    fun merkitseMuuttuneeksi(
        arvioijaId: Int,
        tekija: Oid?,
    ) {
        jdbcTemplate.update(
            """
            UPDATE yki_arvioija
            SET muokattu = now(),
                muokkaaja_oid = ?,
                solkiin_lahetetty = NULL,
                solki_lahetysvirhe = NULL,
                solki_lahetysyritykset = 0
            WHERE id = ?
            """.trimIndent(),
            tekija?.toString(),
            arvioijaId,
        )
    }

    private fun poistaYlimaaraiset(
        arvioijaId: Int,
        sailytettavat: List<Tutkintokieli>,
    ) {
        jdbcTemplate.update({ connection ->
            connection
                .prepareStatement(
                    """
                    DELETE FROM yki_arviointioikeus
                    WHERE arvioija_id = ? AND kieli::text <> ALL (?)
                    """.trimIndent(),
                ).apply {
                    setInt(1, arvioijaId)
                    setArray(2, connection.createArrayOf("text", sailytettavat.map { it.name }.toTypedArray()))
                }
        })
    }

    private fun upsertProjektio(
        arvioijaId: Int,
        tavoite: List<YkiArviointioikeusEntity>,
    ) {
        if (tavoite.isEmpty()) return
        jdbcTemplate.batchUpdate(
            """
            INSERT INTO yki_arviointioikeus
                (arvioija_id, kieli, tasot, tila, kauden_alkupaiva, kauden_paattymispaiva,
                 jatkorekisterointi, ensimmainen_rekisterointipaiva, asha_numero)
            VALUES (?, ?, ?, NULL, ?, ?, ?, ?, ?)
            ON CONFLICT ON CONSTRAINT yki_arviointioikeus_unique_arvioija_kieli DO UPDATE SET
                tasot = EXCLUDED.tasot,
                tila = NULL,
                kauden_alkupaiva = EXCLUDED.kauden_alkupaiva,
                kauden_paattymispaiva = EXCLUDED.kauden_paattymispaiva,
                jatkorekisterointi = EXCLUDED.jatkorekisterointi,
                ensimmainen_rekisterointipaiva = EXCLUDED.ensimmainen_rekisterointipaiva,
                asha_numero = EXCLUDED.asha_numero
            """.trimIndent(),
            tavoite,
            tavoite.size,
        ) { ps: PreparedStatement, oikeus: YkiArviointioikeusEntity ->
            ps.setInt(1, arvioijaId)
            ps.setString(2, oikeus.kieli.toString())
            ps.setArray(3, ps.connection.createArrayOf("text", oikeus.tasot.normalisoidutTasot()))
            ps.setObject(4, oikeus.kaudenAlkupaiva)
            ps.setObject(5, oikeus.kaudenPaattymispaiva)
            ps.setBoolean(6, oikeus.jatkorekisterointi)
            ps.setObject(7, oikeus.ensimmainenRekisterointipaiva)
            ps.setString(8, oikeus.ashaNumero)
        }
    }

    /**
     * Synkronoi kaudet arviointioikeuksista. Kaytossa vain [Tallennuslahde.KITU]-polulla: Solkin
     * payload on kielikohtainen ja sen paivat voivat erota kielittain, joten siita syntyisi
     * paallekkaisia konekirjattuja kausia.
     */
    @WithSpan
    @Transactional
    fun synkronoiKaudet(
        arvioijaId: Int,
        arviointioikeudet: List<YkiArviointioikeusEntity>,
        tekija: Oid?,
    ) {
        // Ryhmittely alkupaivaan, ei paivapariin: sama alkupaiva tarkoittaa samaa kautta, ja
        // kaksi kautta samalla alkupaivalla olisi paallekkainen — sellaisesta projektio valitsisi
        // toisen ja poistaisi toisen kielet. Sama saanto kuin V122:n siirrossa.
        arviointioikeudet
            .filterNot { it.kieli.isLegacy() }
            .filter { it.kaudenAlkupaiva != null }
            .groupBy { it.kaudenAlkupaiva!! }
            .forEach { (alkupaiva, oikeudet) ->
                val paattymispaiva =
                    if (oikeudet.any { it.kaudenPaattymispaiva == null }) {
                        null
                    } else {
                        oikeudet.maxOf { it.kaudenPaattymispaiva!! }
                    }
                val kausiId = upsertKausi(arvioijaId, alkupaiva, paattymispaiva, tekija)
                jdbcTemplate.update(
                    "DELETE FROM yki_arvioija_arviointikausi_oikeus WHERE kausi_id = ?",
                    kausiId,
                )
                kirjoitaOikeudet(kausiId, oikeudet.map { Kausioikeus(it.kieli, it.tasot) })
            }
        paivitaEnsimmainenRekisterointipaiva(arvioijaId)
    }

    private fun upsertKausi(
        arvioijaId: Int,
        alkupaiva: LocalDate,
        paattymispaiva: LocalDate?,
        tekija: Oid?,
    ): Int {
        val olemassa =
            jdbcTemplate
                .queryForList(
                    "SELECT id FROM yki_arvioija_arviointikausi WHERE arvioija_id = ? AND alkupaiva = ? ORDER BY id",
                    Int::class.java,
                    arvioijaId,
                    alkupaiva,
                ).firstOrNull()
        if (olemassa != null) {
            jdbcTemplate.update(
                """
                UPDATE yki_arvioija_arviointikausi
                SET paattymispaiva = ?, muokattu = now(), muokkaaja_oid = ?
                WHERE id = ? AND passivoitu IS NULL AND paattymispaiva IS DISTINCT FROM ?
                """.trimIndent(),
                paattymispaiva,
                tekija?.toString(),
                olemassa,
                paattymispaiva,
            )
            return olemassa
        }
        return jdbcTemplate.queryForObject(
            """
            INSERT INTO yki_arvioija_arviointikausi
                (arvioija_id, alkupaiva, paattymispaiva, luoja_oid, muokkaaja_oid)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT ON CONSTRAINT yki_arvioija_arviointikausi_unique DO UPDATE
                SET muokattu = yki_arvioija_arviointikausi.muokattu
            RETURNING id
            """.trimIndent(),
            Int::class.java,
            arvioijaId,
            alkupaiva,
            paattymispaiva,
            tekija?.toString(),
            tekija?.toString(),
        )!!
    }

    /**
     * Kirjaa lokiin vain ne kielet joiden kausi tosiasiassa muuttui: pelkka yhteystiedon korjaus
     * ei saa kasvattaa muutoshistoriaa.
     */
    @WithSpan
    fun kirjaaMuuttuneet(
        arvioijaId: Int,
        ennen: List<YkiArviointioikeusEntity>,
        jalkeen: List<YkiArviointioikeusEntity>,
        tekija: Oid?,
    ) {
        val aiemmat = ennen.associateBy { it.kieli }
        val muuttuneet = jalkeen.filter { uusi -> aiemmat[uusi.kieli]?.let { !samaKausi(it, uusi) } ?: true }
        if (muuttuneet.isEmpty()) return

        jdbcTemplate.batchUpdate(
            """
            INSERT INTO yki_arvioija_kausi
                (arvioija_id, kieli, tasot, tila, kauden_alkupaiva, kauden_paattymispaiva,
                 jatkorekisterointi, toimenpide, kirjaaja_oid)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """.trimIndent(),
            muuttuneet,
            muuttuneet.size,
        ) { ps: PreparedStatement, oikeus: YkiArviointioikeusEntity ->
            ps.setInt(1, arvioijaId)
            ps.setString(2, oikeus.kieli.toString())
            ps.setArray(3, ps.connection.createArrayOf("text", oikeus.tasot.normalisoidutTasot()))
            ps.setString(4, oikeus.tila?.toString())
            ps.setObject(5, oikeus.kaudenAlkupaiva)
            ps.setObject(6, oikeus.kaudenPaattymispaiva)
            ps.setBoolean(7, oikeus.jatkorekisterointi)
            ps.setString(8, Kausitoimenpide.TALLENNUS.name)
            ps.setString(9, tekija?.toString())
        }
    }

    private fun samaKausi(
        a: YkiArviointioikeusEntity,
        b: YkiArviointioikeusEntity,
    ): Boolean =
        a.tasot == b.tasot &&
            a.tila == b.tila &&
            a.kaudenAlkupaiva == b.kaudenAlkupaiva &&
            a.kaudenPaattymispaiva == b.kaudenPaattymispaiva &&
            a.jatkorekisterointi == b.jatkorekisterointi

    private fun kirjoitaOikeudet(
        kausiId: Int,
        oikeudet: List<Kausioikeus>,
    ) {
        if (oikeudet.isEmpty()) return
        jdbcTemplate.batchUpdate(
            """
            INSERT INTO yki_arvioija_arviointikausi_oikeus (kausi_id, kieli, tasot)
            VALUES (?, ?, ?)
            """.trimIndent(),
            oikeudet,
            oikeudet.size,
        ) { ps: PreparedStatement, oikeus: Kausioikeus ->
            ps.setInt(1, kausiId)
            ps.setString(2, oikeus.kieli.toString())
            ps.setArray(3, ps.connection.createArrayOf("text", oikeus.tasot.normalisoidutTasot()))
        }
    }
}

internal fun Set<Tutkintotaso>.normalisoidutTasot(): Array<String> = map { it.name }.sorted().toTypedArray()
