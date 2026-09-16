package fi.oph.kitu.yki.arvioijat

import fi.oph.kitu.jdbc.SortDirection
import fi.oph.kitu.jdbc.orderSql
import fi.oph.kitu.jdbc.pageSql
import fi.oph.kitu.oid.Oid
import fi.oph.kitu.oid.getOid
import fi.oph.kitu.yki.Tutkintokieli
import fi.oph.kitu.yki.Tutkintotaso
import io.opentelemetry.instrumentation.annotations.WithSpan
import org.springframework.dao.OptimisticLockingFailureException
import org.springframework.data.repository.CrudRepository
import org.springframework.data.repository.PagingAndSortingRepository
import org.springframework.jdbc.core.BatchPreparedStatementSetter
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.core.RowMapper
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.springframework.stereotype.Repository
import org.springframework.transaction.annotation.Transactional
import java.sql.PreparedStatement
import java.time.LocalDate
import java.time.OffsetDateTime

interface CustomYkiArvioijaRepository {
    fun saveAllNewEntities(arvioijat: Iterable<YkiArvioijaEntity>): List<Int>

    /**
     * @param odotettuMuokkaushetki optimistinen lukitus: kun annettu, paivitys tehdaan vain jos
     *   rivin `muokattu` on yha tama. Muuten heittaa [OptimisticLockingFailureException]in.
     */
    fun tallenna(
        arvioija: YkiArvioijaEntity,
        tekija: Oid? = null,
        lahde: Tallennuslahde = Tallennuslahde.KITU,
        odotettuMuokkaushetki: OffsetDateTime? = null,
    ): Int

    fun findByArvioijaOid(arvioijaOid: Oid): YkiArvioijaEntity?

    fun findArvioijaById(id: Int): YkiArvioijaEntity?

    fun findKausihistoria(arvioijaId: Int): List<YkiArvioijaKausiEntity>

    fun findForListView(
        params: YkiArvioijaParams,
        tanaan: LocalDate,
    ): List<YkiArvioijaListRow>

    fun countForListView(
        params: YkiArvioijaParams,
        tanaan: LocalDate,
    ): Int

    /**
     * Solki-lahetysjono: rivit joita ei ole lahetetty muokkauksen jalkeen. [maxYritykset] rajaa
     * nopeisiin uusintoihin; null poimii kaikki, jolloin yollinen ajo yrittaa myos pitkaan
     * epaonnistuneet uudelleen.
     */
    fun findLahetettavat(
        maxYritykset: Int? = null,
        limit: Int = LAHETYSERAN_KOKO,
    ): List<YkiArvioijaEntity>

    fun countSolkiLahetysvirheet(): Long

    /**
     * @param lahetettyVersio rivin `muokattu` lahetyshetkella. Jos rivia on muokattu sen jalkeen,
     *   leimaa ei anneta: muuten samanaikainen muokkaus putoaisi jonosta lahettamatta.
     * @param solkiTunnus Solkin palauttama arvioijatunnus. Tyhja ei nollaa jo tallennettua arvoa.
     */
    fun merkitseLahetetyksi(
        id: Int,
        lahetettyVersio: OffsetDateTime?,
        solkiTunnus: String? = null,
    )

    fun merkitseLahetysvirhe(
        id: Int,
        virhe: String,
    )

    /**
     * Poistaa merkinnat joiden sailytysaika on umpeutunut. Palauttaa poistetut auditlokia varten.
     * [yki_arviointioikeus] ja [yki_arvioija_kausi] poistuvat ON DELETE CASCADE -saannolla.
     */
    fun poistaSailytysajanYlittaneet(
        tanaan: LocalDate,
        raja: LocalDate,
    ): List<Oid>

    fun allArviontioikeudet(
        orderBy: YkiArvioijaColumn = YkiArvioijaColumn.Sukunimi,
        orderByDirection: SortDirection = SortDirection.ASC,
    ): List<YkiArvioijaArviointioikeus>
}

@Repository
class CustomYkiArvioijaRepositoryImpl(
    val jdbcTemplate: JdbcTemplate,
    val namedJdbcTemplate: NamedParameterJdbcTemplate,
    val kausiRepository: YkiArvioijaKausiRepository,
) : CustomYkiArvioijaRepository {
    companion object {
        /**
         * arvioija_id valitaan eksplisiittisesti, koska molemmissa tauluissa on id-sarake
         * eika SELECT * kertoisi kumpi voittaa.
         */
        private val LIST_VIEW_SELECT =
            """
            SELECT yki_arvioija.*,
                   yki_arvioija.id AS arvioija_id,
                   yki_arviointioikeus.kieli,
                   yki_arviointioikeus.tasot,
                   ${Rekisterointitila.SQL} AS tila,
                   yki_arviointioikeus.kauden_alkupaiva,
                   yki_arviointioikeus.kauden_paattymispaiva,
                   yki_arviointioikeus.jatkorekisterointi,
                   yki_arviointioikeus.ensimmainen_rekisterointipaiva,
                   yki_arviointioikeus.asha_numero
            FROM yki_arvioija
            JOIN yki_arviointioikeus ON yki_arvioija.id = yki_arviointioikeus.arvioija_id
            """.trimIndent()
    }

    /**
     * Tallentaa arvioijan ja hanen arviointioikeutensa ja kirjaa muuttuneen kauden
     * kausihistoriaan. [Tallennuslahde.KITU]lla payloadista puuttuvat arviointioikeudet
     * poistetaan, koska kitu on rekisterin master, ja rivi jaa Solki-lahetysjonoon.
     * [Tallennuslahde.SOLKI]lla kumpaakaan ei tehda: Solkin payloadin kattavuudesta ei ole
     * sopimusta, joten osittainen push ei saa pyyhkia muita kielia, eika Solkin omaa dataa
     * lahetata takaisin Solkiin.
     */
    @WithSpan
    @Transactional
    override fun tallenna(
        arvioija: YkiArvioijaEntity,
        tekija: Oid?,
        lahde: Tallennuslahde,
        odotettuMuokkaushetki: OffsetDateTime?,
    ): Int {
        val savedArvioija =
            upsertArvioija(arvioija, tekija, lahde, odotettuMuokkaushetki)
                ?: throw OptimisticLockingFailureException(
                    "Arvioijan ${arvioija.arvioijaOid} tietoja on muokattu samanaikaisesti",
                )
        val arvioijaId = savedArvioija.id!!.toInt()
        val ennen = kausiRepository.findArviointioikeudet(arvioijaId)

        if (lahde == Tallennuslahde.KITU) {
            poistaPuuttuvatArviointioikeudet(arvioijaId, arvioija.arviointioikeudet)
        }
        upsertArviointioikeudet(arvioijaId, arvioija.arviointioikeudet)
        kausiRepository.kirjaaMuuttuneet(arvioijaId, ennen, arvioija.arviointioikeudet, tekija)
        if (lahde == Tallennuslahde.KITU) {
            kausiRepository.synkronoiKaudet(arvioijaId, arvioija.arviointioikeudet, tekija)
        }

        return arvioijaId
    }

    private fun upsertArvioija(
        arvioija: YkiArvioijaEntity,
        tekija: Oid?,
        lahde: Tallennuslahde,
        odotettuMuokkaushetki: OffsetDateTime?,
    ): YkiArvioijaEntity? {
        // Solkista tullut rivi leimataan lahetetyksi kannan omalla now()-arvolla, jotta se on
        // tasmalleen sama kuin muokattu: pienikin ero jattaisi rivin lahetysjonoon (V117:n
        // osittainen indeksi) ja lahettaisi Solkin oman datan takaisin Solkiin. Jos rivi oli jo
        // jonossa, leimaa ei anneta: kitun lahettamaton muutos ei saa kadota jonosta.
        val uudenRivinLahetysleima = if (lahde == Tallennuslahde.SOLKI) "now()" else "NULL"
        val lahetysleima =
            if (lahde == Tallennuslahde.SOLKI) {
                """
                CASE
                    WHEN yki_arvioija.solkiin_lahetetty IS NULL
                        OR yki_arvioija.solkiin_lahetetty < yki_arvioija.muokattu THEN NULL
                    ELSE now()
                END
                """.trimIndent()
            } else {
                "NULL"
            }

        // Passivointihetki syntyy kitussa. Solkin payload ei kanna sita, joten EXCLUDED-arvo
        // olisi aina tyhja ja pyyhkisi sen.
        val kitunOmatKentat =
            if (lahde == Tallennuslahde.KITU) {
                """
                passivoitu = EXCLUDED.passivoitu,
                """.trimIndent()
            } else {
                ""
            }
        val versioehto = odotettuMuokkaushetki?.let { "WHERE yki_arvioija.muokattu = ?" }.orEmpty()

        val parametrit =
            buildList<Any?> {
                add(arvioija.arvioijaOid.toString())
                add(arvioija.henkilotunnus)
                add(arvioija.sukunimi)
                add(arvioija.etunimet)
                add(arvioija.sahkopostiosoite)
                add(arvioija.katuosoite)
                add(arvioija.postinumero)
                add(arvioija.postitoimipaikka)
                add(arvioija.passivoitu)
                add(tekija?.toString())
                add(tekija?.toString())
                odotettuMuokkaushetki?.let { add(it) }
            }

        return jdbcTemplate
            .query(
                """
                INSERT INTO yki_arvioija (
                    arvioija_oid,
                    henkilotunnus,
                    sukunimi,
                    etunimet,
                    sahkopostiosoite,
                    katuosoite,
                    postinumero,
                    postitoimipaikka,
                    passivoitu,
                    luotu,
                    luoja_oid,
                    muokattu,
                    muokkaaja_oid,
                    solkiin_lahetetty,
                    solki_lahetysvirhe,
                    solki_lahetysyritykset
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, now(), ?, now(), ?, $uudenRivinLahetysleima, NULL, 0)
                ON CONFLICT (arvioija_oid) DO UPDATE
                SET
                    -- henkilotunnus paivitetaan EXCLUDED-arvosta, jotta validoinnin
                    -- pakottama null paatyy myos tietokantaan eika jaa vanhaa arvoa
                    -- lojumaan paivityksissa.
                    henkilotunnus = EXCLUDED.henkilotunnus,
                    sukunimi = EXCLUDED.sukunimi,
                    etunimet = EXCLUDED.etunimet,
                    sahkopostiosoite = EXCLUDED.sahkopostiosoite,
                    katuosoite = EXCLUDED.katuosoite,
                    postinumero = EXCLUDED.postinumero,
                    postitoimipaikka = EXCLUDED.postitoimipaikka,
                    $kitunOmatKentat
                    -- luotu ja luoja_oid sailyvat ennallaan paivityksessa
                    muokattu = now(),
                    muokkaaja_oid = EXCLUDED.muokkaaja_oid,
                    -- kitun oma muutos on lahetettava Solkiin, Solkin push ei
                    solkiin_lahetetty = $lahetysleima,
                    solki_lahetysvirhe = NULL,
                    solki_lahetysyritykset = 0
                $versioehto
                RETURNING *
                """.trimIndent(),
                YkiArvioijaEntity.fromRow,
                *parametrit.toTypedArray(),
            ).firstOrNull()
    }

    /**
     * Legacy-kielet sailytetaan aina: arviointioikeusmatriisi ei renderoi niita, joten ne
     * puuttuvat lomakkeen payloadista eika niiden poistoa ole tarkoitettu.
     */
    private fun poistaPuuttuvatArviointioikeudet(
        arvioijaId: Int,
        arviointioikeudet: List<YkiArviointioikeusEntity>,
    ) {
        val sailytettavat =
            (arviointioikeudet.map { it.kieli } + Tutkintokieli.entries.filter { it.isLegacy() })
                .distinct()
                .map { it.toString() }
                .toTypedArray()
        jdbcTemplate.update({ connection ->
            connection
                .prepareStatement(
                    """
                    DELETE FROM yki_arviointioikeus
                    WHERE arvioija_id = ? AND kieli::text <> ALL (?)
                    """.trimIndent(),
                ).apply {
                    setInt(1, arvioijaId)
                    setArray(2, connection.createArrayOf("text", sailytettavat))
                }
        })
    }

    private fun upsertArviointioikeudet(
        arvioijaId: Int,
        arviointioikeudet: List<YkiArviointioikeusEntity>,
    ) {
        if (arviointioikeudet.isEmpty()) return
        jdbcTemplate.batchUpdate(
            """
            INSERT INTO yki_arviointioikeus(
                arvioija_id,
                kieli,
                tasot,
                tila,
                kauden_alkupaiva,
                kauden_paattymispaiva,
                jatkorekisterointi,
                ensimmainen_rekisterointipaiva
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT ON CONSTRAINT yki_arviointioikeus_unique_arvioija_kieli DO UPDATE SET
                tasot = EXCLUDED.tasot,
                tila = EXCLUDED.tila,
                kauden_alkupaiva = EXCLUDED.kauden_alkupaiva,
                kauden_paattymispaiva = EXCLUDED.kauden_paattymispaiva,
                jatkorekisterointi = EXCLUDED.jatkorekisterointi,
                ensimmainen_rekisterointipaiva = EXCLUDED.ensimmainen_rekisterointipaiva
            """.trimIndent(),
            object : BatchPreparedStatementSetter {
                override fun setValues(
                    ps: PreparedStatement,
                    i: Int,
                ) {
                    arviointioikeudet.elementAt(i).let {
                        ps.setInt(1, arvioijaId)
                        ps.setString(2, it.kieli.toString())
                        ps.setArray(3, ps.connection.createArrayOf("YKI_TUTKINTOTASO", it.tasot.normalisoitu()))
                        ps.setString(4, it.tila?.toString())
                        ps.setObject(5, it.kaudenAlkupaiva)
                        ps.setObject(6, it.kaudenPaattymispaiva)
                        ps.setBoolean(7, it.jatkorekisterointi)
                        ps.setObject(8, it.ensimmainenRekisterointipaiva)
                    }
                }

                override fun getBatchSize(): Int = arviointioikeudet.count()
            },
        )
    }

    @WithSpan
    override fun findByArvioijaOid(arvioijaOid: Oid): YkiArvioijaEntity? =
        jdbcTemplate
            .query(
                "SELECT * FROM yki_arvioija WHERE arvioija_oid = ?",
                YkiArvioijaEntity.fromRow,
                arvioijaOid.toString(),
            ).firstOrNull()
            ?.withArviointioikeudet()

    @WithSpan
    override fun findArvioijaById(id: Int): YkiArvioijaEntity? =
        jdbcTemplate
            .query(
                "SELECT * FROM yki_arvioija WHERE id = ?",
                YkiArvioijaEntity.fromRow,
                id,
            ).firstOrNull()
            ?.withArviointioikeudet()

    private fun YkiArvioijaEntity.withArviointioikeudet(): YkiArvioijaEntity =
        copy(
            arviointioikeudet =
                jdbcTemplate.query(
                    "SELECT * FROM yki_arviointioikeus WHERE arvioija_id = ? ORDER BY kieli",
                    YkiArviointioikeusEntity.fromRow,
                    id!!.toInt(),
                ),
        )

    @WithSpan
    override fun findKausihistoria(arvioijaId: Int): List<YkiArvioijaKausiEntity> =
        jdbcTemplate.query(
            "SELECT * FROM yki_arvioija_kausi WHERE arvioija_id = ? ORDER BY kirjattu DESC, id DESC",
            YkiArvioijaKausiEntity.fromRow,
            arvioijaId,
        )

    @WithSpan
    override fun findForListView(
        params: YkiArvioijaParams,
        tanaan: LocalDate,
    ): List<YkiArvioijaListRow> {
        val order = params.toOrder()
        return namedJdbcTemplate.query(
            """
            SELECT * FROM ($LIST_VIEW_SELECT) arvioijarivi
            ${params.whereSql().orEmpty()}
            ORDER BY ${order.orderSql()}, kieli, arvioija_id
            ${order.pageSql().orEmpty()}
            """.trimIndent(),
            params.sqlParams() + ("tanaan" to tanaan),
            YkiArvioijaListRow.fromRow,
        )
    }

    @WithSpan
    override fun countForListView(
        params: YkiArvioijaParams,
        tanaan: LocalDate,
    ): Int =
        namedJdbcTemplate.queryForObject(
            """
            SELECT count(*) FROM ($LIST_VIEW_SELECT) arvioijarivi
            ${params.whereSql().orEmpty()}
            """.trimIndent(),
            params.sqlParams() + ("tanaan" to tanaan),
            Int::class.java,
        ) ?: 0

    @WithSpan
    override fun saveAllNewEntities(arvioijat: Iterable<YkiArvioijaEntity>): List<Int> = arvioijat.map { tallenna(it) }

    /**
     * Era on rajattu, koska lahetys tehdaan peraikkain HTTP-kutsuina saman ajon sisalla ja
     * arviointioikeudet haetaan riveittain: kytkimen avaaminen ei saa tuottaa yhta ajoa, joka
     * lahettaa koko kertyneen jonon ja ylittaa tehtavan aikarajan. Jono valuu useassa ajossa.
     */
    @WithSpan
    override fun findLahetettavat(
        maxYritykset: Int?,
        limit: Int,
    ): List<YkiArvioijaEntity> =
        namedJdbcTemplate
            .query(
                """
                SELECT * FROM yki_arvioija
                WHERE (solkiin_lahetetty IS NULL OR solkiin_lahetetty < muokattu)
                ${maxYritykset?.let { "AND solki_lahetysyritykset < :maxYritykset" }.orEmpty()}
                ORDER BY muokattu
                LIMIT :limit
                """.trimIndent(),
                maxYritykset?.let { mapOf("maxYritykset" to it) }.orEmpty() + mapOf("limit" to limit),
                YkiArvioijaEntity.fromRow,
            ).map { it.withArviointioikeudet() }

    /** Sama rivitarkkuus kuin listanakymassa (arvioija x kieli), jotta laskuri ja lista tasmaavat. */
    @WithSpan
    override fun countSolkiLahetysvirheet(): Long =
        jdbcTemplate.queryForObject(
            """
            SELECT count(*)
            FROM yki_arvioija
            JOIN yki_arviointioikeus ON yki_arvioija.id = yki_arviointioikeus.arvioija_id
            WHERE solki_lahetysvirhe IS NOT NULL
            """.trimIndent(),
            Long::class.java,
        ) ?: 0

    @WithSpan
    override fun merkitseLahetetyksi(
        id: Int,
        lahetettyVersio: OffsetDateTime?,
        solkiTunnus: String?,
    ) {
        jdbcTemplate.update(
            """
            UPDATE yki_arvioija
            SET solkiin_lahetetty = now(),
                solki_tunnus = COALESCE(?, solki_tunnus),
                solki_lahetysvirhe = NULL,
                solki_lahetysyritykset = 0,
                solki_viimeisin_lahetysyritys = now()
            WHERE id = ?
              AND muokattu IS NOT DISTINCT FROM ?
            """.trimIndent(),
            solkiTunnus,
            id,
            lahetettyVersio,
        )
    }

    /**
     * Yrityslaskuri kasvaa vain virheesta: se rajaa nopeat uusinnat, eika ole kaikkien
     * lahetysten kokonaismaara.
     */
    @WithSpan
    override fun merkitseLahetysvirhe(
        id: Int,
        virhe: String,
    ) {
        jdbcTemplate.update(
            """
            UPDATE yki_arvioija
            SET solki_lahetysvirhe = ?,
                solki_lahetysyritykset = solki_lahetysyritykset + 1,
                solki_viimeisin_lahetysyritys = now()
            WHERE id = ?
            """.trimIndent(),
            virhe,
            id,
        )
    }

    /**
     * Kolme suojaa poistolta: merkinta jolla on yksikin ei-passiivinen arviointioikeus, merkinta
     * jolta arviointioikeudet on poistettu kokonaan (NOT EXISTS olisi tyhjalle joukolle tosi), ja
     * merkinta jonka sailytysajan alkuhetkea ei tiedeta — silloin COALESCE on NULL ja vertailu
     * epatosi. Passiivisuus luetaan samasta lausekkeesta kuin nakymissa, jottei "paattynyt"
     * tarkoita poistossa eri asiaa.
     */
    @WithSpan
    @Transactional
    override fun poistaSailytysajanYlittaneet(
        tanaan: LocalDate,
        raja: LocalDate,
    ): List<Oid> =
        namedJdbcTemplate.query(
            """
            DELETE FROM yki_arvioija
            WHERE EXISTS (
                      SELECT 1 FROM yki_arviointioikeus
                      WHERE yki_arviointioikeus.arvioija_id = yki_arvioija.id
                  )
              AND NOT EXISTS (
                      SELECT 1 FROM yki_arviointioikeus
                      WHERE yki_arviointioikeus.arvioija_id = yki_arvioija.id
                        AND ${Rekisterointitila.SQL} <> '${Rekisterointitila.PASSIVOITU}'
                  )
              AND NOT EXISTS (
                      SELECT 1 FROM yki_arvioija_arviointikausi kausi
                      WHERE kausi.arvioija_id = yki_arvioija.id
                        AND (kausi.paattymispaiva IS NULL OR kausi.paattymispaiva >= :tanaan)
                  )
              AND ${Sailytysaika.ALKUHETKI_SQL} < :raja
            RETURNING arvioija_oid
            """.trimIndent(),
            mapOf("tanaan" to tanaan, "raja" to raja),
        ) { rs, _ -> rs.getOid("arvioija_oid") }

    @WithSpan
    override fun allArviontioikeudet(
        orderBy: YkiArvioijaColumn,
        orderByDirection: SortDirection,
    ): List<YkiArvioijaArviointioikeus> =
        jdbcTemplate
            .query(
                """
                SELECT *
                FROM yki_arvioija
                JOIN yki_arviointioikeus ON yki_arvioija.id = yki_arviointioikeus.arvioija_id
                ORDER BY ${orderBy.entityName} $orderByDirection
                """.trimIndent(),
                YkiArvioijaArviointioikeus.fromRow,
            ).filterNotNull()
}

/**
 * Taulukon jarjestys on osa sen identiteettia kausihistorian uniikkiehdossa, joten tasot
 * kirjoitetaan aina samassa jarjestyksessa riippumatta siita mista suunnasta ne tulivat.
 */
private fun Set<Tutkintotaso>.normalisoitu(): Array<String> = map { it.name }.sorted().toTypedArray()

/** Yhdessa ajossa lahetettavien rivien enimmaismaara. */
const val LAHETYSERAN_KOKO = 100

@Repository
interface YkiArvioijaRepository :
    CrudRepository<YkiArvioijaEntity, Int>,
    PagingAndSortingRepository<YkiArvioijaEntity, Int>,
    CustomYkiArvioijaRepository

data class YkiArvioijaArviointioikeus(
    val arvioijanOppijanumero: Oid,
    val henkilotunnus: String?,
    val sukunimi: String,
    val etunimet: String,
    val sahkopostiosoite: String?,
    val katuosoite: String,
    val postinumero: String,
    val postitoimipaikka: String,
    val kieli: Tutkintokieli,
    val tasot: Set<Tutkintotaso>,
    val tila: YkiArvioijaTila?,
    val kaudenAlkupaiva: LocalDate?,
    val kaudenPaattymispaiva: LocalDate?,
    val jatkorekisterointi: Boolean,
    val ensimmainenRekisterointipaiva: LocalDate,
    val rekisteriintuontiaika: OffsetDateTime?,
) {
    companion object {
        fun join(
            arvioija: YkiArvioijaEntity?,
            arviointioikeus: YkiArviointioikeusEntity?,
        ) = arvioija?.let {
            arviointioikeus?.let {
                YkiArvioijaArviointioikeus(
                    arvioijanOppijanumero = arvioija.arvioijaOid,
                    henkilotunnus = arvioija.henkilotunnus,
                    sukunimi = arvioija.sukunimi,
                    etunimet = arvioija.etunimet,
                    sahkopostiosoite = arvioija.sahkopostiosoite,
                    katuosoite = arvioija.katuosoite,
                    postinumero = arvioija.postinumero,
                    postitoimipaikka = arvioija.postitoimipaikka,
                    kieli = arviointioikeus.kieli,
                    tasot = arviointioikeus.tasot,
                    tila = arviointioikeus.tila,
                    kaudenAlkupaiva = arviointioikeus.kaudenAlkupaiva,
                    kaudenPaattymispaiva = arviointioikeus.kaudenPaattymispaiva,
                    jatkorekisterointi = arviointioikeus.jatkorekisterointi,
                    ensimmainenRekisterointipaiva = arviointioikeus.ensimmainenRekisterointipaiva,
                    rekisteriintuontiaika = arviointioikeus.rekisteriintuontiaika,
                )
            }
        }

        val fromRow =
            RowMapper { rs, _ ->
                join(
                    YkiArvioijaEntity.fromRow.mapRow(rs, 0),
                    YkiArviointioikeusEntity.fromRow.mapRow(rs, 0),
                )
            }
    }
}
