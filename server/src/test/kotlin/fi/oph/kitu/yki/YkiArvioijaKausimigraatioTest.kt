package fi.oph.kitu.yki

import fi.oph.kitu.DBContainerConfiguration
import fi.oph.kitu.oid.Oid
import fi.oph.kitu.util.result.getOrThrow
import fi.oph.kitu.yki.arvioijat.YkiArvioijaEntity
import fi.oph.kitu.yki.arvioijat.YkiArvioijaKausiRepository
import fi.oph.kitu.yki.arvioijat.YkiArvioijaRepository
import fi.oph.kitu.yki.arvioijat.YkiArviointioikeusEntity
import org.junit.jupiter.api.BeforeEach
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.core.io.ClassPathResource
import org.springframework.jdbc.core.JdbcTemplate
import java.time.LocalDate
import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * V126 siirtaa vanhalla saannolla (alkupaiva + 5 v samana paivana) lasketut paattymispaivat
 * paivaa aikaisemmiksi. Migraatio luetaan luokkapolusta ja ajetaan uudelleen, koska Flyway on jo
 * ajanut sen tyhjaan kantaan: testin arvo on siina etta se syottaa migraatiolle ne rivimuodot
 * joita tuotannossa on, ja kiinnittaa mihin ehto osuu ja mihin ei.
 */
@SpringBootTest
@Import(DBContainerConfiguration::class)
class YkiArvioijaKausimigraatioTest(
    @param:Autowired private val repository: YkiArvioijaRepository,
    @param:Autowired private val kausiRepository: YkiArvioijaKausiRepository,
    @param:Autowired private val jdbcTemplate: JdbcTemplate,
) {
    private companion object {
        val ALKU: LocalDate = LocalDate.of(2021, 3, 1)
        val VANHA_SAANTO: LocalDate = LocalDate.of(2026, 3, 1)
        val KORJATTU: LocalDate = LocalDate.of(2026, 2, 28)
    }

    @BeforeEach
    fun nukeDb() {
        repository.deleteAll()
    }

    private fun ajaMigraatio() =
        jdbcTemplate.execute(
            ClassPathResource("db/migration/V126__korjaa_arviointikauden_paattymispaiva.sql")
                .inputStream
                .readAllBytes()
                .decodeToString(),
        )

    @Test
    fun `vanhalla saannolla laskettu kausi siirtyy paivaa aikaisemmaksi`() {
        val id = arvioija(paattymispaiva = VANHA_SAANTO)

        ajaMigraatio()

        assertEquals(KORJATTU, master(id), "master korjaantuu")
        assertEquals(KORJATTU, projektio(id), "projektio pysyy masterin kanssa samassa linjassa")
    }

    @Test
    fun `karkauspaivalta alkava kausi siirtyy kuten plusYears`() {
        // java.timen plusYears leikkaa 2024-02-29 + 5 v -> 2029-02-28, ja Postgresin interval
        // tekee saman. Ilman sita ehto ei osuisi juuri niihin riveihin jotka vanha laskenta tuotti.
        val id = arvioija(alkupaiva = LocalDate.of(2024, 2, 29), paattymispaiva = LocalDate.of(2029, 2, 28))

        ajaMigraatio()

        assertEquals(LocalDate.of(2029, 2, 27), master(id))
        assertEquals(LocalDate.of(2029, 2, 27), projektio(id))
    }

    @Test
    fun `passivoidun kauden katkaistu paattymispaiva ei siirry`() {
        // Passivointipaiva on hallintopaatos, ei laskettu arvo. Kausi on tassa katkaistu sattumalta
        // tasan vanhan saannon mukaisena paivana, joten vain passivoitu-sarake erottaa sen.
        val id = arvioija(paattymispaiva = VANHA_SAANTO)
        passivoiKausi(id)

        ajaMigraatio()

        assertEquals(VANHA_SAANTO, master(id), "katkaistua paivaa ei saa siirtaa")
        assertEquals(VANHA_SAANTO, projektio(id), "projektio seuraa masteria")
    }

    @Test
    fun `saannosta poikkeava tuotu paivapari ei siirry`() {
        val poikkeava = LocalDate.of(2026, 6, 30)
        val id = arvioija(paattymispaiva = poikkeava)

        ajaMigraatio()

        assertEquals(poikkeava, master(id))
        assertEquals(poikkeava, projektio(id))
    }

    @Test
    fun `jaadytetty vanhentuneen kielen rivi ei siirry`() {
        // Legacy-kielet jaivat V122:ssa ilman kautta. Sama alkupaiva kuin hallitulla kaudella ei
        // saa vetaa niita mukaan, joten tassa arvioijalla on molemmat.
        val id = arvioija(paattymispaiva = VANHA_SAANTO)
        lisaaJaadytettyOikeus(id, Tutkintokieli.SWE10, ALKU, VANHA_SAANTO)

        ajaMigraatio()

        assertEquals(KORJATTU, projektio(id, Tutkintokieli.FIN), "hallittu kieli korjaantuu")
        assertEquals(VANHA_SAANTO, projektio(id, Tutkintokieli.SWE10), "jaadytetty rivi sailyy")
    }

    @Test
    fun `projektio ilman kautta ei siirry`() {
        // V122 ei siirtanyt alkupaivatonta arvioijaa masteriin, joten sen projektiolla ei ole
        // kautta jota seurata. Siirto jattaisi rivin eri linjalle kuin master.
        val id = arvioija(paattymispaiva = VANHA_SAANTO)
        poistaKaudet(id)

        ajaMigraatio()

        assertEquals(VANHA_SAANTO, projektio(id))
    }

    @Test
    fun `korjaus ei aja arvioijaa Solki-lahetysjonoon`() {
        val id = arvioija(paattymispaiva = VANHA_SAANTO)
        merkitseLahetetyksi(id)

        ajaMigraatio()

        assertEquals(KORJATTU, master(id), "korjaus tehtiin")
        assertEquals(
            0,
            repository.findLahetettavat().size,
            "muokattu jatetaan koskematta, jottei koko rekisteri lahde uudelleen Solkiin",
        )
    }

    private fun master(arvioijaId: Int): LocalDate? = kausiRepository.findKaudet(arvioijaId).single().paattymispaiva

    private fun projektio(
        arvioijaId: Int,
        kieli: Tutkintokieli = Tutkintokieli.FIN,
    ): LocalDate? =
        kausiRepository
            .findArviointioikeudet(arvioijaId)
            .single { it.kieli == kieli }
            .kaudenPaattymispaiva

    private fun passivoiKausi(arvioijaId: Int) {
        jdbcTemplate.update(
            "UPDATE yki_arvioija_arviointikausi SET passivoitu = now() WHERE arvioija_id = ?",
            arvioijaId,
        )
    }

    private fun poistaKaudet(arvioijaId: Int) {
        jdbcTemplate.update("DELETE FROM yki_arvioija_arviointikausi WHERE arvioija_id = ?", arvioijaId)
    }

    private fun lisaaJaadytettyOikeus(
        arvioijaId: Int,
        kieli: Tutkintokieli,
        alkupaiva: LocalDate,
        paattymispaiva: LocalDate,
    ) {
        jdbcTemplate.update(
            """
            INSERT INTO yki_arviointioikeus
                (arvioija_id, kieli, tasot, kauden_alkupaiva, kauden_paattymispaiva,
                 jatkorekisterointi, ensimmainen_rekisterointipaiva)
            VALUES (?, ?::yki_tutkintokieli, ARRAY ['PT'], ?, ?, false, ?)
            """.trimIndent(),
            arvioijaId,
            kieli.toString(),
            alkupaiva,
            paattymispaiva,
            alkupaiva,
        )
    }

    private fun merkitseLahetetyksi(arvioijaId: Int) {
        jdbcTemplate.update("UPDATE yki_arvioija SET solkiin_lahetetty = now() WHERE id = ?", arvioijaId)
    }

    /** Tallennus synkronoi kaudet arviointioikeuksista, joten master ja projektio syntyvat yhdessa. */
    private fun arvioija(
        alkupaiva: LocalDate = ALKU,
        paattymispaiva: LocalDate,
    ): Int =
        repository.tallenna(
            YkiArvioijaEntity(
                id = null,
                arvioijaOid = Oid.parse("1.2.246.562.24.59267607404").getOrThrow(),
                henkilotunnus = null,
                sukunimi = "Kivinen-Testi",
                etunimet = "Petro Testi",
                sahkopostiosoite = "testi@testi.fi",
                katuosoite = "Testikuja 5",
                postinumero = "40100",
                postitoimipaikka = "Testilä",
                arviointioikeudet =
                    listOf(
                        YkiArviointioikeusEntity(
                            id = null,
                            arvioijaId = null,
                            kieli = Tutkintokieli.FIN,
                            tasot = setOf(Tutkintotaso.PT),
                            tila = null,
                            kaudenAlkupaiva = alkupaiva,
                            kaudenPaattymispaiva = paattymispaiva,
                            jatkorekisterointi = false,
                            ensimmainenRekisterointipaiva = alkupaiva,
                            rekisteriintuontiaika = null,
                        ),
                    ),
            ),
        )
}
