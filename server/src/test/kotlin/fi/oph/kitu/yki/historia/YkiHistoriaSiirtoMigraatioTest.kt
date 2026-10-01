package fi.oph.kitu.yki.historia

import fi.oph.kitu.DBContainerConfiguration
import fi.oph.kitu.yki.Arviointitila
import fi.oph.kitu.yki.TutkinnonOsa
import fi.oph.kitu.yki.suoritukset.YkiSuoritusRepository
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.core.io.ClassPathResource
import org.springframework.jdbc.core.JdbcTemplate
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * V129 siirtaa karanteenitaulun rivit rekisteriin. Flyway ajaa sen kannan luonnissa tyhjalle
 * taululle, joten siirrosta ei voi vaittaa silloin mitaan: migraatio on pelkkaa idempotenttia
 * DML:aa juuri siksi, etta testi voi kylvaa rivit ja ajaa saman tiedoston uudelleen.
 *
 * Koko tiedosto annetaan yhtena jdbc.execute-kutsuna eika lauseiksi pilkottuna, koska
 * valiaikaistaulut on luotu ON COMMIT DROP -maareella: Postgres ajaa monilauseisen kyselyn
 * yhtena implisiittisena transaktiona, joten taulut elavat lauseiden yli.
 */
@SpringBootTest
@Import(DBContainerConfiguration::class)
class YkiHistoriaSiirtoMigraatioTest(
    @param:Autowired private val jdbc: JdbcTemplate,
    @param:Autowired private val suoritusRepository: YkiSuoritusRepository,
) {
    @BeforeEach
    fun tyhjennaTaulut() {
        jdbc.execute("DELETE FROM yki_historia_siirtymaton")
        suoritusRepository.deleteAll()
        // deleteAll truncaten CASCADE osuu vain yki_suoritukseen viittaaviin tauluihin, eika
        // yki_tarkistusarviointi viittaa siihen, joten se jaisi edellisen testin riveineen.
        jdbc.execute("TRUNCATE TABLE yki_tarkistusarviointi CASCADE")
    }

    @Test
    fun `oppijanumeroton rivi siirtyy rekisteriin ja poistuu karanteenista`() {
        kylva(solkiId = "1001", suorittajanOid = null)

        ajaSiirtomigraatio()

        assertEquals(0, karanteenissa(), "rivi jai karanteeniin")
        val suoritus = suoritusRepository.findAll().single()
        assertNull(suoritus.suorittajanOID, "oppijanumero ei ole null")
        assertEquals(1001, suoritus.solkiId)
        assertEquals("yki.1001", suoritus.lahdejarjestelmanTunnus)
        assertEquals("010180-9026", suoritus.hetu, "hetu talletetaan sellaisenaan")
        assertEquals(Arviointitila.ARVIOITU, suoritus.arviointitila)
        assertEquals(6, osakokeita(suoritus.id!!))
    }

    @Test
    fun `oppijanumeron kantava rivi siirtyy oppijanumero mukana`() {
        kylva(solkiId = "1002", suorittajanOid = "1.2.246.562.24.11111111111")

        ajaSiirtomigraatio()

        assertEquals(
            "1.2.246.562.24.11111111111",
            suoritusRepository
                .findAll()
                .single()
                .suorittajanOID
                ?.toString(),
        )
    }

    @Test
    fun `jasentymaton last_modified ei esta siirtoa koska aikaleima otetaan nyt-hetkesta`() {
        kylva(
            solkiId = "1003",
            lastModified = "0000-00-00 00:00:00",
            syyluokka = YkiHistoriaSiirtymattomyydenSyy.LAST_MODIFIED_EI_JASENNY,
        )

        ajaSiirtomigraatio()

        assertEquals(0, karanteenissa())
        assertEquals(1003, suoritusRepository.findAll().single().solkiId)
    }

    @Test
    fun `rikkinainen rivi jaa karanteeniin ja luokitellaan uudelleen`() {
        jdbc.update(
            """
            INSERT INTO yki_historia_siirtymaton (syy, syyluokka, raw_rivi, lahdetiedosto)
            VALUES ('ei oppijanumeroa', 'EI_OPPIJANUMEROA'::yki_historia_siirtymattomyyden_syy,
                    'a,b,c', 'yki-historia-testi.csv')
            """.trimIndent(),
        )

        ajaSiirtomigraatio()

        assertEquals(0, suoritusRepository.findAll().size)
        val rivi = jdbc.queryForMap("SELECT syy, syyluokka FROM yki_historia_siirtymaton")
        assertEquals("RIKKINAINEN_RIVI", rivi["syyluokka"].toString())
        assertTrue(rivi["syy"].toString().contains("rikkinäinen lähderivi"), "syy: ${rivi["syy"]}")
    }

    @Test
    fun `osakkeeton rivi jaa karanteeniin oikealla syylla`() {
        kylva(solkiId = "1004", arvosanat = null)

        ajaSiirtomigraatio()

        assertEquals(0, suoritusRepository.findAll().size)
        assertEquals("ei yhtään osakoetta", syy())
        assertEquals("PAIKALLINEN_VALIDOINTI", syyluokka())
    }

    @Test
    fun `tutkintotasolle kielletty arvosana jaa karanteeniin`() {
        kylva(solkiId = "1005", tutkintotaso = "PT", arvosanat = "6")

        ajaSiirtomigraatio()

        assertEquals(0, suoritusRepository.findAll().size)
        assertEquals("arvosana ei ole sallittu tutkintotasolle PT", syy())
    }

    @Test
    fun `jarjestajan nimen puuttuminen estaa siirron koska entiteetti ei salli nullia`() {
        kylva(solkiId = "1006", jarjestajanNimi = null)

        ajaSiirtomigraatio()

        assertEquals(0, suoritusRepository.findAll().size)
        assertEquals("järjestäjän nimi puuttuu", syy())
    }

    @Test
    fun `jo rekisterissa oleva solki_id jaa karanteeniin eika tuplaannu`() {
        kylva(solkiId = "1007")
        ajaSiirtomigraatio()
        assertEquals(1, suoritusRepository.findAll().size)

        kylva(solkiId = "1007")
        ajaSiirtomigraatio()

        assertEquals(1, suoritusRepository.findAll().size, "suoritus tuplaantui")
        assertEquals(1, karanteenissa(), "rivi poistettiin vaikka se ei siirtynyt")
        assertEquals("suoritus on jo rekisterissä", syy())
    }

    @Test
    fun `tarkistusarviointi puretaan bittimaskista`() {
        kylva(
            solkiId = "1008",
            tarkSaapumisPvm = "2016-07-01",
            tarkAsiatunnus = "OPH-1234-2016",
            tarkKasittelyPvm = "2016-07-20",
            // PU=1 ja TY=4
            tarkOsakokeet = "5",
            arvosanaMuuttui = "4",
            perustelu = "Tarkistettu",
        )

        ajaSiirtomigraatio()

        val suoritus = suoritusRepository.findAll().single()
        assertEquals(Arviointitila.TARKISTUSARVIOITU, suoritus.arviointitila)
        assertEquals(setOf(TutkinnonOsa.PU, TutkinnonOsa.TY), suoritus.tarkistusarvioidutOsakokeet)
        assertEquals(setOf(TutkinnonOsa.TY), suoritus.arvosanaMuuttui)
        assertEquals("OPH-1234-2016", suoritus.tarkistusarvioinninAsiatunnus)
        assertEquals("Tarkistettu", suoritus.perustelu)
    }

    @Test
    fun `nollamaski ei luo tarkistusarviointia`() {
        kylva(solkiId = "1009", tarkOsakokeet = "0", arvosanaMuuttui = "0")

        ajaSiirtomigraatio()

        val suoritus = suoritusRepository.findAll().single()
        assertEquals(Arviointitila.ARVIOITU, suoritus.arviointitila)
        assertNull(suoritus.tarkistusarvioinninAsiatunnus)
        assertEquals(0, jdbc.queryForObject("SELECT count(*) FROM yki_tarkistusarviointi", Int::class.java))
    }

    @Test
    fun `uudelleenajo on idempotentti`() {
        kylva(solkiId = "1010")
        kylva(solkiId = "1011", arvosanat = null)

        ajaSiirtomigraatio()
        ajaSiirtomigraatio()

        assertEquals(1, suoritusRepository.findAll().size)
        assertEquals(1, karanteenissa())
        assertEquals("ei yhtään osakoetta", syy(), "syy kasvoi uudelleenajossa")
    }

    private fun ajaSiirtomigraatio() {
        val sql =
            ClassPathResource("db/migration/V129__siirra_yki_historia_rekisteriin.sql")
                .inputStream
                .bufferedReader()
                .use { it.readText() }
        jdbc.execute(sql)
    }

    private fun karanteenissa() = jdbc.queryForObject("SELECT count(*) FROM yki_historia_siirtymaton", Int::class.java)

    private fun osakokeita(suoritusId: Int) =
        jdbc.queryForObject(
            "SELECT count(*) FROM yki_osakoe WHERE suoritus_id = ?",
            Int::class.java,
            suoritusId,
        )

    private fun syy() = jdbc.queryForObject("SELECT syy FROM yki_historia_siirtymaton", String::class.java)

    private fun syyluokka() =
        jdbc.queryForObject("SELECT syyluokka::text FROM yki_historia_siirtymaton", String::class.java)

    @Suppress("LongParameterList")
    private fun kylva(
        solkiId: String?,
        suorittajanOid: String? = null,
        lastModified: String? = "2016-05-14 10:00:00",
        tutkintotaso: String = "YT",
        jarjestajanNimi: String? = "Helsingin yliopisto",
        arvosanat: String? = "3",
        tarkSaapumisPvm: String? = null,
        tarkAsiatunnus: String? = null,
        tarkKasittelyPvm: String? = null,
        tarkOsakokeet: String? = "0",
        arvosanaMuuttui: String? = "0",
        perustelu: String? = null,
        syyluokka: YkiHistoriaSiirtymattomyydenSyy = YkiHistoriaSiirtymattomyydenSyy.EI_OPPIJANUMEROA,
    ) {
        jdbc.update(
            """
            INSERT INTO yki_historia_siirtymaton
                (suorittajan_oid, hetu, sukupuoli, sukunimi, etunimet, kansalaisuus, katuosoite,
                 postinumero, postitoimipaikka, email, solki_id, last_modified, tutkintopaiva,
                 tutkintokieli, tutkintotaso, jarjestajan_oid, jarjestajan_nimi, arviointipaiva,
                 as_ty, as_ki, as_rs, as_py, as_pu, as_yl, tark_saapumis_pvm, tark_asiatunnus,
                 tark_osakokeet, arvosana_muuttui, perustelu, tark_kasittely_pvm,
                 syy, syyluokka, lahdetiedosto)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?::yki_historia_siirtymattomyyden_syy, ?)
            """.trimIndent(),
            suorittajanOid,
            "010180-9026",
            "M",
            "Testaaja",
            "Tessa Tellervo",
            "FIN",
            "Testikatu 1",
            "00100",
            "Helsinki",
            null,
            solkiId,
            lastModified,
            "2016-05-14",
            "fin",
            tutkintotaso,
            "1.2.246.562.10.12345678901",
            jarjestajanNimi,
            "2016-06-01",
            arvosanat,
            arvosanat,
            arvosanat,
            arvosanat,
            arvosanat,
            arvosanat,
            tarkSaapumisPvm,
            tarkAsiatunnus,
            tarkOsakokeet,
            arvosanaMuuttui,
            perustelu,
            tarkKasittelyPvm,
            "ei oppijanumeroa",
            syyluokka.name,
            "yki-historia-testi.csv",
        )
    }
}
