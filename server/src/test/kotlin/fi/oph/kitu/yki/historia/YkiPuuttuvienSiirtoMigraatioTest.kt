package fi.oph.kitu.yki.historia

import fi.oph.kitu.DBContainerConfiguration
import fi.oph.kitu.yki.Arviointitila
import fi.oph.kitu.yki.suoritukset.YkiSuoritusRepository
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.core.io.ClassPathResource
import org.springframework.jdbc.core.JdbcTemplate
import java.time.LocalDate
import kotlin.test.assertEquals
import kotlin.test.assertNull

@SpringBootTest
@Import(DBContainerConfiguration::class)
class YkiPuuttuvienSiirtoMigraatioTest(
    @param:Autowired private val jdbc: JdbcTemplate,
    @param:Autowired private val suoritusRepository: YkiSuoritusRepository,
) {
    @BeforeEach
    fun tyhjennaTaulut() {
        jdbc.execute("DELETE FROM yki_historia_siirtymaton")
        suoritusRepository.deleteAll()
        jdbc.execute("TRUNCATE TABLE yki_tarkistusarviointi CASCADE")
    }

    @Test
    fun `oppijanumeroton rivi siirtyy rekisteriin ja poistuu karanteenista`() {
        kylva(solkiId = "3001")

        ajaSiirtomigraatio()

        assertEquals(0, karanteenissa(), "rivi jäi karanteeniin")
        val suoritus = suoritusRepository.findAll().single()
        assertNull(suoritus.suorittajanOID)
        assertEquals(3001, suoritus.solkiId)
        assertEquals(LocalDate.of(2023, 5, 13), suoritus.tutkintopaiva)
        assertEquals("010180-9026", suoritus.hetu)
        assertEquals(Arviointitila.ARVIOITU, suoritus.arviointitila)
        assertEquals(6, osakokeita(suoritus.id))
    }

    @Test
    fun `vuoden 2026 suoritus siirtyy ilman hetua`() {
        kylva(solkiId = "3002", tutkintopaiva = "2026-01-24")

        ajaSiirtomigraatio()

        assertNull(suoritusRepository.findAll().single().hetu)
    }

    @Test
    fun `todistuskielen rajapaivasta alkaen rivi jaa karanteeniin`() {
        kylva(solkiId = "3003", tutkintopaiva = "2026-03-31")
        kylva(solkiId = "3004", tutkintopaiva = "2026-04-01")

        ajaSiirtomigraatio()

        assertEquals(3003, suoritusRepository.findAll().single().solkiId)
        assertEquals("todistuskieli on pakollinen 1.4.2026 alkaen, eikä lähteessä ole sitä", syy())
        assertEquals("PAIKALLINEN_VALIDOINTI", syyluokka())
    }

    @Test
    fun `tarkistusarvioitu suoritus siirtyy tarkistusarviointeineen`() {
        kylva(
            solkiId = "3005",
            tarkSaapumisPvm = "2023-06-01",
            tarkKasittelyPvm = "2023-06-10",
            tarkAsiatunnus = "OPH-123-2023",
            tarkOsakokeet = "3",
            arvosanaMuuttui = "1",
        )

        ajaSiirtomigraatio()

        val suoritus = suoritusRepository.findAll().single()
        assertEquals(Arviointitila.TARKISTUSARVIOITU, suoritus.arviointitila)
        assertEquals(
            listOf("KI" to false, "PU" to true),
            jdbc.query(
                """
                SELECT o.tyyppi::text, ot.arvosana_muuttui
                FROM yki_osakoe_tarkistusarviointi ot
                    JOIN yki_osakoe o ON o.id = ot.osakoe_id
                WHERE o.suoritus_id = ?
                ORDER BY 1
                """.trimIndent(),
                { rs, _ -> rs.getString(1) to rs.getBoolean(2) },
                suoritus.id,
            ),
        )
    }

    @Test
    fun `kasittelypaiva ennen saapumispaivaa jaa karanteeniin`() {
        kylva(
            solkiId = "3006",
            tarkSaapumisPvm = "2023-06-10",
            tarkKasittelyPvm = "2023-06-01",
            tarkAsiatunnus = "OPH-124-2023",
            tarkOsakokeet = "1",
        )

        ajaSiirtomigraatio()

        assertEquals(0, suoritusRepository.findAll().size)
        assertEquals("tarkistusarvioinnin käsittelypäivä on ennen saapumispäivää", syy())
    }

    @Test
    fun `muuttunut osakoe joka ei ole tarkistettu jaa karanteeniin`() {
        kylva(
            solkiId = "3007",
            tarkSaapumisPvm = "2023-06-01",
            tarkKasittelyPvm = "2023-06-10",
            tarkAsiatunnus = "OPH-125-2023",
            tarkOsakokeet = "1",
            arvosanaMuuttui = "2",
        )

        ajaSiirtomigraatio()

        assertEquals(0, suoritusRepository.findAll().size)
        assertEquals("tarkistusarvioinnin muuttuneet osakokeet eivät ole tarkistettujen joukossa", syy())
    }

    @Test
    fun `oppijanumerollinen rivi jaa karanteeniin rajapinnan antamalla syylla`() {
        val rajapinnanSyy = """HTTP 400: {"virheet":["Henkilöä ei löytynyt"]}"""
        kylva(
            solkiId = "3008",
            suorittajanOid = "1.2.246.562.24.11111111111",
            syy = rajapinnanSyy,
            syyluokka = "API_HYLKASI",
        )

        ajaSiirtomigraatio()

        assertEquals(0, suoritusRepository.findAll().size)
        assertEquals(rajapinnanSyy, syy())
        assertEquals("API_HYLKASI", syyluokka())
    }

    @Test
    fun `uudelleenajo on idempotentti`() {
        kylva(solkiId = "3009")
        kylva(solkiId = "3010", arvosanat = null)

        ajaSiirtomigraatio()
        ajaSiirtomigraatio()

        assertEquals(1, suoritusRepository.findAll().size)
        assertEquals(1, karanteenissa())
        assertEquals("ei yhtään osakoetta", syy())
    }

    private fun ajaSiirtomigraatio() {
        val sql =
            ClassPathResource("db/migration/V133__siirra_yki_puuttuvat_rekisteriin.sql")
                .inputStream
                .bufferedReader()
                .use { it.readText() }
        jdbc.execute(sql)
    }

    private fun karanteenissa() = jdbc.queryForObject("SELECT count(*) FROM yki_historia_siirtymaton", Int::class.java)

    private fun osakokeita(suoritusId: Int?) =
        jdbc.queryForObject("SELECT count(*) FROM yki_osakoe WHERE suoritus_id = ?", Int::class.java, suoritusId)

    private fun syy() = jdbc.queryForObject("SELECT syy FROM yki_historia_siirtymaton", String::class.java)

    private fun syyluokka() =
        jdbc.queryForObject("SELECT syyluokka::text FROM yki_historia_siirtymaton", String::class.java)

    @Suppress("LongParameterList")
    private fun kylva(
        solkiId: String,
        suorittajanOid: String? = null,
        tutkintopaiva: String = "2023-05-13",
        arvosanat: String? = "3",
        tarkSaapumisPvm: String? = null,
        tarkKasittelyPvm: String? = null,
        tarkAsiatunnus: String? = null,
        tarkOsakokeet: String = "0",
        arvosanaMuuttui: String = "0",
        syy: String = "ei oppijanumeroa",
        syyluokka: String = "EI_OPPIJANUMEROA",
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
            "2023-03-01 10:00:00",
            tutkintopaiva,
            "fin",
            "YT",
            "1.2.246.562.10.12345678901",
            "Helsingin yliopisto",
            "2023-06-01",
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
            tarkAsiatunnus?.let { "Perustelu" },
            tarkKasittelyPvm,
            syy,
            syyluokka,
            "solki-2021-2026.csv#puuttuvat",
        )
    }
}
