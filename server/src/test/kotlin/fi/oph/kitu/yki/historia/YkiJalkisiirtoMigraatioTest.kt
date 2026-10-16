package fi.oph.kitu.yki.historia

import fi.oph.kitu.DBContainerConfiguration
import fi.oph.kitu.tyhjennaYkiSuoritukset
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
class YkiJalkisiirtoMigraatioTest(
    @param:Autowired private val jdbc: JdbcTemplate,
    @param:Autowired private val suoritusRepository: YkiSuoritusRepository,
) {
    @BeforeEach
    fun tyhjennaTaulut() {
        jdbc.execute("DELETE FROM yki_historia_siirtymaton")
        jdbc.tyhjennaYkiSuoritukset()
        jdbc.execute("TRUNCATE TABLE yki_tarkistusarviointi CASCADE")
    }

    @Test
    fun `oppijanumeroton rivi siirtyy rekisteriin ja poistuu karanteenista`() {
        kylva(solkiId = "2001")

        ajaSiirtomigraatio()

        assertEquals(0, karanteenissa(), "rivi jäi karanteeniin")
        val suoritus = suoritusRepository.findAll().single()
        assertNull(suoritus.suorittajanOID)
        assertEquals(2001, suoritus.solkiId)
        assertEquals(LocalDate.of(2018, 5, 14), suoritus.tutkintopaiva)
        assertEquals(Arviointitila.ARVIOITU, suoritus.arviointitila)
        assertEquals(
            6,
            jdbc.queryForObject(
                "SELECT count(*) FROM yki_osakoe WHERE suoritus_id = ?",
                Int::class.java,
                suoritus.id,
            ),
        )
    }

    @Test
    fun `oppijanumerollinen rivi jaa karanteeniin rajapinnan antamalla syylla`() {
        val rajapinnanSyy = """HTTP 400: {"virheet":["Henkilöä ei löytynyt"]}"""
        kylva(
            solkiId = "2002",
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
    fun `vanhentunut kielikoodi vuodesta 2017 alkaen jaa karanteeniin`() {
        kylva(solkiId = "2003", tutkintokieli = "eng11")

        ajaSiirtomigraatio()

        assertEquals(0, suoritusRepository.findAll().size)
        assertEquals("käytöstä poistunutta kielikoodia ei voi käyttää vuodesta 2017 alkaen", syy())
        assertEquals("PAIKALLINEN_VALIDOINTI", syyluokka())
    }

    @Test
    fun `vanhentunut kielikoodi ennen vuotta 2017 siirtyy`() {
        kylva(solkiId = "2004", tutkintokieli = "eng11", tutkintopaiva = "2016-09-10")

        ajaSiirtomigraatio()

        assertEquals(0, karanteenissa())
        assertEquals(2004, suoritusRepository.findAll().single().solkiId)
    }

    @Test
    fun `jo rekisterissa oleva solki_id jaa karanteeniin eika tuplaannu`() {
        kylva(solkiId = "2005")
        ajaSiirtomigraatio()

        kylva(solkiId = "2005")
        ajaSiirtomigraatio()

        assertEquals(1, suoritusRepository.findAll().size, "suoritus tuplaantui")
        assertEquals("suoritus on jo rekisterissä", syy())
    }

    @Test
    fun `uudelleenajo on idempotentti`() {
        kylva(solkiId = "2006")
        kylva(solkiId = "2007", arvosanat = null)

        ajaSiirtomigraatio()
        ajaSiirtomigraatio()

        assertEquals(1, suoritusRepository.findAll().size)
        assertEquals(1, karanteenissa())
        assertEquals("ei yhtään osakoetta", syy())
    }

    private fun ajaSiirtomigraatio() {
        val sql =
            ClassPathResource("db/migration/V132__siirra_yki_jalkisiirto_rekisteriin.sql")
                .inputStream
                .bufferedReader()
                .use { it.readText() }
        jdbc.execute(sql)
    }

    private fun karanteenissa() = jdbc.queryForObject("SELECT count(*) FROM yki_historia_siirtymaton", Int::class.java)

    private fun syy() = jdbc.queryForObject("SELECT syy FROM yki_historia_siirtymaton", String::class.java)

    private fun syyluokka() =
        jdbc.queryForObject("SELECT syyluokka::text FROM yki_historia_siirtymaton", String::class.java)

    @Suppress("LongParameterList")
    private fun kylva(
        solkiId: String,
        suorittajanOid: String? = null,
        tutkintopaiva: String = "2018-05-14",
        tutkintokieli: String = "fin",
        arvosanat: String? = "3",
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
            "2018-03-01 10:00:00",
            tutkintopaiva,
            tutkintokieli,
            "YT",
            "1.2.246.562.10.12345678901",
            "Helsingin yliopisto",
            "2018-06-01",
            arvosanat,
            arvosanat,
            arvosanat,
            arvosanat,
            arvosanat,
            arvosanat,
            null,
            null,
            "0",
            "0",
            null,
            null,
            syy,
            syyluokka,
            "yki-historia-2.csv#jalkisiirto",
        )
    }
}
