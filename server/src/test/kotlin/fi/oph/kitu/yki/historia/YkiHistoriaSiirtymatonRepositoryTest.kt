package fi.oph.kitu.yki.historia

import fi.oph.kitu.DBContainerConfiguration
import fi.oph.kitu.jdbc.SortDirection
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.jdbc.core.JdbcTemplate
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

@SpringBootTest
@Import(DBContainerConfiguration::class)
class YkiHistoriaSiirtymatonRepositoryTest(
    @param:Autowired private val repository: YkiHistoriaSiirtymatonRepository,
    @param:Autowired private val jdbc: JdbcTemplate,
) {
    @BeforeEach
    fun cleanTables() {
        jdbc.execute("DELETE FROM yki_historia_siirtymaton")
    }

    @Test
    fun `tallentaa ja lukee lahderivin sellaisenaan`() {
        seed(
            solkiId = "12345",
            hetu = "010180-9026",
            sukunimi = "Testaaja",
            etunimet = "Tessa Tellervo",
            tutkintopaiva = "2016-05-14",
            syy = "ei oppijanumeroa",
            syyluokka = YkiHistoriaSiirtymattomyydenSyy.EI_OPPIJANUMEROA,
            oidHaunSyy = "ei löytynyt oppijanumerorekisteristä (hetulista)",
        )

        val rivi = repository.haeSivullinen(YkiHistoriaSiirtymatonParams()).single()

        assertEquals("12345", rivi.solkiId)
        assertEquals("010180-9026", rivi.hetu)
        assertEquals("Tessa Tellervo", rivi.etunimet)
        assertEquals("2016-05-14", rivi.tutkintopaiva)
        assertEquals("ei oppijanumeroa", rivi.syy)
        assertEquals(YkiHistoriaSiirtymattomyydenSyy.EI_OPPIJANUMEROA, rivi.syyluokka)
        assertEquals("ei löytynyt oppijanumerorekisteristä (hetulista)", rivi.oidHaunSyy)
        assertEquals("yki-historia-testi.csv", rivi.lahdetiedosto)
        assertTrue(rivi.ladattu != null)
    }

    @Test
    fun `sailyttaa jasentymattomat arvot ja tyhjat kentat`() {
        seed(
            solkiId = "234",
            hetu = "010180-",
            lastModified = "ei-aikaleima",
            tutkintopaiva = null,
            syy = "last_modified ei jäsenny, ei voi suodattaa",
            syyluokka = YkiHistoriaSiirtymattomyydenSyy.LAST_MODIFIED_EI_JASENNY,
        )

        val rivi = repository.haeSivullinen(YkiHistoriaSiirtymatonParams()).single()

        assertEquals("010180-", rivi.hetu)
        assertEquals("ei-aikaleima", rivi.lastModified)
        assertNull(rivi.tutkintopaiva)
        assertNull(rivi.oidHaunSyy)
    }

    @Test
    fun `rikkinainen rivi ilman solki_id ta menee talteen`() {
        seed(
            solkiId = null,
            syy = "odotettiin 30 saraketta, saatiin 28",
            syyluokka = YkiHistoriaSiirtymattomyydenSyy.RIKKINAINEN_RIVI,
            rawRivi = "a,b,c",
        )
        seed(
            solkiId = null,
            syy = "odotettiin 30 saraketta, saatiin 27",
            syyluokka = YkiHistoriaSiirtymattomyydenSyy.RIKKINAINEN_RIVI,
            rawRivi = "d,e,f",
        )

        val rivit = repository.haeSivullinen(YkiHistoriaSiirtymatonParams())

        assertEquals(2, rivit.size)
        assertEquals(setOf("a,b,c", "d,e,f"), rivit.mapNotNull { it.rawRivi }.toSet())
    }

    @Test
    fun `suodattaa syyluokalla ja laskee osumat`() {
        seed(solkiId = "1", syy = "ei oppijanumeroa", syyluokka = YkiHistoriaSiirtymattomyydenSyy.EI_OPPIJANUMEROA)
        seed(solkiId = "2", syy = "ei oppijanumeroa", syyluokka = YkiHistoriaSiirtymattomyydenSyy.EI_OPPIJANUMEROA)
        seed(
            solkiId = "3",
            syy = "ei yhtään osakoetta",
            syyluokka = YkiHistoriaSiirtymattomyydenSyy.PAIKALLINEN_VALIDOINTI,
        )

        val params = YkiHistoriaSiirtymatonParams(syyluokka = YkiHistoriaSiirtymattomyydenSyy.EI_OPPIJANUMEROA)

        assertEquals(2, repository.laske(params))
        assertEquals(listOf("1", "2"), repository.haeSivullinen(params).mapNotNull { it.solkiId }.sorted())
        assertEquals(3, repository.laske(YkiHistoriaSiirtymatonParams()))
    }

    @Test
    fun `hakusana osuu nimeen hetuun ja jarjestajaan`() {
        seed(
            solkiId = "1",
            sukunimi = "Virtanen",
            etunimet = "Ville",
            hetu = "010180-9026",
            jarjestajanNimi = "Helsingin yliopisto",
        )
        seed(
            solkiId = "2",
            sukunimi = "Korhonen",
            etunimet = "Kaisa",
            hetu = "020280-903A",
            jarjestajanNimi = "Tampereen yliopisto",
        )

        fun haeIdt(search: String) =
            repository.haeSivullinen(YkiHistoriaSiirtymatonParams(search = search)).mapNotNull { it.solkiId }

        assertEquals(listOf("1"), haeIdt("virtanen"))
        assertEquals(listOf("2"), haeIdt("020280-903A"))
        assertEquals(listOf("1"), haeIdt("Helsingin"))
        // Jokaisen hakusanan on osuttava, mutta ne voivat osua eri sarakkeisiin.
        assertEquals(listOf("1"), haeIdt("Ville Virtanen"))
        assertEquals(emptyList<String>(), haeIdt("Ville Korhonen"))
    }

    @Test
    fun `lajittelee ja sivuttaa`() {
        seed(solkiId = "1", tutkintopaiva = "2011-01-01")
        seed(solkiId = "2", tutkintopaiva = "2013-01-01")
        seed(solkiId = "3", tutkintopaiva = "2016-01-01")

        val laskeva =
            YkiHistoriaSiirtymatonParams(
                sortColumn = YkiHistoriaSiirtymatonColumn.Tutkintopaiva,
                sortDirection = SortDirection.DESC,
            )
        assertEquals(listOf("3", "2", "1"), repository.haeSivullinen(laskeva).mapNotNull { it.solkiId })

        val nouseva = laskeva.copy(sortDirection = SortDirection.ASC)
        assertEquals(listOf("1", "2", "3"), repository.haeSivullinen(nouseva).mapNotNull { it.solkiId })

        val toinenSivu = nouseva.copy(limit = 2, page = 2)
        assertEquals(listOf("3"), repository.haeSivullinen(toinenSivu).mapNotNull { it.solkiId })
        // Sivutus ei koske kokonaismaaraa eika CSV-vientia.
        assertEquals(3, repository.laske(toinenSivu))
        assertEquals(3, repository.haeKaikki(toinenSivu).size)
    }

    /**
     * entityName paatyy suoraan ORDER BY -lauseeseen, joten kirjoitusvirhe tai taulusta
     * poistettu sarake nakyisi vasta ajonaikaisena SQL-virheena. Lajitellaan siis kerran
     * jokaisella sarakkeella.
     */
    @Test
    fun `jokaisen sarakkeen entityName on oikea tietokantasarake`() {
        seed(solkiId = "1")

        for (sarake in YkiHistoriaSiirtymatonColumn.entries) {
            val params = YkiHistoriaSiirtymatonParams(sortColumn = sarake)
            assertEquals(1, repository.haeSivullinen(params).size, "lajittelu sarakkeella $sarake")
        }
    }

    @Suppress("LongParameterList")
    private fun seed(
        solkiId: String?,
        hetu: String? = null,
        sukunimi: String? = null,
        etunimet: String? = null,
        jarjestajanNimi: String? = null,
        lastModified: String? = null,
        tutkintopaiva: String? = null,
        syy: String = "ei oppijanumeroa",
        syyluokka: YkiHistoriaSiirtymattomyydenSyy = YkiHistoriaSiirtymattomyydenSyy.EI_OPPIJANUMEROA,
        oidHaunSyy: String? = null,
        rawRivi: String? = null,
    ) {
        jdbc.update(
            """
            INSERT INTO yki_historia_siirtymaton
                (solki_id, hetu, sukunimi, etunimet, jarjestajan_nimi, last_modified, tutkintopaiva,
                 syy, syyluokka, oid_haun_syy, raw_rivi, lahdetiedosto)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?::yki_historia_siirtymattomyyden_syy, ?, ?, ?)
            """.trimIndent(),
            solkiId,
            hetu,
            sukunimi,
            etunimet,
            jarjestajanNimi,
            lastModified,
            tutkintopaiva,
            syy,
            syyluokka.name,
            oidHaunSyy,
            rawRivi,
            "yki-historia-testi.csv",
        )
    }
}
