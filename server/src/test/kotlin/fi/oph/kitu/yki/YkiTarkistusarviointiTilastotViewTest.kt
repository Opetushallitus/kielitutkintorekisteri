package fi.oph.kitu.yki

import fi.oph.kitu.DBContainerConfiguration
import fi.oph.kitu.dev.mockdata.generateRandomYkiSuoritusEntity
import fi.oph.kitu.virkailijaSession
import fi.oph.kitu.yki.suoritukset.YkiSuoritusEntity
import fi.oph.kitu.yki.suoritukset.YkiSuoritusRepository
import org.jsoup.Jsoup
import org.jsoup.nodes.Document
import org.junit.jupiter.api.BeforeEach
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.http.HttpHeaders
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.security.test.web.servlet.setup.SecurityMockMvcConfigurers.springSecurity
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.MvcResult
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.asyncDispatch
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.request
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.status
import org.springframework.test.web.servlet.setup.DefaultMockMvcBuilder
import org.springframework.test.web.servlet.setup.MockMvcBuilders
import org.springframework.web.context.WebApplicationContext
import java.time.Instant
import java.time.LocalDate
import kotlin.test.Test
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertFalse

private const val TILASTOT = "/yki/tarkistusarvioinnit/tilastot"
private const val HYVAKSYTTY = "tila=Hyvaksytty"
private const val ODOTTAA = "tila=OdottaaHyvaksyntaa"

@SpringBootTest
@Import(DBContainerConfiguration::class)
class YkiTarkistusarviointiTilastotViewTest(
    @param:Autowired private val context: WebApplicationContext,
    @param:Autowired private val suoritusRepository: YkiSuoritusRepository,
    @param:Autowired private val jdbc: JdbcTemplate,
) {
    private lateinit var mockMvc: MockMvc

    @BeforeEach
    fun setup() {
        mockMvc =
            MockMvcBuilders
                .webAppContextSetup(context)
                .apply<DefaultMockMvcBuilder>(springSecurity())
                .build()

        jdbc.execute("TRUNCATE TABLE yki_suoritus CASCADE")
    }

    private fun tallenna(
        asiatunnus: String,
        osakokeet: Set<TutkinnonOsa> = setOf(TutkinnonOsa.PU),
        muuttuneet: Set<TutkinnonOsa> = emptySet(),
        tila: Arviointitila = Arviointitila.TARKISTUSARVIOITU,
        tutkintopaiva: LocalDate = LocalDate.of(2025, 1, 1),
        tutkintokieli: Tutkintokieli = Tutkintokieli.FIN,
        tutkintotaso: Tutkintotaso = Tutkintotaso.PT,
    ): YkiSuoritusEntity =
        generateRandomYkiSuoritusEntity()
            .copy(
                tutkintopaiva = tutkintopaiva,
                tutkintokieli = tutkintokieli,
                tutkintotaso = tutkintotaso,
                tarkistusarvioinninAsiatunnus = asiatunnus,
                tarkistusarvioinninSaapumisPvm = LocalDate.of(2025, 2, 1),
                tarkistusarvioinninKasittelyPvm = LocalDate.of(2025, 3, 1),
                tarkistusarvioidutOsakokeet = osakokeet,
                arvosanaMuuttui = muuttuneet.ifEmpty { null },
                perustelu = "Perustelu",
                arviointitila = tila,
            ).also { suoritusRepository.save(it, false) }

    private fun hyvaksy(suoritus: YkiSuoritusEntity) {
        suoritusRepository.hyvaksyTarkistusarvioinnit(listOf(suoritus.solkiId), LocalDate.of(2025, 5, 1))
    }

    private fun sivu(query: String = ""): Document =
        Jsoup.parse(
            mockMvc
                .perform(get("$TILASTOT?$query").session(virkailijaSession()))
                .andExpect(status().isOk)
                .andReturn()
                .response.contentAsString,
        )

    private fun Document.rivi(kohde: String): List<String> =
        select("tr.tilastorivi")
            .single { it.selectFirst("td[data-testid=Kohde]")?.text() == kohde }
            .let { rivi ->
                listOf("Yhteensa", "ArvosanaMuuttui", "EiMuuttunut").map {
                    rivi.selectFirst("td[data-testid=$it]")!!.text()
                }
            }

    private fun Document.yhteensa(): String = selectFirst("[data-testid=numberOfRows]")!!.text()

    private fun listanYhteensa(polku: String): String =
        Jsoup
            .parse(
                mockMvc
                    .perform(get(polku).session(virkailijaSession()))
                    .andExpect(status().isOk)
                    .andReturn()
                    .response.contentAsString,
            ).selectFirst("[data-testid=numberOfRows]")!!
            .text()

    private fun getCsv(url: String): MvcResult {
        val alkanut =
            mockMvc
                .perform(get(url).session(virkailijaSession()))
                .andExpect(request().asyncStarted())
                .andReturn()
        return mockMvc
            .perform(asyncDispatch(alkanut))
            .andExpect(status().isOk)
            .andReturn()
    }

    private fun MvcResult.csvRivit(): List<String> =
        response
            .getContentAsString(Charsets.UTF_8)
            .removePrefix("﻿")
            .lines()
            .filter { it.isNotBlank() }

    @Test
    fun `sivu laskee tarkistusarvioinnit osakokeet ja arvosanamuutokset`() {
        hyvaksy(
            tallenna(
                "OPH-1",
                osakokeet = setOf(TutkinnonOsa.PU, TutkinnonOsa.KI),
                muuttuneet = setOf(TutkinnonOsa.PU),
            ),
        )
        hyvaksy(tallenna("OPH-2", osakokeet = setOf(TutkinnonOsa.PU)))
        hyvaksy(tallenna("OPH-3", osakokeet = setOf(TutkinnonOsa.TY), muuttuneet = setOf(TutkinnonOsa.TY)))

        val sivu = sivu(HYVAKSYTTY)

        assertEquals("3", sivu.yhteensa())
        assertEquals(listOf("3", "2", "1"), sivu.rivi("Tarkistusarvioinnit"))
        assertEquals(listOf("4", "2", "2"), sivu.rivi("Osakokeet yhteensä"))
        assertEquals(listOf("2", "1", "1"), sivu.rivi("Puhuminen"))
        assertEquals(listOf("1", "0", "1"), sivu.rivi("Kirjoittaminen"))
        assertEquals(listOf("1", "1", "0"), sivu.rivi("Tekstin ymmärtäminen"))
        assertEquals(listOf("0", "0", "0"), sivu.rivi("Puheen ymmärtäminen"))
    }

    @Test
    fun `kumpikin tila vastaa listasivuaan ja tyhja valinta on niiden summa`() {
        hyvaksy(tallenna("OPH-1"))
        tallenna("OPH-2")
        tallenna("OPH-3")

        assertEquals(listanYhteensa("/yki/tarkistusarvioinnit/hyvaksytyt"), sivu(HYVAKSYTTY).yhteensa())
        assertEquals("1", sivu(HYVAKSYTTY).yhteensa())
        assertEquals(listanYhteensa("/yki/tarkistusarvioinnit"), sivu(ODOTTAA).yhteensa())
        assertEquals("2", sivu(ODOTTAA).yhteensa())
        assertEquals("3", sivu().yhteensa())
        assertEquals("3", sivu("tila=Hyvaksytty,OdottaaHyvaksyntaa").yhteensa())
    }

    @Test
    fun `muut tilat eivat koskaan tule mukaan`() {
        tallenna("OPH-1")
        generateRandomYkiSuoritusEntity()
            .copy(
                arviointitila = Arviointitila.ARVIOITU,
                tarkistusarvioinninAsiatunnus = null,
                tarkistusarvioinninSaapumisPvm = null,
                tarkistusarvioinninKasittelyPvm = null,
                tarkistusarvioidutOsakokeet = null,
                arvosanaMuuttui = null,
                perustelu = null,
            ).also { suoritusRepository.save(it, false) }
        tallenna("OPH-3", tila = Arviointitila.TARKISTUSARVIOITAVA)

        assertEquals("1", sivu().yhteensa())
    }

    @Test
    fun `vain viimeisin versio lasketaan`() {
        val suoritus = tallenna("OPH-1", osakokeet = setOf(TutkinnonOsa.PU), muuttuneet = setOf(TutkinnonOsa.PU))
        suoritusRepository.save(
            suoritus.copy(
                id = null,
                lastModified = Instant.parse("2030-01-01T00:00:00Z"),
                tarkistusarvioidutOsakokeet = setOf(TutkinnonOsa.KI),
                arvosanaMuuttui = null,
            ),
            false,
        )

        val sivu = sivu()

        assertEquals(listOf("1", "0", "1"), sivu.rivi("Tarkistusarvioinnit"))
        assertEquals(listOf("0", "0", "0"), sivu.rivi("Puhuminen"))
        assertEquals(listOf("1", "0", "1"), sivu.rivi("Kirjoittaminen"))
    }

    @Test
    fun `osakoeliitokseton suoritus nakyy omana rivinaan`() {
        tallenna("OPH-1")
        jdbc.execute("DELETE FROM yki_osakoe_tarkistusarviointi")

        val sivu = sivu()

        assertEquals("1", sivu.yhteensa())
        assertEquals(listOf("1", "0", "0"), sivu.rivi("Tarkistusarvioinnit"))
        assertEquals(listOf("1", "", ""), sivu.rivi("Ilman osakoetietoa"))
    }

    @Test
    fun `ryhmittely toistaa lohkon ryhmittain uusin ensin`() {
        tallenna("OPH-1", tutkintopaiva = LocalDate.of(2023, 5, 1), tutkintokieli = Tutkintokieli.SWE)
        tallenna("OPH-2", tutkintopaiva = LocalDate.of(2025, 5, 1), tutkintokieli = Tutkintokieli.FIN)

        val sivu = sivu("aikaryhmittely=Tutkintovuosi&ryhmittely=tutkintokieli")

        assertEquals("2", sivu.yhteensa())
        assertEquals(
            listOf("2025", "2023"),
            sivu.select("td[data-testid=Tutkintovuosi]").map { it.text() }.distinct(),
        )
        assertEquals(
            listOf("suomi", "ruotsi"),
            sivu.select("td[data-testid=Tutkintokieli]").map { it.text() }.distinct(),
        )
        assertEquals(12, sivu.select("tr.tilastorivi").size)
    }

    @Test
    fun `oletuksena ei ryhmitella ja ryhmittely sailyy lomakkeilla`() {
        tallenna("OPH-1")

        val oletus = sivu()
        assertEquals(0, oletus.select("td[data-testid=Tutkintopaiva]").size)
        assertEquals(0, oletus.select("td[data-testid=Tutkintovuosi]").size)
        assertEquals("Ei", oletus.selectFirst("select[name=aikaryhmittely] option[selected]")!!.attr("value"))

        val ryhmitelty = sivu("aikaryhmittely=Tutkintovuosi&ryhmittely=tutkintotaso&$HYVAKSYTTY")
        val rajaus = ryhmitelty.selectFirst("dialog#table-filter-dialog")!!
        assertEquals("Tutkintovuosi", rajaus.selectFirst("input[name=aikaryhmittely]")!!.attr("value"))
        assertEquals("tutkintotaso", rajaus.selectFirst("input[name=ryhmittely]")!!.attr("value"))
        assertEquals(
            "Hyvaksytty",
            ryhmitelty.selectFirst("form.ryhmittely-lomake input[name=tila]")!!.attr("value"),
        )
    }

    @Test
    fun `listasivujen tilastolinkit esivalitsevat oman tilansa ja sailyttavat rajauksen`() {
        tallenna("OPH-1", tutkintokieli = Tutkintokieli.FIN)

        fun linkki(polku: String) =
            Jsoup
                .parse(
                    mockMvc
                        .perform(get(polku).session(virkailijaSession()))
                        .andExpect(status().isOk)
                        .andReturn()
                        .response.contentAsString,
                ).selectFirst("[data-testid=tilastot-linkki]")!!
                .attr("href")

        val odottavat = linkki("/yki/tarkistusarvioinnit?tutkintokieli=FIN&limit=1&page=1")
        assertContains(odottavat, TILASTOT)
        assertContains(odottavat, ODOTTAA)
        assertContains(odottavat, "tutkintokieli=FIN")
        assertFalse(odottavat.contains("page="))
        assertFalse(odottavat.contains("limit="))

        assertContains(linkki("/yki/tarkistusarvioinnit/hyvaksytyt"), HYVAKSYTTY)
    }

    @Test
    fun `listasivut eivat valita tilasta`() {
        hyvaksy(tallenna("OPH-1"))
        tallenna("OPH-2")

        assertEquals("1", listanYhteensa("/yki/tarkistusarvioinnit?$HYVAKSYTTY"))
        assertEquals("1", listanYhteensa("/yki/tarkistusarvioinnit/hyvaksytyt?$ODOTTAA"))
    }

    @Test
    fun `takaisinlinkki vie hyvaksyttyihin vain kun rajaus on pelkka hyvaksytty`() {
        tallenna("OPH-1")

        fun takaisin(query: String) =
            sivu(query).selectFirst("[data-testid=takaisin-tarkistusarviointeihin]")!!.attr("href")

        assertContains(takaisin(HYVAKSYTTY), "/yki/tarkistusarvioinnit/hyvaksytyt")
        assertFalse(takaisin(ODOTTAA).contains("/hyvaksytyt"))
        assertFalse(takaisin("").contains("/hyvaksytyt"))
    }

    @Test
    fun `tilarajaus nakyy kuvauksessa ja dialogissa ja haku rajaa`() {
        hyvaksy(tallenna("OPH-1"))
        tallenna("OPH-2")

        val sivu = sivu("$HYVAKSYTTY&search=OPH-1")

        assertContains(sivu.text(), "Tila: Hyväksytty")
        assertEquals("1", sivu.yhteensa())
        assertEquals(
            listOf("Hyvaksytty"),
            sivu.select("[data-testid=tila-valinta] input[checked]").map { it.attr("value") },
        )
        assertEquals("", sivu(ODOTTAA + "&search=OPH-1").select("tr.tilastorivi").text())
    }

    @Test
    fun `tyhja tulos kertoo ettei tarkistusarviointeja ole`() {
        val sivu = sivu()

        assertContains(sivu.text(), "Ei tarkistusarviointeja valituilla rajauksilla")
        assertEquals("0", sivu.yhteensa())
    }

    @Test
    fun `CSV sisaltaa samat rivit kuin sivu ilman henkilotietoja`() {
        hyvaksy(tallenna("OPH-1", osakokeet = setOf(TutkinnonOsa.PU), muuttuneet = setOf(TutkinnonOsa.PU)))

        val csv = getCsv("/yki/api/tarkistusarvioinnit/tilastot?$HYVAKSYTTY&aikaryhmittely=Tutkintovuosi")

        assertEquals(
            listOf(
                "Tutkintovuosi;Kohde;Yhteensä;Arvosana muuttui;Ei muuttunut",
                "2025;Tarkistusarvioinnit;1;1;0",
                "2025;Osakokeet yhteensä;1;1;0",
                "2025;Puhuminen;1;1;0",
                "2025;Kirjoittaminen;0;0;0",
                "2025;Tekstin ymmärtäminen;0;0;0",
                "2025;Puheen ymmärtäminen;0;0;0",
            ),
            csv.csvRivit(),
        )
        assertContains(
            csv.response.getHeader(HttpHeaders.CONTENT_DISPOSITION).orEmpty(),
            "yki_tarkistusarviointitilastot_hyvaksytty_ryhmittely_tutkintovuosi.csv",
        )
    }

    @Test
    fun `sivu ja CSV vaativat kirjautumisen`() {
        mockMvc.perform(get(TILASTOT)).andExpect(status().is3xxRedirection)
        mockMvc.perform(get("/yki/api/tarkistusarvioinnit/tilastot")).andExpect(status().is3xxRedirection)
    }
}
