package fi.oph.kitu.yki

import fi.oph.kitu.DBContainerConfiguration
import fi.oph.kitu.TestTimeService
import fi.oph.kitu.dev.mockdata.generateRandomYkiSuoritusEntity
import fi.oph.kitu.jdbc.SortDirection
import fi.oph.kitu.virkailijaSession
import fi.oph.kitu.yki.suoritukset.YkiSuoritusEntity
import fi.oph.kitu.yki.suoritukset.YkiSuoritusRepository
import fi.oph.kitu.yki.suoritukset.YkiTarkistusarvioinnitParams
import fi.oph.kitu.yki.suoritukset.YkiTarkistusarviointiColumn
import org.jsoup.Jsoup
import org.jsoup.nodes.Element
import org.junit.jupiter.api.BeforeEach
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.csrf
import org.springframework.security.test.web.servlet.setup.SecurityMockMvcConfigurers.springSecurity
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post
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
import kotlin.test.assertTrue

@SpringBootTest
@Import(DBContainerConfiguration::class)
class YkiTarkistusarvioinnitViewTest(
    @param:Autowired private val context: WebApplicationContext,
    @param:Autowired private val suoritusRepository: YkiSuoritusRepository,
    @param:Autowired private val timeService: TestTimeService,
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
        sukunimi: String,
        asiatunnus: String = "OPH-$sukunimi",
        tutkintokieli: Tutkintokieli = Tutkintokieli.FIN,
        tutkintotaso: Tutkintotaso = Tutkintotaso.PT,
        tutkintopaiva: LocalDate = LocalDate.of(2025, 1, 1),
        saapunut: LocalDate = LocalDate.of(2025, 2, 1),
        kasitelty: LocalDate? = LocalDate.of(2025, 3, 1),
        arvosanaMuuttui: Boolean = true,
        arviointitila: Arviointitila = Arviointitila.TARKISTUSARVIOITU,
        hetu: String? = null,
        osakokeet: Set<TutkinnonOsa> = setOf(TutkinnonOsa.puhuminen),
        muuttuneet: Set<TutkinnonOsa> = osakokeet,
        perustelu: String = "Perustelu $sukunimi",
    ): YkiSuoritusEntity =
        generateRandomYkiSuoritusEntity()
            .copy(
                sukunimi = sukunimi,
                etunimet = "Testi",
                hetu = hetu,
                tutkintokieli = tutkintokieli,
                tutkintotaso = tutkintotaso,
                tutkintopaiva = tutkintopaiva,
                tarkistusarvioinninAsiatunnus = asiatunnus,
                tarkistusarvioinninSaapumisPvm = saapunut,
                tarkistusarvioinninKasittelyPvm = kasitelty,
                tarkistusarvioidutOsakokeet = osakokeet,
                arvosanaMuuttui = if (arvosanaMuuttui) muuttuneet else null,
                perustelu = perustelu,
                arviointitila = arviointitila,
            ).also { suoritusRepository.save(it, false) }

    private fun hae(
        params: YkiTarkistusarvioinnitParams,
        arviointitila: Arviointitila = Arviointitila.TARKISTUSARVIOITU,
    ): List<String> = suoritusRepository.findTarkistusarvoidutSuoritukset(arviointitila, params).map { it.sukunimi }

    private fun rivi(
        html: String,
        sukunimi: String,
    ): Element =
        Jsoup
            .parse(html)
            .select("tbody tr")
            .single { it.selectFirst("td[data-testid=sukunimi]")?.text() == sukunimi }

    private fun Element.sarake(testId: String): Element = selectFirst("td[data-testid=$testId]")!!

    private fun Element.listanRivit(): List<String> = select("li").map { it.text() }

    private fun getHtml(url: String): String =
        mockMvc
            .perform(get(url).session(virkailijaSession()))
            .andExpect(status().isOk)
            .andReturn()
            .response.contentAsString

    @Test
    fun `hyvaksymispaivan oletus on Suomen aikavyohykkeen tama paiva`() {
        tallenna("Virtanen")

        val html =
            timeService.runWithFixedClock(Instant.parse("2026-01-01T22:30:00Z")) {
                getHtml("/yki/tarkistusarvioinnit")
            }

        assertContains(html, """value="2026-01-02"""")
    }

    @Test
    fun `oletusjarjestys on uusin kasittelypaiva ja sen jalkeen uusin saapumispaiva`() {
        tallenna("Vanha", kasitelty = LocalDate.of(2025, 3, 1))
        tallenna("Uusi", kasitelty = LocalDate.of(2025, 4, 1))
        tallenna(
            "Uusi ja myohemmin saapunut",
            kasitelty = LocalDate.of(2025, 4, 1),
            saapunut = LocalDate.of(2025, 3, 1),
        )

        assertEquals(
            listOf("Uusi ja myohemmin saapunut", "Uusi", "Vanha"),
            hae(YkiTarkistusarvioinnitParams()),
        )
    }

    @Test
    fun `sukunimet jarjestetaan suomen aakkosten mukaan molempiin suuntiin`() {
        tallenna("Öhman")
        tallenna("Zetterberg")
        tallenna("Aalto")

        assertEquals(
            listOf("Aalto", "Zetterberg", "Öhman"),
            hae(
                YkiTarkistusarvioinnitParams(
                    sortColumn = YkiTarkistusarviointiColumn.Sukunimi,
                    sortDirection = SortDirection.ASC,
                ),
            ),
        )
        assertEquals(
            listOf("Öhman", "Zetterberg", "Aalto"),
            hae(
                YkiTarkistusarvioinnitParams(
                    sortColumn = YkiTarkistusarviointiColumn.Sukunimi,
                    sortDirection = SortDirection.DESC,
                ),
            ),
        )
    }

    @Test
    fun `puuttuva hyvaksymispaiva jarjestetaan loppuun molempiin suuntiin`() {
        val aiempi = tallenna("Aiempi", arviointitila = Arviointitila.TARKISTUSARVIOITU)
        val myohempi = tallenna("Myohempi", arviointitila = Arviointitila.TARKISTUSARVIOITU)
        tallenna("Ennen ominaisuutta", arviointitila = Arviointitila.TARKISTUSARVIOINTI_HYVAKSYTTY)
        suoritusRepository.hyvaksyTarkistusarvioinnit(listOf(aiempi.solkiId), LocalDate.of(2025, 5, 1))
        suoritusRepository.hyvaksyTarkistusarvioinnit(listOf(myohempi.solkiId), LocalDate.of(2025, 6, 1))

        fun jarjestys(suunta: SortDirection) =
            hae(
                YkiTarkistusarvioinnitParams(
                    sortColumn = YkiTarkistusarviointiColumn.Hyvaksymispaiva,
                    sortDirection = suunta,
                ),
                Arviointitila.TARKISTUSARVIOINTI_HYVAKSYTTY,
            )

        assertEquals(listOf("Aiempi", "Myohempi", "Ennen ominaisuutta"), jarjestys(SortDirection.ASC))
        assertEquals(listOf("Myohempi", "Aiempi", "Ennen ominaisuutta"), jarjestys(SortDirection.DESC))
    }

    @Test
    fun `haku loytaa nimella oppijanumerolla ja asiatunnuksella`() {
        tallenna("Öhman", asiatunnus = "OPH-1-2025")
        val virtanen = tallenna("Virtanen", asiatunnus = "OPH-2-2025")

        assertEquals(listOf("Öhman"), hae(YkiTarkistusarvioinnitParams(search = "öhm")))
        assertEquals(listOf("Virtanen"), hae(YkiTarkistusarvioinnitParams(search = "oph-2")))
        assertEquals(
            listOf("Virtanen"),
            hae(YkiTarkistusarvioinnitParams(search = virtanen.suorittajanOID.toString())),
        )
    }

    @Test
    fun `jokaisen hakusanan on osuttava johonkin sarakkeeseen`() {
        tallenna("Virtanen", asiatunnus = "OPH-2-2025")

        assertEquals(listOf("Virtanen"), hae(YkiTarkistusarvioinnitParams(search = "Virtanen OPH-2")))
        assertEquals(emptyList(), hae(YkiTarkistusarvioinnitParams(search = "Virtanen OPH-1")))
    }

    @Test
    fun `henkilotunnuksella ei voi hakea koska hakusana kulkee URL-parametrina`() {
        tallenna("Virtanen", hetu = "010101A123X")

        assertEquals(emptyList(), hae(YkiTarkistusarvioinnitParams(search = "010101A123X")))
    }

    @Test
    fun `kieli- ja tasosuodattimet ovat monivalintaisia`() {
        tallenna("Suomi perus", tutkintokieli = Tutkintokieli.FIN, tutkintotaso = Tutkintotaso.PT)
        tallenna("Ruotsi perus", tutkintokieli = Tutkintokieli.SWE, tutkintotaso = Tutkintotaso.PT)
        tallenna("Englanti perus", tutkintokieli = Tutkintokieli.ENG, tutkintotaso = Tutkintotaso.PT)
        tallenna("Suomi ylin", tutkintokieli = Tutkintokieli.FIN, tutkintotaso = Tutkintotaso.YT)

        assertEquals(
            setOf("Suomi perus", "Ruotsi perus"),
            hae(
                YkiTarkistusarvioinnitParams(
                    tutkintokieli = listOf(Tutkintokieli.FIN, Tutkintokieli.SWE),
                    tutkintotaso = listOf(Tutkintotaso.PT),
                ),
            ).toSet(),
        )
    }

    @Test
    fun `paivarajaukset ovat molemmista paista inklusiivisia ja koskevat omaa saraketta`() {
        tallenna("Rajalla", tutkintopaiva = LocalDate.of(2025, 1, 10))
        tallenna("Ennen", tutkintopaiva = LocalDate.of(2025, 1, 9))
        tallenna("Jalkeen", tutkintopaiva = LocalDate.of(2025, 1, 11))
        assertEquals(
            listOf("Rajalla"),
            hae(
                YkiTarkistusarvioinnitParams(
                    tutkintoalku = LocalDate.of(2025, 1, 10),
                    tutkintoloppu = LocalDate.of(2025, 1, 10),
                ),
            ),
        )

        tallenna("Saapui", saapunut = LocalDate.of(2024, 6, 1))
        assertEquals(
            listOf("Saapui"),
            hae(
                YkiTarkistusarvioinnitParams(
                    saapumisalku = LocalDate.of(2024, 6, 1),
                    saapumisloppu = LocalDate.of(2024, 6, 1),
                ),
            ),
        )

        tallenna("Kasiteltiin", kasitelty = LocalDate.of(2024, 7, 1))
        assertEquals(
            listOf("Kasiteltiin"),
            hae(
                YkiTarkistusarvioinnitParams(
                    kasittelyalku = LocalDate.of(2024, 7, 1),
                    kasittelyloppu = LocalDate.of(2024, 7, 1),
                ),
            ),
        )
    }

    @Test
    fun `hyvaksymispaivan rajaus kohdistuu hyvaksymispaivaan ja jattaa puuttuvat pois`() {
        val rajalla = tallenna("Rajalla")
        val muualla = tallenna("Muualla")
        tallenna("Ennen ominaisuutta", arviointitila = Arviointitila.TARKISTUSARVIOINTI_HYVAKSYTTY)
        suoritusRepository.hyvaksyTarkistusarvioinnit(listOf(rajalla.solkiId), LocalDate.of(2025, 5, 1))
        suoritusRepository.hyvaksyTarkistusarvioinnit(listOf(muualla.solkiId), LocalDate.of(2025, 5, 2))

        assertEquals(
            listOf("Rajalla"),
            hae(
                YkiTarkistusarvioinnitParams(
                    hyvaksyttyalku = LocalDate.of(2025, 5, 1),
                    hyvaksyttyloppu = LocalDate.of(2025, 5, 1),
                ),
                Arviointitila.TARKISTUSARVIOINTI_HYVAKSYTTY,
            ),
        )
    }

    @Test
    fun `arvosana muuttui -suodatin erottaa muuttuneet muuttumattomista`() {
        tallenna("Muuttui", arvosanaMuuttui = true)
        tallenna("Ei muuttunut", arvosanaMuuttui = false)

        assertEquals(listOf("Muuttui"), hae(YkiTarkistusarvioinnitParams(arvosanaMuuttui = true)))
        assertEquals(listOf("Ei muuttunut"), hae(YkiTarkistusarvioinnitParams(arvosanaMuuttui = false)))
        assertEquals(setOf("Muuttui", "Ei muuttunut"), hae(YkiTarkistusarvioinnitParams()).toSet())
    }

    @Test
    fun `rajaus koskee vain viimeisinta versiota ja pyydettya tilaa`() {
        val hyvaksytty = tallenna("Hyvaksytty", tutkintokieli = Tutkintokieli.FIN)
        tallenna("Odottava", tutkintokieli = Tutkintokieli.FIN)
        suoritusRepository.hyvaksyTarkistusarvioinnit(listOf(hyvaksytty.solkiId), LocalDate.of(2025, 5, 1))

        val params = YkiTarkistusarvioinnitParams(tutkintokieli = listOf(Tutkintokieli.FIN))
        assertEquals(listOf("Odottava"), hae(params))
        assertEquals(listOf("Hyvaksytty"), hae(params, Arviointitila.TARKISTUSARVIOINTI_HYVAKSYTTY))
    }

    @Test
    fun `rajaukset ja jarjestys luetaan URL-parametreista`() {
        tallenna("Aalto", tutkintokieli = Tutkintokieli.FIN)
        tallenna("Öhman", tutkintokieli = Tutkintokieli.SWE)
        tallenna("Rajattu pois", tutkintokieli = Tutkintokieli.ENG)

        listOf(
            "tutkintokieli=FIN,SWE",
            "tutkintokieli=FIN&tutkintokieli=SWE",
        ).forEach { kielet ->
            val html =
                getHtml(
                    "/yki/tarkistusarvioinnit?$kielet&arvosanaMuuttui=true&tutkintoalku=2025-01-01" +
                        "&sortColumn=sukunimi&sortDirection=DESC",
                )

            assertFalse(html.contains("Rajattu pois"))
            assertTrue(html.indexOf("Öhman") < html.indexOf("Aalto"), "Väärä järjestys parametreilla $kielet")
        }
    }

    @Test
    fun `tyhjina lahetetyt rajauskentat eivat kaada nakymaa`() {
        tallenna("Virtanen")

        val html =
            getHtml(
                "/yki/tarkistusarvioinnit?search=&tutkintoalku=&tutkintoloppu=&saapumisalku=&saapumisloppu=" +
                    "&kasittelyalku=&kasittelyloppu=&arvosanaMuuttui=",
            )

        assertContains(html, "Virtanen")
    }

    @Test
    fun `osakoesarakkeiden otsikot eivat ole lajittelulinkkeja`() {
        tallenna("Virtanen")

        val html = getHtml("/yki/tarkistusarvioinnit")

        assertContains(html, "sortColumn=sukunimi")
        assertContains(html, "sortColumn=saapunut")
        assertFalse(html.contains("sortColumn=osat"))
        assertFalse(html.contains("sortColumn=muuttuneet"))
    }

    @Test
    fun `osat ja muuttuneet listataan omissa sarakkeissaan osakokeiden jarjestyksessa`() {
        tallenna(
            "Virtanen",
            osakokeet = setOf(TutkinnonOsa.kirjoittaminen, TutkinnonOsa.puhuminen),
            muuttuneet = setOf(TutkinnonOsa.kirjoittaminen),
        )

        val rivi = rivi(getHtml("/yki/tarkistusarvioinnit"), "Virtanen")

        assertEquals(listOf("Puhuminen", "Kirjoittaminen"), rivi.sarake("osat").listanRivit())
        assertEquals(listOf("Kirjoittaminen"), rivi.sarake("muuttuneet").listanRivit())
    }

    @Test
    fun `muuttuneissa on viiva kun kasitelty tarkistusarviointi ei muuttanut arvosanoja`() {
        tallenna("Virtanen", arvosanaMuuttui = false)

        val solu = rivi(getHtml("/yki/tarkistusarvioinnit"), "Virtanen").sarake("muuttuneet")

        assertEquals("–", solu.selectFirst(".muuttuneet-osakokeet > div")!!.text())
        assertEquals(emptyList(), solu.selectFirst(".muuttuneet-osakokeet > div")!!.select("li").map { it.text() })
    }

    @Test
    fun `muuttuneet on tyhja kun kasittelypaivaa ei ole`() {
        tallenna(
            "Kesken",
            kasitelty = null,
            arvosanaMuuttui = false,
            arviointitila = Arviointitila.TARKISTUSARVIOINTI_HYVAKSYTTY,
        )

        val solu = rivi(getHtml("/yki/tarkistusarvioinnit/hyvaksytyt"), "Kesken").sarake("muuttuneet")

        assertEquals("", solu.selectFirst(".muuttuneet-osakokeet > div")!!.text())
    }

    @Test
    fun `perustelu avataan rivikohtaisesta modaalista`() {
        val virtanen = tallenna("Virtanen", perustelu = "Perustelu <b>Virtaselle</b>")
        val aalto = tallenna("Aalto", perustelu = "Perustelu Aallolle")

        val html = getHtml("/yki/tarkistusarvioinnit")

        listOf(
            Triple("Virtanen", virtanen.solkiId, "Perustelu <b>Virtaselle</b>"),
            Triple("Aalto", aalto.solkiId, "Perustelu Aallolle"),
        ).forEach { (sukunimi, solkiId, perustelu) ->
            val solu = rivi(html, sukunimi).sarake("muuttuneet")
            val nappi = solu.selectFirst("button.info-button")!!
            assertEquals("perustelu-$solkiId", nappi.attr("commandfor"))
            assertEquals("show-modal", nappi.attr("command"))
            assertEquals("button", nappi.attr("type"))
            assertEquals("Näytä perustelu", nappi.attr("aria-label"))

            val modaali = solu.selectFirst("dialog#perustelu-$solkiId")!!
            assertEquals(perustelu, modaali.selectFirst("p")!!.text())
            assertEquals("close", modaali.selectFirst("footer button")!!.attr("command"))
            assertEquals("button", modaali.selectFirst("footer button")!!.attr("type"))
        }
        assertFalse(html.contains("<b>Virtaselle</b>"))
    }

    @Test
    fun `tyhjalle perustelulle ei nayteta info-painiketta`() {
        tallenna("Virtanen", perustelu = "   ")

        val solu = rivi(getHtml("/yki/tarkistusarvioinnit"), "Virtanen").sarake("muuttuneet")

        assertEquals(null, solu.selectFirst("button.info-button"))
        assertEquals(null, solu.selectFirst("dialog"))
    }

    @Test
    fun `hyvaksymispaivan sarake ja rajaus ovat vain hyvaksyttyjen sivulla`() {
        tallenna("Odottava")
        val hyvaksytty = tallenna("Hyvaksytty")
        suoritusRepository.hyvaksyTarkistusarvioinnit(listOf(hyvaksytty.solkiId), LocalDate.of(2025, 5, 1))

        val odottavat = getHtml("/yki/tarkistusarvioinnit")
        assertFalse(odottavat.contains("sortColumn=hyvaksytty"))
        assertFalse(odottavat.contains("""name="hyvaksyttyalku""""))

        val hyvaksytyt = getHtml("/yki/tarkistusarvioinnit/hyvaksytyt")
        assertContains(hyvaksytyt, "sortColumn=hyvaksytty")
        assertContains(hyvaksytyt, """name="hyvaksyttyalku"""")
        assertContains(hyvaksytyt, "1.5.2025")
    }

    @Test
    fun `tyhja hakutulos sailyttaa haun ja rajauksen muttei hyvaksyntalomaketta`() {
        tallenna("Virtanen")

        val html = getHtml("/yki/tarkistusarvioinnit?search=eiloydy")

        assertContains(html, """name="search"""")
        assertContains(html, """value="eiloydy"""")
        assertContains(html, "table-filter-dialog")
        assertFalse(html.contains("odottaaHyvaksyntaaTable"))
        assertFalse(html.contains("""name="hyvaksyttyPvm""""))
    }

    @Test
    fun `hyvaksynnan jalkeen palataan samaan rajaukseen ja jarjestykseen`() {
        listOf(
            "/yki/tarkistusarvioinnit",
            "/yki/tarkistusarvioinnit/hyvaksytyt",
        ).forEach { polku ->
            val location =
                mockMvc
                    .perform(
                        post("$polku?search=Öhman&tutkintokieli=FIN,SWE&sortColumn=sukunimi&sortDirection=ASC")
                            .session(virkailijaSession())
                            .with(csrf())
                            .param("hyvaksyttyPvm", "2025-05-01"),
                    ).andExpect(status().is3xxRedirection)
                    .andReturn()
                    .response.redirectedUrl
                    .orEmpty()

            assertContains(location, "$polku?")
            assertContains(location, "search=%C3%96hman")
            assertContains(location, "tutkintokieli=FIN%2CSWE")
            assertContains(location, "sortColumn=sukunimi")
            assertContains(location, "sortDirection=ASC")
            assertFalse(location.contains("hyvaksyttyPvm"))
        }
    }
}
