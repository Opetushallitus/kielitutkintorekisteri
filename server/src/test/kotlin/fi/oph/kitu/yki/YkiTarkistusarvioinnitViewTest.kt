package fi.oph.kitu.yki

import arrow.core.Either
import arrow.core.left
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import fi.oph.kitu.DBContainerConfiguration
import fi.oph.kitu.TestTimeService
import fi.oph.kitu.VIRKAILIJAN_OID
import fi.oph.kitu.auditlogs.AUDIT_LOGGER_NAME
import fi.oph.kitu.dev.mockdata.generateRandomOppijaOid
import fi.oph.kitu.dev.mockdata.generateRandomYkiSuoritusEntity
import fi.oph.kitu.jdbc.SortDirection
import fi.oph.kitu.oid.Oid
import fi.oph.kitu.oppijanumero.OppijanumeroException
import fi.oph.kitu.oppijanumero.OppijanumeroService
import fi.oph.kitu.virkailijaSession
import fi.oph.kitu.yki.suoritukset.YkiSuoritusEntity
import fi.oph.kitu.yki.suoritukset.YkiSuoritusRepository
import fi.oph.kitu.yki.suoritukset.YkiTarkistusarvioinnitParams
import fi.oph.kitu.yki.suoritukset.YkiTarkistusarviointiColumn
import org.jsoup.Jsoup
import org.jsoup.nodes.Element
import org.junit.jupiter.api.BeforeEach
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.mock.web.MockHttpServletRequest
import org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.csrf
import org.springframework.security.test.web.servlet.setup.SecurityMockMvcConfigurers.springSecurity
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.MvcResult
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.asyncDispatch
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.request
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.status
import org.springframework.test.web.servlet.setup.DefaultMockMvcBuilder
import org.springframework.test.web.servlet.setup.MockMvcBuilders
import org.springframework.web.context.WebApplicationContext
import org.springframework.web.context.request.RequestContextHolder
import org.springframework.web.context.request.ServletRequestAttributes
import java.time.Instant
import java.time.LocalDate
import kotlin.test.Test
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue
import ch.qos.logback.classic.Logger as LogbackLogger

private const val PAAHENKILON_OID = "1.2.246.562.24.67409348034"
private const val DUPLIKAATIN_OID = "1.2.246.562.24.00000000177"

@SpringBootTest
@Import(DBContainerConfiguration::class)
class YkiTarkistusarvioinnitViewTest(
    @param:Autowired private val context: WebApplicationContext,
    @param:Autowired private val suoritusRepository: YkiSuoritusRepository,
    @param:Autowired private val timeService: TestTimeService,
    @param:Autowired private val jdbc: JdbcTemplate,
) {
    private lateinit var mockMvc: MockMvc
    private var seuraavaSolkiId = 100_000

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
        oppijanumero: Oid? = generateRandomOppijaOid(),
    ): YkiSuoritusEntity =
        generateRandomYkiSuoritusEntity()
            .copy(
                solkiId = seuraavaSolkiId++,
                suorittajanOID = oppijanumero,
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
    ): List<String> =
        suoritusRepository
            .findTarkistusarvoidutSuoritukset(arviointitila, params.toFilter())
            .map { it.sukunimi }

    private inline fun <reified T : Any> bean(): T = context.getBean(T::class.java)

    private fun rikkinainenOnr(): OppijanumeroService =
        object : OppijanumeroService by bean<OppijanumeroService>() {
            override fun getLinkedOids(henkiloOid: Oid): Either<OppijanumeroException, Set<Oid>> =
                OppijanumeroException.MalformedOppijanumero(oppijanumero = "rikki").left()
        }

    private fun palveluIlmanOnr() = YkiService(suoritusRepository, bean(), rikkinainenOnr(), timeService, "0s")

    private fun <T> pyynnossa(block: () -> T): T {
        RequestContextHolder.setRequestAttributes(ServletRequestAttributes(MockHttpServletRequest()))
        try {
            return block()
        } finally {
            RequestContextHolder.resetRequestAttributes()
        }
    }

    private fun sukunimet(html: String): Set<String> =
        Jsoup
            .parse(html)
            .select("td[data-testid=sukunimi]")
            .map { it.text() }
            .toSet()

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

    private fun odotaAuditoitu(
        loki: ListAppender<ILoggingEvent>,
        suoritusId: String,
    ): Boolean {
        val loppu = System.currentTimeMillis() + 5_000
        while (System.currentTimeMillis() < loppu) {
            val auditoitu =
                synchronized(loki) { loki.list.toList() }
                    .filter { it.message == "Yki suoritus viewed" }
                    .flatMap { it.keyValuePairs.orEmpty() }
                    .any { it.key == "suoritus.id" && it.value.toString() == suoritusId }
            if (auditoitu) return true
            Thread.sleep(20)
        }
        return false
    }

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
    fun `oppijanumerohaku loytaa yhdistetyn duplikaatin suoritukset molemmilla oppijanumeroilla`() {
        tallenna("Paahenkilo", oppijanumero = Oid(PAAHENKILON_OID))
        tallenna("Duplikaatti", oppijanumero = Oid(DUPLIKAATIN_OID))
        tallenna("Muu")
        val molemmat = setOf("Paahenkilo", "Duplikaatti")

        listOf(PAAHENKILON_OID, DUPLIKAATIN_OID).forEach { oid ->
            assertEquals(molemmat, sukunimet(getHtml("/yki/tarkistusarvioinnit?search=$oid")))
            assertEquals(
                molemmat,
                getCsv("/yki/api/tarkistusarvioinnit?search=$oid")
                    .csvRivit()
                    .drop(1)
                    .map { it.split(";")[1] }
                    .toSet(),
            )
        }
    }

    @Test
    fun `oppijanumerohaku rajautuu myos muilla hakusanoilla`() {
        tallenna("Paahenkilo", asiatunnus = "OPH-1", oppijanumero = Oid(PAAHENKILON_OID))
        tallenna("Duplikaatti", asiatunnus = "OPH-2", oppijanumero = Oid(DUPLIKAATIN_OID))

        assertEquals(
            setOf("Duplikaatti"),
            sukunimet(getHtml("/yki/tarkistusarvioinnit?search=$PAAHENKILON_OID,OPH-2")),
        )
    }

    @Test
    fun `oppijanumeron ja asiatunnuksen osilla haetaan yha tekstina`() {
        tallenna("Virtanen", asiatunnus = "OPH-5000-1", oppijanumero = Oid("1.2.246.562.24.20281155246"))
        tallenna("Muu", asiatunnus = "OPH-7", oppijanumero = Oid("1.2.246.562.24.10000000001"))

        listOf("20281155246", ".24.2028", "5000", "OPH-5000").forEach { hakusana ->
            assertEquals(listOf("Virtanen"), hae(YkiTarkistusarvioinnitParams(search = hakusana)), hakusana)
        }
    }

    @Test
    fun `sivut varoittavat ja hakevat annetulla oppijanumerolla kun oppijanumerorekisteri ei vastaa`() {
        tallenna("Duplikaatti", oppijanumero = Oid(DUPLIKAATIN_OID))
        tallenna("Paahenkilo", oppijanumero = Oid(PAAHENKILON_OID))
        val controller =
            YkiViewController(palveluIlmanOnr(), bean(), bean(), bean(), rikkinainenOnr(), bean(), timeService)
        val params = YkiTarkistusarvioinnitParams(search = DUPLIKAATIN_OID)
        val varoitus = "Oppijanumerorekisteriin ei juuri nyt saatu yhteyttä"

        val odottavat = pyynnossa { controller.tarkistusArvioinnitView(params) }.body.orEmpty()
        assertContains(odottavat, varoitus)
        assertEquals(setOf("Duplikaatti"), sukunimet(odottavat))

        assertContains(pyynnossa { controller.hyvaksytytTarkistusArvioinnitView(params) }.body.orEmpty(), varoitus)
    }

    @Test
    fun `sivu ei varoita kun haussa ei ole oppijanumeroa`() {
        tallenna("Virtanen")
        val controller =
            YkiViewController(palveluIlmanOnr(), bean(), bean(), bean(), rikkinainenOnr(), bean(), timeService)

        val html =
            pyynnossa {
                controller.tarkistusArvioinnitView(YkiTarkistusarvioinnitParams(search = "Virtanen"))
            }.body.orEmpty()

        assertFalse(html.contains("Oppijanumerorekisteriin ei juuri nyt saatu yhteyttä"))
        assertEquals(setOf("Virtanen"), sukunimet(html))
    }

    @Test
    fun `CSV epaonnistuu oppijanumerohaulla kun oppijanumerorekisteri ei vastaa`() {
        val api = YkiApiController(palveluIlmanOnr(), bean(), bean(), bean(), bean(), bean(), bean(), bean())
        val oppijanumerolla = YkiTarkistusarvioinnitParams(search = DUPLIKAATIN_OID)

        assertFailsWith<OppijanumeroException.MalformedOppijanumero> {
            api.getTarkistusarvioinnitCsv(oppijanumerolla)
        }
        assertFailsWith<OppijanumeroException.MalformedOppijanumero> {
            api.getHyvaksytytTarkistusarvioinnitCsv(oppijanumerolla)
        }
        assertEquals(
            HttpStatus.OK,
            api.getTarkistusarvioinnitCsv(YkiTarkistusarvioinnitParams(search = "Virtanen")).statusCode,
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
    fun `osasuodatin on monivalintainen ja katsoo vain tarkistusarvioituja osia`() {
        tallenna("Puhuminen", osakokeet = setOf(TutkinnonOsa.puhuminen))
        tallenna("Kirjoittaminen", osakokeet = setOf(TutkinnonOsa.kirjoittaminen))
        tallenna("Molemmat", osakokeet = setOf(TutkinnonOsa.puhuminen, TutkinnonOsa.kirjoittaminen))
        tallenna("Tekstin ymmartaminen", osakokeet = setOf(TutkinnonOsa.tekstinYmmartaminen))

        assertEquals(
            setOf("Puhuminen", "Molemmat"),
            hae(YkiTarkistusarvioinnitParams(osa = listOf(TutkinnonOsa.puhuminen))).toSet(),
        )
        assertEquals(
            setOf("Puhuminen", "Kirjoittaminen", "Molemmat"),
            hae(
                YkiTarkistusarvioinnitParams(osa = listOf(TutkinnonOsa.puhuminen, TutkinnonOsa.kirjoittaminen)),
            ).toSet(),
        )
        assertEquals(emptyList(), hae(YkiTarkistusarvioinnitParams(osa = listOf(TutkinnonOsa.puheenYmmartaminen))))
    }

    @Test
    fun `osasuodatin luetaan URL-parametreista ja se nakyy rajausdialogissa kuvauksessa ja lajittelulinkeissa`() {
        tallenna("Puhuminen", osakokeet = setOf(TutkinnonOsa.puhuminen))
        tallenna("Kirjoittaminen", osakokeet = setOf(TutkinnonOsa.kirjoittaminen))
        tallenna("Tekstin ymmartaminen", osakokeet = setOf(TutkinnonOsa.tekstinYmmartaminen))

        listOf("osa=PU,KI", "osa=PU&osa=KI").forEach { osat ->
            val html = getHtml("/yki/tarkistusarvioinnit?$osat")
            val sivu = Jsoup.parse(html)

            assertEquals(
                setOf("Puhuminen", "Kirjoittaminen"),
                sivu.select("td[data-testid=sukunimi]").map { it.text() }.toSet(),
            )

            val valikko = sivu.selectFirst("[data-testid=osa-valinta]")!!
            assertEquals(listOf("PU", "KI"), valikko.select("input[checked]").map { it.attr("value") })
            assertEquals(
                listOf(
                    "Puhuminen",
                    "Kirjoittaminen",
                    "Tekstin ymmärtäminen",
                    "Puheen ymmärtäminen",
                ),
                valikko.select("label").map { it.text() },
            )

            assertContains(html, "Osat: Puhuminen, Kirjoittaminen")
            assertContains(sivu.selectFirst("th a[href*=sortColumn=sukunimi]")!!.attr("href"), "osa=PU")
        }
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
                    "&kasittelyalku=&kasittelyloppu=&arvosanaMuuttui=&osa=",
            )

        assertContains(html, "Virtanen")
    }

    @Test
    fun `oppijanumero naytetaan kopioitavana ja jarjestettavana molemmilla sivuilla`() {
        val odottava = tallenna("Odottava")
        val hyvaksytty = tallenna("Hyvaksytty")
        suoritusRepository.hyvaksyTarkistusarvioinnit(listOf(hyvaksytty.solkiId), LocalDate.of(2025, 5, 1))

        listOf(
            "/yki/tarkistusarvioinnit" to odottava,
            "/yki/tarkistusarvioinnit/hyvaksytyt" to hyvaksytty,
        ).forEach { (polku, suoritus) ->
            val html = getHtml(polku)
            val solu = rivi(html, suoritus.sukunimi).sarake("suorittajanoid")

            assertEquals(suoritus.suorittajanOID.toString(), solu.text())
            assertEquals(suoritus.suorittajanOID.toString(), solu.selectFirst("button.copy-button")!!.attr("data-copy"))
            assertContains(html, "sortColumn=suorittajanoid")
        }
    }

    @Test
    fun `oppijanumeron mukaan jarjestetaan ja puuttuva oppijanumero jaa loppuun molempiin suuntiin`() {
        tallenna("Suurempi", oppijanumero = Oid("1.2.246.562.24.20000000000"))
        tallenna("Pienempi", oppijanumero = Oid("1.2.246.562.24.10000000000"))
        tallenna("Historia", oppijanumero = null)

        fun jarjestys(suunta: SortDirection) =
            hae(
                YkiTarkistusarvioinnitParams(
                    sortColumn = YkiTarkistusarviointiColumn.SuorittajanOid,
                    sortDirection = suunta,
                ),
            )

        assertEquals(listOf("Pienempi", "Suurempi", "Historia"), jarjestys(SortDirection.ASC))
        assertEquals(listOf("Suurempi", "Pienempi", "Historia"), jarjestys(SortDirection.DESC))
    }

    @Test
    fun `oppijanumeron solu on tyhja ilman kopiointipainiketta kun oppijanumeroa ei ole`() {
        tallenna("Historia", oppijanumero = null)

        val solu = rivi(getHtml("/yki/tarkistusarvioinnit"), "Historia").sarake("suorittajanoid")

        assertEquals("", solu.text())
        assertEquals(null, solu.selectFirst("button.copy-button"))
    }

    @Test
    fun `odottavien CSV sisaltaa taulukon sarakkeet ja perustelun`() {
        val virtanen =
            tallenna(
                "Virtanen",
                osakokeet = setOf(TutkinnonOsa.kirjoittaminen, TutkinnonOsa.puhuminen),
                muuttuneet = setOf(TutkinnonOsa.kirjoittaminen),
                perustelu = "Perustelu Virtaselle",
            )

        assertEquals(
            listOf(
                listOf(
                    "Oppijanumero",
                    "Sukunimi",
                    "Etunimet",
                    "Kieli",
                    "Taso",
                    "Tutkintopäivä",
                    "Saapunut",
                    "Käsitelty",
                    "Asiatunnus",
                    "Osat",
                    "Muuttuneet",
                    "Perustelu",
                ),
                listOf(
                    virtanen.suorittajanOID.toString(),
                    "Virtanen",
                    "Testi",
                    "FIN",
                    "PT",
                    "1.1.2025",
                    "1.2.2025",
                    "1.3.2025",
                    "OPH-Virtanen",
                    "Puhuminen, Kirjoittaminen",
                    "Kirjoittaminen",
                    "Perustelu Virtaselle",
                ),
            ).map { it.joinToString(";") },
            getCsv("/yki/api/tarkistusarvioinnit").csvRivit(),
        )
    }

    @Test
    fun `hyvaksyttyjen CSV sisaltaa hyvaksymispaivan eika odottavia`() {
        val hyvaksytty = tallenna("Hyvaksytty")
        tallenna("Odottava")
        suoritusRepository.hyvaksyTarkistusarvioinnit(listOf(hyvaksytty.solkiId), LocalDate.of(2025, 5, 1))

        val rivit = getCsv("/yki/api/tarkistusarvioinnit/hyvaksytyt").csvRivit()

        assertEquals(2, rivit.size)
        assertContains(rivit[0], "Käsitelty;Hyväksytty;Asiatunnus")
        assertContains(rivit[1], ";Hyvaksytty;")
        assertContains(rivit[1], ";1.3.2025;1.5.2025;OPH-Hyvaksytty;")
    }

    @Test
    fun `CSV noudattaa rajausta ja jarjestysta ja tiedostonimi kertoo rajauksen`() {
        tallenna("Aalto", tutkintokieli = Tutkintokieli.FIN, osakokeet = setOf(TutkinnonOsa.puhuminen))
        tallenna("Öhman", tutkintokieli = Tutkintokieli.SWE, osakokeet = setOf(TutkinnonOsa.kirjoittaminen))
        tallenna("Muu kieli", tutkintokieli = Tutkintokieli.ENG, osakokeet = setOf(TutkinnonOsa.puhuminen))
        tallenna("Muu osa", tutkintokieli = Tutkintokieli.FIN, osakokeet = setOf(TutkinnonOsa.tekstinYmmartaminen))

        val csv =
            getCsv(
                "/yki/api/tarkistusarvioinnit?tutkintokieli=FIN,SWE&osa=PU,KI&kasittelyalku=2025-03-01" +
                    "&arvosanaMuuttui=true&sortColumn=sukunimi&sortDirection=DESC",
            )

        assertEquals(listOf("Öhman", "Aalto"), csv.csvRivit().drop(1).map { it.split(";")[1] })
        assertContains(
            csv.response.getHeader(HttpHeaders.CONTENT_DISPOSITION).orEmpty(),
            "yki_tarkistusarvioinnit_henkilotiedot_FIN-SWE_PU-KI_kasitelty_2025-03-01_arvosana_muuttui.csv",
        )
    }

    @Test
    fun `sivun CSV-linkki osoittaa sivun omaan vientiin ja sailyttaa rajauksen`() {
        tallenna("Virtanen")

        fun csvLinkki(url: String) = Jsoup.parse(getHtml(url)).selectFirst("a[download]")!!.attr("href")

        val odottavat = csvLinkki("/yki/tarkistusarvioinnit?osa=PU&tutkintokieli=FIN&search=Virtanen")
        assertContains(odottavat, "/yki/api/tarkistusarvioinnit?")
        listOf("osa=PU", "tutkintokieli=FIN", "search=Virtanen", "sortColumn=kasitelty").forEach {
            assertContains(odottavat, it)
        }

        assertContains(csvLinkki("/yki/tarkistusarvioinnit/hyvaksytyt"), "/yki/api/tarkistusarvioinnit/hyvaksytyt?")
    }

    @Test
    fun `CSV ei sisalla henkilotunnusta`() {
        tallenna("Virtanen", hetu = "010101A123X")

        assertFalse(getCsv("/yki/api/tarkistusarvioinnit").csvRivit().any { it.contains("010101A123X") })
    }

    @Test
    fun `CSV vaatii kirjautumisen`() {
        mockMvc.perform(get("/yki/api/tarkistusarvioinnit")).andExpect(status().is3xxRedirection)
    }

    @Test
    fun `perustelu ei ole taulukossa omana sarakkeenaan`() {
        tallenna("Virtanen")

        val html = getHtml("/yki/tarkistusarvioinnit")

        assertEquals(null, rivi(html, "Virtanen").selectFirst("td[data-testid=perustelu]"))
        assertFalse(html.contains("sortColumn=perustelu"))
    }

    @Test
    fun `taulukko ja CSV kirjaavat naytetyt suoritukset auditlokiin`() {
        tallenna("Virtanen")
        val id =
            suoritusRepository
                .findTarkistusarvoidutSuoritukset(Arviointitila.TARKISTUSARVIOITU)
                .single()
                .id
                .toString()
        val auditLogger = LoggerFactory.getLogger(AUDIT_LOGGER_NAME) as LogbackLogger

        listOf<() -> Unit>(
            { getHtml("/yki/tarkistusarvioinnit") },
            { getCsv("/yki/api/tarkistusarvioinnit") },
        ).forEach { nayta ->
            val loki = ListAppender<ILoggingEvent>().apply { start() }
            auditLogger.addAppender(loki)
            try {
                nayta()
                assertTrue(odotaAuditoitu(loki, id))
            } finally {
                auditLogger.detachAppender(loki)
                loki.stop()
            }
        }
    }

    private fun hyvaksy(vararg suoritukset: YkiSuoritusEntity) {
        mockMvc
            .perform(
                post("/yki/tarkistusarvioinnit")
                    .session(virkailijaSession())
                    .with(csrf())
                    .param("suoritukset", *suoritukset.map { it.solkiId.toString() }.toTypedArray())
                    .param("hyvaksyttyPvm", "2025-05-01"),
            ).andExpect(status().is3xxRedirection)
    }

    private fun <T> auditlokilla(block: (ListAppender<ILoggingEvent>) -> T): T {
        val auditLogger = LoggerFactory.getLogger(AUDIT_LOGGER_NAME) as LogbackLogger
        val loki = ListAppender<ILoggingEvent>().apply { start() }
        auditLogger.addAppender(loki)
        try {
            return block(loki)
        } finally {
            auditLogger.detachAppender(loki)
            loki.stop()
        }
    }

    private fun ListAppender<ILoggingEvent>.tapahtumat(): List<ILoggingEvent> = synchronized(this) { list.toList() }

    private fun ListAppender<ILoggingEvent>.hyvaksyntaoperaatiot(): List<String> =
        tapahtumat().map { it.formattedMessage }.filter { it.contains("YkiTarkistusarviointiHyvaksytty") }

    private fun odotaHyvaksyntarivit(
        loki: ListAppender<ILoggingEvent>,
        maara: Int,
    ): List<Map<String, String?>> {
        val loppu = System.currentTimeMillis() + 5_000
        while (true) {
            val rivit =
                loki
                    .tapahtumat()
                    .filter { it.message == "Yki tarkistusarviointi approved" }
                    .map { tapahtuma -> tapahtuma.keyValuePairs.orEmpty().associate { it.key to it.value?.toString() } }
            if (rivit.size >= maara || System.currentTimeMillis() > loppu) return rivit
            Thread.sleep(20)
        }
    }

    @Test
    fun `hyvaksynta kirjataan auditlokiin myos ilman oppijanumeroa`() {
        val virtanen = tallenna("Virtanen")
        val nieminen = tallenna("Nieminen", oppijanumero = null)

        auditlokilla { loki ->
            hyvaksy(virtanen, nieminen)

            val hyvaksytytVersiot =
                suoritusRepository
                    .findLatestBySolkiIds(listOf(virtanen.solkiId, nieminen.solkiId))
                    .associateBy { it.solkiId }
            assertEquals(
                listOf(virtanen, nieminen)
                    .map { suoritus ->
                        mapOf(
                            "suoritus.id" to hyvaksytytVersiot.getValue(suoritus.solkiId).id.toString(),
                            "suoritus.solki_id" to suoritus.solkiId.toString(),
                            "suoritus.suorittajan_oid" to suoritus.suorittajanOID?.toString(),
                            "tarkistusarviointi.hyvaksymispaiva" to "2025-05-01",
                            "principal.oid" to VIRKAILIJAN_OID,
                        )
                    }.toSet(),
                odotaHyvaksyntarivit(loki, 2).toSet(),
            )

            val operaatio = loki.hyvaksyntaoperaatiot().single()
            assertContains(operaatio, virtanen.suorittajanOID.toString())
            assertContains(operaatio, VIRKAILIJAN_OID)
        }
    }

    @Test
    fun `epaonnistunutta hyvaksyntaa ei kirjata auditlokiin`() {
        val kesken = tallenna("Kesken", kasitelty = null)

        auditlokilla { loki ->
            hyvaksy(kesken)

            assertEquals(emptyList(), loki.hyvaksyntaoperaatiot())
        }
    }

    private fun rivimaara(html: String): String = Jsoup.parse(html).selectFirst("[data-testid=numberOfRows]")!!.text()

    private val sukunimijarjestys = "/yki/tarkistusarvioinnit?sortColumn=sukunimi&sortDirection=ASC"

    @Test
    fun `sivutus rajaa rivit mutta yhteismaara kertoo kaikki`() {
        listOf("Aalto", "Berg", "Cedar").forEach { tallenna(it) }

        val ensimmainen = getHtml("$sukunimijarjestys&limit=2")
        assertEquals(setOf("Aalto", "Berg"), sukunimet(ensimmainen))
        assertEquals("3", rivimaara(ensimmainen))
        assertNotNull(Jsoup.parse(ensimmainen).selectFirst("nav.pagination"))

        val toinen = getHtml("$sukunimijarjestys&limit=2&page=2")
        assertEquals(setOf("Cedar"), sukunimet(toinen))
        assertEquals("3", rivimaara(toinen))
    }

    @Test
    fun `oletussivukoko on sata ja sivutus nakyy vasta kun rivit eivat mahdu yhdelle sivulle`() {
        repeat(100) { tallenna("Henkilo$it") }
        val tasan = getHtml("/yki/tarkistusarvioinnit")
        assertEquals(100, sukunimet(tasan).size)
        assertEquals(null, Jsoup.parse(tasan).selectFirst("nav.pagination"))

        tallenna("Henkilo100")
        val yli = getHtml("/yki/tarkistusarvioinnit")
        assertEquals(100, sukunimet(yli).size)
        assertEquals("101", rivimaara(yli))
        assertNotNull(Jsoup.parse(yli).selectFirst("nav.pagination"))
    }

    @Test
    fun `sivunumero ja sivukoko rajataan kelvollisiin arvoihin`() {
        listOf("Aalto", "Berg", "Cedar").forEach { tallenna(it) }

        assertEquals(setOf("Cedar"), sukunimet(getHtml("$sukunimijarjestys&limit=2&page=99")))
        assertEquals(setOf("Aalto", "Berg"), sukunimet(getHtml("$sukunimijarjestys&limit=2&page=0")))
        assertEquals(setOf("Aalto", "Berg"), sukunimet(getHtml("$sukunimijarjestys&limit=2&page=-1")))
        assertEquals(setOf("Aalto"), sukunimet(getHtml("$sukunimijarjestys&limit=0")))
        assertEquals(setOf("Aalto"), sukunimet(getHtml("$sukunimijarjestys&limit=-5")))
    }

    @Test
    fun `viimeisen sivun tyhjentava hyvaksynta nayttaa edellisen sivun lomakkeineen`() {
        tallenna("Aalto")
        val berg = tallenna("Berg")
        suoritusRepository.hyvaksyTarkistusarvioinnit(listOf(berg.solkiId), LocalDate.of(2025, 5, 1))

        val html = getHtml("$sukunimijarjestys&limit=1&page=2")

        assertEquals(setOf("Aalto"), sukunimet(html))
        assertEquals("1", rivimaara(html))
        assertContains(html, """name="hyvaksyttyPvm"""")
    }

    @Test
    fun `sivulinkit sailyttavat rajauksen mutta uusi haku ja rajaus palaavat alkuun`() {
        listOf("Aalto", "Berg", "Cedar").forEach { tallenna(it) }

        val sivu = Jsoup.parse(getHtml("$sukunimijarjestys&limit=1&page=2&tutkintokieli=FIN"))

        val seuraava = sivu.selectFirst("nav.pagination li.next a")!!.attr("href")
        listOf("page=3", "limit=1", "tutkintokieli=FIN", "sortColumn=sukunimi").forEach {
            assertContains(seuraava, it)
        }
        assertEquals(null, sivu.selectFirst("form:has(input[name=search]) input[name=page]"))
        assertEquals(null, sivu.selectFirst("dialog#table-filter-dialog input[name=page]"))
    }

    @Test
    fun `CSV sisaltaa kaikki rivit eika sen linkki kanna sivua`() {
        listOf("Aalto", "Berg", "Cedar").forEach { tallenna(it) }

        val rivit = getCsv("/yki/api/tarkistusarvioinnit?limit=1&page=2").csvRivit()
        assertEquals(setOf("Aalto", "Berg", "Cedar"), rivit.drop(1).map { it.split(";")[1] }.toSet())

        val linkki =
            Jsoup
                .parse(getHtml("$sukunimijarjestys&limit=1&page=2"))
                .selectFirst("a[download]")!!
                .attr("href")
        assertFalse(linkki.contains("page="))
        assertFalse(linkki.contains("limit="))
    }

    @Test
    fun `hyvaksynta palaa samalle sivulle`() {
        val location =
            mockMvc
                .perform(
                    post("/yki/tarkistusarvioinnit?page=2&limit=1&sortColumn=sukunimi")
                        .session(virkailijaSession())
                        .with(csrf())
                        .param("hyvaksyttyPvm", "2025-05-01"),
                ).andExpect(status().is3xxRedirection)
                .andReturn()
                .response.redirectedUrl
                .orEmpty()

        assertContains(location, "page=2")
        assertContains(location, "limit=1")
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
