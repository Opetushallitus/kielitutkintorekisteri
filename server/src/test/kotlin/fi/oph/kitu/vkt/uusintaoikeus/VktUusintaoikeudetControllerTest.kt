package fi.oph.kitu.vkt.uusintaoikeus

import arrow.core.Either
import arrow.core.left
import fi.oph.kitu.DBContainerConfiguration
import fi.oph.kitu.TestTimeService
import fi.oph.kitu.auditlogs.AuditLogger
import fi.oph.kitu.i18n.LocalizationService
import fi.oph.kitu.koodisto.Koodisto
import fi.oph.kitu.oid.Oid
import fi.oph.kitu.oppijanumero.OppijanumeroException
import fi.oph.kitu.oppijanumero.OppijanumeroService
import fi.oph.kitu.oppijanumero.OppijanumerorekisteriHenkilo
import fi.oph.kitu.util.result.getOrThrow
import fi.oph.kitu.virkailijaSession
import fi.oph.kitu.vkt.VktApiController
import fi.oph.kitu.vkt.VktSuoritusEntity
import fi.oph.kitu.vkt.VktSuoritusRepository
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.http.HttpHeaders
import org.springframework.http.ResponseEntity
import org.springframework.security.test.web.servlet.setup.SecurityMockMvcConfigurers.springSecurity
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.status
import org.springframework.test.web.servlet.setup.DefaultMockMvcBuilder
import org.springframework.test.web.servlet.setup.MockMvcBuilders
import org.springframework.web.client.ResourceAccessException
import org.springframework.web.context.WebApplicationContext
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody
import java.io.ByteArrayOutputStream
import java.time.Instant
import java.time.LocalDate
import kotlin.test.Test
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertFalse

@SpringBootTest
@Import(DBContainerConfiguration::class)
class VktUusintaoikeudetControllerTest(
    @param:Autowired private val context: WebApplicationContext,
    @param:Autowired private val apiController: VktApiController,
    @param:Autowired private val suoritusRepository: VktSuoritusRepository,
    @param:Autowired private val timeService: TestTimeService,
    @param:Autowired private val repository: VktUusintaoikeusRepository,
    @param:Autowired private val localizationService: LocalizationService,
    @param:Autowired private val auditLogger: AuditLogger,
    @param:Autowired private val oppijanumeroService: OppijanumeroService,
) {
    private lateinit var mockMvc: MockMvc

    @BeforeEach
    fun setup() {
        suoritusRepository.deleteAll()
        tallenna(HENKILO, "KIOS:1", sukunimi = "Testinen", etunimet = "Testi")
        mockMvc =
            MockMvcBuilders
                .webAppContextSetup(context)
                .apply<DefaultMockMvcBuilder>(springSecurity())
                .build()
    }

    @AfterEach
    fun tyhjennaSuoritukset() {
        suoritusRepository.deleteAll()
    }

    @Test
    fun `CSV sisaltaa uusittavat osakokeet`() {
        val response =
            timeService.runWithFixedClock(TARKASTELUHETKI) {
                apiController.getUusintaoikeudetCsv(VktUusintaoikeusParams())
            }

        assertEquals(
            listOf(
                "Oppijanumero;Sukunimi;Etunimet;Tutkintokieli;Taitotaso;Uusittava osakoe;Tutkinnot;Hylkäämispäätös;" +
                    "Oikeus päättyy;Puhelinnumero;Sähköposti;Katuosoite;Postinumero;Postitoimipaikka;Maa",
                "$HENKILO;Testinen;Testi;Suomi;Hyvä ja tyydyttävä;<vktosakoe:kirjoittaminen>;" +
                    "<vktkielitaito:kirjallinen>;15.3.2025;15.3.2028;;;;;;",
            ),
            csvRivit(response),
        )
        assertContains(
            response.headers.getFirst(HttpHeaders.CONTENT_DISPOSITION).orEmpty(),
            "vkt_uusintaoikeudet_henkilotiedot_2026-10-21.csv",
        )
    }

    @Test
    fun `CSV sisaltaa yhteystiedot oppijanumerorekisterista`() {
        tallenna(YHTEYSTIEDOLLINEN, "KIOS:2", sukunimi = "Uusija-Testi", etunimet = "Ulla")

        val response =
            timeService.runWithFixedClock(TARKASTELUHETKI) {
                apiController.getUusintaoikeudetCsv(VktUusintaoikeusParams())
            }

        assertEquals(
            "$YHTEYSTIEDOLLINEN;Uusija-Testi;Ulla;Suomi;Hyvä ja tyydyttävä;<vktosakoe:kirjoittaminen>;" +
                "<vktkielitaito:kirjallinen>;15.3.2025;15.3.2028;" +
                "'+358 40 123 4567;ulla@example.com;Uusintakatu 3 B 7;33100;TAMPERE;Suomi",
            csvRivit(response).single { it.startsWith(YHTEYSTIEDOLLINEN) },
        )
    }

    @Test
    fun `CSV toimitetaan ilman yhteystietoja kun oppijanumerorekisteri ei vastaa`() {
        tallenna(YHTEYSTIEDOLLINEN, "KIOS:2", sukunimi = "Uusija-Testi", etunimet = "Ulla")
        val heittaa =
            palvelu { _ -> throw ResourceAccessException("Oppijanumerorekisteri ei vastaa") }
        val virhe =
            palvelu { _ -> OppijanumeroException.MalformedOppijanumero(oppijanumero = "rikki").left() }

        listOf(heittaa, virhe).forEach { service ->
            val raportti =
                timeService.runWithFixedClock(
                    TARKASTELUHETKI,
                ) { service.findForCsv(VktUusintaoikeusParams()) }

            assertEquals(2, raportti.rivit.size)
            assertEquals(listOf(null, null), raportti.rivit.map { it.yhteystiedot })
        }
    }

    @Test
    fun `piilotetuilla henkilotiedoilla oppijanumerorekisteria ei kutsuta`() {
        var kutsuttu = false
        val service =
            palvelu { oidit ->
                kutsuttu = true
                oppijanumeroService.getHenkilotByHenkiloOids(oidit)
            }

        timeService.runWithFixedClock(TARKASTELUHETKI) {
            service.findForCsv(VktUusintaoikeusParams(piilotaHenkilotiedot = true))
        }

        assertFalse(kutsuttu)
    }

    @Test
    fun `CSV-viennista voi piilottaa henkilotiedot`() {
        val response =
            timeService.runWithFixedClock(TARKASTELUHETKI) {
                apiController.getUusintaoikeudetCsv(VktUusintaoikeusParams(piilotaHenkilotiedot = true))
            }

        assertEquals(
            listOf(
                "Tutkintokieli;Taitotaso;Uusittava osakoe;Tutkinnot;Hylkäämispäätös;Oikeus päättyy",
                "Suomi;Hyvä ja tyydyttävä;<vktosakoe:kirjoittaminen>;<vktkielitaito:kirjallinen>;15.3.2025;15.3.2028",
            ),
            csvRivit(response),
        )
        assertContains(
            response.headers.getFirst(HttpHeaders.CONTENT_DISPOSITION).orEmpty(),
            "vkt_uusintaoikeudet_2026-10-21.csv",
        )
    }

    @Test
    fun `CSV noudattaa rajauksia`() {
        val response =
            timeService.runWithFixedClock(TARKASTELUHETKI) {
                apiController.getUusintaoikeudetCsv(
                    VktUusintaoikeusParams(
                        tarkastelupaiva = LocalDate.of(2028, 1, 1),
                        taitotaso = listOf(Koodisto.VktTaitotaso.Erinomainen),
                    ),
                )
            }

        assertEquals(1, csvRivit(response).size)
        assertContains(
            response.headers.getFirst(HttpHeaders.CONTENT_DISPOSITION).orEmpty(),
            "vkt_uusintaoikeudet_henkilotiedot_2028-01-01_erinomainen.csv",
        )
    }

    @Test
    fun `raportti naytaa yhteenvedon ja uusittavat osakokeet`() {
        val html = getHtml("/vkt/uusintaoikeudet")

        assertContains(html, """<span data-testid="tarkastelupaiva">21.10.2026</span>""")
        assertContains(html, """name="tarkastelupaiva" value="2026-10-21"""")
        assertContains(
            html,
            """<tr data-testid="yhteenveto-hyvajatyydyttava"><th>Hyvä ja tyydyttävä</th>""" +
                """<td data-testid="henkiloita">1</td><td data-testid="osakokeita">1</td></tr>""",
        )
        assertContains(html, """data-testid="$HENKILO-FIN-hyvajatyydyttava-kirjoittaminen"""")
        assertContains(html, """/vkt/suoritukset/$HENKILO/FIN/Hyv%C3%A4JaTyydytt%C3%A4v%C3%A4"""")
    }

    @Test
    fun `raportin voi jarjestaa sarakkeen mukaan`() {
        assertContains(getHtml("/vkt/uusintaoikeudet?sortColumn=oikeusPaattyy&sortDirection=DESC"), HENKILO)
    }

    @Test
    fun `tarkastelupaivan voi valita`() {
        val voimassa = getHtml("/vkt/uusintaoikeudet?tarkastelupaiva=2028-03-15")
        assertContains(voimassa, """<span data-testid="tarkastelupaiva">15.3.2028</span>""")
        assertContains(voimassa, """name="tarkastelupaiva" value="2028-03-15"""")
        assertContains(voimassa, """data-testid="$HENKILO-FIN-hyvajatyydyttava-kirjoittaminen"""")

        val paattynyt = getHtml("/vkt/uusintaoikeudet?tarkastelupaiva=2028-03-16")
        assertContains(paattynyt, """<span data-testid="tarkastelupaiva">16.3.2028</span>""")
        assertContains(paattynyt, "Ei uusintaan oikeutettuja henkilöitä.")
    }

    @Test
    fun `tyhja tarkastelupaiva tarkoittaa tata paivaa`() {
        assertContains(
            getHtml("/vkt/uusintaoikeudet?tarkastelupaiva="),
            """<span data-testid="tarkastelupaiva">21.10.2026</span>""",
        )
    }

    @Test
    fun `raportin voi rajata kielen ja taitotason mukaan`() {
        val erinomainen = getHtml("/vkt/uusintaoikeudet?taitotaso=Erinomainen")
        assertContains(erinomainen, "<li>Taitotaso: Erinomainen</li>")
        assertContains(erinomainen, "Ei uusintaan oikeutettuja henkilöitä.")
        assertContains(
            erinomainen,
            """<tr data-testid="yhteenveto-hyvajatyydyttava"><th>Hyvä ja tyydyttävä</th>""" +
                """<td data-testid="henkiloita">0</td><td data-testid="osakokeita">0</td></tr>""",
        )

        val suomi = getHtml("/vkt/uusintaoikeudet?tutkintokieli=FIN")
        assertContains(suomi, "<li>Tutkintokieli: Suomi</li>")
        assertContains(suomi, """data-testid="$HENKILO-FIN-hyvajatyydyttava-kirjoittaminen"""")
    }

    @Test
    fun `henkilotiedot voi piilottaa`() {
        val html = getHtml("/vkt/uusintaoikeudet?piilotaHenkilotiedot=on")

        assertContains(html, "<li>Henkilötiedot piilotettu</li>")
        assertFalse(html.contains("Testinen"))
        assertFalse(html.contains("/vkt/suoritukset/$HENKILO"))
    }

    private fun csvRivit(response: ResponseEntity<StreamingResponseBody>): List<String> =
        ByteArrayOutputStream()
            .also { response.body!!.writeTo(it) }
            .toString(Charsets.UTF_8)
            .removePrefix("\uFEFF")
            .lines()
            .filter { it.isNotBlank() }

    private fun getHtml(url: String): String =
        timeService.runWithFixedClock(TARKASTELUHETKI) {
            mockMvc
                .perform(get(url).session(virkailijaSession()))
                .andExpect(status().isOk)
                .andReturn()
                .response
                .getContentAsString(Charsets.UTF_8)
                .replace(Regex(">\\s+<"), "><")
        }

    private fun palvelu(
        haeHenkilot: (Collection<Oid>) -> Either<OppijanumeroException, Map<Oid, OppijanumerorekisteriHenkilo>>,
    ) = VktUusintaoikeusService(
        repository,
        timeService,
        localizationService,
        auditLogger,
        object : OppijanumeroService by oppijanumeroService {
            override fun getHenkilotByHenkiloOids(
                henkiloOids: Collection<Oid>,
            ): Either<OppijanumeroException, Map<Oid, OppijanumerorekisteriHenkilo>> = haeHenkilot(henkiloOids)
        },
    )

    private fun tallenna(
        henkilo: String,
        ilmoittautumisenId: String,
        sukunimi: String,
        etunimet: String,
    ) {
        suoritusRepository.save(
            VktSuoritusEntity(
                ilmoittautumisenId = ilmoittautumisenId,
                suorittajanOid = Oid.parse(henkilo).getOrThrow(),
                etunimet = etunimet,
                sukunimi = sukunimi,
                tutkintokieli = Koodisto.Tutkintokieli.FIN,
                suorituspaikkakunta = "091",
                taitotaso = Koodisto.VktTaitotaso.HyväJaTyydyttävä,
                suorituksenVastaanottaja = null,
                osakokeet =
                    setOf(
                        osakoe(Koodisto.VktOsakoe.Kirjoittaminen, Koodisto.VktArvosana.Hylätty),
                        osakoe(Koodisto.VktOsakoe.TekstinYmmärtäminen, Koodisto.VktArvosana.Hyvä),
                    ),
                tutkinnot = emptySet(),
            ),
        )
    }

    private fun osakoe(
        tyyppi: Koodisto.VktOsakoe,
        arvosana: Koodisto.VktArvosana,
    ) = VktSuoritusEntity.VktOsakoe(
        tyyppi = tyyppi,
        tutkintopaiva = LocalDate.of(2025, 3, 1),
        arviointipaiva = LocalDate.of(2025, 3, 15),
        arvosana = arvosana,
        merkittyPoistettavaksi = null,
    )

    companion object {
        private const val HENKILO = "1.2.246.562.24.12345678910"
        private const val YHTEYSTIEDOLLINEN = "1.2.246.562.24.77777777777"
        private val TARKASTELUHETKI = Instant.parse("2026-10-21T09:00:00Z")
    }
}
