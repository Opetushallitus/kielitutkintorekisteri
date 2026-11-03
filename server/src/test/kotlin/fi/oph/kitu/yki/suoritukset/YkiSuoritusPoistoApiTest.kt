package fi.oph.kitu.yki.suoritukset

import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import fi.oph.kitu.DBContainerConfiguration
import fi.oph.kitu.auditlogs.AUDIT_LOGGER_NAME
import fi.oph.kitu.dev.mockdata.generateRandomYkiSuoritusEntity
import fi.oph.kitu.ilmoittautumisjarjestelma.IlmoittautumisjarjestelmaClientMock
import fi.oph.kitu.oid.Oid
import fi.oph.kitu.security.Authority
import fi.oph.kitu.tiedontuontischema.Henkilo
import fi.oph.kitu.tiedontuontischema.Henkilosuoritus
import fi.oph.kitu.tiedontuontischema.Lahdejarjestelma
import fi.oph.kitu.tiedontuontischema.LahdejarjestelmanTunniste
import fi.oph.kitu.tiedontuontischema.YkiJarjestaja
import fi.oph.kitu.tiedontuontischema.YkiOsa
import fi.oph.kitu.tiedontuontischema.YkiSuoritus
import fi.oph.kitu.tyhjennaYkiSuoritukset
import fi.oph.kitu.util.result.getOrThrow
import fi.oph.kitu.virkailijaSession
import fi.oph.kitu.yki.Arviointitila
import fi.oph.kitu.yki.Sukupuoli
import fi.oph.kitu.yki.TutkinnonOsa
import fi.oph.kitu.yki.Tutkintokieli
import fi.oph.kitu.yki.Tutkintotaso
import fi.oph.kitu.yki.YkiApiController
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.http.MediaType
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.mock.web.MockHttpServletRequest
import org.springframework.security.core.authority.SimpleGrantedAuthority
import org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.jwt
import org.springframework.security.test.web.servlet.setup.SecurityMockMvcConfigurers.springSecurity
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete
import org.springframework.test.web.servlet.request.RequestPostProcessor
import org.springframework.test.web.servlet.setup.DefaultMockMvcBuilder
import org.springframework.test.web.servlet.setup.MockMvcBuilders
import org.springframework.web.context.WebApplicationContext
import java.time.Instant
import java.time.LocalDate
import kotlin.test.assertContains
import kotlin.test.assertEquals
import ch.qos.logback.classic.Logger as LogbackLogger

@SpringBootTest
@Import(DBContainerConfiguration::class)
class YkiSuoritusPoistoApiTest(
    @param:Autowired private val suoritusRepository: YkiSuoritusRepository,
    @param:Autowired private val ykiApiController: YkiApiController,
    @param:Autowired private val jdbcTemplate: JdbcTemplate,
    @param:Autowired private val ilmoittautumisjarjestelmaClient: IlmoittautumisjarjestelmaClientMock,
    @param:Autowired private val context: WebApplicationContext,
) {
    private val auditLogger = LoggerFactory.getLogger(AUDIT_LOGGER_NAME) as LogbackLogger
    private val auditLoki = ListAppender<ILoggingEvent>()
    private lateinit var mockMvc: MockMvc

    private val solkiId = 1001

    private val tallentaja =
        RequestPostProcessor { request: MockHttpServletRequest ->
            request.also {
                it.session = virkailijaSession(authorities = listOf(Authority.VIRKAILIJA, Authority.YKI_TALLENNUS))
            }
        }

    private val koneasiakas =
        jwt()
            .jwt { it.subject("solki") }
            .authorities(SimpleGrantedAuthority(Authority.YKI_TALLENNUS.role()))

    @BeforeEach
    fun valmistele() {
        jdbcTemplate.tyhjennaYkiSuoritukset()
        ilmoittautumisjarjestelmaClient.reset()
        mockMvc =
            MockMvcBuilders
                .webAppContextSetup(context)
                .apply<DefaultMockMvcBuilder>(springSecurity())
                .build()
        auditLoki.start()
        auditLogger.addAppender(auditLoki)
    }

    @AfterEach
    fun siivoa() {
        auditLoki.stop()
        auditLogger.detachAppender(auditLoki)
        jdbcTemplate.tyhjennaYkiSuoritukset()
    }

    private fun tallennaSuoritus(arviointitila: Arviointitila) {
        suoritusRepository.save(
            generateRandomYkiSuoritusEntity().copy(
                solkiId = solkiId,
                lahdejarjestelmanTunnus = "yki.$solkiId",
                arviointitila = arviointitila,
                lastModified = Instant.parse("2025-01-01T10:00:00Z"),
            ),
            false,
        )
    }

    private fun tunnisteRunko(
        id: Any = solkiId,
        lahde: String = "Solki",
    ) = """{"id": "$id", "lahde": "$lahde"}"""

    private fun deleteSuoritus(
        runko: String = tunnisteRunko(),
        kutsuja: RequestPostProcessor = tallentaja,
    ) = mockMvc
        .perform(
            delete("/yki/api/suoritus")
                .with(kutsuja)
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON)
                .content(runko),
        ).andReturn()
        .response

    private fun viimeisin() = suoritusRepository.findLatestBySolkiIds(listOf(solkiId)).single()

    private fun versiomaara(): Int =
        jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM yki_suoritus WHERE solki_id = ?",
            Int::class.javaObjectType,
            solkiId,
        )!!

    private fun odotaPoistonAuditointi(): Map<String, Any?> {
        val loppu = System.currentTimeMillis() + 5_000
        while (System.currentTimeMillis() < loppu) {
            val tapahtuma =
                synchronized(auditLoki) { auditLoki.list.toList() }
                    .firstOrNull { it.message == "Yki suoritus deleted" }
            if (tapahtuma != null) return tapahtuma.keyValuePairs.associate { it.key to it.value }
            Thread.sleep(20)
        }
        error("Poistoa ei kirjattu auditlokiin")
    }

    private fun ilmoittautumisenPush() =
        Henkilosuoritus(
            henkilo =
                Henkilo(
                    oid = Oid.parse("1.2.246.562.24.10691606777").getOrThrow(),
                    etunimet = "Eeli Heikki",
                    sukunimi = "Aalto",
                    hetu = "010180-9026",
                    sukupuoli = Sukupuoli.N,
                    kansalaisuus = "EST",
                    katuosoite = "Testikuja 1",
                    postinumero = "12345",
                    postitoimipaikka = "Testilä",
                    email = "eeli@email.com",
                ),
            suoritus =
                YkiSuoritus(
                    tutkintotaso = Tutkintotaso.KT,
                    kieli = Tutkintokieli.FIN,
                    todistuskieli = Todistuskieli.FIN,
                    jarjestaja =
                        YkiJarjestaja(
                            oid = Oid.parse("1.2.246.562.10.14893989377").getOrThrow(),
                            nimi = "Soveltavan kielentutkimuksen keskus",
                        ),
                    tutkintopaiva = LocalDate.of(2020, 1, 1),
                    arviointipaiva = null,
                    arviointitila = Arviointitila.ILMOITTAUTUNUT,
                    osat =
                        listOf(
                            YkiOsa(tyyppi = TutkinnonOsa.puheenYmmartaminen, arvosana = null),
                            YkiOsa(tyyppi = TutkinnonOsa.puhuminen, arvosana = null),
                            YkiOsa(tyyppi = TutkinnonOsa.kirjoittaminen, arvosana = null),
                            YkiOsa(tyyppi = TutkinnonOsa.tekstinYmmartaminen, arvosana = null),
                        ),
                    lahdejarjestelmanId = LahdejarjestelmanTunniste(solkiId.toString(), Lahdejarjestelma.Solki),
                ),
        )

    @Test
    fun `ILMOITTAUTUNUT-tilassa poisto vastaa 200 ja kirjoittaa EI_SUORITUSTA-version`() {
        tallennaSuoritus(Arviointitila.ILMOITTAUTUNUT)

        val vastaus = deleteSuoritus()

        assertEquals(200, vastaus.status, vastaus.contentAsString)
        assertContains(vastaus.contentAsString, "OK")
        assertEquals(Arviointitila.EI_SUORITUSTA, viimeisin().arviointitila)
        assertEquals(2, versiomaara())
    }

    @Test
    fun `kielletyssä tilassa poisto vastaa 409 eikä kirjoita mitään`() {
        tallennaSuoritus(Arviointitila.ARVIOITU)

        val vastaus = deleteSuoritus()

        assertEquals(409, vastaus.status, vastaus.contentAsString)
        assertContains(vastaus.contentAsString, "Failed")
        assertContains(vastaus.contentAsString, "ARVIOITU")
        assertEquals(Arviointitila.ARVIOITU, viimeisin().arviointitila)
        assertEquals(1, versiomaara())
    }

    @Test
    fun `PERUTTU-tilassa poisto vastaa 409`() {
        tallennaSuoritus(Arviointitila.PERUTTU)

        val vastaus = deleteSuoritus()

        assertEquals(409, vastaus.status, vastaus.contentAsString)
        assertContains(vastaus.contentAsString, "PERUTTU")
        assertEquals(1, versiomaara())
    }

    @Test
    fun `uusi poisto vastaa 409 eikä kirjoita uutta versiota`() {
        tallennaSuoritus(Arviointitila.ILMOITTAUTUNUT)
        assertEquals(200, deleteSuoritus().status)

        val vastaus = deleteSuoritus()

        assertEquals(409, vastaus.status, vastaus.contentAsString)
        assertContains(vastaus.contentAsString, "EI_SUORITUSTA")
        assertEquals(2, versiomaara())
    }

    @Test
    fun `tuntematon tunniste vastaa 404`() {
        tallennaSuoritus(Arviointitila.ILMOITTAUTUNUT)

        val vastaus = deleteSuoritus(tunnisteRunko(id = 999999))

        assertEquals(404, vastaus.status, vastaus.contentAsString)
        assertContains(vastaus.contentAsString, "Failed")
        assertEquals(1, versiomaara())
    }

    @Test
    fun `saman numeron tunniste toisesta lähdejärjestelmästä vastaa 404`() {
        tallennaSuoritus(Arviointitila.ILMOITTAUTUNUT)

        val vastaus = deleteSuoritus(tunnisteRunko(lahde = "OPH"))

        assertEquals(404, vastaus.status, vastaus.contentAsString)
        assertEquals(1, versiomaara())
    }

    @Test
    fun `virheellinen runko vastaa 400 eikä kirjoita mitään`() {
        tallennaSuoritus(Arviointitila.ILMOITTAUTUNUT)

        listOf(
            "",
            "{}",
            """{"id": "$solkiId"}""",
            """{"lahde": "Solki"}""",
            tunnisteRunko(lahde = "EiOle"),
        ).forEach { runko ->
            val vastaus = deleteSuoritus(runko)
            assertEquals(400, vastaus.status, "Runko '$runko': ${vastaus.contentAsString}")
        }
        assertEquals(1, versiomaara())
    }

    @Test
    fun `pelkkä virkailijaoikeus ei riitä poistoon istunnollakaan`() {
        tallennaSuoritus(Arviointitila.ILMOITTAUTUNUT)
        val virkailija =
            RequestPostProcessor { request: MockHttpServletRequest ->
                request.also { it.session = virkailijaSession() }
            }

        val vastaus = deleteSuoritus(kutsuja = virkailija)

        assertEquals(403, vastaus.status, vastaus.contentAsString)
        assertEquals(1, versiomaara())
    }

    @Test
    fun `koneasiakkaan poisto kirjataan auditlokiin kutsujan kanssa`() {
        tallennaSuoritus(Arviointitila.ILMOITTAUTUNUT)
        val suorittaja = viimeisin().suorittajanOID

        val vastaus = deleteSuoritus(kutsuja = koneasiakas)

        assertEquals(200, vastaus.status, vastaus.contentAsString)
        val audit = odotaPoistonAuditointi()
        assertEquals("solki", audit["auth.principal_oid"])
        assertEquals(viimeisin().id, audit["suoritus.id"])
        assertEquals(solkiId, audit["suoritus.solki_id"])
        assertEquals("yki.$solkiId", audit["suoritus.lahdejarjestelmantunnus"])
        assertEquals(suorittaja, audit["suoritus.suorittajan_oid"])
    }

    @Test
    fun `pushattu ilmoittautuminen poistuu ja seuraava push tuo sen takaisin uutena versiona`() {
        ykiApiController.postHenkilosuoritus(ilmoittautumisenPush())

        assertEquals(200, deleteSuoritus().status)
        val poistettu = viimeisin()
        assertEquals(Arviointitila.EI_SUORITUSTA, poistettu.arviointitila)
        assertEquals(12, poistettu.puhuminen)
        assertEquals(12, poistettu.puheenYmmartaminen)
        assertEquals(12, poistettu.kirjoittaminen)
        assertEquals(12, poistettu.tekstinYmmartaminen)

        ykiApiController.postHenkilosuoritus(ilmoittautumisenPush())

        assertEquals(Arviointitila.ILMOITTAUTUNUT, viimeisin().arviointitila)
        assertEquals(3, versiomaara())
    }
}
