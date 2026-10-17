package fi.oph.kitu.koski

import arrow.core.Either
import fi.oph.kitu.DBContainerConfiguration
import fi.oph.kitu.TestTimeService
import fi.oph.kitu.auditlogs.OpenTelemetryTestConfig
import fi.oph.kitu.dev.mockdata.generateRandomYkiSuoritusEntity
import fi.oph.kitu.tiedontuontischema.Lahdejarjestelma
import fi.oph.kitu.tiedontuontischema.LahdejarjestelmanTunniste
import fi.oph.kitu.tyhjennaYkiSuoritukset
import fi.oph.kitu.util.TimeService
import fi.oph.kitu.util.result.getOrThrow
import fi.oph.kitu.vkt.CustomVktSuoritusRepository
import fi.oph.kitu.vkt.VktSuoritusRepository
import fi.oph.kitu.vkt.VktSuoritusService
import fi.oph.kitu.yki.Tutkintokieli
import fi.oph.kitu.yki.Tutkintotaso
import fi.oph.kitu.yki.YkiService
import fi.oph.kitu.yki.suoritukset.YkiSuoritusEntity
import fi.oph.kitu.yki.suoritukset.YkiSuoritusFilter
import fi.oph.kitu.yki.suoritukset.YkiSuoritusRepository
import io.opentelemetry.api.trace.Tracer
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.assertNotNull
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.http.MediaType
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.test.web.client.ExpectedCount
import org.springframework.test.web.client.MockRestServiceServer
import org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo
import org.springframework.test.web.client.response.MockRestResponseCreators.withBadRequest
import org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess
import org.springframework.web.client.RestClient
import org.testcontainers.postgresql.PostgreSQLContainer
import java.time.LocalDateTime
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

@SpringBootTest
@Import(OpenTelemetryTestConfig::class, DBContainerConfiguration::class)
class KoskiServiceTest(
    @param:Autowired private val koskiYkiRequestMapper: KoskiYkiRequestMapper,
    @param:Autowired private val koskiVktRequestMapper: KoskiVktRequestMapper,
    @param:Autowired private val ykiSuoritusRepository: YkiSuoritusRepository,
    @param:Autowired private val jdbcTemplate: JdbcTemplate,
    @param:Autowired private val mockRestClientBuilder: RestClient.Builder,
    @param:Autowired private val inMemorySpanExporter: InMemorySpanExporter,
    @param:Autowired private val koskiErrorService: KoskiErrorService,
    @param:Autowired private val postgres: PostgreSQLContainer,
) {
    @Autowired
    private lateinit var vktSuoritusService: VktSuoritusService

    @Autowired
    private lateinit var customVktSuoritusRepository: CustomVktSuoritusRepository

    @Autowired
    private lateinit var ykiService: YkiService

    @Autowired
    private lateinit var timeService: TestTimeService

    @BeforeEach
    fun nukeDb() {
        jdbcTemplate.tyhjennaYkiSuoritukset()
        inMemorySpanExporter.reset()
        timeService.resetClock()
    }

    @Test
    fun `test sending koski request`() {
        val suoritus = generateRandomYkiSuoritusEntity()
        val koskiResponse = successfulKoskiResponseFor(suoritus)
        val mockServer = MockRestServiceServer.bindTo(mockRestClientBuilder).build()
        mockServer
            .expect(requestTo("oppija"))
            .andRespond(
                withSuccess(
                    koskiResponse,
                    MediaType.APPLICATION_JSON,
                ),
            )

        val service =
            KoskiService(
                mockRestClientBuilder.build(),
                koskiYkiRequestMapper,
                koskiVktRequestMapper,
                ykiSuoritusRepository,
                customVktSuoritusRepository,
                vktSuoritusService,
                koskiErrorService,
                timeService,
            )

        val updatedSuoritus = service.sendYkiSuoritusToKoski(suoritus).getOrThrow()
        assertEquals("1.2.246.562.15.50209741037", updatedSuoritus.koskiOpiskeluoikeus.toString())

        // KoskiService.sendYkiSuoritusToKoski span ei tallennu, koska tämä testi instantioi
        // KoskiServicen käsin ohittaen Springin AOP-proxyn, jonka @WithSpan tarvitsisi.
        // KoskiYkiRequestMapper sen sijaan tulee Springin DI:n kautta, joten @WithSpan toimii sille.
        val spans = inMemorySpanExporter.finishedSpanItems
        assertNotNull(spans)
        assertNotNull(spans.find { it.name == "KoskiYkiRequestMapper.ykiSuoritusToKoskiRequest" })
    }

    @Test
    fun `test failed koski request`() {
        val expectedResponse =
            """
            [{"key": "notFound.oppijaaEiLöydy","message": "Oppijaa 1.2.246.562.24.00000000000 ei löydy."}]
            """.trimIndent()
        val mockServer = MockRestServiceServer.bindTo(mockRestClientBuilder).build()
        mockServer
            .expect(requestTo("oppija"))
            .andRespond(
                withBadRequest().body(expectedResponse),
            )

        val service =
            KoskiService(
                mockRestClientBuilder.build(),
                koskiYkiRequestMapper,
                koskiVktRequestMapper,
                ykiSuoritusRepository,
                customVktSuoritusRepository,
                vktSuoritusService,
                koskiErrorService,
                timeService,
            )
        val suoritus =
            generateRandomYkiSuoritusEntity().copy(id = 1)

        val updatedSuoritus = service.sendYkiSuoritusToKoski(suoritus)
        assertTrue(updatedSuoritus is Either.Left)
        assertEquals(YkiMappingId(suoritus.solkiId), updatedSuoritus.value.suoritusId)
    }

    @Test
    fun `test sending all yki suoritukset to KOSKI`() {
        val koskiResponse = successfulKoskiResponseFor("1.2.246.562.24.20281155246", 183424)
        val mockServer = MockRestServiceServer.bindTo(mockRestClientBuilder).build()
        mockServer
            .expect(ExpectedCount.times(3), requestTo("oppija"))
            .andRespond(
                withSuccess(
                    koskiResponse,
                    MediaType.APPLICATION_JSON,
                ),
            )

        val service =
            KoskiService(
                mockRestClientBuilder.build(),
                koskiYkiRequestMapper,
                koskiVktRequestMapper,
                ykiSuoritusRepository,
                customVktSuoritusRepository,
                vktSuoritusService,
                koskiErrorService,
                timeService,
            )

        ykiSuoritusRepository.saveAllNewEntities(
            listOf(
                generateRandomYkiSuoritusEntity(),
                generateRandomYkiSuoritusEntity(),
                generateRandomYkiSuoritusEntity(),
            ),
        )
        service.sendYkiSuorituksetToKoski().getOrThrow()
        val updatedSuoritukset = ykiService.allSuoritukset(versionHistory = false)
        assertEquals(3, updatedSuoritukset.size)
        updatedSuoritukset.forEach {
            assertEquals(true, it.koskiSiirtoKasitelty)
            assertEquals("1.2.246.562.15.50209741037", it.koskiOpiskeluoikeus.toString())
        }
    }

    @Test
    fun `test failing to send one suoritus to KOSKI throws error and saves succesfull ones`() {
        val koskiResponse = successfulKoskiResponseFor("1.2.246.562.24.20281155246", 183424)
        val mockServer = MockRestServiceServer.bindTo(mockRestClientBuilder).build()
        mockServer
            .expect(requestTo("oppija"))
            .andRespond(
                withBadRequest().body(
                    "[{\"key\": \"notFound.oppijaaEiLöydy\",\"message\": \"Oppijaa 1.2.246.562.24.00000000000 ei löydy.\"}]",
                ),
            )
        mockServer
            .expect(ExpectedCount.times(2), requestTo("oppija"))
            .andRespond(
                withSuccess(
                    koskiResponse,
                    MediaType.APPLICATION_JSON,
                ),
            )

        val service =
            KoskiService(
                mockRestClientBuilder.build(),
                koskiYkiRequestMapper,
                koskiVktRequestMapper,
                ykiSuoritusRepository,
                customVktSuoritusRepository,
                vktSuoritusService,
                koskiErrorService,
                timeService,
            )

        ykiSuoritusRepository.saveAllNewEntities(
            listOf(
                generateRandomYkiSuoritusEntity(),
                generateRandomYkiSuoritusEntity(),
                generateRandomYkiSuoritusEntity(),
            ),
        )

        service.sendYkiSuorituksetToKoski().getOrThrow()

        val updatedSuoritukset = ykiService.allSuoritukset(versionHistory = false)
        assertEquals(3, updatedSuoritukset.size)
        assertEquals(2, updatedSuoritukset.filter { it.koskiOpiskeluoikeus != null }.size)
    }

    @Test
    fun `Yhden suorituksen mappausvirhe ei keskeytä Koski-eräajoa`() {
        val koskiResponse = successfulKoskiResponseFor("1.2.246.562.24.20281155246", 183424)
        val mockServer = MockRestServiceServer.bindTo(mockRestClientBuilder).build()
        mockServer
            .expect(ExpectedCount.times(2), requestTo("oppija"))
            .andRespond(withSuccess(koskiResponse, MediaType.APPLICATION_JSON))

        val service =
            KoskiService(
                mockRestClientBuilder.build(),
                koskiYkiRequestMapper,
                koskiVktRequestMapper,
                ykiSuoritusRepository,
                customVktSuoritusRepository,
                vktSuoritusService,
                koskiErrorService,
                timeService,
            )

        val invalid =
            generateRandomYkiSuoritusEntity().copy(
                tutkintotaso = Tutkintotaso.PT,
                tekstinYmmartaminen = 1,
                kirjoittaminen = 1,
                rakenteetJaSanasto = 1,
                puheenYmmartaminen = 1,
                puhuminen = 5,
                yleisarvosana = 1,
            )
        ykiSuoritusRepository.saveAllNewEntities(
            listOf(
                generateRandomYkiSuoritusEntity(),
                invalid,
                generateRandomYkiSuoritusEntity(),
            ),
        )

        service.sendYkiSuorituksetToKoski().getOrThrow()

        val updatedSuoritukset = ykiService.allSuoritukset(versionHistory = false)
        assertEquals(3, updatedSuoritukset.size)
        assertEquals(2, updatedSuoritukset.filter { it.koskiOpiskeluoikeus != null }.size)

        val errorEntity = koskiErrorService.findById(YkiMappingId(invalid.solkiId))
        assertNotNull(errorEntity)
        assertTrue(errorEntity.message.contains("Koski-pyynnöksi"))
    }

    @Test
    fun `Vanhentuneen tutkintokielen mappausvirhe ei keskeytä Koski-eräajoa`() {
        val koskiResponse = successfulKoskiResponseFor("1.2.246.562.24.20281155246", 183424)
        val mockServer = MockRestServiceServer.bindTo(mockRestClientBuilder).build()
        mockServer
            .expect(ExpectedCount.times(2), requestTo("oppija"))
            .andRespond(withSuccess(koskiResponse, MediaType.APPLICATION_JSON))

        val service =
            KoskiService(
                mockRestClientBuilder.build(),
                koskiYkiRequestMapper,
                koskiVktRequestMapper,
                ykiSuoritusRepository,
                customVktSuoritusRepository,
                vktSuoritusService,
                koskiErrorService,
                timeService,
            )

        val legacy =
            generateRandomYkiSuoritusEntity().copy(
                tutkintokieli = Tutkintokieli.SWE10,
            )
        ykiSuoritusRepository.saveAllNewEntities(
            listOf(
                generateRandomYkiSuoritusEntity(),
                legacy,
                generateRandomYkiSuoritusEntity(),
            ),
        )

        service.sendYkiSuorituksetToKoski().getOrThrow()

        val updatedSuoritukset = ykiService.allSuoritukset(versionHistory = false)
        assertEquals(3, updatedSuoritukset.size)
        assertEquals(2, updatedSuoritukset.filter { it.koskiOpiskeluoikeus != null }.size)

        val errorEntity = koskiErrorService.findById(YkiMappingId(legacy.solkiId))
        assertNotNull(errorEntity)
        assertTrue(errorEntity.message.contains("Koski-pyynnöksi"))
        assertTrue(errorEntity.message.contains("SWE10"))
    }

    @Test
    fun `verify that the correct version of suoritus is processed at every step`() {
        val viimeisinVersio = generateRandomYkiSuoritusEntity()
        val toinenVersio = viimeisinVersio.copy(puhuminen = null)
        val ekaVersio =
            toinenVersio.copy(
                lastModified = viimeisinVersio.lastModified.minusSeconds(1000),
            )

        ykiSuoritusRepository.save(ekaVersio, true)
        ykiSuoritusRepository.save(toinenVersio, true)
        ykiSuoritusRepository.save(viimeisinVersio, true)

        val suoritusKoskeen =
            ykiSuoritusRepository
                .findKoskeenLahettamattomatSuoritukset()
                .find { it.solkiId == ekaVersio.solkiId }!!

        assertEquals(viimeisinVersio.puhuminen, suoritusKoskeen.puhuminen)

        val koskiService = setupKoskiMock(successfulKoskiResponseFor(viimeisinVersio))

        val lahetettyVersio = koskiService.sendYkiSuoritusToKoski(suoritusKoskeen).getOrThrow()
        assertEquals(viimeisinVersio.puhuminen, lahetettyVersio.puhuminen)

        val lahetetyt =
            ykiSuoritusRepository
                .find(YkiSuoritusFilter(), distinct = true)
                .filter { it.koskiOpiskeluoikeus != null }
        assertEquals(lahetetyt.map { it.solkiId }, listOf(viimeisinVersio.solkiId))
    }

    @Test
    fun `YKI-lähetys KOSKI-palveluun estetään kun blockedUntil on tulevaisuudessa`() {
        val mockServer = MockRestServiceServer.bindTo(mockRestClientBuilder).build()
        val blockedUntil = LocalDateTime.of(2026, 6, 22, 9, 0)

        val service =
            KoskiService(
                mockRestClientBuilder.build(),
                koskiYkiRequestMapper,
                koskiVktRequestMapper,
                ykiSuoritusRepository,
                customVktSuoritusRepository,
                vktSuoritusService,
                koskiErrorService,
                timeService,
                ykiTransferBlockedUntil = blockedUntil,
            )

        ykiSuoritusRepository.saveAllNewEntities(listOf(generateRandomYkiSuoritusEntity()))

        timeService.fixClock(
            blockedUntil
                .minusMinutes(1)
                .atZone(TimeService.zoneId)
                .toInstant(),
        )

        val report = service.sendYkiSuorituksetToKoski().getOrThrow()

        assertEquals(0, report.successfulTransfers)
        assertEquals(0, report.totalCount)
        assertEquals(blockedUntil, report.blockedUntil)
        assertTrue(report.toString().contains("estetty"))
        assertTrue(report.toString().contains(blockedUntil.toString()))

        val storedSuoritukset = ykiService.allSuoritukset(versionHistory = false)
        assertEquals(1, storedSuoritukset.size)
        assertEquals(false, storedSuoritukset[0].koskiSiirtoKasitelty)

        mockServer.verify()
    }

    @Test
    fun `YKI-lähetys KOSKI-palveluun etenee kun blockedUntil on menneisyydessä`() {
        val koskiResponse = successfulKoskiResponseFor("1.2.246.562.24.20281155246", 183424)
        val mockServer = MockRestServiceServer.bindTo(mockRestClientBuilder).build()
        mockServer
            .expect(requestTo("oppija"))
            .andRespond(withSuccess(koskiResponse, MediaType.APPLICATION_JSON))

        val blockedUntil = LocalDateTime.of(2026, 6, 22, 9, 0)

        val service =
            KoskiService(
                mockRestClientBuilder.build(),
                koskiYkiRequestMapper,
                koskiVktRequestMapper,
                ykiSuoritusRepository,
                customVktSuoritusRepository,
                vktSuoritusService,
                koskiErrorService,
                timeService,
                ykiTransferBlockedUntil = blockedUntil,
            )

        ykiSuoritusRepository.saveAllNewEntities(listOf(generateRandomYkiSuoritusEntity()))

        timeService.fixClock(
            blockedUntil
                .plusMinutes(1)
                .atZone(TimeService.zoneId)
                .toInstant(),
        )

        val report = service.sendYkiSuorituksetToKoski().getOrThrow()

        assertEquals(1, report.successfulTransfers)
        assertEquals(1, report.totalCount)
        assertEquals(null, report.blockedUntil)

        mockServer.verify()
    }

    @Test
    fun `OPHTesti-suoritus lähetetään KOSKI-palveluun vaikka muiden lähetys on estetty`() {
        val ophTestiSuoritus =
            generateRandomYkiSuoritusEntity()
                .copy(
                    lahdejarjestelmanTunnus =
                        LahdejarjestelmanTunniste("123456", Lahdejarjestelma.OPHTesti).toTunnus(),
                )
        val solkiSuoritus = generateRandomYkiSuoritusEntity()

        val mockServer = MockRestServiceServer.bindTo(mockRestClientBuilder).build()
        mockServer
            .expect(requestTo("oppija"))
            .andRespond(withSuccess(successfulKoskiResponseFor(ophTestiSuoritus), MediaType.APPLICATION_JSON))

        val blockedUntil = LocalDateTime.of(2026, 6, 22, 9, 0)

        val service =
            KoskiService(
                mockRestClientBuilder.build(),
                koskiYkiRequestMapper,
                koskiVktRequestMapper,
                ykiSuoritusRepository,
                customVktSuoritusRepository,
                vktSuoritusService,
                koskiErrorService,
                timeService,
                ykiTransferBlockedUntil = blockedUntil,
            )

        ykiSuoritusRepository.saveAllNewEntities(listOf(ophTestiSuoritus, solkiSuoritus))

        timeService.fixClock(
            blockedUntil
                .minusMinutes(1)
                .atZone(TimeService.zoneId)
                .toInstant(),
        )

        val report = service.sendYkiSuorituksetToKoski().getOrThrow()

        assertEquals(1, report.successfulTransfers)
        assertEquals(1, report.totalCount)
        assertEquals(blockedUntil, report.blockedUntil)
        assertTrue(report.toString().contains("estetty"))
        assertTrue(report.toString().contains("Siirretty 1 / 1"))

        val storedSuoritukset = ykiService.allSuoritukset(versionHistory = false)
        assertEquals(true, storedSuoritukset.first { it.isOphTesti() }.koskiSiirtoKasitelty)
        assertEquals(false, storedSuoritukset.first { !it.isOphTesti() }.koskiSiirtoKasitelty)

        mockServer.verify()
    }

    private fun setupKoskiMock(response: String): KoskiService {
        val mockServer = MockRestServiceServer.bindTo(mockRestClientBuilder).build()
        mockServer
            .expect(requestTo("oppija"))
            .andRespond(
                withSuccess(
                    response,
                    MediaType.APPLICATION_JSON,
                ),
            )

        return KoskiService(
            mockRestClientBuilder.build(),
            koskiYkiRequestMapper,
            koskiVktRequestMapper,
            ykiSuoritusRepository,
            customVktSuoritusRepository,
            vktSuoritusService,
            koskiErrorService,
            timeService,
        )
    }

    private fun successfulKoskiResponseFor(suoritus: YkiSuoritusEntity): String =
        successfulKoskiResponseFor(suoritus.suorittajanOID.toString(), suoritus.solkiId)

    private fun successfulKoskiResponseFor(
        oppijanumero: String,
        solkiId: Int,
    ): String =
        """
        {
          "henkilö": {
            "oid": "$oppijanumero"
          },
          "opiskeluoikeudet": [
            {
              "oid": "1.2.246.562.15.50209741037",
              "versionumero": 1,
              "lähdejärjestelmänId": {
                "id": "$solkiId",
                "lähdejärjestelmä": {
                  "koodiarvo": "kielitutkintorekisteri",
                  "nimi": {
                    "fi": "Kielitutkintorekisteri"
                  },
                  "koodistoUri": "lahdejarjestelma",
                  "koodistoVersio": 1
                }
              }
            }
          ]
        }
        """.trimIndent()
}
