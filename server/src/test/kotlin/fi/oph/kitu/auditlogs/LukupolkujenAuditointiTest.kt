package fi.oph.kitu.auditlogs

import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import fi.oph.kitu.DBContainerConfiguration
import fi.oph.kitu.dev.mockdata.VktSuoritusMockGenerator
import fi.oph.kitu.dev.mockdata.generateRandomYkiSuoritusEntity
import fi.oph.kitu.tiedontuontischema.VktValidation
import fi.oph.kitu.tyhjennaYkiSuoritukset
import fi.oph.kitu.virkailijaSession
import fi.oph.kitu.vkt.VktSuoritusRepository
import fi.oph.kitu.yki.Arviointitila
import fi.oph.kitu.yki.suoritukset.YkiSuoritusRepository
import org.junit.jupiter.api.BeforeEach
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.security.test.web.servlet.setup.SecurityMockMvcConfigurers.springSecurity
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.status
import org.springframework.test.web.servlet.setup.DefaultMockMvcBuilder
import org.springframework.test.web.servlet.setup.MockMvcBuilders
import org.springframework.web.context.WebApplicationContext
import kotlin.test.Test
import kotlin.test.assertTrue
import ch.qos.logback.classic.Logger as LogbackLogger

@SpringBootTest
@Import(DBContainerConfiguration::class)
class LukupolkujenAuditointiTest(
    @param:Autowired private val context: WebApplicationContext,
    @param:Autowired private val ykiSuoritukset: YkiSuoritusRepository,
    @param:Autowired private val vktSuoritukset: VktSuoritusRepository,
    @param:Autowired private val vktValidation: VktValidation,
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
        jdbc.tyhjennaYkiSuoritukset()
        vktSuoritukset.deleteAll()
    }

    private fun auditoi(
        polku: String,
        avain: String,
        arvo: Any,
    ) {
        val auditLogger = LoggerFactory.getLogger(AUDIT_LOGGER_NAME) as LogbackLogger
        val loki = ListAppender<ILoggingEvent>().apply { start() }
        auditLogger.addAppender(loki)
        try {
            mockMvc.perform(get(polku).session(virkailijaSession())).andExpect(status().isOk)
            val loppu = System.currentTimeMillis() + 5_000
            while (System.currentTimeMillis() < loppu) {
                val loytyi =
                    loki.list
                        .toList()
                        .flatMap { it.keyValuePairs.orEmpty() }
                        .any { it.key == avain && it.value.toString() == arvo.toString() }
                if (loytyi) return
                Thread.sleep(20)
            }
            assertTrue(false, "$polku ei kirjannut auditlokiin $avain=$arvo")
        } finally {
            auditLogger.detachAppender(loki)
            loki.stop()
        }
    }

    @Test
    fun `VKT-suoritusten lista kirjataan auditlokiin`() {
        val suoritus = vktSuoritukset.save(VktSuoritusMockGenerator().generateRandomVktSuoritusEntity(vktValidation))

        auditoi("/vkt/", "suoritus.id", suoritus.id!!)
    }

    @Test
    fun `YKI-suorituksen KOSKI-pyynto kirjataan auditlokiin`() {
        val suoritus = generateRandomYkiSuoritusEntity().copy(solkiId = 4242, arviointitila = Arviointitila.ARVIOITU)
        ykiSuoritukset.save(suoritus, false)

        auditoi("/yki/koski-request/4242", "suoritus.solki_id", 4242)
    }

    @Test
    fun `oppijanumerottoman YKI-suorituksen tietosivu kirjataan auditlokiin`() {
        val id =
            ykiSuoritukset.save(
                generateRandomYkiSuoritusEntity().copy(solkiId = 4343, suorittajanOID = null),
                false,
            )!!

        auditoi("/yki/suoritukset/$id", "suoritus.id", id)
    }
}
