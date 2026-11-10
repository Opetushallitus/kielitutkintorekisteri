package fi.oph.kitu.vkt

import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import fi.oph.kitu.DBContainerConfiguration
import fi.oph.kitu.auditlogs.AUDIT_LOGGER_NAME
import fi.oph.kitu.dev.mockdata.VktSuoritusMockGenerator
import fi.oph.kitu.koodisto.Koodisto
import fi.oph.kitu.tiedontuontischema.VktValidation
import fi.oph.kitu.virkailijaSession
import org.junit.jupiter.api.BeforeEach
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.http.MediaType
import org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.csrf
import org.springframework.security.test.web.servlet.setup.SecurityMockMvcConfigurers.springSecurity
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.status
import org.springframework.test.web.servlet.setup.DefaultMockMvcBuilder
import org.springframework.test.web.servlet.setup.MockMvcBuilders
import org.springframework.web.context.WebApplicationContext
import java.time.LocalDate
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue
import ch.qos.logback.classic.Logger as LogbackLogger

@SpringBootTest
@Import(DBContainerConfiguration::class)
class VktErinomaisenArviointiTest(
    @param:Autowired private val context: WebApplicationContext,
    @param:Autowired private val suoritusRepository: VktSuoritusRepository,
    @param:Autowired private val vktValidation: VktValidation,
) {
    private lateinit var mockMvc: MockMvc
    private val generaattori = VktSuoritusMockGenerator()

    @BeforeEach
    fun setup() {
        mockMvc =
            MockMvcBuilders
                .webAppContextSetup(context)
                .apply<DefaultMockMvcBuilder>(springSecurity())
                .build()
        suoritusRepository.deleteAll()
    }

    private fun tallennettu(taso: Koodisto.VktTaitotaso): VktSuoritusEntity =
        suoritusRepository.save(
            generateSequence { generaattori.generateRandomVktSuoritusEntity(vktValidation) }
                .first { it.taitotaso == taso },
        )

    private fun polku(suoritus: VktSuoritusEntity) =
        "/vkt/suoritukset/${suoritus.suorittajanOid}/${suoritus.tutkintokieli.name}/${suoritus.taitotaso.name}"

    private fun arvosanaKannassa(osakoeId: Int): Koodisto.VktArvosana? =
        suoritusRepository
            .findAll()
            .flatMap { it.osakokeet }
            .single { it.id == osakoeId }
            .arvosana

    private fun tallenna(
        suoritus: VktSuoritusEntity,
        osakoeId: Int,
    ) = mockMvc.perform(
        post(polku(suoritus))
            .session(virkailijaSession())
            .with(csrf())
            .contentType(MediaType.APPLICATION_FORM_URLENCODED)
            .param("id", osakoeId.toString())
            .param("arvosana", Koodisto.VktArvosana.Hylätty.name)
            .param("arviointipaiva", LocalDate.of(2026, 11, 1).toString()),
    )

    @Test
    fun `toisen henkilon osakoetta ei voi arvioida`() {
        val oma = tallennettu(Koodisto.VktTaitotaso.Erinomainen)
        val vieras = tallennettu(Koodisto.VktTaitotaso.Erinomainen)
        val vierasOsakoe = vieras.osakokeet.first()

        tallenna(oma, vierasOsakoe.id!!).andExpect(status().isBadRequest)

        assertEquals(vierasOsakoe.arvosana, arvosanaKannassa(vierasOsakoe.id!!))
    }

    @Test
    fun `hyvan ja tyydyttavan tason osakoetta ei voi arvioida kitussa`() {
        val suoritus = tallennettu(Koodisto.VktTaitotaso.HyväJaTyydyttävä)
        val osakoe = suoritus.osakokeet.first()

        tallenna(suoritus, osakoe.id!!).andExpect(status().isBadRequest)

        assertEquals(osakoe.arvosana, arvosanaKannassa(osakoe.id!!))
    }

    @Test
    fun `oman osakokeen arviointi tallentuu ja kirjataan auditlokiin`() {
        val suoritus = tallennettu(Koodisto.VktTaitotaso.Erinomainen)
        val osakoeId = suoritus.osakokeet.first().id!!
        val auditLogger = LoggerFactory.getLogger(AUDIT_LOGGER_NAME) as LogbackLogger
        val loki = ListAppender<ILoggingEvent>().apply { start() }
        auditLogger.addAppender(loki)
        try {
            tallenna(suoritus, osakoeId).andExpect(status().is3xxRedirection)
            assertTrue(odotaAuditoitu(loki, osakoeId), "Osakoetta $osakoeId ei kirjattu auditlokiin")
        } finally {
            auditLogger.detachAppender(loki)
            loki.stop()
        }

        assertEquals(Koodisto.VktArvosana.Hylätty, arvosanaKannassa(osakoeId))
        assertTrue(loki.list.any { it.formattedMessage.contains("VktArviointiTallennettu") })
    }

    private fun odotaAuditoitu(
        loki: ListAppender<ILoggingEvent>,
        osakoeId: Int,
    ): Boolean {
        val loppu = System.currentTimeMillis() + 5_000
        while (System.currentTimeMillis() < loppu) {
            val loytyi =
                loki.list
                    .toList()
                    .flatMap { it.keyValuePairs.orEmpty() }
                    .any { it.key == "osakoe.id" && it.value == osakoeId }
            if (loytyi) return true
            Thread.sleep(20)
        }
        return false
    }

    @Test
    fun `tuntematon osakoe hylataan`() {
        val suoritus = tallennettu(Koodisto.VktTaitotaso.Erinomainen)

        tallenna(suoritus, Int.MAX_VALUE).andExpect(status().isBadRequest)

        assertNull(suoritusRepository.findAll().flatMap { it.osakokeet }.firstOrNull { it.id == Int.MAX_VALUE })
    }
}
