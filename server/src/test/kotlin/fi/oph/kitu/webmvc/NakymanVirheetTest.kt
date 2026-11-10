package fi.oph.kitu.webmvc

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import fi.oph.kitu.DBContainerConfiguration
import fi.oph.kitu.virkailijaSession
import org.junit.jupiter.api.BeforeEach
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Import
import org.springframework.security.test.web.servlet.setup.SecurityMockMvcConfigurers.springSecurity
import org.springframework.stereotype.Controller
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.status
import org.springframework.test.web.servlet.setup.DefaultMockMvcBuilder
import org.springframework.test.web.servlet.setup.MockMvcBuilders
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.context.WebApplicationContext
import kotlin.test.Test
import kotlin.test.assertTrue
import ch.qos.logback.classic.Logger as LogbackLogger

@SpringBootTest
@Import(DBContainerConfiguration::class, NakymanVirheetTest.KaatuvaNakyma::class)
class NakymanVirheetTest(
    @param:Autowired private val context: WebApplicationContext,
) {
    @TestConfiguration
    class KaatuvaNakyma {
        @Controller
        class Kaatuu {
            @GetMapping("/testi/kaatuu", produces = ["text/html"])
            fun kaatuu(): String = throw IllegalStateException("testin odottamaton virhe")
        }
    }

    private lateinit var mockMvc: MockMvc

    @BeforeEach
    fun setup() {
        mockMvc =
            MockMvcBuilders
                .webAppContextSetup(context)
                .apply<DefaultMockMvcBuilder>(springSecurity())
                .build()
    }

    @Test
    fun `virheelliset sivutus- ja jarjestysparametrit eivat kaada listoja`() {
        listOf(
            "/yki/suoritukset?page=0",
            "/yki/suoritukset?page=-1",
            "/yki/suoritukset?limit=0",
            "/yki/suoritukset?sortColumn=Arviointipaiva",
            "/koto-kielitesti/suoritukset?page=0",
            "/vkt/?pageNumber=-1",
        ).forEach { polku ->
            mockMvc.perform(get(polku).session(virkailijaSession())).andExpect(status().isOk)
        }
    }

    @Test
    fun `vaara HTTP-metodi on 405 eika 500`() {
        mockMvc
            .perform(get("/kaannokset/tyhjenna-valimuisti").session(virkailijaSession()))
            .andExpect(status().isMethodNotAllowed)
    }

    @Test
    fun `odottamaton virhe kirjataan lokiin virheena`() {
        val logger = LoggerFactory.getLogger(GlobalControllerExceptionHandler::class.java) as LogbackLogger
        val loki = ListAppender<ILoggingEvent>().apply { start() }
        logger.addAppender(loki)
        try {
            mockMvc
                .perform(get("/testi/kaatuu").session(virkailijaSession()))
                .andExpect(status().isInternalServerError)
        } finally {
            logger.detachAppender(loki)
            loki.stop()
        }

        assertTrue(
            loki.list.any { it.level == Level.ERROR && it.throwableProxy?.message == "testin odottamaton virhe" },
        )
    }
}
