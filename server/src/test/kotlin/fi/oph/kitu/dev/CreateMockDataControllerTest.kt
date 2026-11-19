package fi.oph.kitu.dev

import fi.oph.kitu.DBContainerConfiguration
import fi.oph.kitu.virkailijaSession
import org.junit.jupiter.api.BeforeEach
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.security.test.web.servlet.setup.SecurityMockMvcConfigurers.springSecurity
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.content
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.status
import org.springframework.test.web.servlet.setup.DefaultMockMvcBuilder
import org.springframework.test.web.servlet.setup.MockMvcBuilders
import org.springframework.web.context.WebApplicationContext
import kotlin.test.Test

@SpringBootTest
@ActiveProfiles("test", "local")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@Import(DBContainerConfiguration::class)
class CreateMockDataControllerTest(
    @param:Autowired private val context: WebApplicationContext,
) {
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
    fun `selaimen Accept-otsakkeella mock-data palautetaan JSONina`() {
        mockMvc
            .perform(
                get("/dev/mockdata/yki/arvioija/1")
                    .session(virkailijaSession())
                    .header("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"),
            ).andExpect(status().isOk)
            .andExpect(content().contentTypeCompatibleWith("application/json"))
            .andExpect(jsonPath("$").isArray)
    }
}
