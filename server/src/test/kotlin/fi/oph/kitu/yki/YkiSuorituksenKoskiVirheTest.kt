package fi.oph.kitu.yki

import fi.oph.kitu.DBContainerConfiguration
import fi.oph.kitu.dev.mockdata.generateRandomYkiSuoritusEntity
import fi.oph.kitu.koski.KoskiErrorService
import fi.oph.kitu.koski.YkiMappingId
import fi.oph.kitu.tyhjennaYkiSuoritukset
import fi.oph.kitu.virkailijaSession
import fi.oph.kitu.yki.suoritukset.YkiSuoritusRepository
import org.junit.jupiter.api.BeforeEach
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
import kotlin.test.assertContains
import kotlin.test.assertNotEquals

@SpringBootTest
@Import(DBContainerConfiguration::class)
class YkiSuorituksenKoskiVirheTest(
    @param:Autowired private val context: WebApplicationContext,
    @param:Autowired private val suoritusRepository: YkiSuoritusRepository,
    @param:Autowired private val koskiErrorService: KoskiErrorService,
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
    }

    @Test
    fun `tietosivu nayttaa Solki-tunnisteelle kirjatun KOSKI-virheen`() {
        val solkiId = 515_151
        val id = suoritusRepository.save(generateRandomYkiSuoritusEntity().copy(solkiId = solkiId), false)!!
        assertNotEquals(solkiId, id)
        koskiErrorService.save(
            YkiMappingId(solkiId),
            "400 Bad Request: \"[{\"key\":\"badRequest.testivirhe\"}]\"",
        )

        val sivu =
            mockMvc
                .perform(get("/yki/suoritukset/$id").session(virkailijaSession()))
                .andExpect(status().isOk)
                .andReturn()
                .response
                .contentAsString

        assertContains(sivu, "badRequest.testivirhe")
    }
}
