package fi.oph.kitu.security.cas

import fi.oph.kitu.DBContainerConfiguration
import jakarta.servlet.http.Cookie
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.security.test.web.servlet.setup.SecurityMockMvcConfigurers.springSecurity
import org.springframework.session.web.http.SessionRepositoryFilter
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.status
import org.springframework.test.web.servlet.setup.DefaultMockMvcBuilder
import org.springframework.test.web.servlet.setup.MockMvcBuilders
import org.springframework.web.context.WebApplicationContext
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

@SpringBootTest
@Import(DBContainerConfiguration::class, CasSingleLogoutTest.HyvaksyKaikkiTiketit::class)
class CasKirjautumisenPaluuTest(
    @param:Autowired private val context: WebApplicationContext,
    @param:Autowired private val sessionRepositoryFilter: SessionRepositoryFilter<*>,
) {
    private lateinit var mockMvc: MockMvc

    @BeforeEach
    fun setup() {
        mockMvc =
            MockMvcBuilders
                .webAppContextSetup(context)
                .addFilters<DefaultMockMvcBuilder>(sessionRepositoryFilter)
                .apply<DefaultMockMvcBuilder>(springSecurity())
                .build()
    }

    @ParameterizedTest
    @ValueSource(strings = ["/style.css", "/pico.min.css"])
    fun `tyylitiedosto ei ole kirjautumisen paluuosoite`(tyylitiedosto: String) {
        val evaste: Cookie =
            assertNotNull(
                mockMvc
                    .perform(get("/yki/suoritukset"))
                    .andExpect(status().is3xxRedirection)
                    .andReturn()
                    .response
                    .getCookie("SESSION"),
            )

        mockMvc.perform(get(tyylitiedosto).cookie(evaste)).andExpect(status().isOk)

        val paluu =
            mockMvc
                .perform(get("/j_spring_cas_security_check?ticket=ST-paluu").cookie(evaste))
                .andExpect(status().is3xxRedirection)
                .andReturn()
                .response
                .redirectedUrl
                .orEmpty()

        assertEquals("/yki/suoritukset", paluu.substringAfter("localhost").substringBefore("?"))
    }
}
