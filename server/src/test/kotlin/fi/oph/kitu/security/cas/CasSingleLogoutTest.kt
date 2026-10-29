package fi.oph.kitu.security.cas

import fi.oph.kitu.DBContainerConfiguration
import fi.oph.kitu.security.Authority
import org.apereo.cas.client.authentication.AttributePrincipalImpl
import org.apereo.cas.client.validation.AssertionImpl
import org.apereo.cas.client.validation.TicketValidator
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Import
import org.springframework.context.annotation.Primary
import org.springframework.http.MediaType
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.security.test.web.servlet.setup.SecurityMockMvcConfigurers.springSecurity
import org.springframework.session.Session
import org.springframework.session.SessionRepository
import org.springframework.session.web.http.SessionRepositoryFilter
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.status
import org.springframework.test.web.servlet.setup.DefaultMockMvcBuilder
import org.springframework.test.web.servlet.setup.MockMvcBuilders
import org.springframework.web.context.WebApplicationContext
import kotlin.test.assertContains
import kotlin.test.assertNotEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull

@SpringBootTest
@Import(DBContainerConfiguration::class, CasSingleLogoutTest.HyvaksyKaikkiTiketit::class)
class CasSingleLogoutTest(
    @param:Autowired private val context: WebApplicationContext,
    @param:Autowired private val sessionRepositoryFilter: SessionRepositoryFilter<*>,
    @param:Autowired private val sessionRepository: SessionRepository<out Session>,
    @param:Autowired private val jdbcTemplate: JdbcTemplate,
) {
    @TestConfiguration(proxyBeanMethods = false)
    class HyvaksyKaikkiTiketit {
        @Bean
        @Primary
        fun hyvaksyKaikkiTiketit(): TicketValidator =
            TicketValidator { _, _ ->
                AssertionImpl(
                    AttributePrincipalImpl(
                        "test-virkailija",
                        mapOf(
                            "oidHenkilo" to "1.2.246.562.24.20281155246",
                            "kayttajaTyyppi" to "VIRKAILIJA",
                            "roles" to listOf(Authority.VIRKAILIJA.role()),
                        ),
                    ),
                )
            }
    }

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

    private fun istuntoTiketille(tiketti: String): String? =
        jdbcTemplate
            .queryForList("SELECT session_id FROM cas_client_session WHERE mapping_id = ?", String::class.java, tiketti)
            .firstOrNull()

    private fun logoutRequest(tiketti: String) =
        """<samlp:LogoutRequest xmlns:samlp="urn:oasis:names:tc:SAML:2.0:protocol" """ +
            """xmlns:saml="urn:oasis:names:tc:SAML:2.0:assertion" ID="LR-1" Version="2.0" """ +
            """IssueInstant="2026-10-29T10:00:00Z"><saml:NameID>@NOT_USED@</saml:NameID>""" +
            """<samlp:SessionIndex>$tiketti</samlp:SessionIndex></samlp:LogoutRequest>"""

    @Test
    fun `CASin uloskirjautumispyynto poistaa istunnon ja uudelleenkirjautuminen saa uuden istunnon`() {
        val evaste =
            mockMvc
                .perform(get("/j_spring_cas_security_check?ticket=ST-1"))
                .andExpect(status().is3xxRedirection)
                .andReturn()
                .response
                .getCookie("SESSION")
        assertNotNull(evaste)
        val istunnonId = assertNotNull(istuntoTiketille("ST-1"))
        assertNotNull(sessionRepository.findById(istunnonId))

        mockMvc.perform(get("/").cookie(evaste)).andExpect(status().isOk)

        mockMvc
            .perform(
                post("/j_spring_cas_security_check")
                    .contentType(MediaType.APPLICATION_FORM_URLENCODED)
                    .param("logoutRequest", logoutRequest("ST-1")),
            ).andExpect(status().isOk)

        assertNull(sessionRepository.findById(istunnonId))
        assertNull(istuntoTiketille("ST-1"))

        val uudelleen =
            mockMvc
                .perform(get("/").cookie(evaste))
                .andExpect(status().is3xxRedirection)
                .andReturn()
                .response
        assertContains(uudelleen.redirectedUrl.orEmpty(), "/cas/login")
        val uusiEvaste = assertNotNull(uudelleen.getCookie("SESSION"))
        assertNotEquals(evaste.value, uusiEvaste.value)
    }
}
