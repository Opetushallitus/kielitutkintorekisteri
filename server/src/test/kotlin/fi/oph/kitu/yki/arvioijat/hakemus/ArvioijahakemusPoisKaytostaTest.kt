package fi.oph.kitu.yki.arvioijat.hakemus

import fi.oph.kitu.DBContainerConfiguration
import fi.oph.kitu.oid.Oid
import fi.oph.kitu.security.Authority
import fi.oph.kitu.security.cas.CasUserDetails
import fi.oph.kitu.util.result.getOrThrow
import org.junit.jupiter.api.BeforeEach
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.mock.web.MockHttpSession
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken
import org.springframework.security.core.authority.SimpleGrantedAuthority
import org.springframework.security.core.context.SecurityContextHolder
import org.springframework.security.test.web.servlet.setup.SecurityMockMvcConfigurers.springSecurity
import org.springframework.security.web.context.HttpSessionSecurityContextRepository
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.status
import org.springframework.test.web.servlet.setup.DefaultMockMvcBuilder
import org.springframework.test.web.servlet.setup.MockMvcBuilders
import org.springframework.web.context.WebApplicationContext
import kotlin.test.Test
import kotlin.test.assertFalse

@SpringBootTest
@Import(DBContainerConfiguration::class)
class ArvioijahakemusPoisKaytostaTest(
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
    fun `hakemusnakyma on 404 kun tuonti ei ole kaytossa`() {
        mockMvc
            .perform(get("/yki/arvioijat/hakemukset").session(session()))
            .andExpect(status().isNotFound)
    }

    @Test
    fun `arvioijalistalla ei ole hakemuslinkkia kun tuonti ei ole kaytossa`() {
        val html =
            mockMvc
                .perform(get("/yki/arvioijat").session(session()))
                .andExpect(status().isOk)
                .andReturn()
                .response.contentAsString

        assertFalse(html.contains("arvioijahakemukset"))
    }

    private fun session(): MockHttpSession {
        val authorities = listOf(Authority.VIRKAILIJA, Authority.YKI_ARVIOIJAREKISTERI)
        val principal =
            CasUserDetails(
                name = "test-virkailija",
                oid = Oid.parse("1.2.246.562.24.20281155246").getOrThrow(),
                strongAuth = false,
                kayttajaTyyppi = "VIRKAILIJA",
                asiointikieli = null,
                authorities = authorities.map { SimpleGrantedAuthority(it.role()) },
            )
        val securityContext =
            SecurityContextHolder.createEmptyContext().apply {
                authentication = UsernamePasswordAuthenticationToken(principal, null, principal.authorities)
            }
        return MockHttpSession().also {
            it.setAttribute(HttpSessionSecurityContextRepository.SPRING_SECURITY_CONTEXT_KEY, securityContext)
        }
    }
}
