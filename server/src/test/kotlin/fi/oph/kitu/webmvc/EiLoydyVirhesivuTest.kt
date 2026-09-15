package fi.oph.kitu.webmvc

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
import org.springframework.security.core.Authentication
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
import kotlin.test.assertContains
import kotlin.test.assertEquals

@SpringBootTest
@Import(DBContainerConfiguration::class)
class EiLoydyVirhesivuTest(
    @param:Autowired private val context: WebApplicationContext,
) {
    private lateinit var mockMvc: MockMvc

    private val tuntematonOid = "1.2.246.562.24.99999999999"

    @BeforeEach
    fun setup() {
        mockMvc =
            MockMvcBuilders
                .webAppContextSetup(context)
                .apply<DefaultMockMvcBuilder>(springSecurity())
                .build()
    }

    @Test
    fun `html-nakymat nayttavat virhesivun kun kohdetta ei ole`() {
        val polut =
            listOf(
                "/yki/suoritukset/999999",
                "/yki/arvioijat/999999",
                "/yki/arvioijat/999999/muokkaa",
                "/yki/arvioijat/999999/kaudet/uusi",
                "/koto-kielitesti/suoritukset/999999",
                "/vkt/suoritukset/$tuntematonOid/FIN/Erinomainen",
            )

        for (polku in polut) {
            val vastaus =
                mockMvc
                    .perform(get(polku).session(virkailijaSession()))
                    .andReturn()
                    .response

            assertEquals(404, vastaus.status, polku)
            assertContains(vastaus.contentAsString, "Sivua ei löydy", message = polku)
        }
    }

    /** Koneluettava paattypiste ei saa alkaa palauttamaan HTML-virhesivua. */
    @Test
    fun `json-paattypisteet palauttavat tyhjan 404n`() {
        val vastaus =
            mockMvc
                .perform(get("/yki/koski-request/999999").session(virkailijaSession()))
                .andExpect(status().isNotFound)
                .andReturn()
                .response

        assertEquals("", vastaus.contentAsString)
    }

    private fun virkailijaSession(): MockHttpSession {
        val principal =
            CasUserDetails(
                name = "test-virkailija",
                oid = Oid.parse("1.2.246.562.24.20281155246").getOrThrow(),
                strongAuth = false,
                kayttajaTyyppi = "VIRKAILIJA",
                asiointikieli = null,
                authorities =
                    listOf(Authority.VIRKAILIJA, Authority.YKI_ARVIOIJAREKISTERI).map {
                        SimpleGrantedAuthority(it.role())
                    },
            )
        val authentication: Authentication =
            UsernamePasswordAuthenticationToken(principal, null, principal.authorities)
        val securityContext =
            SecurityContextHolder.createEmptyContext().apply {
                this.authentication = authentication
            }
        return MockHttpSession().also { session ->
            session.setAttribute(
                HttpSessionSecurityContextRepository.SPRING_SECURITY_CONTEXT_KEY,
                securityContext,
            )
        }
    }
}
