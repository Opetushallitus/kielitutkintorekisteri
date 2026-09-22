package fi.oph.kitu.i18n

import fi.oph.kitu.DBContainerConfiguration
import fi.oph.kitu.oid.Oid
import fi.oph.kitu.security.Authority
import fi.oph.kitu.security.cas.CasUserDetails
import fi.oph.kitu.util.result.getOrThrow
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.mock.web.MockHttpSession
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken
import org.springframework.security.core.Authentication
import org.springframework.security.core.authority.SimpleGrantedAuthority
import org.springframework.security.core.context.SecurityContextHolder
import org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.csrf
import org.springframework.security.test.web.servlet.setup.SecurityMockMvcConfigurers.springSecurity
import org.springframework.security.web.context.HttpSessionSecurityContextRepository
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.redirectedUrl
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.status
import org.springframework.test.web.servlet.setup.DefaultMockMvcBuilder
import org.springframework.test.web.servlet.setup.MockMvcBuilders
import org.springframework.web.context.WebApplicationContext
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull

@SpringBootTest
@Import(DBContainerConfiguration::class)
class LokalisointiViewControllerTest(
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

    @AfterEach
    fun clearTolgee() = TolgeeMessages.clear()

    @Test
    fun `reitti tyhjentaa kaannosvalimuistin ja ohjaa etusivulle`() {
        TolgeeMessages.set(mapOf("nav.yki" to LocalizedString(sv = "Allmän språkexamen")))

        mockMvc
            .perform(post("/kaannokset/tyhjenna-valimuisti").session(virkailijaSession()).with(csrf()))
            .andExpect(status().is3xxRedirection)
            .andExpect(redirectedUrl("http://localhost/"))

        assertNull(TolgeeMessages.get("nav.yki"))
    }

    @Test
    fun `tyhjennyksen jalkeen kaannokset eivat enaa nay ja etusivu kertoo tuloksen`() {
        TolgeeMessages.set(mapOf("nav.yki" to LocalizedString(sv = "Allmän språkexamen")))
        val session = virkailijaSession()

        mockMvc
            .perform(post("/kaannokset/tyhjenna-valimuisti").session(session).with(csrf()))
            .andExpect(status().is3xxRedirection)

        val etusivu =
            mockMvc
                .perform(get("/?lang=sv").session(session))
                .andExpect(status().isOk)
                .andReturn()
                .response.contentAsString

        assertContains(etusivu, "Käännösvälimuisti tyhjennettiin")
        assertFalse(
            etusivu.contains("Allmän språkexamen"),
            "Tyhjennetystä välimuistista ei enää saada ruotsinkielistä käännöstä",
        )
    }

    @Test
    fun `tyhjennys vaatii kirjautumisen`() {
        TolgeeMessages.set(mapOf("nav.yki" to LocalizedString(sv = "Allmän språkexamen")))

        mockMvc
            .perform(post("/kaannokset/tyhjenna-valimuisti").with(csrf()))
            .andExpect(status().is3xxRedirection)

        assertEquals("Allmän språkexamen", TolgeeMessages.get("nav.yki")?.sv)
    }

    private fun virkailijaSession(): MockHttpSession {
        val principal =
            CasUserDetails(
                name = "test-virkailija",
                oid = Oid.parse("1.2.246.562.24.20281155246").getOrThrow(),
                strongAuth = false,
                kayttajaTyyppi = "VIRKAILIJA",
                authorities = listOf(SimpleGrantedAuthority(Authority.VIRKAILIJA.role())),
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
