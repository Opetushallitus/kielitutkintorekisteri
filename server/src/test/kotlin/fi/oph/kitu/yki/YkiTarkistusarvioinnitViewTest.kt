package fi.oph.kitu.yki

import fi.oph.kitu.DBContainerConfiguration
import fi.oph.kitu.TestTimeService
import fi.oph.kitu.dev.mockdata.generateRandomYkiSuoritusEntity
import fi.oph.kitu.oid.Oid
import fi.oph.kitu.security.Authority
import fi.oph.kitu.security.cas.CasUserDetails
import fi.oph.kitu.util.result.getOrThrow
import fi.oph.kitu.yki.suoritukset.YkiSuoritusRepository
import org.junit.jupiter.api.BeforeEach
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.jdbc.core.JdbcTemplate
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
import java.time.Instant
import kotlin.test.Test
import kotlin.test.assertContains

@SpringBootTest
@Import(DBContainerConfiguration::class)
class YkiTarkistusarvioinnitViewTest(
    @param:Autowired private val context: WebApplicationContext,
    @param:Autowired private val suoritusRepository: YkiSuoritusRepository,
    @param:Autowired private val timeService: TestTimeService,
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

        jdbc.execute("TRUNCATE TABLE yki_suoritus CASCADE")
        suoritusRepository.save(
            generateRandomYkiSuoritusEntity().copy(arviointitila = Arviointitila.TARKISTUSARVIOITU),
            false,
        )
    }

    @Test
    fun `hyvaksymispaivan oletus on Suomen aikavyohykkeen tama paiva`() {
        val html =
            timeService.runWithFixedClock(Instant.parse("2026-01-01T22:30:00Z")) {
                mockMvc
                    .perform(get("/yki/tarkistusarvioinnit").session(virkailijaSession()))
                    .andExpect(status().isOk)
                    .andReturn()
                    .response.contentAsString
            }

        assertContains(html, """value="2026-01-02"""")
    }

    private fun virkailijaSession(): MockHttpSession {
        val principal =
            CasUserDetails(
                name = "test-virkailija",
                oid = Oid.parse("1.2.246.562.24.20281155246").getOrThrow(),
                strongAuth = false,
                kayttajaTyyppi = "VIRKAILIJA",
                asiointikieli = null,
                authorities = listOf(SimpleGrantedAuthority(Authority.VIRKAILIJA.role())),
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
