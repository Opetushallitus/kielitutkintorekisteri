package fi.oph.kitu.vkt

import fi.oph.kitu.DBContainerConfiguration
import fi.oph.kitu.oid.Oid
import fi.oph.kitu.schema.SchemaTests
import fi.oph.kitu.security.Authority
import fi.oph.kitu.security.cas.CasUserDetails
import fi.oph.kitu.util.defaultObjectMapper
import fi.oph.kitu.util.result.getOrThrow
import org.junit.jupiter.api.BeforeEach
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.http.MediaType
import org.springframework.mock.web.MockHttpSession
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken
import org.springframework.security.core.Authentication
import org.springframework.security.core.authority.SimpleGrantedAuthority
import org.springframework.security.core.context.SecurityContextHolder
import org.springframework.security.test.web.servlet.setup.SecurityMockMvcConfigurers.springSecurity
import org.springframework.security.web.context.HttpSessionSecurityContextRepository
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.status
import org.springframework.test.web.servlet.setup.DefaultMockMvcBuilder
import org.springframework.test.web.servlet.setup.MockMvcBuilders
import org.springframework.web.context.WebApplicationContext
import org.testcontainers.postgresql.PostgreSQLContainer
import kotlin.test.Test

@SpringBootTest
@Import(DBContainerConfiguration::class)
class VktCasAuthorizationTest {
    @Autowired
    private lateinit var context: WebApplicationContext

    @Autowired
    private lateinit var vktRepository: VktSuoritusRepository

    @Autowired private var postgres: PostgreSQLContainer? = null
    private var mockMvc: MockMvc? = null

    @BeforeEach
    fun setup() {
        vktRepository.deleteAll()
        mockMvc =
            MockMvcBuilders
                .webAppContextSetup(context)
                .apply<DefaultMockMvcBuilder>(springSecurity())
                .build()
    }

    @Test
    fun `PUT api vkt kios via CAS session with VKT_TALLENNUS authority is allowed`() {
        mockMvc!!
            .perform(
                put("/api/vkt/kios")
                    .session(sessionFor(Authority.VKT_TALLENNUS))
                    .contentType(MediaType.APPLICATION_JSON)
                    .accept(MediaType.APPLICATION_JSON)
                    .content(defaultObjectMapper.writeValueAsString(SchemaTests.vktHenkilosuoritus)),
            ).andExpect(status().isOk)
    }

    @Test
    fun `PUT api vkt kios via CAS session without VKT_TALLENNUS is forbidden`() {
        mockMvc!!
            .perform(
                put("/api/vkt/kios")
                    .session(sessionFor(Authority.VIRKAILIJA))
                    .contentType(MediaType.APPLICATION_JSON)
                    .accept(MediaType.APPLICATION_JSON)
                    .content(defaultObjectMapper.writeValueAsString(SchemaTests.vktHenkilosuoritus)),
            ).andExpect(status().isForbidden)
    }

    private fun sessionFor(vararg authorities: Authority): MockHttpSession {
        val principal =
            CasUserDetails(
                name = "test-kios-user",
                oid = Oid.parse("1.2.246.562.24.20281155246").getOrThrow(),
                strongAuth = false,
                kayttajaTyyppi = "PALVELU",
                authorities = authorities.map { SimpleGrantedAuthority(it.role()) },
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
