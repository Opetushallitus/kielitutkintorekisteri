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
import java.time.OffsetDateTime
import kotlin.test.Test
import kotlin.test.assertContains
import kotlin.test.assertFalse

@SpringBootTest
@Import(DBContainerConfiguration::class)
class ArvioijahakemusViewControllerTest(
    @param:Autowired private val context: WebApplicationContext,
    @param:Autowired private val repository: ArvioijahakemusRepository,
) {
    private lateinit var mockMvc: MockMvc

    @BeforeEach
    fun setup() {
        mockMvc =
            MockMvcBuilders
                .webAppContextSetup(context)
                .apply<DefaultMockMvcBuilder>(springSecurity())
                .build()
        repository.deleteAll()
    }

    private fun rivi(
        oid: String,
        tila: ArvioijahakemuksenTila,
        syy: String? = null,
    ) = ArvioijahakemusEntity(oid, "1.2.246.562.24.59267607404", tila, syy, null, null, OffsetDateTime.now())

    @Test
    fun `nakyma listaa vain kasittelemattomat hakemukset`() {
        repository.tallenna(rivi("hylatty-hakemus", ArvioijahakemuksenTila.HYLATTY, "Henkilöä ei ole yksilöity"))
        repository.tallenna(rivi("kasitelty-hakemus", ArvioijahakemuksenTila.KASITELTY))

        val html =
            mockMvc
                .perform(
                    get(
                        "/yki/arvioijat/hakemukset",
                    ).session(session(Authority.VIRKAILIJA, Authority.YKI_ARVIOIJAREKISTERI)),
                ).andExpect(status().isOk)
                .andReturn()
                .response.contentAsString

        assertContains(html, "hylatty-hakemus")
        assertContains(html, "Henkilöä ei ole yksilöity")
        assertFalse(html.contains("kasitelty-hakemus"))
    }

    @Test
    fun `nakyma vaatii arvioijarekisterin oikeuden`() {
        mockMvc
            .perform(get("/yki/arvioijat/hakemukset").session(session(Authority.VIRKAILIJA)))
            .andExpect(status().isForbidden)
    }

    private fun session(vararg authorities: Authority): MockHttpSession {
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
