package fi.oph.kitu.yki.arvioijat.hakemus

import arrow.core.nonEmptyListOf
import fi.oph.kitu.DBContainerConfiguration
import fi.oph.kitu.oid.Oid
import fi.oph.kitu.security.Authority
import fi.oph.kitu.security.cas.CasUserDetails
import fi.oph.kitu.tyhjennaArvioijahakemukset
import fi.oph.kitu.util.defaultObjectMapper
import fi.oph.kitu.util.result.getOrThrow
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
import java.time.OffsetDateTime
import kotlin.test.Test
import kotlin.test.assertContains
import kotlin.test.assertFalse

@SpringBootTest(properties = ["kitu.ataru.service.url=http://localhost/mock-ataru", "kitu.dashboard.cache.ttl=0s"])
@Import(DBContainerConfiguration::class)
class ArvioijahakemusViewControllerTest(
    @param:Autowired private val context: WebApplicationContext,
    @param:Autowired private val repository: ArvioijahakemusRepository,
    @param:Autowired private val virheRepository: ArvioijahakemusVirheRepository,
    @param:Autowired private val jdbcTemplate: JdbcTemplate,
) {
    private lateinit var mockMvc: MockMvc

    @BeforeEach
    fun setup() {
        mockMvc =
            MockMvcBuilders
                .webAppContextSetup(context)
                .apply<DefaultMockMvcBuilder>(springSecurity())
                .build()
        jdbcTemplate.tyhjennaArvioijahakemukset()
    }

    private fun rivi(
        oid: String,
        tila: ArvioijahakemuksenTila,
        syy: String? = null,
    ) = ArvioijahakemusEntity(oid, "1.2.246.562.24.59267607404", tila, syy, null, null, OffsetDateTime.now())

    @Test
    fun `nakyma listaa vain kasittelemattomat hakemukset`() {
        repository.tallenna(
            rivi("odottava-hakemus", ArvioijahakemuksenTila.ODOTTAA_YKSILOINTIA, "Henkilöä ei ole yksilöity"),
        )
        repository.tallenna(rivi("kasitelty-hakemus", ArvioijahakemuksenTila.KASITELTY))

        val html = sivu("/yki/arvioijat/hakemukset")

        assertContains(html, "odottava-hakemus")
        assertContains(html, "Henkilöä ei ole yksilöity")
        assertFalse(html.contains("kasitelty-hakemus"))
        assertFalse(html.contains("/yki/arvioijat/hakemukset/virheet"), "ei virheitä, ei virheilmoitusta")
    }

    @Test
    fun `virhenakyma nayttaa virheet ja virheellisen arvon`() {
        virheRepository.tallenna(
            "virheellinen-hakemus",
            "1.2.246.562.24.59267607404",
            nonEmptyListOf(
                ArvioijahakemuksenVirhe(
                    ArvioijahakemuksenVirhetyyppi.VAARA_TIETOTYYPPI,
                    "email",
                    defaultObjectMapper.valueToTree(listOf("<b>a@oph.fi</b>")),
                ),
                ArvioijahakemuksenVirhe(
                    ArvioijahakemuksenVirhetyyppi.VALIDOINTIVIRHE,
                    "postinumero",
                    viesti = "Postinumeron on oltava viisi numeroa",
                ),
            ),
            OffsetDateTime.now(),
        )

        val html = sivu("/yki/arvioijat/hakemukset/virheet")

        assertContains(html, "virheellinen-hakemus")
        assertContains(html, "Kentän arvo ei ole tekstiä")
        assertContains(html, "<code>email</code>")
        assertContains(html, "&lt;b&gt;a@oph.fi&lt;/b&gt;")
        assertFalse(html.contains("<b>a@oph.fi</b>"), "hakemuksen arvo escapetaan")
        assertContains(html, "Arvioijan tiedot eivät kelpaa")
        assertContains(html, "Postinumeron on oltava viisi numeroa")
    }

    @Test
    fun `virheista ilmoitetaan hakemusnakymassa ja etusivulla`() {
        virheRepository.tallenna(
            "virheellinen-hakemus",
            null,
            nonEmptyListOf(ArvioijahakemuksenVirhe(ArvioijahakemuksenVirhetyyppi.HENKILOA_EI_LOYDY)),
            OffsetDateTime.now(),
        )

        assertContains(sivu("/yki/arvioijat/hakemukset"), "/yki/arvioijat/hakemukset/virheet")

        val etusivu = sivu("/dashboard/yki")
        assertContains(etusivu, "Virheelliset arvioijahakemukset")
        assertContains(etusivu, "/yki/arvioijat/hakemukset/virheet")
    }

    @Test
    fun `nakymat vaativat arvioijarekisterin oikeuden`() {
        mockMvc
            .perform(get("/yki/arvioijat/hakemukset").session(session(Authority.VIRKAILIJA)))
            .andExpect(status().isForbidden)
        mockMvc
            .perform(get("/yki/arvioijat/hakemukset/virheet").session(session(Authority.VIRKAILIJA)))
            .andExpect(status().isForbidden)
    }

    private fun sivu(polku: String): String =
        mockMvc
            .perform(get(polku).session(session(Authority.VIRKAILIJA, Authority.YKI_ARVIOIJAREKISTERI)))
            .andExpect(status().isOk)
            .andReturn()
            .response.contentAsString

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
