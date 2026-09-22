package fi.oph.kitu.webmvc

import fi.oph.kitu.DBContainerConfiguration
import fi.oph.kitu.i18n.LocalizedString
import fi.oph.kitu.i18n.TolgeeMessages
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
import org.springframework.security.test.web.servlet.setup.SecurityMockMvcConfigurers.springSecurity
import org.springframework.security.web.context.HttpSessionSecurityContextRepository
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.status
import org.springframework.test.web.servlet.setup.DefaultMockMvcBuilder
import org.springframework.test.web.servlet.setup.MockMvcBuilders
import org.springframework.web.context.WebApplicationContext
import kotlin.test.assertContains
import kotlin.test.assertFalse

@SpringBootTest
@Import(DBContainerConfiguration::class)
class HomeControllerTest(
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
    fun clearTolgee() = TolgeeMessages.set(emptyMap())

    @Test
    fun `etusivu renderoityy ja sisaltaa nelja sektiokorttia`() {
        val response = getHtml("/")

        assertContains(response, "Kielitutkintorekisteri")
        assertFalse(
            response.contains("warning-text"),
            "Tolgee-varoitusta ei näytetä kun synkronointia ei ole ajettu",
        )
        assertContains(response, """data-testid="dashboard"""")
        assertContains(response, """data-testid="yki-links"""")
        assertContains(response, """data-testid="vkt-links"""")
        assertContains(response, """data-testid="koto-kielitesti-links"""")
        assertContains(response, """data-testid="admin-links"""")
    }

    @Test
    fun `etusivulla on sektion otsikot`() {
        val response = getHtml("/")

        assertContains(response, "Yleinen kielitutkinto")
        assertContains(response, "Valtionhallinnon kielitutkinto")
        assertContains(response, "Kotoutumiskoulutuksen kielitaidon päättötesti")
        assertContains(response, "Ylläpito")
    }

    @Test
    fun `lang-parametri vaihtaa navigaation kielen ja html-lang-attribuutin`() {
        TolgeeMessages.set(mapOf("nav.yki" to LocalizedString(sv = "Allmän språkexamen")))

        val response = getHtml("/?lang=sv")

        assertContains(response, """lang="sv"""")
        assertContains(response, "Allmän språkexamen")
    }

    @Test
    fun `Tolgeen suomenkielinen teksti nakyy navigaatiossa koodin oletuksen sijaan`() {
        TolgeeMessages.set(mapOf("nav.tehtavapaketit" to LocalizedString(fi = "Testipaketit")))

        val response = getHtml("/")

        assertContains(response, "Testipaketit")
        assertFalse(
            response.contains("Tehtäväpaketit"),
            "Koodin oletusteksti korvautuu Tolgeen suomennoksella",
        )
    }

    @Test
    fun `kayttajan asiointikieli ohjaa oletuskielen kun kielivalintaa ei ole tehty`() {
        TolgeeMessages.set(mapOf("nav.yki" to LocalizedString(sv = "Allmän språkexamen")))

        val response =
            mockMvc
                .perform(get("/").session(virkailijaSession(asiointikieli = "sv")))
                .andExpect(status().isOk)
                .andReturn()
                .response.contentAsString

        assertContains(response, """lang="sv"""")
        assertContains(response, "Allmän språkexamen")
    }

    @Test
    fun `nimenomainen lang-parametri ohittaa kayttajan asiointikielen`() {
        TolgeeMessages.set(
            mapOf("nav.yki" to LocalizedString(sv = "Allmän språkexamen", en = "General Language Examination")),
        )

        val response =
            mockMvc
                .perform(get("/?lang=en").session(virkailijaSession(asiointikieli = "sv")))
                .andExpect(status().isOk)
                .andReturn()
                .response.contentAsString

        assertContains(response, """lang="en"""")
        assertContains(response, "General Language Examination")
    }

    @Test
    fun `etusivulla on placeholderit ja loader-skripti mutta ei yki vkt koto statirivien sisaltoa`() {
        val response = getHtml("/")

        assertContains(response, """data-card-content="yki"""")
        assertContains(response, """data-card-content="vkt"""")
        assertContains(response, """data-card-content="koto"""")
        assertContains(response, "skeleton-row")
        assertContains(response, """aria-busy="true"""")
        assertContains(response, """data-card-content="admin"""")
        assertContains(response, "/dashboard/yki")
        assertContains(response, "/dashboard/vkt")
        assertContains(response, "/dashboard/koto")
        assertContains(response, "/dashboard/admin")
        assertFalse(
            response.contains("Viimeisin saapunut suoritus"),
            "\"Viimeisin saapunut suoritus\"-rivi esiintyy vain fragmenteissa, ei yläpalkin navigaatiossa",
        )
    }

    @Test
    fun `dashboard admin -fragmentti palauttaa erajaotilastot ja hallintalinkin`() {
        val fragment = getHtml("/dashboard/admin")

        assertContains(fragment, """class="dashboard-stats"""")
        assertContains(fragment, "Käynnissä olevat eräajot")
        assertContains(fragment, "Eräajot virhetilassa")
        assertContains(fragment, "Eräajojen hallinta")
        assertContains(fragment, "/kielitutkinnot/db-scheduler")
        assertFalse(fragment.contains("<html"), "Fragmenttivastaus ei sisällä sivun kuorta")
    }

    @Test
    fun `kojelaudan statirivien otsikot kaantyvat`() {
        TolgeeMessages.set(
            mapOf(
                "etusivu.kaynnissaOlevatErajot" to LocalizedString(sv = "Pågående satskörningar"),
                "nav.erajojenHallinta" to LocalizedString(sv = "Hantering av satskörningar"),
            ),
        )

        val fragment =
            mockMvc
                .perform(get("/dashboard/admin?lang=sv").session(virkailijaSession()))
                .andExpect(status().isOk)
                .andReturn()
                .response.contentAsString

        assertContains(fragment, "Pågående satskörningar", message = "Etusivun oma avain kääntyy")
        assertContains(fragment, "Hantering av satskörningar", message = "Navigaatiosta jaettu avain kääntyy")
        assertFalse(fragment.contains("Käynnissä olevat eräajot"), "Suomenkielinen oletus ei jää näkyviin")
    }

    @Test
    fun `dashboard yki -fragmentti palauttaa statirivit`() {
        val fragment = getHtml("/dashboard/yki")

        assertContains(fragment, """class="dashboard-stats"""")
        assertContains(fragment, "Suoritukset")
        assertContains(fragment, "Arvioijat")
        assertContains(fragment, "Tarkistusarvioinnit")
        assertContains(fragment, "Viimeisin saapunut suoritus")
        assertFalse(fragment.contains("<html"), "Fragmenttivastaus ei sisällä sivun kuorta")
        assertFalse(fragment.contains("<head"), "Fragmenttivastaus ei sisällä sivun kuorta")
    }

    @Test
    fun `dashboard vkt -fragmentti palauttaa statirivit`() {
        val fragment = getHtml("/dashboard/vkt")

        assertContains(fragment, """class="dashboard-stats"""")
        assertContains(fragment, "Kaikki suoritukset")
        assertContains(fragment, "Erinomaisen taidon ilmoittautuneet")
        assertContains(fragment, "Hyvän ja tyydyttävän taidon suoritukset")
        assertFalse(fragment.contains("<html"), "Fragmenttivastaus ei sisällä sivun kuorta")
    }

    @Test
    fun `dashboard koto -fragmentti palauttaa statirivit`() {
        val fragment = getHtml("/dashboard/koto")

        assertContains(fragment, """class="dashboard-stats"""")
        assertContains(fragment, "Suoritukset")
        assertContains(fragment, "Tehtäväpaketit")
        assertContains(fragment, "Tuonnin virheet")
        assertFalse(fragment.contains("<html"), "Fragmenttivastaus ei sisällä sivun kuorta")
    }

    private fun getHtml(path: String): String =
        mockMvc
            .perform(get(path).session(virkailijaSession()))
            .andExpect(status().isOk)
            .andReturn()
            .response.contentAsString

    private fun virkailijaSession(asiointikieli: String? = null): MockHttpSession {
        val principal =
            CasUserDetails(
                name = "test-virkailija",
                oid = Oid.parse("1.2.246.562.24.20281155246").getOrThrow(),
                strongAuth = false,
                kayttajaTyyppi = "VIRKAILIJA",
                asiointikieli = asiointikieli,
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
