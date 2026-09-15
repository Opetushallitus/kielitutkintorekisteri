package fi.oph.kitu.yki

import fi.oph.kitu.DBContainerConfiguration
import fi.oph.kitu.oid.Oid
import fi.oph.kitu.security.Authority
import fi.oph.kitu.security.cas.CasUserDetails
import fi.oph.kitu.util.result.getOrThrow
import fi.oph.kitu.yki.arvioijat.YkiArvioijaEntity
import fi.oph.kitu.yki.arvioijat.YkiArvioijaRepository
import fi.oph.kitu.yki.arvioijat.YkiArvioijaTila
import fi.oph.kitu.yki.arvioijat.YkiArviointioikeusEntity
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
import java.time.LocalDate
import kotlin.test.Test
import kotlin.test.assertContains
import kotlin.test.assertFalse

@SpringBootTest
@Import(DBContainerConfiguration::class)
class YkiArvioijaViewControllerTest(
    @param:Autowired private val context: WebApplicationContext,
    @param:Autowired private val repository: YkiArvioijaRepository,
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
        repository.tallenna(arvioija("1.2.246.562.24.20281155246", "Öhman-Testi", "Ranja Testi"))
        repository.tallenna(arvioija("1.2.246.562.24.59267607404", "Kivinen-Testi", "Petro Testi"))
    }

    private fun arvioija(
        oid: String,
        sukunimi: String,
        etunimet: String,
    ) = YkiArvioijaEntity(
        id = null,
        arvioijaOid = Oid.parse(oid).getOrThrow(),
        henkilotunnus = null,
        sukunimi = sukunimi,
        etunimet = etunimet,
        sahkopostiosoite = "testi@testi.fi",
        katuosoite = "Testikuja 5",
        postinumero = "40100",
        postitoimipaikka = "Testilä",
        arviointioikeudet =
            listOf(
                YkiArviointioikeusEntity(
                    id = null,
                    arvioijaId = null,
                    kieli = Tutkintokieli.FIN,
                    tasot = setOf(Tutkintotaso.PT),
                    tila = YkiArvioijaTila.AKTIIVINEN,
                    kaudenAlkupaiva = LocalDate.of(2021, 1, 1),
                    kaudenPaattymispaiva = LocalDate.of(2026, 1, 1),
                    jatkorekisterointi = false,
                    ensimmainenRekisterointipaiva = LocalDate.of(2021, 1, 1),
                    rekisteriintuontiaika = null,
                ),
            ),
    )

    @Test
    fun `listasivu renderoityy ja sisaltaa hakulomakkeen`() {
        val html = getHtml("/yki/arvioijat")

        assertContains(html, """data-testid="arvioijaSearch"""")
        assertContains(html, """data-testid="arvioijat"""")
        assertContains(html, "Öhman-Testi")
        assertContains(html, "Kivinen-Testi")
    }

    @Test
    fun `hakusana rajaa listan ja sailyy lomakkeen arvona`() {
        val html = getHtml("/yki/arvioijat", "search" to "Kivinen")

        assertContains(html, "Kivinen-Testi")
        assertFalse(html.contains("Öhman-Testi"), "hakusanan ulkopuolinen arvioija ei saa nakya")
        assertContains(html, """value="Kivinen"""")
    }

    @Test
    fun `monisanainen hakusana toimii myos HTTP-kerroksen lapi`() {
        val html = getHtml("/yki/arvioijat", "search" to "Petro Kivinen")

        assertContains(html, "Kivinen-Testi")
        assertFalse(html.contains("Öhman-Testi"), "kaikkien hakusanojen on osuttava")
    }

    @Test
    fun `enum-arvot renderoityvat kaannettyina eivatka tunnuksina`() {
        val html = getHtml("/yki/arvioijat")

        assertContains(html, "Aktiivinen")
        assertContains(html, "Perustaso")
        assertContains(html, "suomi")
    }

    @Test
    fun `listarivilta on linkki arvioijan tietosivulle`() {
        val id = arvioijanId("1.2.246.562.24.59267607404")

        val html = getHtml("/yki/arvioijat")

        assertContains(html, """data-testid="Linkki"""")
        assertContains(html, """/yki/arvioijat/$id"""")
    }

    @Test
    fun `rivilinkki sailyy kun henkilotiedot on piilotettu`() {
        val id = arvioijanId("1.2.246.562.24.59267607404")

        val html = getHtml("/yki/arvioijat", "piilotaHenkilotiedot" to "true")

        assertContains(html, """/yki/arvioijat/$id"""")
        assertFalse(html.contains("Kivinen-Testi"), "henkilotiedot on piilotettu")
    }

    private fun arvioijanId(oid: String): Int = repository.findByArvioijaOid(Oid.parse(oid).getOrThrow())!!.id!!.toInt()

    @Test
    fun `hakusana sailyy lajittelulinkeissa`() {
        val html = getHtml("/yki/arvioijat", "search" to "Kivinen")

        assertContains(html, "search=Kivinen", message = "lajittelulinkkien on sailytettava hakusana")
    }

    @Test
    fun `tuntemattoman arvioijan tietosivu nayttaa saman virhesivun kuin muut 404t`() {
        val html =
            mockMvc
                .perform(get("/yki/arvioijat/12345").session(virkailijaSession()))
                .andExpect(status().isNotFound)
                .andReturn()
                .response.contentAsString

        assertContains(html, "Sivua ei löydy")
    }

    private fun getHtml(
        path: String,
        vararg params: Pair<String, String>,
    ): String =
        mockMvc
            .perform(
                get(path).session(virkailijaSession()).apply {
                    params.forEach { (name, value) -> param(name, value) }
                },
            ).andExpect(status().isOk)
            .andReturn()
            .response.contentAsString

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
