package fi.oph.kitu.yki.arvioijat.hakemus

import arrow.core.nonEmptyListOf
import fi.oph.kitu.DBContainerConfiguration
import fi.oph.kitu.oid.Oid
import fi.oph.kitu.security.Authority
import fi.oph.kitu.security.cas.CasUserDetails
import fi.oph.kitu.tyhjennaArvioijahakemukset
import fi.oph.kitu.util.defaultObjectMapper
import fi.oph.kitu.util.result.getOrThrow
import fi.oph.kitu.yki.arvioijat.YkiArvioijaRepository
import fi.oph.kitu.yki.arvioijat.hakemus.ArvioijahakemusTestidata.PETRO_OID
import fi.oph.kitu.yki.arvioijat.hakemus.ArvioijahakemusTestidata.TANAAN
import fi.oph.kitu.yki.arvioijat.hakemus.ArvioijahakemusTestidata.kirjaaKasitellyksi
import fi.oph.kitu.yki.arvioijat.hakemus.ArvioijahakemusTestidata.oid
import fi.oph.kitu.yki.arvioijat.hakemus.ArvioijahakemusTestidata.tarkistettava
import org.junit.jupiter.api.BeforeEach
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.mock.web.MockHttpSession
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken
import org.springframework.security.core.authority.SimpleGrantedAuthority
import org.springframework.security.core.context.SecurityContextHolder
import org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.csrf
import org.springframework.security.test.web.servlet.setup.SecurityMockMvcConfigurers.springSecurity
import org.springframework.security.web.context.HttpSessionSecurityContextRepository
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.status
import org.springframework.test.web.servlet.setup.DefaultMockMvcBuilder
import org.springframework.test.web.servlet.setup.MockMvcBuilders
import org.springframework.web.context.WebApplicationContext
import java.time.LocalDate
import java.time.OffsetDateTime
import kotlin.test.Test
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull

@SpringBootTest(properties = ["kitu.ataru.service.url=http://localhost/mock-ataru", "kitu.dashboard.cache.ttl=0s"])
@Import(DBContainerConfiguration::class)
class ArvioijahakemusViewControllerTest(
    @param:Autowired private val context: WebApplicationContext,
    @param:Autowired private val repository: ArvioijahakemusRepository,
    @param:Autowired private val tarkistettavaRepository: ArvioijahakemusTarkistettavaRepository,
    @param:Autowired private val arvioijaRepository: YkiArvioijaRepository,
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
        arvioijaRepository.deleteAll()
    }

    private val kayttaja = session(Authority.VIRKAILIJA, Authority.YKI_ARVIOIJAREKISTERI)

    @Test
    fun `nakyma listaa tarkistettavat ja muut kasittelemattomat hakemukset`() {
        tarkistettavaRepository.tallenna(tarkistettava("tarkistettava-hakemus"), OffsetDateTime.now())
        tarkistettavaRepository.tallenna(tarkistettava("keskeytynyt-hakemus"), OffsetDateTime.now())
        repository.varaa("keskeytynyt-hakemus", PETRO_OID, TANAAN, OffsetDateTime.now())
        repository.odottaaYksilointia(
            "odottava-hakemus",
            PETRO_OID,
            "Henkilöä ei ole yksilöity",
            OffsetDateTime.now(),
        )
        repository.kirjaaKasitellyksi("kasitelty-hakemus")

        val html = sivu("/yki/arvioijat/hakemukset")

        assertContains(html, "/yki/arvioijat/hakemukset/tarkistettava-hakemus/tarkista")
        assertContains(html, "Kivinen-Testi, Petro Testi")
        assertContains(html, "odottava-hakemus")
        assertContains(html, "Henkilöä ei ole yksilöity")
        assertContains(html, "Käsittely kesken")
        assertFalse(html.contains("/yki/arvioijat/hakemukset/keskeytynyt-hakemus/tarkista"))
        assertFalse(html.contains("kasitelty-hakemus"))
        assertFalse(html.contains("/yki/arvioijat/hakemukset/virheet"), "ei virheitä, ei virheilmoitusta")
    }

    @Test
    fun `tarkistuslomake esitaytetaan hakemukselta`() {
        tarkistettavaRepository.tallenna(
            tarkistettava("h1").let { it.copy(hakemus = it.hakemus.copy(sahkopostiosoite = "hakemus@oph.fi")) },
            OffsetDateTime.now(),
        )

        val html = sivu("/yki/arvioijat/hakemukset/h1/tarkista")

        assertContains(html, "Tarkista arvioijahakemus")
        assertContains(html, "/yki/arvioijat/hakemukset/h1/tarkista\"")
        assertContains(html, "hakemus@oph.fi")
        assertContains(html, "Kivinenkatu 2 A 3")
        assertContains(html, "Vahvista ja tallenna rekisteriin")
    }

    @Test
    fun `vahvistus vaatii ASHA-numeron ja onnistunut vahvistus ohjaa arvioijan sivulle`() {
        tarkistettavaRepository.tallenna(tarkistettava("h1"), OffsetDateTime.now())

        fun vahvistus(asha: String) =
            post("/yki/arvioijat/hakemukset/h1/tarkista")
                .session(kayttaja)
                .with(csrf())
                .param("sukunimi", "Kivinen-Testi")
                .param("etunimet", "Petro Testi")
                .param("katuosoite", "Katu 1")
                .param("postinumero", "00100")
                .param("postitoimipaikka", "HELSINKI")
                .param("kaudenAlkupaiva", LocalDate.now().toString())
                .param("ashaNumero", asha)
                .param("arviointioikeus", "FIN:PT")

        val virhe =
            mockMvc
                .perform(vahvistus(""))
                .andExpect(status().isOk)
                .andReturn()
                .response.contentAsString
        assertContains(virhe, "Hallintopäätöksen ASHA-numero on pakollinen tieto")

        val ohjaus =
            mockMvc
                .perform(vahvistus("OPH-1-2026"))
                .andExpect(status().isSeeOther)
                .andReturn()
                .response
                .getHeader("Location")
        val arvioija = assertNotNull(arvioijaRepository.findByArvioijaOid(oid(PETRO_OID)))
        assertContains(ohjaus.orEmpty(), "/yki/arvioijat/${arvioija.id}")
        assertEquals(ArvioijahakemuksenTila.KASITELTY, repository.haeKaikki().single().tila)
    }

    @Test
    fun `epaonnistunut haku naytetaan ilmoituksena`() {
        val vastaus =
            mockMvc
                .perform(post("/yki/arvioijat/hakemukset/hae").session(kayttaja).with(csrf()))
                .andExpect(status().isSeeOther)
                .andReturn()
                .response
        assertContains(vastaus.getHeader("Location").orEmpty(), "/yki/arvioijat/hakemukset")

        assertContains(sivu("/yki/arvioijat/hakemukset"), "Hakemusten haku hakemuspalvelusta epäonnistui")
    }

    @Test
    fun `tarkistettavista ilmoitetaan etusivulla`() {
        tarkistettavaRepository.tallenna(tarkistettava("h1"), OffsetDateTime.now())

        val etusivu = sivu("/dashboard/yki")

        assertContains(etusivu, "Tarkistettavat arvioijahakemukset")
        assertContains(etusivu, "/yki/arvioijat/hakemukset")
    }

    @Test
    fun `tarkistamaton tai tuntematon hakemus on 404`() {
        mockMvc
            .perform(get("/yki/arvioijat/hakemukset/tuntematon/tarkista").session(kayttaja))
            .andExpect(status().isNotFound)
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
        mockMvc
            .perform(get("/yki/arvioijat/hakemukset/h1/tarkista").session(session(Authority.VIRKAILIJA)))
            .andExpect(status().isForbidden)
        mockMvc
            .perform(post("/yki/arvioijat/hakemukset/hae").session(session(Authority.VIRKAILIJA)).with(csrf()))
            .andExpect(status().isForbidden)
    }

    private fun sivu(polku: String): String =
        mockMvc
            .perform(get(polku).session(kayttaja))
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
