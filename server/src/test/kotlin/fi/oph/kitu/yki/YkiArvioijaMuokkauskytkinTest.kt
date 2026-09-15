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
import kotlin.test.Test
import kotlin.test.assertContains
import kotlin.test.assertFalse
import kotlin.test.fail

@SpringBootTest(properties = ["kitu.yki.arvioijarekisteri.muokkaus.enabled=false"])
@Import(DBContainerConfiguration::class)
class YkiArvioijaMuokkauskytkinTest(
    @param:Autowired private val context: WebApplicationContext,
    @param:Autowired private val repository: YkiArvioijaRepository,
) {
    private lateinit var mockMvc: MockMvc
    private var arvioijaId: Int = 0

    @BeforeEach
    fun setup() {
        mockMvc =
            MockMvcBuilders
                .webAppContextSetup(context)
                .apply<DefaultMockMvcBuilder>(springSecurity())
                .build()
        repository.deleteAll()
        arvioijaId = repository.tallenna(arvioija())
    }

    @Test
    fun `lisaysnappi nakyy mutta ei ole klikattavissa`() {
        val nappi = nappi(html(get("/yki/arvioijat").session(session())), "lisaaArvioija")

        assertContains(nappi, """aria-disabled="true"""")
        assertFalse(nappi.contains("href="), "napista ei saa jaada linkkia: $nappi")
    }

    @Test
    fun `muokkausnappi nakyy mutta ei ole klikattavissa`() {
        val nappi = nappi(html(get("/yki/arvioijat/$arvioijaId").session(session())), "muokkaaArvioijaa")

        assertContains(nappi, """aria-disabled="true"""")
        assertFalse(nappi.contains("href="), "napista ei saa jaada linkkia: $nappi")
    }

    @Test
    fun `lisayslomake on suljettu`() {
        mockMvc
            .perform(get("/yki/arvioijat/uusi").session(session()))
            .andExpect(status().isForbidden)
    }

    @Test
    fun `muokkauslomake on suljettu`() {
        mockMvc
            .perform(get("/yki/arvioijat/$arvioijaId/muokkaa").session(session()))
            .andExpect(status().isForbidden)
    }

    @Test
    fun `tallennus on suljettu`() {
        mockMvc
            .perform(post("/yki/arvioijat/uusi").session(session()).with(csrf()))
            .andExpect(status().isForbidden)

        mockMvc
            .perform(post("/yki/arvioijat/uusi/haku").session(session()).with(csrf()))
            .andExpect(status().isForbidden)

        mockMvc
            .perform(post("/yki/arvioijat/$arvioijaId").session(session()).with(csrf()))
            .andExpect(status().isForbidden)
    }

    @Test
    fun `kausireitit ovat suljettuja`() {
        // Turvasaanto ajetaan ennen kontrolleria, joten tuntematon kausiId riittaa.
        listOf(
            get("/yki/arvioijat/$arvioijaId/kaudet/uusi"),
            get("/yki/arvioijat/$arvioijaId/kaudet/1/muokkaa"),
        ).forEach { mockMvc.perform(it.session(session())).andExpect(status().isForbidden) }

        listOf(
            post("/yki/arvioijat/$arvioijaId/kaudet"),
            post("/yki/arvioijat/$arvioijaId/kaudet/1"),
            post("/yki/arvioijat/$arvioijaId/kaudet/1/passivoi"),
            post("/yki/arvioijat/$arvioijaId/kaudet/1/poista"),
            post("/yki/arvioijat/$arvioijaId/passivoi"),
            post("/yki/arvioijat/$arvioijaId/laheta"),
        ).forEach { mockMvc.perform(it.session(session()).with(csrf())).andExpect(status().isForbidden) }
    }

    @Test
    fun `kausinapit nakyvat mutta eivat ole klikattavissa`() {
        val sivu = html(get("/yki/arvioijat/$arvioijaId").session(session()))

        assertContains(nappi(sivu, "uusiKausi"), """aria-disabled="true"""")
        assertContains(nappi(sivu, "muokkaaKautta"), """aria-disabled="true"""")
    }

    @Test
    fun `Solkiin lahetys on estetty ja esto on selitetty kaareessa`() {
        val sivu = html(get("/yki/arvioijat/$arvioijaId").session(session()))

        assertContains(nappipainike(sivu, "lahetaArvioijaSolkiin"), "disabled")
        // Pico asettaa disabloidulle napille pointer-events: none, joten selite ei nakyisi
        // napin omassa data-tooltipissa lainkaan — sen on oltava kaareessa.
        val kaare =
            Regex("""<span [^>]*data-tooltip="[^"]*"[^>]*>""").find(sivu)?.value
                ?: fail("eston selite kuuluu kaareeseen, ei disabloituun nappiin:\n$sivu")
        assertContains(kaare, "Arvioijarekisterin ylläpito ei ole vielä käytössä")
    }

    @Test
    fun `listaus ja tietosivu ovat yha luettavissa`() {
        assertContains(html(get("/yki/arvioijat").session(session())), "Kivinen-Testi")
        assertContains(html(get("/yki/arvioijat/$arvioijaId").session(session())), "Kivinen-Testi")
    }

    private fun nappi(
        html: String,
        testId: String,
    ): String =
        Regex("""<a [^>]*data-testid="$testId"[^>]*>""").find(html)?.value
            ?: fail("nappia $testId ei loytynyt sivulta:\n$html")

    /** Kausitoiminnot ovat <button>eja, eivat linkkeja, joten niille tarvitaan oma hakunsa. */
    private fun nappipainike(
        html: String,
        testId: String,
    ): String =
        Regex("""<button [^>]*data-testid="$testId"[^>]*>""").find(html)?.value
            ?: fail("nappia $testId ei loytynyt sivulta:\n$html")

    private fun html(request: org.springframework.test.web.servlet.RequestBuilder): String {
        val result = mockMvc.perform(request).andReturn()
        return result.response.contentAsString
    }

    private fun arvioija() =
        YkiArvioijaEntity(
            id = null,
            arvioijaOid = Oid.parse("1.2.246.562.24.59267607404").getOrThrow(),
            henkilotunnus = null,
            sukunimi = "Kivinen-Testi",
            etunimet = "Petro Testi",
            sahkopostiosoite = "kivinen-testi@oph.fi",
            katuosoite = "Kivinenkatu 2 A 3",
            postinumero = "00100",
            postitoimipaikka = "HELSINKI",
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

    private fun session(
        vararg authorities: Authority = arrayOf(Authority.VIRKAILIJA, Authority.YKI_ARVIOIJAREKISTERI),
    ): MockHttpSession {
        val principal =
            CasUserDetails(
                name = "test-virkailija",
                oid = Oid.parse("1.2.246.562.24.20281155246").getOrThrow(),
                strongAuth = false,
                kayttajaTyyppi = "VIRKAILIJA",
                asiointikieli = null,
                authorities = authorities.map { SimpleGrantedAuthority(it.role()) },
            )
        val authentication: Authentication =
            UsernamePasswordAuthenticationToken(principal, null, principal.authorities)
        val securityContext =
            SecurityContextHolder.createEmptyContext().apply { this.authentication = authentication }
        return MockHttpSession().also { session ->
            session.setAttribute(
                HttpSessionSecurityContextRepository.SPRING_SECURITY_CONTEXT_KEY,
                securityContext,
            )
        }
    }
}
