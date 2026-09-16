package fi.oph.kitu.yki

import fi.oph.kitu.DBContainerConfiguration
import fi.oph.kitu.TestTimeService
import fi.oph.kitu.oid.Oid
import fi.oph.kitu.security.Authority
import fi.oph.kitu.security.cas.CasUserDetails
import fi.oph.kitu.util.result.getOrThrow
import fi.oph.kitu.yki.arvioijat.Kausitoimenpide
import fi.oph.kitu.yki.arvioijat.Rekisterointitila
import fi.oph.kitu.yki.arvioijat.YkiArvioijaEntity
import fi.oph.kitu.yki.arvioijat.YkiArvioijaKausiRepository
import fi.oph.kitu.yki.arvioijat.YkiArvioijaRepository
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
import java.time.Instant
import java.time.LocalDate
import kotlin.test.Test
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

@SpringBootTest
@Import(DBContainerConfiguration::class)
class YkiArvioijaKausiTest(
    @param:Autowired private val context: WebApplicationContext,
    @param:Autowired private val repository: YkiArvioijaRepository,
    @param:Autowired private val kausiRepository: YkiArvioijaKausiRepository,
    @param:Autowired private val timeService: TestTimeService,
) {
    private lateinit var mockMvc: MockMvc

    companion object {
        private val HETKI: Instant = Instant.parse("2026-06-01T09:00:00Z")
        private val TANAAN: LocalDate = LocalDate.of(2026, 6, 1)

        /** Seedatty kausi on TANAAN jo paattynyt, joten uusi kausi on jatkorekisterointi. */
        private val VANHA_ALKU: LocalDate = LocalDate.of(2021, 1, 1)
        private val VANHA_LOPPU: LocalDate = LocalDate.of(2026, 1, 1)
    }

    private val petro = "1.2.246.562.24.59267607404"

    @BeforeEach
    fun setup() {
        mockMvc =
            MockMvcBuilders
                .webAppContextSetup(context)
                .apply<DefaultMockMvcBuilder>(springSecurity())
                .build()
        repository.deleteAll()
        repository.tallenna(arvioija(petro))
    }

    @Test
    fun `uusi kausi nakyy listalla ja siirtyy projektioon`() {
        val id = idOf(petro)

        timeService.runWithFixedClock(HETKI) {
            lisaaKausi(id, "2026-02-01", "FIN:PT", "FIN:KT")

            val kaudet = kausiRepository.findKaudet(id)
            assertEquals(2, kaudet.size, "vanha kausi sailyy ja uusi lisataan")

            val oikeudet = kausiRepository.findArviointioikeudet(id)
            assertEquals(LocalDate.of(2026, 2, 1), oikeudet.single().kaudenAlkupaiva)
            assertEquals(
                LocalDate.of(2031, 1, 31),
                oikeudet.single().kaudenPaattymispaiva,
                "paattymispaiva lasketaan viiden vuoden saannolla, viimeinen voimassaolopaiva",
            )
            assertTrue(oikeudet.single().jatkorekisterointi, "myohemmin alkava kausi on jatkokausi")
            assertEquals(
                VANHA_ALKU,
                oikeudet.single().ensimmainenRekisterointipaiva,
                "ensimmainen rekisterointipaiva sailyy vanhimmasta kaudesta",
            )
        }
    }

    @Test
    fun `alkupaivan muokkaus laskee paattymispaivan uudelleen`() {
        val id = idOf(petro)

        timeService.runWithFixedClock(HETKI) {
            val kausiId =
                kausiRepository
                    .findKaudet(id)
                    .single()
                    .id!!
                    .toInt()

            mockMvc
                .perform(
                    post("/yki/arvioijat/$id/kaudet/$kausiId")
                        .session(session())
                        .with(csrf())
                        .param("alkupaiva", "2022-03-01")
                        .param("arviointioikeus", "FIN:PT"),
                ).andExpect(status().isSeeOther)

            val kausi = kausiRepository.findKaudet(id).single()
            assertEquals(LocalDate.of(2022, 3, 1), kausi.alkupaiva)
            assertEquals(LocalDate.of(2027, 2, 28), kausi.paattymispaiva)
        }
    }

    @Test
    fun `passivointi paattaa kauden tahan paivaan`() {
        val id = idOf(petro)

        timeService.runWithFixedClock(HETKI) {
            lisaaKausi(id, "2026-02-01", "FIN:PT")
            val kausiId = aktiivisenKaudenId(id)

            mockMvc
                .perform(post("/yki/arvioijat/$id/kaudet/$kausiId/passivoi").session(session()).with(csrf()))
                .andExpect(status().isSeeOther)

            val kausi = kausiRepository.findKausi(kausiId)!!
            assertEquals(TANAAN, kausi.paattymispaiva, "kausi paattyy tahan paivaan")
            assertNotNull(kausi.passivoitu, "passivointihetki kirjataan")
            assertEquals(
                Rekisterointitila.AKTIIVINEN,
                Rekisterointitila.laske(kausi, TANAAN),
                "paattymispaiva on inklusiivinen, joten kausi on voimassa viela passivointipaivan",
            )
            assertEquals(
                Rekisterointitila.PASSIVOITU,
                Rekisterointitila.laske(kausi, TANAAN.plusDays(1)),
                "vasta seuraavana paivana merkinta on passiivinen",
            )
            assertEquals(
                TANAAN,
                kausiRepository.findArviointioikeudet(id).single().kaudenPaattymispaiva,
                "projektio seuraa katkaistua kautta",
            )
        }
    }

    @Test
    fun `passivoidun kauden alkupaivan korjaus ei elvyta kautta`() {
        val id = idOf(petro)

        timeService.runWithFixedClock(HETKI) {
            lisaaKausi(id, "2026-02-01", "FIN:PT")
            val kausiId = aktiivisenKaudenId(id)
            mockMvc
                .perform(post("/yki/arvioijat/$id/kaudet/$kausiId/passivoi").session(session()).with(csrf()))
                .andExpect(status().isSeeOther)

            mockMvc
                .perform(
                    post("/yki/arvioijat/$id/kaudet/$kausiId")
                        .session(session())
                        .with(csrf())
                        .param("alkupaiva", "2026-03-01")
                        .param("arviointioikeus", "FIN:PT"),
                ).andExpect(status().isSeeOther)

            val kausi = kausiRepository.findKausi(kausiId)!!
            assertEquals(LocalDate.of(2026, 3, 1), kausi.alkupaiva, "alkupaiva korjaantuu")
            assertEquals(TANAAN, kausi.paattymispaiva, "katkaistu paattymispaiva ei palaudu")
        }
    }

    @Test
    fun `vain aktiivisen kauden voi passivoida`() {
        val id = idOf(petro)

        timeService.runWithFixedClock(HETKI) {
            val paattynyt =
                kausiRepository
                    .findKaudet(id)
                    .single()
                    .id!!
                    .toInt()

            mockMvc
                .perform(post("/yki/arvioijat/$id/kaudet/$paattynyt/passivoi").session(session()).with(csrf()))
                .andExpect(status().isSeeOther)

            assertNull(
                kausiRepository.findKausi(paattynyt)!!.passivoitu,
                "paattyneen kauden passivointi siirtaisi sailytysajan alkua eteenpain",
            )
            assertEquals(VANHA_LOPPU, kausiRepository.findKausi(paattynyt)!!.paattymispaiva)
        }
    }

    @Test
    fun `kauden poisto palauttaa projektion edelliseen kauteen`() {
        val id = idOf(petro)

        timeService.runWithFixedClock(HETKI) {
            lisaaKausi(id, "2026-02-01", "FIN:PT")
            val uusi = aktiivisenKaudenId(id)

            mockMvc
                .perform(post("/yki/arvioijat/$id/kaudet/$uusi/poista").session(session()).with(csrf()))
                .andExpect(status().isSeeOther)

            assertEquals(1, kausiRepository.findKaudet(id).size)
            assertEquals(
                VANHA_ALKU,
                kausiRepository.findArviointioikeudet(id).single().kaudenAlkupaiva,
                "projektio palaa jaljelle jaavaan kauteen",
            )
        }
    }

    @Test
    fun `viimeista kautta ei voi poistaa`() {
        val id = idOf(petro)

        timeService.runWithFixedClock(HETKI) {
            val kausiId =
                kausiRepository
                    .findKaudet(id)
                    .single()
                    .id!!
                    .toInt()

            mockMvc
                .perform(post("/yki/arvioijat/$id/kaudet/$kausiId/poista").session(session()).with(csrf()))
                .andExpect(status().isSeeOther)

            assertEquals(
                1,
                kausiRepository.findKaudet(id).size,
                "poisto jattaisi arvioijan ilman arviointioikeuksia ja katoamaan listalta",
            )
        }
    }

    @Test
    fun `paallekkainen kausi hylataan`() {
        val id = idOf(petro)

        timeService.runWithFixedClock(HETKI) {
            val html =
                mockMvc
                    .perform(
                        post("/yki/arvioijat/$id/kaudet")
                            .session(session())
                            .with(csrf())
                            .param("alkupaiva", "2025-01-01")
                            .param("arviointioikeus", "FIN:PT"),
                    ).andExpect(status().isOk)
                    .andReturn()
                    .response.contentAsString

            assertContains(html, "päällekkäin")
            assertEquals(1, kausiRepository.findKaudet(id).size, "paallekkaista kautta ei tallenneta")
        }
    }

    @Test
    fun `kausi ei voi alkaa edellisen viimeisena voimassaolopaivana`() {
        val id = idOf(petro)

        timeService.runWithFixedClock(HETKI) {
            // Seedattu kausi on voimassa 1.1.2021-1.1.2026, paattymispaiva mukaan lukien.
            mockMvc
                .perform(
                    post("/yki/arvioijat/$id/kaudet")
                        .session(session())
                        .with(csrf())
                        .param("alkupaiva", "$VANHA_LOPPU")
                        .param("arviointioikeus", "FIN:PT"),
                ).andExpect(status().isOk)

            assertEquals(1, kausiRepository.findKaudet(id).size, "paattymispaiva kuuluu viela edelliseen kauteen")

            lisaaKausi(id, "${VANHA_LOPPU.plusDays(1)}", "FIN:PT")

            assertEquals(2, kausiRepository.findKaudet(id).size, "seuraava paiva on vapaa")
        }
    }

    @Test
    fun `jokaisella kaudella on oma hallintopaatoksensa`() {
        val id = idOf(petro)

        timeService.runWithFixedClock(HETKI) {
            lisaaKausi(id, "2026-02-01", "FIN:PT", ashaNumero = "OPH-77-2026")

            val kaudet = kausiRepository.findKaudet(id).associateBy { it.alkupaiva }
            assertEquals("OPH-77-2026", kaudet.getValue(LocalDate.of(2026, 2, 1)).ashaNumero)
            assertNull(
                kaudet.getValue(VANHA_ALKU).ashaNumero,
                "aiemman kauden paatosviitetta ei monisteta uudelle kaudelle",
            )

            assertEquals(
                "OPH-77-2026",
                kausiRepository.findArviointioikeudet(id).single().ashaNumero,
                "projektio kantaa voimassa olevan kauden paatosviitteen",
            )
        }
    }

    @Test
    fun `tulevan kauden passivointi ei synnyta haamukautta`() {
        val id = idOf(petro)

        timeService.runWithFixedClock(HETKI) {
            // Tuleva kausi: alkupaiva korkeintaan vuoden paassa, ei paallekkain seedatun kanssa.
            lisaaKausi(id, "2026-12-01", "FIN:PT")
            assertEquals(2, kausiRepository.findKaudet(id).size)

            mockMvc
                .perform(post("/yki/arvioijat/$id/passivoi").session(session()).with(csrf()))
                .andExpect(status().isSeeOther)

            val kaudet = kausiRepository.findKaudet(id)
            assertEquals(
                2,
                kaudet.size,
                "katkaisun on osuttava olemassa olevaan kauteen, ei lisattava uutta",
            )
            assertTrue(
                kaudet.none { it.alkupaiva == TANAAN },
                "tanaan alkavaa kautta ei ole olemassa: sellainen olisi tallennan synnyttama haamu",
            )
        }
    }

    @Test
    fun `toimenpiteet kirjataan muutoslokiin`() {
        val id = idOf(petro)

        timeService.runWithFixedClock(HETKI) {
            lisaaKausi(id, "2026-02-01", "FIN:PT")

            val toimenpiteet = kausiRepository.findMuutosloki(id).mapNotNull { it.toimenpide }
            assertContains(toimenpiteet, Kausitoimenpide.LISAYS)
        }
    }

    @Test
    fun `tietosivu nayttaa kaudet ja piilottaa muutoshistorian`() {
        val id = idOf(petro)

        timeService.runWithFixedClock(HETKI) {
            val html =
                mockMvc
                    .perform(get("/yki/arvioijat/$id").session(session()))
                    .andReturn()
                    .response.contentAsString

            assertContains(html, """data-testid="arviointikaudet"""")
            assertContains(html, """data-testid="uusiKausi"""")
            assertContains(html, """role="group"""", message = "vierekkaiset napit ryhmitellaan")
            assertContains(html, """data-testid="naytaMuutoshistoria"""")
            assertFalse(
                html.contains("""data-testid="arviointioikeusTila""""),
                "erillinen arviointioikeustaulukko korvattiin kausitaulukolla",
            )
        }
    }

    @Test
    fun `lukuoikeus ei paase kausilomakkeelle`() {
        val id = idOf(petro)

        mockMvc
            .perform(get("/yki/arvioijat/$id/kaudet/uusi").session(session(Authority.VIRKAILIJA)))
            .andExpect(status().isForbidden)
    }

    private fun lisaaKausi(
        id: Int,
        alkupaiva: String,
        vararg oikeudet: String,
        ashaNumero: String? = null,
    ) {
        val request =
            post("/yki/arvioijat/$id/kaudet")
                .session(session())
                .with(csrf())
                .param("alkupaiva", alkupaiva)
        ashaNumero?.let { request.param("ashaNumero", it) }
        oikeudet.forEach { request.param("arviointioikeus", it) }
        mockMvc.perform(request).andExpect(status().isSeeOther)
    }

    private fun aktiivisenKaudenId(id: Int): Int =
        kausiRepository
            .findKaudet(id)
            .first { Rekisterointitila.laske(it, TANAAN) == Rekisterointitila.AKTIIVINEN }
            .id!!
            .toInt()

    private fun idOf(oid: String): Int = repository.findByArvioijaOid(Oid.parse(oid).getOrThrow())!!.id!!.toInt()

    private fun arvioija(oid: String) =
        YkiArvioijaEntity(
            id = null,
            arvioijaOid = Oid.parse(oid).getOrThrow(),
            henkilotunnus = null,
            sukunimi = "Kivinen-Testi",
            etunimet = "Petro Testi",
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
                        tila = null,
                        kaudenAlkupaiva = VANHA_ALKU,
                        kaudenPaattymispaiva = VANHA_LOPPU,
                        jatkorekisterointi = false,
                        ensimmainenRekisterointipaiva = VANHA_ALKU,
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
