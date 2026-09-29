package fi.oph.kitu.yki

import fi.oph.kitu.DBContainerConfiguration
import fi.oph.kitu.TestTimeService
import fi.oph.kitu.oid.Oid
import fi.oph.kitu.security.Authority
import fi.oph.kitu.security.cas.CasUserDetails
import fi.oph.kitu.util.result.getOrThrow
import fi.oph.kitu.yki.arvioijat.Arviointikausi
import fi.oph.kitu.yki.arvioijat.Rekisterointitila
import fi.oph.kitu.yki.arvioijat.YkiArvioijaEntity
import fi.oph.kitu.yki.arvioijat.YkiArvioijaKausiRepository
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
import org.springframework.test.web.servlet.RequestBuilder
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.status
import org.springframework.test.web.servlet.setup.DefaultMockMvcBuilder
import org.springframework.test.web.servlet.setup.MockMvcBuilders
import org.springframework.web.context.WebApplicationContext
import java.time.Instant
import java.time.LocalDate
import java.time.OffsetDateTime
import java.time.ZoneOffset
import kotlin.test.Test
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

@SpringBootTest
@Import(DBContainerConfiguration::class)
class YkiArvioijaMuokkausTest(
    @param:Autowired private val context: WebApplicationContext,
    @param:Autowired private val repository: YkiArvioijaRepository,
    @param:Autowired private val kausiRepository: YkiArvioijaKausiRepository,
    @param:Autowired private val timeService: TestTimeService,
) {
    private lateinit var mockMvc: MockMvc

    companion object {
        /** Kiinteä tarkasteluhetki, jotta laskettu tila ei riipu ajohetkestä. */
        private val HETKI: Instant = Instant.parse("2026-06-01T09:00:00Z")
        private val TANAAN: LocalDate = LocalDate.of(2026, 6, 1)

        /** Kausi joka on TANAAN viela voimassa: 1.1.2024-1.1.2029. */
        private val VOIMASSA_ALKAEN: LocalDate = LocalDate.of(2024, 1, 1)
    }

    private val petro = "1.2.246.562.24.59267607404"
    private val ranja = "1.2.246.562.24.20281155246"

    @BeforeEach
    fun setup() {
        mockMvc =
            MockMvcBuilders
                .webAppContextSetup(context)
                .apply<DefaultMockMvcBuilder>(springSecurity())
                .build()

        repository.deleteAll()
        repository.tallenna(arvioija(petro, "Kivinen-Testi", "Petro Testi"))
        repository.tallenna(arvioija(ranja, "Öhman-Testi", "Ranja Testi"))
    }

    @Test
    fun `tietosivulla on muokkausnappi vain kirjoitusoikeudella`() {
        val kirjoittaja = html(get("/yki/arvioijat/${idOf(petro)}").session(session()))
        assertContains(kirjoittaja, """data-testid="muokkaaArvioijaa"""")

        val lukija = html(get("/yki/arvioijat/${idOf(petro)}").session(session(Authority.VIRKAILIJA)))
        assertFalse(lukija.contains("""data-testid="muokkaaArvioijaa""""), "lukijalle ei nayteta muokkausnappia")
    }

    @Test
    fun `muokkauslomake on esitaytetty arvioijan tiedoilla`() {
        val html = html(get("/yki/arvioijat/${idOf(petro)}/muokkaa").session(session()))

        assertContains(html, """value="Kivinen-Testi"""")
        assertContains(html, """value="Testikuja 5"""")
        assertContains(html, """data-testid="tallennaArvioija"""")
        assertContains(html, """data-testid="peruutaMuokkaus"""")
    }

    @Test
    fun `muokkauslomake postaa tallennusosoitteeseen eika muokkausosoitteeseen`() {
        val id = idOf(petro)

        val html = html(get("/yki/arvioijat/$id/muokkaa").session(session()))

        val action = Regex("""<form[^>]*action="([^"]*)"""").find(html)?.groupValues?.get(1)
        assertContains(action.orEmpty(), "/yki/arvioijat/$id")
        assertFalse(action.orEmpty().endsWith("/muokkaa"), "muokkausosoite vastaa vain GETtiin")
    }

    @Test
    fun `kausilomake sailyttaa valitut arviointioikeudet valittuina`() {
        val id = idOf(petro)
        val kausiId =
            kausiRepository
                .findKaudet(id)
                .single()
                .id!!
                .toInt()

        val html = html(get("/yki/arvioijat/$id/kaudet/$kausiId/muokkaa").session(session()))

        val valittu = Regex("""data-testid="arviointioikeus-FIN:PT"[^>]*checked""")
        assertTrue(valittu.containsMatchIn(html), "tallennetun oikeuden on oltava valittuna:\n$html")
    }

    @Test
    fun `muutosten tallennus ohjaa tietosivulle ja paivittaa kannan`() {
        val id = idOf(petro)

        val result =
            mockMvc
                .perform(
                    muokkaus(id, petro)
                        .param("postitoimipaikka", "TAMPERE")
                        .param("arviointioikeus", "FIN:PT"),
                ).andReturn()

        assertEquals(303, result.response.status, "poikkeus: ${result.resolvedException}")
        assertContains(result.response.getHeader("Location").orEmpty(), "/yki/arvioijat/$id")

        val paivitetty = repository.findArvioijaById(id)!!
        assertEquals("TAMPERE", paivitetty.postitoimipaikka)
        assertEquals("Muokattu Sukunimi", paivitetty.sukunimi)
    }

    @Test
    fun `kielen poisto poistaa arviointioikeuden`() {
        val id = idOf(petro)

        muokkaaKautta(id, "2021-01-01", "SWE:YT")

        val oikeudet = repository.findArvioijaById(id)!!.arviointioikeudet
        assertEquals(setOf(Tutkintokieli.SWE), oikeudet.map { it.kieli }.toSet(), "FIN-oikeuden on poistuttava")
    }

    @Test
    fun `virheellinen syote palauttaa lomakkeen eivatka arvot katoa`() {
        val id = idOf(petro)

        val html =
            html(
                muokkaus(id, petro)
                    .param("postitoimipaikka", "Testilä")
                    .param("postinumero", "1")
                    .param("arviointioikeus", "FIN:PT"),
            )

        assertContains(html, "Postinumeron on oltava viisi numeroa")
        assertContains(html, """value="Muokattu Sukunimi"""")
        assertEquals("Kivinen-Testi", repository.findArvioijaById(id)!!.sukunimi, "kantaa ei saa muuttaa")
    }

    @Test
    fun `piilokentan oid ei voi ohjata muutosta toiselle arvioijalle`() {
        val petronId = idOf(petro)
        val ranjanId = idOf(ranja)

        mockMvc
            .perform(
                // Lomake vaittaa muokkaavansa Ranjaa, mutta polku osoittaa Petroon.
                muokkaus(petronId, ranja)
                    .param("postitoimipaikka", "KAAPATTU")
                    .param("arviointioikeus", "FIN:PT"),
            ).andReturn()

        assertEquals("KAAPATTU", repository.findArvioijaById(petronId)!!.postitoimipaikka)
        assertEquals("Testilä", repository.findArvioijaById(ranjanId)!!.postitoimipaikka, "toista ei saa muuttaa")
        assertEquals("Öhman-Testi", repository.findArvioijaById(ranjanId)!!.sukunimi)
    }

    @Test
    fun `samanaikainen muokkaus ei ylikirjoita toisen muutoksia hiljaisesti`() {
        val id = idOf(petro)
        val lomake = html(get("/yki/arvioijat/$id/muokkaa").session(session()))
        val tunniste = piilokentanArvo(lomake, "muokattu")

        // Ensimmainen valilehti tallentaa ja siirtaa muokkaushetkea.
        val ensimmainen =
            mockMvc
                .perform(
                    post("/yki/arvioijat/$id")
                        .session(session())
                        .with(csrf())
                        .param("arvioijaOid", petro)
                        .param("muokattu", tunniste)
                        .param("sukunimi", "Ensimmainen")
                        .param("etunimet", "Petro Testi")
                        .param("katuosoite", "Testikuja 5")
                        .param("postinumero", "40100")
                        .param("postitoimipaikka", "HELSINKI")
                        .param("kaudenAlkupaiva", "2025-01-01")
                        .param("arviointioikeus", "FIN:PT"),
                ).andReturn()
        assertEquals(303, ensimmainen.response.status, "poikkeus: ${ensimmainen.resolvedException}")

        // Toinen valilehti tallentaa saman, nyt vanhentuneen tunnisteen kanssa.
        val virhesivu =
            html(
                post("/yki/arvioijat/$id")
                    .session(session())
                    .with(csrf())
                    .param("arvioijaOid", petro)
                    .param("muokattu", tunniste)
                    .param("sukunimi", "Toinen")
                    .param("etunimet", "Petro Testi")
                    .param("katuosoite", "Testikuja 5")
                    .param("postinumero", "40100")
                    .param("postitoimipaikka", "HELSINKI")
                    .param("kaudenAlkupaiva", "2025-01-01")
                    .param("arviointioikeus", "FIN:PT"),
            )

        assertContains(virhesivu, "Toinen käyttäjä ehti muokata")
        assertEquals(
            "Ensimmainen",
            repository.findArvioijaById(id)?.sukunimi,
            "ensimmaisen tallentajan muutos ei saa kadota",
        )
    }

    private fun piilokentanArvo(
        html: String,
        name: String,
    ): String {
        val kentta =
            Regex("""<input[^>]*name="$name"[^>]*>""").find(html)?.value
                ?: throw AssertionError("piilokenttaa $name ei loytynyt lomakkeelta:\n$html")
        return Regex("""value="([^"]*)"""").find(kentta)?.groupValues?.get(1)
            ?: throw AssertionError("piilokentalla $name ei ole arvoa: $kentta")
    }

    @Test
    fun `vanhentunut tutkintokieli sailyy vaikka lomake ei sita tunne`() {
        val id = idOf(petro)
        lisaaLegacyOikeus(id)

        mockMvc
            .perform(
                muokkaus(id, petro)
                    .param("postitoimipaikka", "TAMPERE")
                    .param("arviointioikeus", "FIN:PT"),
            ).andReturn()

        val kielet = repository.findArvioijaById(id)!!.arviointioikeudet.map { it.kieli }
        assertTrue(
            kielet.contains(Tutkintokieli.SWE10),
            "matriisi ei renderoi legacy-kielta, joten sita ei saa myoskaan poistaa: $kielet",
        )
    }

    @Test
    fun `vanhentunut tutkintokieli nakyy tietosivulla omassa osiossaan`() {
        val id = idOf(petro)
        lisaaLegacyOikeus(id)

        val tietosivu = html(get("/yki/arvioijat/$id").session(session()))

        assertContains(
            tietosivu,
            """data-testid="vanhentuneetOikeudet"""",
            message = "vanhentunut oikeus ei kuulu kauteen, joten se naytetaan erikseen",
        )
        assertContains(tietosivu, "ruotsi (vanha koodi)")
    }

    private fun lisaaLegacyOikeus(id: Int) {
        val arvioija = repository.findArvioijaById(id)!!
        repository.tallenna(
            arvioija.copy(
                arviointioikeudet =
                    arvioija.arviointioikeudet +
                        arvioija.arviointioikeudet.first().copy(
                            id = null,
                            kieli = Tutkintokieli.SWE10,
                            tasot = setOf(Tutkintotaso.PT),
                        ),
            ),
        )
    }

    @Test
    fun `yhteystiedon korjaus ei elvyta Solkin passivoimaa arvioijaa`() {
        val id = idOf(petro)
        solkiPassivoi(id, kaudenAlkupaiva = VOIMASSA_ALKAEN)

        timeService.runWithFixedClock(HETKI) {
            // lomake() lahettaa postitoimipaikaksi HELSINGIN, fixtuurissa se on Testila.
            mockMvc
                .perform(lomake(id, kaudenAlkupaiva = "$VOIMASSA_ALKAEN", oikeudet = listOf("FIN:PT")))
                .andExpect(status().isSeeOther)
        }

        val paivitetty = repository.findArvioijaById(id)!!
        assertEquals("HELSINKI", paivitetty.postitoimipaikka, "muutoksen on tallennuttava")
        assertEquals(
            listOf(Rekisterointitila.PASSIVOITU),
            paivitetty.arviointioikeudet.map { Rekisterointitila.laske(it, TANAAN) },
            "tallennus nollaa tallennetun tilan, joten Solkin kannanotto on kaannettava kauden paivamaariin",
        )
    }

    @Test
    fun `yhteystiedon korjaus ei nollaa sailytysajan alkuhetkea`() {
        val id = idOf(petro)
        solkiPassivoi(id, kaudenAlkupaiva = VOIMASSA_ALKAEN, passivointihetki = HETKI.atOffset(ZoneOffset.UTC))

        timeService.runWithFixedClock(HETKI) {
            mockMvc
                .perform(lomake(id, kaudenAlkupaiva = "$VOIMASSA_ALKAEN", oikeudet = listOf("FIN:PT")))
                .andExpect(status().isSeeOther)
        }

        val paivitetty = repository.findArvioijaById(id)!!
        assertEquals(
            listOf(Rekisterointitila.PASSIVOITU),
            paivitetty.arviointioikeudet.map { Rekisterointitila.laske(it, TANAAN) },
        )
        assertNotNull(
            paivitetty.passivoitu,
            "passiiviseksi jaava merkinta ei saa menettaa sailytysajan alkuhetkea",
        )
    }

    @Test
    fun `uusi kausi aktivoi Solkin passivoiman arvioijan`() {
        val id = idOf(petro)
        solkiPassivoi(id, kaudenAlkupaiva = LocalDate.of(2019, 1, 1))

        timeService.runWithFixedClock(HETKI) {
            uusiKausi(id, "2026-05-01", "FIN:PT")
        }

        assertEquals(
            listOf(Rekisterointitila.AKTIIVINEN),
            repository.findArvioijaById(id)!!.arviointioikeudet.map { Rekisterointitila.laske(it, TANAAN) },
            "uusi kausi on uusi rekisterointi, joka kumoaa aiemman passivoinnin",
        )
    }

    @Test
    fun `uusi kausi aktivoi passivoidun arvioijan`() {
        val id = idOf(petro)

        timeService.runWithFixedClock(HETKI) {
            mockMvc
                .perform(post("/yki/arvioijat/$id/passivoi").session(session()).with(csrf()))
                .andExpect(status().isSeeOther)

            uusiKausi(id, "2026-05-01", "FIN:PT")
        }

        val paivitetty = repository.findArvioijaById(id)!!
        assertEquals(
            listOf(Rekisterointitila.AKTIIVINEN),
            paivitetty.arviointioikeudet.map { Rekisterointitila.laske(it, TANAAN) },
            "voimassa oleva kausi tarkoittaa aktiivista merkintaa",
        )
        assertNull(paivitetty.passivoitu, "voimassa oleva kausi paattaa sailytysajan laskennan")
    }

    @Test
    fun `passivointi paattaa kauden tahan paivaan`() {
        val id = idOf(petro)

        timeService.runWithFixedClock(HETKI) {
            uusiKausi(id, "2026-05-01", "FIN:PT")
            mockMvc
                .perform(post("/yki/arvioijat/$id/passivoi").session(session()).with(csrf()))
                .andExpect(status().isSeeOther)
        }

        val oikeus = repository.findArvioijaById(id)!!.arviointioikeudet.single()
        assertEquals(TANAAN, oikeus.kaudenPaattymispaiva, "kausi paattyy passivointipaivaan")
        assertNull(oikeus.tila, "tila lasketaan kaudesta, sita ei kirjoiteta")
        assertEquals(
            Rekisterointitila.PASSIVOITU,
            Rekisterointitila.laske(oikeus, TANAAN.plusDays(1)),
            "paattymispaiva on inklusiivinen, joten merkinta on passiivinen vasta huomenna",
        )
        assertTrue(
            repository.findKausihistoria(id).any { it.kaudenPaattymispaiva == TANAAN },
            "katkaistu kausi kuuluu kausihistoriaan",
        )
    }

    @Test
    fun `passivointi ei pidenna jo paattynytta kautta`() {
        val id = idOf(petro)
        val ennen = repository.findArvioijaById(id)!!.arviointioikeudet.single()

        timeService.runWithFixedClock(HETKI) {
            mockMvc
                .perform(post("/yki/arvioijat/$id/passivoi").session(session()).with(csrf()))
                .andExpect(status().isSeeOther)
        }

        val jalkeen = repository.findArvioijaById(id)!!
        assertEquals(
            ennen.kaudenPaattymispaiva,
            jalkeen.arviointioikeudet.single().kaudenPaattymispaiva,
            "vanhentunutta arviointioikeutta ei saa pidentaa passivoimalla",
        )
        assertNotNull(jalkeen.passivoitu, "sailytysajan alkuhetki on silti asetettava")
    }

    @Test
    fun `jatkorekisterointi johdetaan kauden alkupaivasta`() {
        val id = idOf(petro)

        timeService.runWithFixedClock(HETKI) {
            mockMvc
            muokkaaKautta(id, "2021-01-01", "FIN:PT")
            assertFalse(
                repository
                    .findArvioijaById(id)!!
                    .arviointioikeudet
                    .single()
                    .jatkorekisterointi,
                "ensimmainen kausi ei ole jatkorekisterointi",
            )

            uusiKausi(id, "2026-05-01", "FIN:PT")
        }

        assertTrue(
            repository
                .findArvioijaById(id)!!
                .arviointioikeudet
                .single()
                .jatkorekisterointi,
            "myohemmin alkava kausi on jatkorekisterointi",
        )
    }

    @Test
    fun `yhteystiedon korjaus ei kasvata kausihistoriaa`() {
        val id = idOf(petro)

        timeService.runWithFixedClock(HETKI) {
            mockMvc
                .perform(lomake(id, kaudenAlkupaiva = "2021-01-01", oikeudet = listOf("FIN:PT")))
                .andExpect(status().isSeeOther)
            val historia = repository.findKausihistoria(id).size

            mockMvc
                .perform(
                    lomake(id, kaudenAlkupaiva = "2021-01-01", oikeudet = listOf("FIN:PT"))
                        .param("sahkopostiosoite", "uusi@testi.fi"),
                ).andExpect(status().isSeeOther)

            assertEquals(historia, repository.findKausihistoria(id).size, "kausi ei muuttunut")
        }
    }

    @Test
    fun `tietosivu nayttaa arviointikauden lasketun tilan`() {
        val id = idOf(petro)

        timeService.runWithFixedClock(HETKI) {
            uusiKausi(id, "2026-05-01", "FIN:PT")

            val tietosivu = html(get("/yki/arvioijat/$id").session(session()))
            assertContains(tietosivu, """data-testid="arviointikaudet"""")
            assertContains(tietosivu, """data-testid="kausiTila"""")
            assertContains(tietosivu, "Aktiivinen")
        }
    }

    /**
     * Solki on ainoa taho joka voi kirjata tilan; kitu kirjoittaa aina NULLin. Kausi kirjataan
     * sellaisena kuin lomakkeen viiden vuoden laskenta sen tuottaa, jotta muuttumattoman kauden
     * tallennus tunnistetaan muuttumattomaksi.
     */
    private fun solkiPassivoi(
        id: Int,
        kaudenAlkupaiva: LocalDate,
        passivointihetki: OffsetDateTime? = null,
    ) {
        val arvioija = repository.findArvioijaById(id)!!
        repository.tallenna(
            arvioija.copy(
                passivoitu = passivointihetki,
                arviointioikeudet =
                    arvioija.arviointioikeudet.map {
                        it.copy(
                            tila = YkiArvioijaTila.PASSIVOITU,
                            kaudenAlkupaiva = kaudenAlkupaiva,
                            kaudenPaattymispaiva = Arviointikausi.paattymispaiva(kaudenAlkupaiva),
                        )
                    },
            ),
        )
    }

    @Test
    fun `tietosivu nayttaa kausihistorian`() {
        val id = idOf(petro)

        // Uusi kausi synnyttaa lokirivin alkuperaisen rinnalle.
        uusiKausi(id, "2026-05-01", "FIN:PT")

        val tietosivu = html(get("/yki/arvioijat/$id").session(session()))

        assertContains(tietosivu, """data-testid="kausihistoria"""")
        assertContains(tietosivu, "1.1.2021", message = "alkuperainen kausi sailyy historiassa")
        assertContains(tietosivu, "1.5.2026", message = "uusi kausi kirjautuu historiaan")
    }

    @Test
    fun `passivointi merkitsee kaikki arviointioikeudet passiivisiksi`() {
        val id = idOf(petro)

        mockMvc
            .perform(post("/yki/arvioijat/$id/passivoi").session(session()).with(csrf()))
            .andExpect(status().isSeeOther)

        val passivoitu = repository.findArvioijaById(id)!!
        assertEquals(
            listOf(Rekisterointitila.PASSIVOITU),
            passivoitu.arviointioikeudet.map { Rekisterointitila.laske(it, LocalDate.now().plusDays(1)) }.distinct(),
        )
        assertNotNull(passivoitu.passivoitu, "sailytysajan laskenta alkaa passivointihetkesta")
    }

    @Test
    fun `toinen passivointi ei siirra sailytysajan alkuhetkea`() {
        val id = idOf(petro)
        val passivoi = post("/yki/arvioijat/$id/passivoi").session(session()).with(csrf())

        mockMvc.perform(passivoi).andExpect(status().isSeeOther)
        val ensimmainen = repository.findArvioijaById(id)!!.passivoitu

        mockMvc.perform(passivoi).andExpect(status().isSeeOther)

        assertEquals(ensimmainen, repository.findArvioijaById(id)!!.passivoitu)
    }

    @Test
    fun `passivointinappi estetaan perusteluineen kun arvioija on jo passivoitu`() {
        val id = idOf(petro)

        timeService.runWithFixedClock(HETKI) {
            uusiKausi(id, "2026-05-01", "FIN:PT")
            assertFalse(
                passivointiNappi(id).contains("aria-disabled"),
                "voimassa olevan merkinnan voi passivoida",
            )

            mockMvc
                .perform(post("/yki/arvioijat/$id/passivoi").session(session()).with(csrf()))
                .andExpect(status().isSeeOther)

            val nappi = passivointiNappi(id)
            assertContains(nappi, """aria-disabled="true"""", message = "passivointia ei tarjota uudelleen")
            assertContains(nappi, "merkitty passiiviseksi 1.6.2026", message = "esto on perusteltava tooltipilla")
        }
    }

    @Test
    fun `paattynyt kausi estaa passivoinnin mutta ei kirjaa passivointihetkea`() {
        val id = idOf(petro)
        var nappi = ""

        // Fixtuurin kausi paattyi 1.1.2026, eli ennen tarkasteluhetkea.
        timeService.runWithFixedClock(HETKI) { nappi = passivointiNappi(id) }

        assertContains(nappi, """aria-disabled="true"""", message = "paattynytta merkintaa ei passivoida uudelleen")
        assertContains(nappi, "Rekisteröintikausi on päättynyt", message = "esto on perusteltava tooltipilla")
        assertNull(
            repository.findArvioijaById(id)!!.passivoitu,
            "kauden umpeutuminen ei ole passivointihetki: sailytysaika lasketaan kauden paattymisesta",
        )
    }

    /** Nappi renderoityy joko modaalin avaavana buttonina tai estettyna linkkina. */
    private fun passivointiNappi(id: Int): String {
        val sivu = html(get("/yki/arvioijat/$id").session(session()))
        return Regex("""<[^>]*data-testid="passivoiArvioija"[^>]*>""").find(sivu)?.value
            ?: error("passivointinappia ei loytynyt sivulta:\n$sivu")
    }

    @Test
    fun `tuntemattoman arvioijan passivointi palauttaa 404`() {
        mockMvc
            .perform(post("/yki/arvioijat/999999/passivoi").session(session()).with(csrf()))
            .andExpect(status().isNotFound)
    }

    @Test
    fun `kielen lisays perii voimassa olevan kauden`() {
        val id = idOf(petro)
        val voimassaolevaKausi =
            repository
                .findArvioijaById(id)!!
                .arviointioikeudet
                .first()
                .kaudenAlkupaiva

        muokkaaKautta(id, voimassaolevaKausi.toString(), "FIN:PT", "SWE:YT")

        val oikeudet = repository.findArvioijaById(id)!!.arviointioikeudet
        assertEquals(setOf(Tutkintokieli.FIN, Tutkintokieli.SWE), oikeudet.map { it.kieli }.toSet())
        assertEquals(
            1,
            oikeudet.map { it.kaudenAlkupaiva to it.kaudenPaattymispaiva }.distinct().size,
            "lisatty kieli perii saman kauden, ei aloita uutta",
        )
    }

    @Test
    fun `tuntemattoman arvioijan muokkaus palauttaa 404`() {
        mockMvc
            .perform(get("/yki/arvioijat/999999/muokkaa").session(session()))
            .andReturn()
            .let { assertEquals(404, it.response.status) }
    }

    @Test
    fun `tuntemattoman arvioijan tallennus palauttaa 404`() {
        mockMvc
            .perform(lomake(999999, kaudenAlkupaiva = "2026-05-01", oikeudet = listOf("FIN:PT")))
            .andReturn()
            .let { assertEquals(404, it.response.status) }
    }

    @Test
    fun `tuntemattoman arvioijan tyhja tallennus palauttaa 404 eika lomaketta`() {
        // Olemassaolo on tarkistettava ennen kenttatarkistuksia: muuten vajaa lomake saisi
        // vastaukseksi 200:n ja lomakesivun. Sama odotus on securityconfig-e2e:ssa.
        mockMvc
            .perform(post("/yki/arvioijat/999999").session(session()).with(csrf()))
            .andReturn()
            .let { assertEquals(404, it.response.status) }
    }

    /** Muokkauslomake tunnetuilla arvoilla — toisin kuin [muokkaus], ei aseta kenttia valmiiksi. */
    private fun lomake(
        id: Int,
        kaudenAlkupaiva: String,
        oikeudet: List<String>,
    ) = post("/yki/arvioijat/$id")
        .session(session())
        .with(csrf())
        .param("arvioijaOid", petro)
        .param("sukunimi", "Kivinen-Testi")
        .param("etunimet", "Petro Testi")
        .param("katuosoite", "Testikuja 5")
        .param("postinumero", "40100")
        .param("postitoimipaikka", "HELSINKI")
        .param("kaudenAlkupaiva", kaudenAlkupaiva)
        .apply { oikeudet.forEach { param("arviointioikeus", it) } }

    private fun muokkaus(
        id: Int,
        lomakkeenOid: String,
    ) = post("/yki/arvioijat/$id")
        .session(session())
        .with(csrf())
        .param("arvioijaOid", lomakkeenOid)
        .param("sukunimi", "Muokattu Sukunimi")
        .param("etunimet", "Petro Testi")
        .param("katuosoite", "Testikuja 5")
        .param("postinumero", "40100")
        .param("kaudenAlkupaiva", "2025-01-01")

    /** Kausi ei kulje muokkauslomakkeen kautta, joten testit ohjaavat sita omilla reiteillaan. */
    private fun uusiKausi(
        id: Int,
        alkupaiva: String,
        vararg oikeudet: String,
    ) {
        val request =
            post("/yki/arvioijat/$id/kaudet").session(session()).with(csrf()).param("alkupaiva", alkupaiva)
        oikeudet.forEach { request.param("arviointioikeus", it) }
        mockMvc.perform(request).andExpect(status().isSeeOther)
    }

    private fun muokkaaKautta(
        id: Int,
        alkupaiva: String,
        vararg oikeudet: String,
    ) {
        val kausiId =
            kausiRepository
                .findKaudet(id)
                .first()
                .id!!
                .toInt()
        val request =
            post("/yki/arvioijat/$id/kaudet/$kausiId")
                .session(session())
                .with(csrf())
                .param("alkupaiva", alkupaiva)
        oikeudet.forEach { request.param("arviointioikeus", it) }
        mockMvc.perform(request).andExpect(status().isSeeOther)
    }

    private fun idOf(oid: String): Int = repository.findByArvioijaOid(Oid.parse(oid).getOrThrow())!!.id!!.toInt()

    private fun html(request: RequestBuilder): String {
        val result = mockMvc.perform(request).andReturn()
        assertEquals(200, result.response.status, "poikkeus: ${result.resolvedException}")
        return result.response.contentAsString
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
