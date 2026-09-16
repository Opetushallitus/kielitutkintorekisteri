package fi.oph.kitu.yki

import fi.oph.kitu.DBContainerConfiguration
import fi.oph.kitu.oid.Oid
import fi.oph.kitu.security.Authority
import fi.oph.kitu.security.cas.CasUserDetails
import fi.oph.kitu.util.result.getOrThrow
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
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.status
import org.springframework.test.web.servlet.setup.DefaultMockMvcBuilder
import org.springframework.test.web.servlet.setup.MockMvcBuilders
import org.springframework.web.context.WebApplicationContext
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
class YkiArvioijaLisaysTest(
    @param:Autowired private val context: WebApplicationContext,
    @param:Autowired private val repository: YkiArvioijaRepository,
    @param:Autowired private val kausiRepository: YkiArvioijaKausiRepository,
) {
    private lateinit var mockMvc: MockMvc

    /** Ainoa mock-ONR:n henkilo, jolla on osoite ja sahkoposti. */
    private val petronOid = "1.2.246.562.24.59267607404"

    /** Mock-ONR tuntee taman henkilon, mutta hanella ei ole oppijanumeroa. */
    private val yksiloimatonOid = "1.2.246.562.24.10691606777"

    /** Mock-ONR:lla ei ole tata henkiloa, joten turvakieltokysely epaonnistuu. */
    private val oidJotaEiOleOnrissa = "1.2.246.562.24.99999999999"

    /** Mock-ONR:ssa taman henkilon master-oid on [masterOid]. */
    private val duplikaattiOid = "1.2.246.562.24.88888888888"
    private val masterOid = "1.2.246.562.24.20281155246"

    @BeforeEach
    fun setup() {
        mockMvc =
            MockMvcBuilders
                .webAppContextSetup(context)
                .apply<DefaultMockMvcBuilder>(springSecurity())
                .build()
        repository.deleteAll()
    }

    @Test
    fun `hakulomake renderoi oppijanumerokentan`() {
        val html = html(get("/yki/arvioijat/uusi").session(session()))

        assertContains(html, """data-testid="oppijanumeroHakuLomake"""")
        assertContains(html, """data-testid="oppijanumero"""")
    }

    @Test
    fun `tyhja oppijanumero palauttaa hakulomakkeen virheineen`() {
        val html = haku("oppijanumero" to "")

        assertContains(html, "Oppijanumero on pakollinen tieto")
        assertContains(html, """data-testid="oppijanumeroHakuLomake"""")
    }

    @Test
    fun `oppijanumerolla haettu henkilo esitaytetaan oppijanumerorekisterin tiedoilla`() {
        val html = haku("oppijanumero" to petronOid)

        assertContains(html, """value="Kivinen-Testi"""")
        assertContains(html, """value="Petro Testi"""")
        assertContains(html, """value="kivinen-testi@oph.fi"""")
        assertContains(html, """value="Kivinenkatu 2 A 3"""")
        assertContains(html, """value="00100"""")
        assertContains(html, """value="HELSINKI"""")
        assertContains(html, """data-testid="arviointioikeusMatriisi"""")
    }

    @Test
    fun `tuntematon oppijanumero nayttaa virheen`() {
        val html = haku("oppijanumero" to oidJotaEiOleOnrissa)

        assertContains(html, "oppijanumerorekisteristä")
    }

    @Test
    fun `arvioijan tallennus ohjaa tietosivulle ja kirjoittaa kannan rivin`() {
        val result =
            mockMvc
                .perform(
                    post("/yki/arvioijat/uusi")
                        .session(session())
                        .with(csrf())
                        .param("arvioijaOid", petronOid)
                        .param("sukunimi", "Kivinen-Testi")
                        .param("etunimet", "Petro Testi")
                        .param("sahkopostiosoite", "kivinen-testi@oph.fi")
                        .param("katuosoite", "Kivinenkatu 2 A 3")
                        .param("postinumero", "00100")
                        .param("postitoimipaikka", "HELSINKI")
                        .param("kaudenAlkupaiva", "2025-12-07")
                        .param("ashaNumero", "OPH-1234-2025")
                        .param("arviointioikeus", "FIN:PT")
                        .param("arviointioikeus", "FIN:KT")
                        .param("arviointioikeus", "SWE:YT"),
                ).andExpect(status().isSeeOther)
                .andReturn()

        val tallennettu = repository.findByArvioijaOid(Oid.parse(petronOid).getOrThrow())
        assertNotNull(tallennettu)
        assertNull(tallennettu.henkilotunnus, "hetua ei saa tallentaa")
        assertEquals(
            "OPH-1234-2025",
            kausiRepository.findKaudet(tallennettu.id!!.toInt()).single().ashaNumero,
            "hallintopaatoksen viite kirjataan kaudelle, ei arvioijalle",
        )
        assertContains(
            result.response.getHeader("Location").orEmpty(),
            "/yki/arvioijat/${tallennettu.id!!.toInt()}",
        )

        val oikeudet = tallennettu.arviointioikeudet.associateBy { it.kieli }
        assertEquals(setOf(Tutkintokieli.FIN, Tutkintokieli.SWE), oikeudet.keys)
        assertEquals(setOf(Tutkintotaso.PT, Tutkintotaso.KT), oikeudet[Tutkintokieli.FIN]!!.tasot)
        assertEquals(setOf(Tutkintotaso.YT), oikeudet[Tutkintokieli.SWE]!!.tasot)

        oikeudet.values.forEach { oikeus ->
            assertEquals(LocalDate.of(2025, 12, 7), oikeus.kaudenAlkupaiva, "sama kausi kaikille kielille")
            assertEquals(LocalDate.of(2030, 12, 6), oikeus.kaudenPaattymispaiva, "viimeinen voimassaolopaiva")
        }
    }

    @Test
    fun `puuttuva arviointioikeus palauttaa lomakkeen eivatka syotteet katoa`() {
        val html =
            html(
                post("/yki/arvioijat/uusi")
                    .session(session())
                    .with(csrf())
                    .param("arvioijaOid", petronOid)
                    .param("sukunimi", "Kivinen-Testi")
                    .param("etunimet", "Petro Testi")
                    .param("katuosoite", "Kivinenkatu 2 A 3")
                    .param("postinumero", "00100")
                    .param("postitoimipaikka", "HELSINKI")
                    .param("kaudenAlkupaiva", "2025-12-07"),
            )

        assertContains(html, "Valitse vähintään yksi tutkintokieli ja tutkintotaso")
        assertContains(html, """value="Kivinen-Testi"""")
        assertContains(html, """value="Kivinenkatu 2 A 3"""")
        assertNull(repository.findByArvioijaOid(Oid.parse(petronOid).getOrThrow()))
    }

    @Test
    fun `puuttuva kauden alkupaiva palauttaa lomakkeen eika kaada renderointia`() {
        val html =
            html(
                post("/yki/arvioijat/uusi")
                    .session(session())
                    .with(csrf())
                    .param("arvioijaOid", petronOid)
                    .param("sukunimi", "Kivinen-Testi")
                    .param("etunimet", "Petro Testi")
                    .param("katuosoite", "Kivinenkatu 2 A 3")
                    .param("postinumero", "00100")
                    .param("postitoimipaikka", "HELSINKI")
                    .param("arviointioikeus", "FIN:PT"),
            )

        assertContains(html, "pakollinen tieto")
        assertContains(html, """aria-invalid="true"""")
        assertNull(repository.findByArvioijaOid(Oid.parse(petronOid).getOrThrow()))
    }

    @Test
    fun `virheellinen postinumero merkitaan kenttaan`() {
        val html =
            html(
                post("/yki/arvioijat/uusi")
                    .session(session())
                    .with(csrf())
                    .param("arvioijaOid", petronOid)
                    .param("sukunimi", "Kivinen-Testi")
                    .param("etunimet", "Petro Testi")
                    .param("katuosoite", "Kivinenkatu 2 A 3")
                    .param("postinumero", "1")
                    .param("postitoimipaikka", "HELSINKI")
                    .param("kaudenAlkupaiva", "2025-12-07")
                    .param("arviointioikeus", "FIN:PT"),
            )

        assertContains(html, "Postinumeron on oltava viisi numeroa")
        assertContains(html, """data-testid="postinumero-error"""")
    }

    @Test
    fun `lisaysnappi nakyy vain kirjoitusoikeudella`() {
        val kirjoittaja = html(get("/yki/arvioijat").session(session()))
        assertContains(kirjoittaja, """data-testid="lisaaArvioija"""")
        assertFalse(
            kirjoittaja.contains("""aria-disabled="true""""),
            "kytkimen ollessa paalla lisaysnapin on oltava klikattavissa",
        )

        val lukija = html(get("/yki/arvioijat").session(session(Authority.VIRKAILIJA)))
        assertFalse(lukija.contains("""data-testid="lisaaArvioija""""), "lukijalle ei saa nayttaa lisaysnappia")
    }

    @Test
    fun `jo rekisterissa olevan arvioijan lomake esitaytetaan rekisterin merkinnalla`() {
        tallennaOlemassaolevaMerkinta()

        val html = haku("oppijanumero" to petronOid)

        assertContains(html, "Arvioija on jo rekisterissä")
        assertFalse(
            html.contains("""value="OPH-9-2020""""),
            "uusi kausi on uusi hallintopaatos, joten edellisen viitetta ei esitayteta",
        )
        assertTrue(valittuna(html, "FIN:PT"), "nykyisen merkinnan oikeudet on esitaytettava:\n$html")
        assertTrue(valittuna(html, "SWE:YT"), "muunkielinen oikeus ei saa kadota lomakkeelta:\n$html")
        assertFalse(valittuna(html, "ENG:YT"), "valitsematon oikeus ei saa nakya valittuna")
    }

    @Test
    fun `jatkokausi ilman ASHA-numeroa ei peri edellisen kauden viitetta`() {
        tallennaOlemassaolevaMerkinta()

        mockMvc
            .perform(
                post("/yki/arvioijat/uusi")
                    .session(session())
                    .with(csrf())
                    .param("arvioijaOid", petronOid)
                    .param("sukunimi", "Kivinen-Testi")
                    .param("etunimet", "Petro Testi")
                    .param("katuosoite", "Kivinenkatu 2 A 3")
                    .param("postinumero", "00100")
                    .param("postitoimipaikka", "HELSINKI")
                    .param("kaudenAlkupaiva", "2026-03-01")
                    .param("arviointioikeus", "FIN:PT"),
            ).andExpect(status().isSeeOther)

        val id = repository.findByArvioijaOid(Oid.parse(petronOid).getOrThrow())!!.id!!.toInt()

        assertNull(
            kausiRepository.findArviointioikeudet(id).single().ashaNumero,
            "projektio seuraa voimassa olevaa kautta, jolla ei ole paatosviitetta",
        )
    }

    @Test
    fun `lisayslomakkeelta ei voi kirjata paallekkaista kautta`() {
        tallennaOlemassaolevaMerkinta()

        val html =
            html(
                post("/yki/arvioijat/uusi")
                    .session(session())
                    .with(csrf())
                    .param("arvioijaOid", petronOid)
                    .param("sukunimi", "Kivinen-Testi")
                    .param("etunimet", "Petro Testi")
                    .param("katuosoite", "Kivinenkatu 2 A 3")
                    .param("postinumero", "00100")
                    .param("postitoimipaikka", "HELSINKI")
                    .param("kaudenAlkupaiva", "2024-01-01")
                    .param("arviointioikeus", "FIN:PT"),
            )

        assertContains(html, "päällekkäin")
    }

    @Test
    fun `lisayslomakkeelta tallennus sailyttaa ensimmaisen rekisterointipaivan`() {
        tallennaOlemassaolevaMerkinta()

        mockMvc
            .perform(
                post("/yki/arvioijat/uusi")
                    .session(session())
                    .with(csrf())
                    .param("arvioijaOid", petronOid)
                    .param("sukunimi", "Kivinen-Testi")
                    .param("etunimet", "Petro Testi")
                    .param("katuosoite", "Kivinenkatu 2 A 3")
                    .param("postinumero", "00100")
                    .param("postitoimipaikka", "HELSINKI")
                    .param("kaudenAlkupaiva", "2026-03-01")
                    .param("arviointioikeus", "FIN:PT")
                    .param("arviointioikeus", "SWE:YT"),
            ).andExpect(status().isSeeOther)

        val tallennettu = repository.findByArvioijaOid(Oid.parse(petronOid).getOrThrow())!!
        val oikeudet = tallennettu.arviointioikeudet.associateBy { it.kieli }

        assertEquals(setOf(Tutkintokieli.FIN, Tutkintokieli.SWE), oikeudet.keys)
        oikeudet.values.forEach { oikeus ->
            assertEquals(LocalDate.of(2026, 3, 1), oikeus.kaudenAlkupaiva, "uusi kausi alkaa lomakkeen paivasta")
            assertEquals(
                LocalDate.of(2020, 1, 1),
                oikeus.ensimmainenRekisterointipaiva,
                "ensimmainen rekisterointipaiva on merkinnan historiaa, sita ei saa nollata",
            )
        }
    }

    private fun tallennaOlemassaolevaMerkinta(oid: String = petronOid): Int =
        tallennaMerkinta(oid).also { id ->
            kausiRepository.viimeisteleKausi(id, LocalDate.of(2020, 1, 1), "OPH-9-2020", LocalDate.of(2026, 6, 1))
        }

    private fun tallennaMerkinta(oid: String): Int =
        repository.tallenna(
            YkiArvioijaEntity(
                id = null,
                arvioijaOid = Oid.parse(oid).getOrThrow(),
                henkilotunnus = null,
                sukunimi = "Kivinen-Testi",
                etunimet = "Petro Testi",
                sahkopostiosoite = "kivinen-testi@oph.fi",
                katuosoite = "Kivinenkatu 2 A 3",
                postinumero = "00100",
                postitoimipaikka = "HELSINKI",
                arviointioikeudet =
                    listOf(
                        arviointioikeus(Tutkintokieli.FIN, setOf(Tutkintotaso.PT, Tutkintotaso.KT)),
                        arviointioikeus(Tutkintokieli.SWE, setOf(Tutkintotaso.YT)),
                    ),
            ),
        )

    private fun arviointioikeus(
        kieli: Tutkintokieli,
        tasot: Set<Tutkintotaso>,
    ) = YkiArviointioikeusEntity(
        id = null,
        arvioijaId = null,
        kieli = kieli,
        tasot = tasot,
        tila = YkiArvioijaTila.AKTIIVINEN,
        kaudenAlkupaiva = LocalDate.of(2020, 1, 1),
        kaudenPaattymispaiva = LocalDate.of(2025, 1, 1),
        jatkorekisterointi = false,
        ensimmainenRekisterointipaiva = LocalDate.of(2020, 1, 1),
        rekisteriintuontiaika = null,
    )

    private fun valittuna(
        html: String,
        arvo: String,
    ): Boolean =
        Regex("""<input[^>]*data-testid="arviointioikeus-$arvo"[^>]*>""")
            .find(html)
            ?.value
            ?.contains("checked") == true

    @Test
    fun `tietosivu nayttaa arvioijan tiedot`() {
        mockMvc
            .perform(
                post("/yki/arvioijat/uusi")
                    .session(session())
                    .with(csrf())
                    .param("arvioijaOid", petronOid)
                    .param("sukunimi", "Kivinen-Testi")
                    .param("etunimet", "Petro Testi")
                    .param("katuosoite", "Kivinenkatu 2 A 3")
                    .param("postinumero", "00100")
                    .param("postitoimipaikka", "HELSINKI")
                    .param("kaudenAlkupaiva", "2025-12-07")
                    .param("arviointioikeus", "FIN:PT"),
            ).andExpect(status().isSeeOther)

        val id = repository.findByArvioijaOid(Oid.parse(petronOid).getOrThrow())!!.id!!.toInt()
        val html = html(get("/yki/arvioijat/$id").session(session()))

        assertContains(html, "Petro Testi Kivinen-Testi")
        assertContains(html, petronOid)
        assertContains(html, "Perustaso")
    }

    @Test
    fun `piilokentan validointivirhe nakyy lomakkeella`() {
        val html =
            html(
                post("/yki/arvioijat/uusi")
                    .session(session())
                    .with(csrf())
                    .param("arvioijaOid", oidJotaEiOleOnrissa)
                    .param("sukunimi", "Kivinen-Testi")
                    .param("etunimet", "Petro Testi")
                    .param("katuosoite", "Kivinenkatu 2 A 3")
                    .param("postinumero", "00100")
                    .param("postitoimipaikka", "HELSINKI")
                    .param("kaudenAlkupaiva", "2026-01-01")
                    .param("arviointioikeus", "FIN:PT"),
            )

        assertContains(
            html,
            "ei löydy Oppijanumerorekisteristä",
            message = "arvioijaOid on vain piilokentta, joten virhe jaisi muuten renderoimatta",
        )
        assertContains(html, """data-testid="formErrorSummary"""")
    }

    @Test
    fun `yksiloimattoman henkilon oppijanumero hylataan eika mitaan tallenneta`() {
        val html = haku("oppijanumero" to yksiloimatonOid)

        assertContains(html, """data-testid="oppijanumero-error"""", message = "virheen on osuttava kenttaan")
        assertContains(html, "ei ole yksilöity")
        assertFalse(
            html.contains("""data-testid="tallennaArvioija""""),
            "yksiloimattomalle ei saa avata tallennuslomaketta",
        )
        assertNull(repository.findByArvioijaOid(Oid.parse(yksiloimatonOid).getOrThrow()))
    }

    @Test
    fun `duplikaattioppijanumerolla haettu ohjautuu master-oidin merkintaan`() {
        tallennaOlemassaolevaMerkinta(masterOid)

        val html = haku("oppijanumero" to duplikaattiOid)

        assertContains(html, """value="$masterOid"""", message = "merkinta avaimennetaan master-oidilla")
        assertContains(html, "Arvioija on jo rekisterissä", message = "olemassa oleva merkinta on loydyttava")
        assertFalse(html.contains("""value="$duplikaattiOid""""), "duplikaatti-oidia ei saa tallentaa")
    }

    @Test
    fun `tietosivu varoittaa kun turvakieltoa ei saada tarkistettua`() {
        val id = tallennaOlemassaolevaMerkinta(oidJotaEiOleOnrissa)

        val html = html(get("/yki/arvioijat/$id").session(session()))

        assertContains(html, "Turvakieltoa ei voitu tarkistaa")
    }

    @Test
    fun `muokkauslomake varoittaa kun turvakieltoa ei saada tarkistettua`() {
        val id = tallennaOlemassaolevaMerkinta(oidJotaEiOleOnrissa)

        val html = html(get("/yki/arvioijat/$id/muokkaa").session(session()))

        assertContains(html, "Turvakieltoa ei voitu tarkistaa")
    }

    @Test
    fun `tietosivu ei varoita kun oppijanumerorekisteri kertoo ettei turvakieltoa ole`() {
        val id = tallennaOlemassaolevaMerkinta()

        val html = html(get("/yki/arvioijat/$id").session(session()))

        assertFalse(html.contains("Turvakieltoa ei voitu tarkistaa"), "ONR vastasi, joten tieto on olemassa")
        assertFalse(html.contains("Henkilöllä on turvakielto"))
    }

    @Test
    fun `tuntematon arvioija palauttaa 404`() {
        mockMvc
            .perform(get("/yki/arvioijat/999999").session(session()))
            .andExpect(status().isNotFound)
    }

    private fun haku(vararg params: Pair<String, String>): String =
        html(
            post("/yki/arvioijat/uusi/haku").session(session()).with(csrf()).apply {
                params.forEach { (name, value) -> param(name, value) }
            },
        )

    private fun html(request: org.springframework.test.web.servlet.RequestBuilder): String {
        val result = mockMvc.perform(request).andReturn()
        assertEquals(200, result.response.status, "poikkeus: ${result.resolvedException}")
        return result.response.contentAsString
    }

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
