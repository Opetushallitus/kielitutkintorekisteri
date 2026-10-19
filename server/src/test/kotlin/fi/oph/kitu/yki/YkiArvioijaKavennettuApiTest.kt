package fi.oph.kitu.yki

import fi.oph.kitu.DBContainerConfiguration
import fi.oph.kitu.isBadRequest
import fi.oph.kitu.isOk
import fi.oph.kitu.oid.Oid
import fi.oph.kitu.util.defaultObjectMapper
import fi.oph.kitu.util.result.getOrThrow
import fi.oph.kitu.yki.arvioijat.YkiArvioija
import fi.oph.kitu.yki.arvioijat.YkiArvioijaEntity
import fi.oph.kitu.yki.arvioijat.YkiArvioijaRepository
import fi.oph.kitu.yki.arvioijat.YkiArvioijaTila
import fi.oph.kitu.yki.arvioijat.YkiArviointioikeus
import fi.oph.kitu.yki.arvioijat.YkiArviointioikeusEntity
import fi.oph.kitu.yki.arvioijat.arvioija
import fi.oph.kitu.yki.arvioijat.arvioijaOid
import org.junit.jupiter.api.BeforeEach
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.http.MediaType
import org.springframework.security.test.web.servlet.setup.SecurityMockMvcConfigurers.springSecurity
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.MockMvcResultMatchersDsl
import org.springframework.test.web.servlet.post
import org.springframework.test.web.servlet.setup.MockMvcBuilders
import org.springframework.web.context.WebApplicationContext
import java.time.LocalDate
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

/**
 * Kavennettu sisaantulo (§4.2): kitun ollessa master Solki saa paivittaa vain yhteystiedot.
 * Siirtymavaiheen kayttaytyminen on [YkiArvioijaSiirtymaApiTest]issa.
 */
@SpringBootTest(properties = ["kitu.yki.arvioijarekisteri.integraatio.enabled=true"])
@Import(DBContainerConfiguration::class)
class YkiArvioijaKavennettuApiTest(
    @param:Autowired val repository: YkiArvioijaRepository,
    @param:Autowired val context: WebApplicationContext,
) {
    private val oid = Oid.parse("1.2.246.562.24.59267607404").getOrThrow()
    private lateinit var mockMvc: MockMvc

    @BeforeEach
    fun setup() {
        mockMvc = MockMvcBuilders.webAppContextSetup(context).apply { springSecurity() }.build()
        repository.deleteAll()
    }

    @Test
    fun `push paivittaa vain yhteystiedot`() {
        val id = seed()

        postArvioija(payload(kieli = Tutkintokieli.SWE, sukunimi = "Solki-Sukunimi")) { isOk() }

        val paivitetty = repository.findArvioijaById(id)!!
        assertEquals("devnull-2@oph.fi", paivitetty.sahkopostiosoite, "yhteystieto paivittyy")
        assertEquals("Haltin vanha autiotupa", paivitetty.katuosoite)
        assertEquals("Kivinen-Testi", paivitetty.sukunimi, "nimet omistaa ONR, ei Solki")
        assertEquals(
            listOf(Tutkintokieli.FIN),
            paivitetty.arviointioikeudet.map { it.kieli },
            "kitu on rekisterin master: Solki ei lisaa eika poista arviointioikeuksia",
        )
        assertNull(
            paivitetty.arviointioikeudet.single().tila,
            "Solkin lahettamaa tilaa ei kirjata kun kitu on master",
        )
    }

    @Test
    fun `tuntematonta arvioijaa ei luoda`() {
        postArvioija(payload()) {
            isBadRequest("Arvioijaa $oid ei ole rekisterissa")
        }

        assertEquals(0, repository.findAll().count(), "kitu paattaa kuka rekisterissa on")
    }

    @Test
    fun `yhteystietopaivitys ei paady takaisin Solkiin`() {
        val id = seed()
        // Kitun oma tallennus jattaa rivin jonoon; simuloidaan etta lahetin on jo ajanut sen.
        repository.merkitseLahetetyksi(id, repository.findArvioijaById(id)!!.muokattu)

        postArvioija(payload()) { isOk() }

        assertEquals(0, repository.findLahetettavat().size, "Solkin oma muutos ei kaiu takaisin")
    }

    @Test
    fun `yhteystietopaivitys ei nielaise kitun lahettamatonta muutosta`() {
        seed()

        postArvioija(payload()) { isOk() }

        assertEquals(
            1,
            repository.findLahetettavat().size,
            "jonossa ollut kitun muutos on yha lahetettava",
        )
    }

    private fun seed(): Int =
        repository.tallenna(
            YkiArvioijaEntity(
                id = null,
                arvioijaOid = oid,
                henkilotunnus = null,
                sukunimi = "Kivinen-Testi",
                etunimet = "Petro Testi",
                sahkopostiosoite = "vanha@testi.fi",
                katuosoite = "Vanhakuja 1",
                postinumero = "00100",
                postitoimipaikka = "Helsinki",
                arviointioikeudet =
                    listOf(
                        YkiArviointioikeusEntity(
                            id = null,
                            arvioijaId = null,
                            kieli = Tutkintokieli.FIN,
                            tasot = setOf(Tutkintotaso.PT),
                            tila = null,
                            kaudenAlkupaiva = LocalDate.of(2024, 1, 1),
                            kaudenPaattymispaiva = LocalDate.of(2029, 1, 1),
                            jatkorekisterointi = false,
                            ensimmainenRekisterointipaiva = LocalDate.of(2024, 1, 1),
                            rekisteriintuontiaika = null,
                        ),
                    ),
            ),
        )

    private fun payload(
        kieli: Tutkintokieli = Tutkintokieli.FIN,
        sukunimi: String = "Kivinen-Testi",
    ) = YkiArvioija(
        arvioijaOid = oid,
        sukunimi = sukunimi,
        etunimet = "Petro Testi",
        sahkopostiosoite = "devnull-2@oph.fi",
        katuosoite = "Haltin vanha autiotupa",
        postinumero = "99490",
        postitoimipaikka = "Enontekiö",
        ensimmainenRekisterointipaiva = LocalDate.of(2005, 1, 21),
        arviointioikeudet =
            listOf(
                YkiArviointioikeus(
                    kaudenAlkupaiva = LocalDate.of(2005, 12, 7),
                    kaudenPaattymispaiva = LocalDate.of(2020, 12, 7),
                    jatkorekisterointi = false,
                    tila = YkiArvioijaTila.PASSIVOITU,
                    kieli = kieli,
                    tasot = setOf(Tutkintotaso.PT),
                ),
            ),
    )

    private fun postArvioija(
        arvioija: YkiArvioija,
        block: MockMvcResultMatchersDsl.() -> Unit,
    ) {
        mockMvc
            .post("/yki/api/arvioija") {
                contentType = MediaType.APPLICATION_JSON
                accept = MediaType.APPLICATION_JSON
                content = defaultObjectMapper.writeValueAsString(arvioija)
            }.andExpect { block() }
    }
}
