package fi.oph.kitu.yki

import fi.oph.kitu.DBContainerConfiguration
import fi.oph.kitu.TestTimeService
import fi.oph.kitu.isOk
import fi.oph.kitu.oid.Oid
import fi.oph.kitu.util.defaultObjectMapper
import fi.oph.kitu.util.result.getOrThrow
import fi.oph.kitu.yki.arvioijat.YkiArvioija
import fi.oph.kitu.yki.arvioijat.YkiArvioijaRepository
import fi.oph.kitu.yki.arvioijat.YkiArvioijaTila
import fi.oph.kitu.yki.arvioijat.YkiArviointioikeus
import org.junit.jupiter.api.BeforeEach
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.http.MediaType
import org.springframework.mock.web.MockHttpServletResponse
import org.springframework.security.test.web.servlet.setup.SecurityMockMvcConfigurers.springSecurity
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.MockMvcResultMatchersDsl
import org.springframework.test.web.servlet.post
import org.springframework.test.web.servlet.setup.MockMvcBuilders
import org.springframework.web.context.WebApplicationContext
import java.time.Instant
import java.time.LocalDate
import java.time.ZoneOffset
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

/**
 * Siirtymavaiheen kayttaytyminen: integraatiokytkin on pois paalta, jolloin Solki on yha
 * arvioijarekisterin master ja saa kirjoittaa koko merkinnan. Kavennettu polku on
 * [YkiArvioijaKavennettuApiTest]issa, joka ajaa kytkin paalla.
 */
@SpringBootTest(properties = ["kitu.yki.arvioijarekisteri.integraatio.enabled=false"])
@Import(DBContainerConfiguration::class)
class YkiArvioijaSiirtymaApiTest(
    @param:Autowired val timeService: TestTimeService,
    @param:Autowired val arvioijaRepository: YkiArvioijaRepository,
    @param:Autowired val context: WebApplicationContext,
) {
    private lateinit var mockMvc: MockMvc

    @BeforeEach
    fun setup() {
        mockMvc =
            MockMvcBuilders
                .webAppContextSetup(context)
                .apply { springSecurity() }
                .build()
    }

    private fun LocalDate.toInstant(): Instant = atStartOfDay().toInstant(ZoneOffset.UTC)

    @Test
    fun `Validin yki-arvioijan tallennus rajapinnan kautta onnistuu`() {
        timeService.runWithFixedClock(LocalDate.of(2025, 10, 20).toInstant()) {
            val arvioija =
                YkiArvioija(
                    arvioijaOid = Oid.parse("1.2.246.562.24.59267607404").getOrThrow(),
                    henkilotunnus = "160800A172A",
                    sukunimi = "Kivinen-Testi",
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
                                tila = YkiArvioijaTila.AKTIIVINEN,
                                kieli = Tutkintokieli.FIN,
                                tasot = setOf(Tutkintotaso.PT, Tutkintotaso.KT, Tutkintotaso.YT),
                            ),
                        ),
                )

            postArvioija(arvioija) {
                isOk()
            }
        }
    }

    @Test
    fun `Yki-arvioijan siirto onnistuu 2026 alkaen jättämällä hetu pois`() {
        timeService.runWithFixedClock(LocalDate.of(2026, 1, 1).toInstant()) {
            val arvioija =
                YkiArvioija(
                    arvioijaOid = Oid.parse("1.2.246.562.24.59267607404").getOrThrow(),
                    sukunimi = "Kivinen-Testi",
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
                                tila = YkiArvioijaTila.AKTIIVINEN,
                                kieli = Tutkintokieli.FIN,
                                tasot = setOf(Tutkintotaso.PT, Tutkintotaso.KT, Tutkintotaso.YT),
                            ),
                        ),
                )

            postArvioija(arvioija) {
                isOk()
            }
        }
    }

    @Test
    fun `Yki-arvioijan tallennuksessa puuttuva jatkorekisterointi defaultoituu falseksi`() {
        arvioijaRepository.deleteAll()
        timeService.runWithFixedClock(LocalDate.of(2025, 10, 20).toInstant()) {
            val arvioijaOid = "1.2.246.562.24.59267607404"
            val rawJson =
                """
                {
                  "arvioijaOid": "$arvioijaOid",
                  "henkilotunnus": "160800A172A",
                  "sukunimi": "Kivinen-Testi",
                  "etunimet": "Petro Testi",
                  "sahkopostiosoite": "devnull-2@oph.fi",
                  "katuosoite": "Haltin vanha autiotupa",
                  "postinumero": "99490",
                  "postitoimipaikka": "Enontekiö",
                  "ensimmainenRekisterointipaiva": "2005-01-21",
                  "arviointioikeudet": [
                    {
                      "kieli": "fin",
                      "tasot": ["PT", "KT", "YT"],
                      "tila": "AKTIIVINEN",
                      "kaudenAlkupaiva": "2005-12-07",
                      "kaudenPaattymispaiva": "2020-12-07"
                    }
                  ]
                }
                """.trimIndent()

            post("/yki/api/arvioija", rawJson) { isOk() }

            val saved =
                arvioijaRepository
                    .findAll()
                    .single { it.arvioijaOid.toString() == arvioijaOid }
            assertEquals(false, saved.arviointioikeudet.single().jatkorekisterointi)
        }
    }

    @Test
    fun `Solkin push ei poista arviointioikeuksia jotka puuttuvat payloadista`() {
        arvioijaRepository.deleteAll()
        timeService.runWithFixedClock(LocalDate.of(2026, 1, 1).toInstant()) {
            val arvioijaOid = Oid.parse("1.2.246.562.24.59267607404").getOrThrow()

            postArvioija(solkiArvioija(arvioijaOid, Tutkintokieli.FIN, Tutkintokieli.SWE)) { isOk() }
            postArvioija(solkiArvioija(arvioijaOid, Tutkintokieli.SWE)) { isOk() }

            val saved = arvioijaRepository.findByArvioijaOid(arvioijaOid)
            assertEquals(
                listOf(Tutkintokieli.FIN, Tutkintokieli.SWE),
                saved?.arviointioikeudet?.map { it.kieli }?.sortedBy { it.name },
            )
            assertNotNull(
                saved?.solkiinLahetetty,
                "Solkin omaa dataa ei lahetata takaisin Solkiin",
            )
        }
    }

    private fun solkiArvioija(
        arvioijaOid: Oid,
        vararg kielet: Tutkintokieli,
    ) = YkiArvioija(
        arvioijaOid = arvioijaOid,
        sukunimi = "Kivinen-Testi",
        etunimet = "Petro Testi",
        sahkopostiosoite = "devnull-2@oph.fi",
        katuosoite = "Haltin vanha autiotupa",
        postinumero = "99490",
        postitoimipaikka = "Enontekiö",
        ensimmainenRekisterointipaiva = LocalDate.of(2005, 1, 21),
        arviointioikeudet =
            kielet.map { kieli ->
                YkiArviointioikeus(
                    kaudenAlkupaiva = LocalDate.of(2005, 12, 7),
                    kaudenPaattymispaiva = LocalDate.of(2020, 12, 7),
                    jatkorekisterointi = false,
                    tila = YkiArvioijaTila.AKTIIVINEN,
                    kieli = kieli,
                    tasot = setOf(Tutkintotaso.PT),
                )
            },
    )

    private fun postArvioija(
        arvioija: YkiArvioija,
        block: MockMvcResultMatchersDsl.() -> Unit,
    ) {
        post("/yki/api/arvioija", defaultObjectMapper.writeValueAsString(arvioija), block)
    }

    private fun post(
        url: String,
        json: String,
        block: MockMvcResultMatchersDsl.() -> Unit,
    ): MockHttpServletResponse =
        mockMvc
            .post(url) {
                contentType = MediaType.APPLICATION_JSON
                accept = MediaType.APPLICATION_JSON
                content = json
            }.andExpect {
                content { contentType(MediaType.APPLICATION_JSON) }
                block()
            }.andReturn()
            .response
}
