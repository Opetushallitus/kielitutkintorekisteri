package fi.oph.kitu.yki

import fi.oph.kitu.DBContainerConfiguration
import fi.oph.kitu.TestTimeService
import fi.oph.kitu.dev.mockdata.generateRandomYkiSuoritusEntity
import fi.oph.kitu.dev.mockdata.toInstant
import fi.oph.kitu.html.table.ColumnTag
import fi.oph.kitu.html.table.DisplayTableColumn
import fi.oph.kitu.html.table.DisplayTableCsvRenderer
import fi.oph.kitu.i18n.UiText
import fi.oph.kitu.isBadRequest
import fi.oph.kitu.isOk
import fi.oph.kitu.oid.Oid
import fi.oph.kitu.tiedontuontischema.Henkilo
import fi.oph.kitu.tiedontuontischema.Henkilosuoritus
import fi.oph.kitu.tiedontuontischema.Lahdejarjestelma
import fi.oph.kitu.tiedontuontischema.LahdejarjestelmanTunniste
import fi.oph.kitu.tiedontuontischema.TiedonsiirtoFailure
import fi.oph.kitu.tiedontuontischema.YkiJarjestaja
import fi.oph.kitu.tiedontuontischema.YkiOsa
import fi.oph.kitu.tiedontuontischema.YkiSuoritus
import fi.oph.kitu.tiedontuontischema.YkiTarkastusarviointi
import fi.oph.kitu.util.defaultObjectMapper
import fi.oph.kitu.util.result.getOrThrow
import fi.oph.kitu.verboseContentJson
import fi.oph.kitu.yki.arvioijat.YkiArvioija
import fi.oph.kitu.yki.arvioijat.YkiArvioijaRepository
import fi.oph.kitu.yki.arvioijat.YkiArvioijaTila
import fi.oph.kitu.yki.arvioijat.YkiArviointioikeus
import fi.oph.kitu.yki.suoritukset.Todistuskieli
import fi.oph.kitu.yki.suoritukset.YkiSuoritusColumn
import fi.oph.kitu.yki.suoritukset.YkiSuoritusEntity
import fi.oph.kitu.yki.suoritukset.YkiSuoritusRepository
import org.junit.jupiter.api.BeforeEach
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.core.io.ClassPathResource
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.mock.web.MockHttpServletResponse
import org.springframework.mock.web.MockHttpSession
import org.springframework.security.test.web.reactive.server.SecurityMockServerConfigurers.springSecurity
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.MockMvcResultMatchersDsl
import org.springframework.test.web.servlet.post
import org.springframework.test.web.servlet.setup.MockMvcBuilders
import org.springframework.web.context.WebApplicationContext
import org.testcontainers.postgresql.PostgreSQLContainer
import tools.jackson.databind.JsonNode
import java.io.ByteArrayOutputStream
import java.time.Instant
import java.time.LocalDate
import kotlin.test.Test
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull

@SpringBootTest
@Import(DBContainerConfiguration::class)
class YkiApiControllerTest(
    @param:Autowired val timeService: TestTimeService,
    @param:Autowired val arvioijaRepository: YkiArvioijaRepository,
    @param:Autowired val suoritusRepository: YkiSuoritusRepository,
    @param:Autowired val ykiApiController: YkiApiController,
) {
    @Autowired
    private lateinit var context: WebApplicationContext

    @Autowired private var postgres: PostgreSQLContainer? = null
    private var mockMvc: MockMvc? = null

    @BeforeEach
    fun setup() {
        mockMvc =
            MockMvcBuilders
                .webAppContextSetup(context)
                .apply { springSecurity() }
                .build()
    }

    @Test
    fun `Validin yki-suorituksen tallennus rajapinnan kautta onnistuu`() {
        val suoritus =
            Henkilosuoritus(
                henkilo =
                    Henkilo(
                        oid = Oid.parse("1.2.246.562.24.20281155246").getOrThrow(),
                        etunimet = "Ranja Testi",
                        sukunimi = "Öhman-Testi",
                        hetu = "010180-9026",
                        sukupuoli = Sukupuoli.N,
                        kansalaisuus = "EST",
                        katuosoite = "Testikuja 5",
                        postinumero = "40100",
                        postitoimipaikka = "Testilä",
                        email = "testi@testi.fi",
                    ),
                suoritus =
                    YkiSuoritus(
                        tutkintotaso = Tutkintotaso.YT,
                        kieli = Tutkintokieli.FIN,
                        todistuskieli = Todistuskieli.FIN,
                        jarjestaja =
                            YkiJarjestaja(
                                oid = Oid.parse("1.2.246.562.10.14893989377").getOrThrow(),
                                nimi = "Jyväskylän yliopisto, Soveltavan kielentutkimuksen keskus",
                            ),
                        tutkintopaiva = LocalDate.of(2024, 9, 1),
                        arviointipaiva = LocalDate.of(2024, 12, 13),
                        arviointitila = Arviointitila.TARKISTUSARVIOITU,
                        osat =
                            listOf(
                                YkiOsa(
                                    tyyppi = TutkinnonOsa.puhuminen,
                                    arvosana = 5,
                                ),
                                YkiOsa(
                                    tyyppi = TutkinnonOsa.puheenYmmartaminen,
                                    arvosana = 5,
                                ),
                                YkiOsa(
                                    tyyppi = TutkinnonOsa.kirjoittaminen,
                                    arvosana = 5,
                                ),
                                YkiOsa(
                                    tyyppi = TutkinnonOsa.tekstinYmmartaminen,
                                    arvosana = 5,
                                ),
                                YkiOsa(
                                    tyyppi = TutkinnonOsa.rakenteetJaSanasto,
                                    arvosana = 5,
                                ),
                                YkiOsa(
                                    tyyppi = TutkinnonOsa.yleisarvosana,
                                    arvosana = 5,
                                ),
                            ),
                        tarkistusarviointi =
                            YkiTarkastusarviointi(
                                saapumispaiva = LocalDate.of(2024, 12, 14),
                                kasittelypaiva = LocalDate.of(2024, 12, 14),
                                asiatunnus = "OPH-5000-1234",
                                tarkistusarvioidutOsakokeet = listOf(TutkinnonOsa.puhuminen),
                                arvosanaMuuttui = listOf(TutkinnonOsa.puhuminen),
                                perustelu =
                                    "Suorituksesta jäänyt viimeinen tehtävä arvioimatta. Arvioinnin jälkeen puhumisen taitotasoa 6.",
                            ),
                        lahdejarjestelmanId =
                            LahdejarjestelmanTunniste(
                                id = "183424",
                                lahde = Lahdejarjestelma.Solki,
                            ),
                    ),
            )

        postSuoritus(suoritus) {
            isOk()
        }
    }

    @Test
    fun `Validin yki-suorituksen tallennus rajapinnan kautta onnistuu 2026 alkaen, kunhan hetun jättää pois`() {
        val suoritus =
            Henkilosuoritus(
                henkilo =
                    Henkilo(
                        oid = Oid.parse("1.2.246.562.24.20281155246").getOrThrow(),
                        etunimet = "Ranja Testi",
                        sukunimi = "Öhman-Testi",
                        hetu = null,
                        sukupuoli = Sukupuoli.N,
                        kansalaisuus = "EST",
                        katuosoite = "Testikuja 5",
                        postinumero = "40100",
                        postitoimipaikka = "Testilä",
                        email = "testi@testi.fi",
                    ),
                suoritus =
                    YkiSuoritus(
                        tutkintotaso = Tutkintotaso.YT,
                        kieli = Tutkintokieli.FIN,
                        todistuskieli = Todistuskieli.FIN,
                        jarjestaja =
                            YkiJarjestaja(
                                oid = Oid.parse("1.2.246.562.10.14893989377").getOrThrow(),
                                nimi = "Jyväskylän yliopisto, Soveltavan kielentutkimuksen keskus",
                            ),
                        tutkintopaiva = LocalDate.of(2026, 9, 1),
                        arviointipaiva = LocalDate.of(2026, 12, 13),
                        arviointitila = Arviointitila.TARKISTUSARVIOITU,
                        osat =
                            listOf(
                                YkiOsa(
                                    tyyppi = TutkinnonOsa.puhuminen,
                                    arvosana = 5,
                                ),
                                YkiOsa(
                                    tyyppi = TutkinnonOsa.puheenYmmartaminen,
                                    arvosana = 5,
                                ),
                                YkiOsa(
                                    tyyppi = TutkinnonOsa.kirjoittaminen,
                                    arvosana = 5,
                                ),
                                YkiOsa(
                                    tyyppi = TutkinnonOsa.tekstinYmmartaminen,
                                    arvosana = 5,
                                ),
                                YkiOsa(
                                    tyyppi = TutkinnonOsa.rakenteetJaSanasto,
                                    arvosana = 5,
                                ),
                                YkiOsa(
                                    tyyppi = TutkinnonOsa.yleisarvosana,
                                    arvosana = 5,
                                ),
                            ),
                        tarkistusarviointi =
                            YkiTarkastusarviointi(
                                saapumispaiva = LocalDate.of(2024, 12, 14),
                                kasittelypaiva = LocalDate.of(2024, 12, 14),
                                asiatunnus = "OPH-5000-1234",
                                tarkistusarvioidutOsakokeet = listOf(TutkinnonOsa.puhuminen),
                                arvosanaMuuttui = listOf(TutkinnonOsa.puhuminen),
                                perustelu =
                                    "Suorituksesta jäänyt viimeinen tehtävä arvioimatta. Arvioinnin jälkeen puhumisen taitotasoa 6.",
                            ),
                        lahdejarjestelmanId =
                            LahdejarjestelmanTunniste(
                                id = "183424",
                                lahde = Lahdejarjestelma.Solki,
                            ),
                    ),
            )

        postSuoritus(suoritus) {
            isOk()
        }
    }

    @Test
    fun `Suoritus vanhentuneella kielikoodilla palauttaa virheen`() {
        val suoritus =
            Henkilosuoritus(
                henkilo =
                    Henkilo(
                        oid = Oid.parse("1.2.246.562.24.20281155246").getOrThrow(),
                        etunimet = "Ranja Testi",
                        sukunimi = "Öhman-Testi",
                        hetu = null,
                        sukupuoli = Sukupuoli.N,
                        kansalaisuus = "EST",
                        katuosoite = "Testikuja 5",
                        postinumero = "40100",
                        postitoimipaikka = "Testilä",
                        email = "testi@testi.fi",
                    ),
                suoritus =
                    YkiSuoritus(
                        tutkintotaso = Tutkintotaso.YT,
                        kieli = Tutkintokieli.ENG11,
                        todistuskieli = Todistuskieli.FIN,
                        jarjestaja =
                            YkiJarjestaja(
                                oid = Oid.parse("1.2.246.562.10.14893989377").getOrThrow(),
                                nimi = "Jyväskylän yliopisto, Soveltavan kielentutkimuksen keskus",
                            ),
                        tutkintopaiva = LocalDate.of(2026, 9, 1),
                        arviointipaiva = LocalDate.of(2026, 12, 13),
                        arviointitila = Arviointitila.TARKISTUSARVIOITU,
                        osat =
                            listOf(
                                YkiOsa(
                                    tyyppi = TutkinnonOsa.puhuminen,
                                    arvosana = 5,
                                ),
                                YkiOsa(
                                    tyyppi = TutkinnonOsa.puheenYmmartaminen,
                                    arvosana = 5,
                                ),
                                YkiOsa(
                                    tyyppi = TutkinnonOsa.kirjoittaminen,
                                    arvosana = 5,
                                ),
                                YkiOsa(
                                    tyyppi = TutkinnonOsa.tekstinYmmartaminen,
                                    arvosana = 5,
                                ),
                                YkiOsa(
                                    tyyppi = TutkinnonOsa.rakenteetJaSanasto,
                                    arvosana = 5,
                                ),
                                YkiOsa(
                                    tyyppi = TutkinnonOsa.yleisarvosana,
                                    arvosana = 5,
                                ),
                            ),
                        tarkistusarviointi =
                            YkiTarkastusarviointi(
                                saapumispaiva = LocalDate.of(2024, 12, 14),
                                kasittelypaiva = LocalDate.of(2024, 12, 14),
                                asiatunnus = "OPH-5000-1234",
                                tarkistusarvioidutOsakokeet = listOf(TutkinnonOsa.puhuminen),
                                arvosanaMuuttui = listOf(TutkinnonOsa.puhuminen),
                                perustelu =
                                    "Suorituksesta jäänyt viimeinen tehtävä arvioimatta. Arvioinnin jälkeen puhumisen taitotasoa 6.",
                            ),
                        lahdejarjestelmanId =
                            LahdejarjestelmanTunniste(
                                id = "183424",
                                lahde = Lahdejarjestelma.Solki,
                            ),
                    ),
            )

        postSuoritus(suoritus) {
            isBadRequest("suoritus.kieli: Käytöstä poistuneita kielikoodeja (SWE10, ENG11, ENG12) ei voi käyttää")
        }
    }

    @Test
    fun `YKI-suoritus virheellisellä OIDilla palauttaa virheen`() {
        val data =
            defaultObjectMapper
                .readValue(
                    ClassPathResource("./yki-suoritus-invalid-oid-example.json").file,
                    JsonNode::class.java,
                ).toString()

        post("/yki/api/suoritus", data) {
            isBadRequest(
                "JSON parse error: Cannot construct instance of `fi.oph.kitu.oid.Oid`, problem: Improperly formatted Object Identifier String - 123",
            )
        }
    }

    @Test
    fun `YKI-suoritus puuttuvilla etunimillä palauttaa virheen`() {
        val data =
            defaultObjectMapper
                .readValue(
                    ClassPathResource("./yki-suoritus-missing-firstnames-example.json").file,
                    JsonNode::class.java,
                ).toString()

        post("/yki/api/suoritus", data) {
            isBadRequest(
                "Etunimet puuttuu",
            )
        }
    }

    @Test
    fun `Suoritus muuttuneilla arvosanoilla, joita ei oltu tarkistettavana, palauttaa virheen`() {
        val data =
            defaultObjectMapper
                .readValue(
                    ClassPathResource("./yki-suoritus-invalid-tarkistusarviointi-example.json").file,
                    JsonNode::class.java,
                ).toString()

        post("/yki/api/suoritus", data) {
            isBadRequest(
                "suoritus.tarkistusarviointi.arvosanaMuuttui: Muuttuneet arvosanat sisälsivät osakokeita, jotka eivät olleet osa tarkistettavia osakokeita",
                "suoritus.tarkistusarviointi.kasittelypaiva: Käsittelypäivä on ennen saapumispäivää",
            )
        }
    }

    @Test
    fun `YKI-suorituksen tallennus ilman todistuskieltä onnistuu`() {
        val json = ClassPathResource("./yki-suoritus-without-todistuskieli.json").file
        val data = defaultObjectMapper.readValue(json, JsonNode::class.java).toString()

        post("/yki/api/suoritus", data, { isOk() })
    }

    @Test
    fun `Suoritus virheellisellä todistuskielellä palauttaa virheen`() {
        val data =
            defaultObjectMapper
                .readValue(
                    ClassPathResource("./yki-suoritus-invalid-todistuskieli.json").file,
                    JsonNode::class.java,
                ).toString()

        post("/yki/api/suoritus", data) {
            isBadRequest(
                "JSON parse error: Cannot deserialize value of type `fi.oph.kitu.yki.suoritukset.Todistuskieli` from String \"asdasd\": not one of the values accepted for Enum class: [swe, fin, eng]",
            )
        }
    }

    @Test
    fun `Suoritus jonka tila on ARVIOITU mutta jonka osakokeilla ei ole oikeita arvosanoja aiheuttaa virheen`() {
        val suoritus =
            Henkilosuoritus(
                henkilo =
                    Henkilo(
                        oid = Oid.parse("1.2.246.562.24.20281155246").getOrThrow(),
                        etunimet = "Ranja Testi",
                        sukunimi = "Öhman-Testi",
                        sukupuoli = Sukupuoli.N,
                        kansalaisuus = "EST",
                        katuosoite = "Testikuja 5",
                        postinumero = "40100",
                        postitoimipaikka = "Testilä",
                        maa = "FIN",
                        email = "testi@testi.fi",
                    ),
                suoritus =
                    YkiSuoritus(
                        tutkintotaso = Tutkintotaso.YT,
                        kieli = Tutkintokieli.ENG,
                        todistuskieli = Todistuskieli.FIN,
                        jarjestaja =
                            YkiJarjestaja(
                                oid = Oid.parse("1.2.246.562.10.14893989377").getOrThrow(),
                                nimi = "Jyväskylän yliopisto, Soveltavan kielentutkimuksen keskus",
                            ),
                        tutkintopaiva = LocalDate.of(2026, 9, 1),
                        arviointipaiva = LocalDate.of(2026, 12, 13),
                        arviointitila = Arviointitila.ARVIOITU,
                        osat =
                            listOf(
                                YkiOsa(
                                    tyyppi = TutkinnonOsa.puhuminen,
                                    arvosana = 12,
                                ),
                                YkiOsa(
                                    tyyppi = TutkinnonOsa.puheenYmmartaminen,
                                    arvosana = 12,
                                ),
                                YkiOsa(
                                    tyyppi = TutkinnonOsa.kirjoittaminen,
                                    arvosana = 12,
                                ),
                                YkiOsa(
                                    tyyppi = TutkinnonOsa.tekstinYmmartaminen,
                                    arvosana = 12,
                                ),
                            ),
                        lahdejarjestelmanId =
                            LahdejarjestelmanTunniste(
                                id = "183424",
                                lahde = Lahdejarjestelma.Solki,
                            ),
                    ),
            )

        postSuoritus(suoritus) {
            isBadRequest(
                "suoritus.arviointitila: Arviointitila 'ARVIOITU' edellyttää, " +
                    "että vähintään yhdellä osakokeella on oikea arvosana",
            )
        }
    }

    @Test
    fun `Ilmoittautumisen jossa on vain osa osakokeista tallentaminen onnistuu`() {
        val suoritus =
            Henkilosuoritus(
                henkilo =
                    Henkilo(
                        oid = Oid.parse("1.2.246.562.24.20281155246").getOrThrow(),
                        etunimet = "Ranja Testi",
                        sukunimi = "Öhman-Testi",
                        sukupuoli = Sukupuoli.N,
                        kansalaisuus = "EST",
                        katuosoite = "Testikuja 5",
                        postinumero = "40100",
                        postitoimipaikka = "Testilä",
                        maa = "FIN",
                        email = "testi@testi.fi",
                    ),
                suoritus =
                    YkiSuoritus(
                        tutkintotaso = Tutkintotaso.YT,
                        kieli = Tutkintokieli.ENG,
                        todistuskieli = Todistuskieli.FIN,
                        jarjestaja =
                            YkiJarjestaja(
                                oid = Oid.parse("1.2.246.562.10.14893989377").getOrThrow(),
                                nimi = "Jyväskylän yliopisto, Soveltavan kielentutkimuksen keskus",
                            ),
                        tutkintopaiva = LocalDate.of(2026, 9, 1),
                        arviointipaiva = null,
                        arviointitila = Arviointitila.ARVIOITAVA,
                        osat =
                            listOf(
                                YkiOsa(
                                    tyyppi = TutkinnonOsa.puhuminen,
                                    arvosana = null,
                                ),
                                YkiOsa(
                                    tyyppi = TutkinnonOsa.puheenYmmartaminen,
                                    arvosana = null,
                                ),
                                YkiOsa(
                                    tyyppi = TutkinnonOsa.kirjoittaminen,
                                    arvosana = 12,
                                ),
                                YkiOsa(
                                    tyyppi = TutkinnonOsa.tekstinYmmartaminen,
                                    arvosana = 12,
                                ),
                            ),
                        lahdejarjestelmanId =
                            LahdejarjestelmanTunniste(
                                id = "183424",
                                lahde = Lahdejarjestelma.Solki,
                            ),
                    ),
            )

        postSuoritus(suoritus) {
            isOk()
        }
    }

    @Test
    fun `Suoritus virheellisillä arvosanoilla aiheuttaa virheen`() {
        val suoritus =
            Henkilosuoritus(
                henkilo =
                    Henkilo(
                        oid = Oid.parse("1.2.246.562.24.20281155246").getOrThrow(),
                        etunimet = "Ranja Testi",
                        sukunimi = "Öhman-Testi",
                        sukupuoli = Sukupuoli.N,
                        kansalaisuus = "EST",
                        katuosoite = "Testikuja 5",
                        postinumero = "40100",
                        postitoimipaikka = "Testilä",
                        maa = "FIN",
                        email = "testi@testi.fi",
                    ),
                suoritus =
                    YkiSuoritus(
                        tutkintotaso = Tutkintotaso.YT,
                        kieli = Tutkintokieli.ENG,
                        todistuskieli = Todistuskieli.FIN,
                        jarjestaja =
                            YkiJarjestaja(
                                oid = Oid.parse("1.2.246.562.10.14893989377").getOrThrow(),
                                nimi = "Jyväskylän yliopisto, Soveltavan kielentutkimuksen keskus",
                            ),
                        tutkintopaiva = LocalDate.of(2026, 9, 1),
                        arviointipaiva = LocalDate.of(2026, 12, 13),
                        arviointitila = Arviointitila.ARVIOITU,
                        osat =
                            listOf(
                                YkiOsa(
                                    tyyppi = TutkinnonOsa.puhuminen,
                                    arvosana = 5,
                                ),
                                YkiOsa(
                                    tyyppi = TutkinnonOsa.puheenYmmartaminen,
                                    arvosana = 666,
                                ),
                                YkiOsa(
                                    tyyppi = TutkinnonOsa.kirjoittaminen,
                                    arvosana = 123,
                                ),
                                YkiOsa(
                                    tyyppi = TutkinnonOsa.tekstinYmmartaminen,
                                    arvosana = 12,
                                ),
                            ),
                        lahdejarjestelmanId =
                            LahdejarjestelmanTunniste(
                                id = "183424",
                                lahde = Lahdejarjestelma.Solki,
                            ),
                    ),
            )

        postSuoritus(suoritus) {
            isBadRequest(
                "suoritus.osat.arvosana: Suoritus sisältää tutkintotasolle YT virheellisiä arvosanoja: 666, 123",
            )
        }
    }

    @Test
    fun `Suoritus, jolla on tutkintotasolle liian korkea arvosana, aiheuttaa virheen`() {
        val suoritus =
            Henkilosuoritus(
                henkilo =
                    Henkilo(
                        oid = Oid.parse("1.2.246.562.24.20281155246").getOrThrow(),
                        etunimet = "Ranja Testi",
                        sukunimi = "Öhman-Testi",
                        sukupuoli = Sukupuoli.N,
                        kansalaisuus = "EST",
                        katuosoite = "Testikuja 5",
                        postinumero = "40100",
                        postitoimipaikka = "Testilä",
                        maa = "FIN",
                        email = "testi@testi.fi",
                    ),
                suoritus =
                    YkiSuoritus(
                        tutkintotaso = Tutkintotaso.PT,
                        kieli = Tutkintokieli.ENG,
                        todistuskieli = Todistuskieli.FIN,
                        jarjestaja =
                            YkiJarjestaja(
                                oid = Oid.parse("1.2.246.562.10.14893989377").getOrThrow(),
                                nimi = "Jyväskylän yliopisto, Soveltavan kielentutkimuksen keskus",
                            ),
                        tutkintopaiva = LocalDate.of(2026, 9, 1),
                        arviointipaiva = LocalDate.of(2026, 12, 13),
                        arviointitila = Arviointitila.ARVIOITU,
                        osat =
                            listOf(
                                YkiOsa(tyyppi = TutkinnonOsa.puhuminen, arvosana = 2),
                                YkiOsa(tyyppi = TutkinnonOsa.puheenYmmartaminen, arvosana = 1),
                                YkiOsa(tyyppi = TutkinnonOsa.kirjoittaminen, arvosana = 3),
                                YkiOsa(tyyppi = TutkinnonOsa.tekstinYmmartaminen, arvosana = 5),
                            ),
                        lahdejarjestelmanId =
                            LahdejarjestelmanTunniste(
                                id = "183424",
                                lahde = Lahdejarjestelma.Solki,
                            ),
                    ),
            )

        postSuoritus(suoritus) {
            isBadRequest("suoritus.osat.arvosana: Suoritus sisältää tutkintotasolle PT virheellisiä arvosanoja: 3, 5")
        }
    }

    @Test
    fun `YKI-arvoija-json deserialisoituu YkiArvioijaksi`() {
        val json = ClassPathResource("./yki-arvioija-example.json").file
        val data = defaultObjectMapper.readValue(json, YkiArvioija::class.java)

        assertEquals(Oid.parse("1.2.246.562.24.59267607404").getOrThrow(), data.arvioijaOid)
        assertEquals(setOf(Tutkintotaso.PT, Tutkintotaso.KT, Tutkintotaso.YT), data.arviointioikeudet.first().tasot)
    }

    @Test
    fun `Henkilötunnusta ei voi siirtää yki-arvioijalle vuodesta 2026 alkaen`() {
        timeService.runWithFixedClock(LocalDate.of(2026, 1, 1).toInstant()) {
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
                isBadRequest(
                    "henkilotunnus: Kenttää henkilotunnus ei voi siirtää 1.1.2026 alkaen",
                )
            }
        }
    }

    @Test
    fun `CSV-vienti näyttää taustalta täydennetyn opiskeluoikeus-OIDin`() {
        suoritusRepository.deleteAll()

        val opiskeluoikeusOid = Oid.parse("1.2.246.562.15.00000000001").getOrThrow()
        val vanhempiVersio =
            generateRandomYkiSuoritusEntity().copy(
                koskiOpiskeluoikeus = opiskeluoikeusOid,
                lastModified = Instant.parse("2025-01-01T10:00:00Z"),
                receivedAt = Instant.parse("2025-01-01T10:00:00Z"),
            )
        val viimeisinVersio =
            vanhempiVersio.copy(
                koskiOpiskeluoikeus = null,
                katuosoite = "Uusi katu 1",
                lastModified = Instant.parse("2025-02-01T10:00:00Z"),
                receivedAt = Instant.parse("2025-02-01T10:00:00Z"),
            )
        suoritusRepository.saveAllNewEntities(listOf(vanhempiVersio, viimeisinVersio))

        assertNull(
            suoritusRepository.findLatestBySolkiIds(listOf(viimeisinVersio.solkiId)).first().koskiOpiskeluoikeus,
            "Viimeisimmällä versiolla ei saa olla opiskeluoikeus-OIDia, jotta testi mittaa nimenomaan täydennystä",
        )

        val response = ykiApiController.getSuorituksetAsCsv(YkiSuorituksetParams())
        val csv =
            ByteArrayOutputStream()
                .also { response.body!!.writeTo(it) }
                .toString(Charsets.UTF_8)

        val rows = csv.trim().lines()
        val oidColumnIndex =
            rows
                .first()
                .split(DisplayTableCsvRenderer.SEPARATOR)
                .indexOf(YkiSuoritusColumn.OpiskeluoikeusOid.uiHeaderValue.toString())
        assertContains(csv, opiskeluoikeusOid.toString())
        assertEquals(
            opiskeluoikeusOid.toString(),
            rows.drop(1).single().split(DisplayTableCsvRenderer.SEPARATOR)[oidColumnIndex],
        )
    }

    @Test
    fun `CSV-vienti suodattaa istuntoon tallennetulla hakusanalla`() {
        suoritusRepository.deleteAll()

        val loydettava = generateRandomYkiSuoritusEntity().copy(sukunimi = "Zebrakalastaja")
        val muu = generateRandomYkiSuoritusEntity().copy(sukunimi = "Muukalainen")
        suoritusRepository.saveAllNewEntities(listOf(loydettava, muu))

        val session = MockHttpSession()
        session.setAttribute(YkiViewController.YKI_SEARCH_KEY, "Zebrakalastaja")

        val response =
            ykiApiController.getSuorituksetAsCsv(
                YkiSuorituksetParams(recallSearch = true),
                session,
            )
        val csv =
            ByteArrayOutputStream()
                .also { response.body!!.writeTo(it) }
                .toString(Charsets.UTF_8)

        assertContains(csv, "Zebrakalastaja")
        assertFalse(csv.contains("Muukalainen"), "CSV ei saa sisältää hakusanaan täsmäämättömiä suorituksia")
    }

    @Test
    fun `CSV-vienti sisältää tarkistusarviointitiedot ja rekisteriintuontiajan`() {
        suoritusRepository.deleteAll()

        val suoritus =
            generateRandomYkiSuoritusEntity().copy(
                receivedAt = Instant.parse("2025-01-15T08:30:00Z"),
                tarkistusarvioinninSaapumisPvm = LocalDate.of(2025, 3, 10),
                tarkistusarvioinninKasittelyPvm = LocalDate.of(2025, 3, 20),
                tarkistusarviointiHyvaksyttyPvm = LocalDate.of(2025, 4, 1),
                tarkistusarvioinninAsiatunnus = "OPH-9999-2025",
                tarkistusarvioidutOsakokeet = setOf(TutkinnonOsa.PU, TutkinnonOsa.KI),
                arvosanaMuuttui = setOf(TutkinnonOsa.PU),
                perustelu = "Testiperustelu",
            )
        suoritusRepository.saveAllNewEntities(listOf(suoritus))

        val response = ykiApiController.getSuorituksetAsCsv(YkiSuorituksetParams())
        val csv =
            ByteArrayOutputStream()
                .also { response.body!!.writeTo(it) }
                .toString(Charsets.UTF_8)

        val rows = csv.trim().lines()
        val header = rows.first().split(DisplayTableCsvRenderer.SEPARATOR)
        val dataRow = rows.drop(1).single().split(DisplayTableCsvRenderer.SEPARATOR)

        fun cell(column: YkiSuoritusColumn) = dataRow[header.indexOf(column.uiHeaderValue.toString())]

        assertEquals("10.3.2025", cell(YkiSuoritusColumn.TarkistusarvioinninSaapumisPvm))
        assertEquals("20.3.2025", cell(YkiSuoritusColumn.TarkistusarvioinninKasittelyPvm))
        assertContains(header, YkiSuoritusColumn.TarkistusarviointiHyvaksyttyPvm.uiHeaderValue.toString())
        assertEquals("OPH-9999-2025", cell(YkiSuoritusColumn.TarkistusarvioinninAsiatunnus))
        assertEquals("Puhuminen, Kirjoittaminen", cell(YkiSuoritusColumn.TarkistusarvioidutOsakokeet))
        assertEquals("Puhuminen", cell(YkiSuoritusColumn.ArvosanaMuuttui))
        assertEquals("Testiperustelu", cell(YkiSuoritusColumn.TarkistusarvioinninPerustelu))
        assertContains(
            header,
            UiText.Yki.Sarake.rekisteriintuontiaika
                .toString(),
        )

        val listViewHeaders =
            DisplayTableColumn
                .of<YkiSuoritusColumn, YkiSuoritusEntity>(setOf(ColumnTag.LIST_VIEW))
                .map { it.label }
        assertFalse(
            listViewHeaders.contains(
                UiText.Yki.Sarake.asiatunnus
                    .toString(),
            ),
            "Tarkistusarviointisarakkeet eivät saa näkyä HTML-listanäkymässä",
        )
        assertContains(
            listViewHeaders,
            UiText.Yki.Sarake.rekisteriintuontiaika
                .toString(),
            "Rekisteriintuontiaika näkyy myös HTML-listanäkymässä",
        )
    }

    @Test
    fun `Oppijanumeron haku hetun ja nimien perusteella onnistuu`() {
        post(
            "/yki/api/oppijanumero-haku",
            """{"hetu": "010180-9026", "etunimet": "Ranja Testi", "sukunimi": "Öhman-Testi"}""",
        ) {
            status { isOk() }
            verboseContentJson(OppijanumeroHakuResponse(Oid.parse("1.2.246.562.24.33342764709").getOrThrow()))
        }
    }

    @Test
    fun `Oppijanumeron haku loytaa oppijan, jonka kutsumanimi ei ole ensimmainen etunimi`() {
        post(
            "/yki/api/oppijanumero-haku",
            """{"hetu": "040265-9985", "etunimet": "Minerva Alli Aniitta", "sukunimi": "Marttila"}""",
        ) {
            status { isOk() }
            verboseContentJson(OppijanumeroHakuResponse(Oid.parse("1.2.246.562.24.92472049678").getOrThrow()))
        }
    }

    @Test
    fun `Oppijanumeron haku palauttaa 404, kun oppijaa ei loydy`() {
        post(
            "/yki/api/oppijanumero-haku",
            """{"hetu": "010101-999X", "etunimet": "Tuntematon", "sukunimi": "Testaaja"}""",
        ) {
            status { isNotFound() }
        }
    }

    @Test
    fun `Oppijanumeron haun 502 kertoo oppijanumerorekisterin oman vastauksen`() {
        post(
            "/yki/api/oppijanumero-haku",
            """{"hetu": "INVALID_HETU", "etunimet": "Ranja Testi", "sukunimi": "Öhman-Testi"}""",
        ) {
            status { isBadGateway() }
            verboseContentJson(
                TiedonsiirtoFailure(
                    HttpStatus.BAD_GATEWAY,
                    listOf("Oppijanumeron haku epäonnistui (BadRequest): oppijanumerorekisteri vastasi HTTP 400"),
                ),
            )
        }
    }

    @Test
    fun `Hetulistahaku palauttaa oppijanumerot ilman nimivertailua`() {
        post(
            "/yki/api/oppijanumero-haku-hetulista",
            """{"hetut": ["010180-9026", "010101-999X"]}""",
        ) {
            status { isOk() }
            verboseContentJson(
                OppijanumeroHetulistaResponse(
                    oppijanumerot = mapOf("010180-9026" to "1.2.246.562.24.33342764709"),
                    puuttuvat = listOf("010101-999X"),
                ),
            )
        }
    }

    @Test
    fun `Hetulistahaku hylkaa tyhjan listan`() {
        post("/yki/api/oppijanumero-haku-hetulista", """{"hetut": []}""") {
            isBadRequest("hetut ei saa olla tyhjä")
        }
    }

    @Test
    fun `Hetulistahaku hylkaa liian suuren listan`() {
        val liikaa = (1..1001).joinToString(",") { """"0101$it"""" }
        post("/yki/api/oppijanumero-haku-hetulista", """{"hetut": [$liikaa]}""") {
            isBadRequest("hetut: enintään 1000 kerralla, sai 1001")
        }
    }

    @Test
    fun `Oppijanumeron haku palauttaa 400, kun pakollinen kentta on tyhja`() {
        post(
            "/yki/api/oppijanumero-haku",
            """{"hetu": "", "etunimet": "Ranja Testi", "sukunimi": "Öhman-Testi"}""",
        ) {
            isBadRequest("hetu, etunimet ja sukunimi ovat pakollisia")
        }
    }

    private fun postSuoritus(
        suoritus: Henkilosuoritus<*>,
        block: MockMvcResultMatchersDsl.() -> Unit,
    ) {
        post("/yki/api/suoritus", defaultObjectMapper.writeValueAsString(suoritus), block)
    }

    private fun postArvioija(
        suoritus: YkiArvioija,
        block: MockMvcResultMatchersDsl.() -> Unit,
    ) {
        post("/yki/api/arvioija", defaultObjectMapper.writeValueAsString(suoritus), block)
    }

    private fun post(
        url: String,
        suoritusJson: String,
        block: MockMvcResultMatchersDsl.() -> Unit,
    ): MockHttpServletResponse =
        mockMvc!!
            .post(url) {
                contentType = MediaType.APPLICATION_JSON
                accept = MediaType.APPLICATION_JSON
                content = suoritusJson
            }.andExpect {
                content { contentType(MediaType.APPLICATION_JSON) }
                block()
            }.andReturn()
            .response
}
