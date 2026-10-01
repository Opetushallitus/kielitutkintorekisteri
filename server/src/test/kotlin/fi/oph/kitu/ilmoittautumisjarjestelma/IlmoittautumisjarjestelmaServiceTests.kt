package fi.oph.kitu.ilmoittautumisjarjestelma

import arrow.core.left
import arrow.core.right
import fi.oph.kitu.DBContainerConfiguration
import fi.oph.kitu.dev.YkiController
import fi.oph.kitu.oid.Oid
import fi.oph.kitu.tiedontuontischema.Henkilo
import fi.oph.kitu.tiedontuontischema.Henkilosuoritus
import fi.oph.kitu.tiedontuontischema.Lahdejarjestelma
import fi.oph.kitu.tiedontuontischema.LahdejarjestelmanTunniste
import fi.oph.kitu.tiedontuontischema.YkiJarjestaja
import fi.oph.kitu.tiedontuontischema.YkiOsa
import fi.oph.kitu.tiedontuontischema.YkiSuoritus
import fi.oph.kitu.tiedontuontischema.YkiTarkastusarviointi
import fi.oph.kitu.util.defaultObjectMapper
import fi.oph.kitu.util.result.getOrThrow
import fi.oph.kitu.util.toJsonNode
import fi.oph.kitu.yki.Arviointitila
import fi.oph.kitu.yki.Sukupuoli
import fi.oph.kitu.yki.TutkinnonOsa
import fi.oph.kitu.yki.Tutkintokieli
import fi.oph.kitu.yki.Tutkintotaso
import fi.oph.kitu.yki.YkiApiController
import fi.oph.kitu.yki.YkiService
import fi.oph.kitu.yki.YkiViewController
import fi.oph.kitu.yki.suoritukset.Todistuskieli
import fi.oph.kitu.yki.suoritukset.YkiSuoritusEntity
import fi.oph.kitu.yki.suoritukset.YkiSuoritusRepository
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.assertDoesNotThrow
import org.junit.jupiter.api.assertNull
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.core.io.ClassPathResource
import org.springframework.http.ResponseEntity
import tools.jackson.databind.JsonNode
import java.time.LocalDate
import kotlin.test.Test
import kotlin.test.assertEquals

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT)
@Import(DBContainerConfiguration::class)
class IlmoittautumisjarjestelmaServiceTests(
    @param:Autowired val ykiApi: YkiApiController,
    @param:Autowired val ykiView: YkiViewController,
    @param:Autowired val suoritukset: YkiSuoritusRepository,
    @param:Autowired val ilmoittautumisjarjestelmaClient: IlmoittautumisjarjestelmaClientMock,
    @param:Autowired val ykiCsvImport: YkiService,
    @param:Autowired val ykiDevController: YkiController,
) {
    @BeforeEach
    fun setup() {
        suoritukset.deleteAll()
        ilmoittautumisjarjestelmaClient.reset()
    }

    @Test
    fun `Tietomalli vastaa odotettua`() {
        val mapper = defaultObjectMapper

        val data =
            YkiArvioinninTilaRequest.of(
                listOf(
                    entity,
                    entity.copy(
                        suorittajanOID = Oid.parse("1.2.246.562.24.10691606000").getOrThrow(),
                        arviointitila = Arviointitila.ARVIOITAVA,
                        tutkintopaiva = LocalDate.of(2022, 1, 1),
                        tutkintokieli = Tutkintokieli.SWE,
                        tutkintotaso = Tutkintotaso.PT,
                    ),
                ),
            )

        val expectedJson =
            ClassPathResource("kios-arviointitila-request-example.json").file.readText().toJsonNode()

        val actualJson = mapper.valueToTree<JsonNode>(data)

        assertEquals(expectedJson, actualJson)
    }

    @Test
    fun `YKI-tietojen lisäys triggeröi arviointitilan lähetyksen ilmoittautumisjärjestelmään`() {
        assertNull(ilmoittautumisjarjestelmaClient.latestRequest())
        ykiApi.postHenkilosuoritus(suoritus)
        assertEquals(ilmoittautumisjarjestelmaClient.latestRequest(), YkiArvioinninTilaRequest.of(entity))
    }

    @Test
    fun `Tarkistusarvioinnin hyväksyminen triggeröi arviointitilan lähetyksen ilmoittautumisjärjestelmään`() {
        ykiApi.postHenkilosuoritus(suoritus)

        assertEquals(
            ilmoittautumisjarjestelmaClient.latestRequest(),
            YkiArvioinninTilaRequest.of(entity.copy(arviointitila = Arviointitila.TARKISTUSARVIOITU)),
        )

        val suoritus = suoritukset.findTarkistusarvoidutSuoritukset(Arviointitila.TARKISTUSARVIOITU).first()
        ykiView.hyvaksyTarkistusArvioinnit(listOf(suoritus.solkiId))

        assertEquals(
            ilmoittautumisjarjestelmaClient.latestRequest(),
            YkiArvioinninTilaRequest.of(entity.copy(arviointitila = Arviointitila.TARKISTUSARVIOINTI_HYVAKSYTTY)),
        )
    }

    @Test
    fun `Epäonnistunut kutsu #1 ei aiheuta poikkeusta rajapinnassa, mutta poikkeus tallennetaan virhetauluun`() {
        ilmoittautumisjarjestelmaClient.response =
            IlmoittautumisjarjestelmaResponse(
                hyvaksytyt = 1,
                virheet =
                    listOf(
                        IlmoittautumisjarjestelmaResponseError(
                            suoritus = YkiSuorituksenTunniste.of(entity)!!,
                            tila = entity.arviointitila,
                            virhe = "SUORITUSTA_EI_LOYDY",
                        ),
                    ),
            ).right()

        assertDoesNotThrow {
            ykiApi.postHenkilosuoritus(suoritus)
        }

        val savedSuoritus = ykiSuoritusRepository.findLatestBySolkiIds(listOf(entity.solkiId)).first()
        assertEquals("SUORITUSTA_EI_LOYDY", savedSuoritus.arviointitilanLahetysvirhe)
    }

    @Test
    fun `Epäonnistunut kutsu #2 ei aiheuta poikkeusta rajapinnassa, mutta poikkeus tallennetaan virhetauluun`() {
        ilmoittautumisjarjestelmaClient.response =
            IlmoittautumisjarjestelmaException
                .UnexpectedError(
                    request = YkiArvioinninTilaRequest.of(entity)!!,
                    response = ResponseEntity.notFound().build(),
                ).left()

        assertDoesNotThrow {
            ykiApi.postHenkilosuoritus(suoritus)
        }

        val savedSuoritus = ykiSuoritusRepository.findLatestBySolkiIds(listOf(entity.solkiId)).first()
        assertEquals(
            """Unexpected error; request: {
  "tilat" : [ {
    "suoritus" : {
      "oppijanumero" : "1.2.246.562.24.10691606777",
      "tutkintopaiva" : "2020-01-01",
      "tutkintokieli" : "fin",
      "tutkintotaso" : "KT",
      "osakokeet" : [ "PU", "KI", "TY", "PY" ]
    },
    "tila" : "TARKISTUSARVIOITU"
  } ]
}; response status: 404 NOT_FOUND""",
            savedSuoritus.arviointitilanLahetysvirhe,
        )
    }

    @Autowired
    private lateinit var ykiSuoritusRepository: YkiSuoritusRepository
    val suoritus =
        Henkilosuoritus(
            henkilo =
                Henkilo(
                    oid = Oid.parse("1.2.246.562.24.10691606777").getOrThrow(),
                    etunimet = "Eeli Heikki",
                    sukunimi = "Aalto",
                    hetu = "010180-9026",
                    sukupuoli = Sukupuoli.N,
                    kansalaisuus = "EST",
                    katuosoite = "Testikuja 1",
                    postinumero = "12345",
                    postitoimipaikka = "Testilä",
                    email = "eeli@email.com",
                ),
            suoritus =
                YkiSuoritus(
                    tutkintotaso = Tutkintotaso.KT,
                    kieli = Tutkintokieli.FIN,
                    todistuskieli = Todistuskieli.FIN,
                    jarjestaja =
                        YkiJarjestaja(
                            oid = Oid.parse("1.2.246.562.10.14893989377").getOrThrow(),
                            nimi = "Soveltavan kielentutkimuksen keskus",
                        ),
                    tutkintopaiva = LocalDate.of(2020, 1, 1),
                    arviointipaiva = LocalDate.of(2020, 1, 1),
                    arviointitila = Arviointitila.TARKISTUSARVIOITU,
                    osat =
                        listOf(
                            YkiOsa(
                                tyyppi = TutkinnonOsa.puheenYmmartaminen,
                                arvosana = 3,
                            ),
                            YkiOsa(
                                tyyppi = TutkinnonOsa.puhuminen,
                                arvosana = 3,
                            ),
                            YkiOsa(
                                tyyppi = TutkinnonOsa.kirjoittaminen,
                                arvosana = 3,
                            ),
                            YkiOsa(
                                tyyppi = TutkinnonOsa.tekstinYmmartaminen,
                                arvosana = 3,
                            ),
                        ),
                    tarkistusarviointi =
                        YkiTarkastusarviointi(
                            saapumispaiva = LocalDate.of(2020, 2, 1),
                            kasittelypaiva = LocalDate.of(2020, 2, 1),
                            asiatunnus = "OPH-12345",
                            tarkistusarvioidutOsakokeet = listOf(TutkinnonOsa.puheenYmmartaminen),
                            arvosanaMuuttui = emptyList(),
                            perustelu = "Lorem ipsum dolor sit amet, consectetur adipiscing elit.",
                        ),
                    lahdejarjestelmanId =
                        LahdejarjestelmanTunniste(
                            id = "666",
                            lahde = Lahdejarjestelma.Solki,
                        ),
                    internalId = null,
                    koskiOpiskeluoikeusOid = null,
                    koskiSiirtoKasitelty = false,
                ),
        )

    @Test
    fun `Vilppi-arvosanan sisältävää suoritusta ei lähetetä ilmoittautumisjärjestelmään`() {
        val vilppiSuoritus =
            suoritus.copy(
                suoritus =
                    suoritus.suoritus.copy(
                        osat =
                            listOf(
                                YkiOsa(tyyppi = TutkinnonOsa.puheenYmmartaminen, arvosana = 3),
                                YkiOsa(tyyppi = TutkinnonOsa.puhuminen, arvosana = 11),
                                YkiOsa(tyyppi = TutkinnonOsa.kirjoittaminen, arvosana = 3),
                                YkiOsa(tyyppi = TutkinnonOsa.tekstinYmmartaminen, arvosana = 3),
                            ),
                    ),
            )

        ykiApi.postHenkilosuoritus(vilppiSuoritus)

        assertNull(ilmoittautumisjarjestelmaClient.latestRequest())
    }

    @Test
    fun `YkiSuorituksenTunniste equality`() {
        val tunniste1 = YkiSuorituksenTunniste.of(entity)!!
        val tunniste2 = tunniste1.copy(osakokeet = tunniste1.osakokeet.reversed())

        // Perus samanarvoisuuden testaus
        assertEquals(tunniste1, tunniste2)
        assertEquals(tunniste1.hashCode(), tunniste2.hashCode())

        // Kun käytetään mapin avaimena
        val map = mapOf(tunniste1 to "wahoo")
        assertEquals(map[tunniste2], "wahoo")
    }

    val entity = YkiSuoritusEntity.from(suoritus)
}
