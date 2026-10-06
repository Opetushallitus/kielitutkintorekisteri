package fi.oph.kitu.kotoutumiskoulutus

import fi.oph.kitu.DBContainerConfiguration
import fi.oph.kitu.TestTimeService
import fi.oph.kitu.kotoutumiskoulutus.koealusta.KoealustaService
import fi.oph.kitu.kotoutumiskoulutus.suoritukset.CustomKielitestiSuoritusRepository
import fi.oph.kitu.kotoutumiskoulutus.suoritukset.KielitestiSuoritusRepository
import fi.oph.kitu.kotoutumiskoulutus.suoritukset.KielitestiSuoritusService
import fi.oph.kitu.kotoutumiskoulutus.suoritukset.Testikieli
import fi.oph.kitu.kotoutumiskoulutus.suoritukset.error.KielitestiSuoritusErrorRepository
import fi.oph.kitu.oid.Oid
import fi.oph.kitu.oppijanumero.EI_LOYTYNYT
import fi.oph.kitu.oppijanumero.NIMIYHDISTELMA_LOYTYI
import fi.oph.kitu.util.result.getOrThrow
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.assertAll
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.http.MediaType
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.web.client.ExpectedCount
import org.springframework.test.web.client.MockRestServiceServer
import org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo
import org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess
import org.testcontainers.postgresql.PostgreSQLContainer
import java.time.Instant
import kotlin.test.Test
import kotlin.test.assertEquals

@SpringBootTest(
    properties = [
        "kitu.kotoutumiskoulutus.koealusta.wstoken=token",
        "kitu.kotoutumiskoulutus.koealusta.baseurl=https://localhost:8080/dev/koto",
    ],
)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Import(DBContainerConfiguration::class)
class KoealustaServiceTests(
    @param:Autowired private val postgres: PostgreSQLContainer,
    @param:Autowired private val koealustaService: KoealustaService,
) {
    private val mockServer = MockRestServiceServer.bindTo(koealustaService.restClientBuilder).build()
    private val oletusEnrolmentWindow = koealustaService.keskeneraisetEnrolmentWindow

    val validSuoritus =
        """
        {
              "userid": 1,
              "firstnames": "Ranja Testi",
              "lastname": "Öhman-Testi",
              "preferredname": "Ranja",
              "SSN": "010180-9026",
              "email": "ranja.testi@oph.fi",
              "completions": [
                {
                  "courseid": 32,
                  "coursename": "Integraatio testaus",
                  "lang": "fi",
                  "schoolOID": "1.2.246.562.10.42923230015",
                  "results": [
                    {
                      "name": "luetun ymm\u00e4rt\u00e4minen",
                      "quiz_grade": "Alle A1"
                    },
                    {
                      "name": "kuullun ymm\u00e4rt\u00e4minen",
                      "quiz_grade": "B1"
                    },
                    {
                      "name": "puhuminen",
                      "quiz_grade": "A2"
                    },
                    {
                      "name": "kirjoittaminen",
                      "quiz_grade": "Yli B1"
                    }
                  ],
                  "timecompleted": 1728969131,
                  "teacheremail": "opettaja@testi.oph.fi",
                  "questionbank_release": "fi_tehtäväpaketti_suomi"
                }
              ]
        }
        """.trimIndent()

    val invalidHetu =
        """
        {
          "userid": 2,
          "firstnames": "Antero",
          "lastname": "Testi-Moikka",
          "preferredname": "Antero",
          "SSN": "INVALID_HETU",
          "email": "ranja.testi@oph.fi",
          "completions": [
            {
              "courseid": 32,
              "coursename": "Integraatio testaus",
              "lang": "fi",
              "schoolOID": "1.2.246.562.10.42923230015",
              "results": [
                {
                  "name": "luetun ymm\u00e4rt\u00e4minen",
                  "quiz_grade": "A1"
                },
                {
                  "name": "kuullun ymm\u00e4rt\u00e4minen",
                  "quiz_grade": "B1"
                },
                {
                  "name": "puhuminen",
                  "quiz_grade": "B1"
                },
                {
                  "name": "kirjoittaminen",
                  "quiz_grade": "B1"
                }
              ],
              "timecompleted": 1728969131,
              "teacheremail": "opettaja@testi.oph.fi"
            }
          ]
        }
        """.trimIndent()

    @BeforeEach
    fun nukeDb(
        @Autowired kielitestiSuoritusRepository: KielitestiSuoritusRepository,
        @Autowired kielitestiSuoritusErrorRepository: KielitestiSuoritusErrorRepository,
    ) {
        kielitestiSuoritusRepository.deleteAll()
        kielitestiSuoritusErrorRepository.deleteAll()
        mockServer.reset()
        koealustaService.keskeneraisetEnrolmentWindow = oletusEnrolmentWindow
    }

    @Test
    fun `import with no errors`(
        @Autowired customKielitestiSuoritusRepository: CustomKielitestiSuoritusRepository,
        @Autowired kielitestiSuoritusErrorRepository: KielitestiSuoritusErrorRepository,
        @Autowired koealustaService: KoealustaService,
    ) {
        // Facade
        mockServer
            .expect(
                requestTo(
                    "https://localhost:8080/dev/koto/webservice/rest/server.php?wstoken=token&wsfunction=local_completion_export_get_completions&moodlewsrestformat=json&from=0",
                ),
            ).andRespond(
                withSuccess(
                    """
                    {
                      "users": [$validSuoritus]
                    }
                    """.trimIndent(),
                    MediaType.APPLICATION_JSON,
                ),
            )

        // System under test

        // Test
        val lastSeen = koealustaService.importValmiitSuoritukset(Instant.EPOCH)

        // Verification
        mockServer.verify()

        assertAll(
            fun () = assertEquals(emptyList(), kielitestiSuoritusErrorRepository.findAll()),
            fun () = assertEquals(Instant.parse("2024-10-15T05:12:11Z"), lastSeen),
        )

        val ranja = customKielitestiSuoritusRepository.findAll().single()
        assertAll(
            fun () = assertEquals("Integraatio testaus", ranja.kurssi),
            fun () = assertEquals(Oid.parse("1.2.246.562.10.42923230015").getOrThrow(), ranja.oppilaitosOid),
            fun () = assertEquals(Oid.parse("1.2.246.562.24.33342764709").getOrThrow(), ranja.oppijanumero),
            fun () = assertEquals("fi_tehtäväpaketti_suomi", ranja.tehtavapaketti),
            fun () = assertEquals(Testikieli.FIN, ranja.testikieli),
        )
    }

    @Test
    fun `import with hetu name mismatch`(
        @Autowired kielitestiSuoritusErrorRepository: KielitestiSuoritusErrorRepository,
        @Autowired koealustaService: KoealustaService,
    ) {
        // Facade
        mockServer
            .expect(
                requestTo(
                    "https://localhost:8080/dev/koto/webservice/rest/server.php?wstoken=token&wsfunction=local_completion_export_get_completions&moodlewsrestformat=json&from=0",
                ),
            ).andRespond(
                withSuccess(
                    """
                    {
                      "users": [
                        {
                          "userid": 1,
                          "firstnames": "Antero",
                          "lastname": "Testi-Moikka",
                          "preferredname": "Antero", 
                          "SSN": "WRONG_HETU",
                          "email": "ranja.testi@oph.fi",
                          "completions": [
                            {
                              "courseid": 32,
                              "coursename": "Integraatio testaus",
                              "lang": "fi",
                              "schoolOID": "1.2.246.562.10.42923230015",
                              "results": [
                                {
                                  "name": "luetun ymm\u00e4rt\u00e4minen",
                                  "quiz_grade": "A1"
                                },
                                {
                                  "name": "kuullun ymm\u00e4rt\u00e4minen",
                                  "quiz_grade": "B1"
                                },
                                {
                                  "name": "puhuminen",
                                  "quiz_grade": "B1"
                                },
                                {
                                  "name": "kirjoittaminen",
                                  "quiz_grade": "B1"
                                }
                              ],
                              "timecompleted": 1728969131,
                              "teacheremail": "opettaja@testi.oph.fi"
                            }
                          ]
                        }
                      ]
                    }
                    """.trimIndent(),
                    MediaType.APPLICATION_JSON,
                ),
            )

        // Test
        val lastSeen = koealustaService.importValmiitSuoritukset(Instant.EPOCH)

        // Verification
        mockServer.verify()

        assertEquals(
            expected = Instant.parse("1970-01-01T00:00:00Z"),
            actual = lastSeen,
            message = "since an error was encountered, use the previous `from` parameter as the last seen date",
        )

        val errors = kielitestiSuoritusErrorRepository.findAll().toList()

        assertEquals(1, errors.size)

        val oppijaValidationFailure = errors.first()

        assertAll(
            fun() = assertEquals("WRONG_HETU", oppijaValidationFailure.hetu),
            fun() =
                assertEquals(
                    "Oppijanumeron haku epäonnistui: Oppija YleistunnisteHaeRequest(etunimet=Antero, hetu=WRONG_HETU, kutsumanimi=Antero, sukunimi=Testi-Moikka) is not identified in oppijanumero service",
                    oppijaValidationFailure.viesti,
                ),
        )
    }

    @Test
    fun `import with sukunimi and etunimi swapped adds correct name to onrLisatietoja`(
        @Autowired kielitestiSuoritusErrorRepository: KielitestiSuoritusErrorRepository,
        @Autowired kielitestiSuoritusService: KielitestiSuoritusService,
        @Autowired koealustaService: KoealustaService,
    ) {
        // Facade
        mockServer
            .expect(
                requestTo(
                    "https://localhost:8080/dev/koto/webservice/rest/server.php?wstoken=token&wsfunction=local_completion_export_get_completions&moodlewsrestformat=json&from=0",
                ),
            ).andRespond(
                withSuccess(
                    """
                    {
                      "users": [
                        {
                          "userid": 1,
                          "firstnames": "Öhman-Testi",
                          "lastname": "Ranja Testi",
                          "preferredname": "Testi", 
                          "SSN": "010180-9026",
                          "email": "ranja.testi@oph.fi",
                          "completions": [
                            {
                              "courseid": 32,
                              "coursename": "Integraatio testaus",
                              "lang": "fi",
                              "schoolOID": "1.2.246.562.10.42923230015",
                              "results": [
                                {
                                  "name": "luetun ymm\u00e4rt\u00e4minen",
                                  "quiz_grade": "A1"
                                },
                                {
                                  "name": "kuullun ymm\u00e4rt\u00e4minen",
                                  "quiz_grade": "B1"
                                },
                                {
                                  "name": "puhuminen",
                                  "quiz_grade": "A1"
                                },
                                {
                                  "name": "kirjoittaminen",
                                  "quiz_grade": "A1"
                                }
                              ],
                              "timecompleted": 1728969131,
                              "teacheremail": "opettaja@testi.oph.fi"
                            }
                          ]
                        }
                      ]
                    }
                    """.trimIndent(),
                    MediaType.APPLICATION_JSON,
                ),
            )

        // Test
        val lastSeen = koealustaService.importValmiitSuoritukset(Instant.EPOCH)

        // Verification
        mockServer.verify()

        assertEquals(
            expected = Instant.parse("1970-01-01T00:00:00Z"),
            actual = lastSeen,
            message = "since an error was encountered, use the previous `from` parameter as the last seen date",
        )

        val errors = kielitestiSuoritusErrorRepository.findAll().toList()

        assertEquals(1, errors.size)

        val oppijaValidationFailure = errors.first()

        assertAll(
            fun() =
                assertEquals(
                    "Öhman-Testi",
                    oppijaValidationFailure.etunimet,
                ),
            fun() =
                assertEquals(
                    "Testi",
                    oppijaValidationFailure.kutsumanimi,
                ),
            fun() =
                assertEquals(
                    "Ranja Testi",
                    oppijaValidationFailure.sukunimi,
                ),
            fun() =
                assertEquals(
                    NIMIYHDISTELMA_LOYTYI,
                    oppijaValidationFailure.onrLisatietoja,
                ),
            fun() =
                assertEquals(
                    listOf("Ranja Testi", "Ranja", "Öhman-Testi"),
                    listOf(
                        oppijaValidationFailure.onrEtunimet,
                        oppijaValidationFailure.onrKutsumanimi,
                        oppijaValidationFailure.onrSukunimi,
                    ),
                ),
            fun() =
                assertEquals(
                    "Oppijanumeron haku epäonnistui: Henkilöä ei löydy Oppijanumerorekisteristä",
                    oppijaValidationFailure.viesti,
                ),
        )
    }

    @Test
    fun `import with typo in name adds info to onrLisatietoja`(
        @Autowired kielitestiSuoritusErrorRepository: KielitestiSuoritusErrorRepository,
        @Autowired koealustaService: KoealustaService,
    ) {
        // Facade
        mockServer
            .expect(
                requestTo(
                    "https://localhost:8080/dev/koto/webservice/rest/server.php?wstoken=token&wsfunction=local_completion_export_get_completions&moodlewsrestformat=json&from=0",
                ),
            ).andRespond(
                withSuccess(
                    """
                    {
                      "users": [
                        {
                          "userid": 1,
                          "firstnames": "Vanja Testi",
                          "lastname": "Öhman-Testi",
                          "preferredname": "Testi", 
                          "SSN": "010180-9026",
                          "email": "ranja.testi@oph.fi",
                          "completions": [
                            {
                              "courseid": 32,
                              "coursename": "Integraatio testaus",
                              "lang": "fi",
                              "schoolOID": "1.2.246.562.10.42923230015",
                              "results": [
                                {
                                  "name": "luetun ymm\u00e4rt\u00e4minen",
                                  "quiz_grade": "A1"
                                },
                                {
                                  "name": "kuullun ymm\u00e4rt\u00e4minen",
                                  "quiz_grade": "B1"
                                },
                                {
                                  "name": "puhuminen",
                                  "quiz_grade": "A1"
                                },
                                {
                                  "name": "kirjoittaminen",
                                  "quiz_grade": "A1"
                                }
                              ],
                              "timecompleted": 1728969131,
                              "teacheremail": "opettaja@testi.oph.fi"
                            }
                          ]
                        }
                      ]
                    }
                    """.trimIndent(),
                    MediaType.APPLICATION_JSON,
                ),
            )

        // Test
        val lastSeen = koealustaService.importValmiitSuoritukset(Instant.EPOCH)

        // Verification
        mockServer.verify()

        assertEquals(
            expected = Instant.parse("1970-01-01T00:00:00Z"),
            actual = lastSeen,
            message = "since an error was encountered, use the previous `from` parameter as the last seen date",
        )

        val errors = kielitestiSuoritusErrorRepository.findAll().toList()

        assertEquals(1, errors.size)

        val oppijaValidationFailure = errors.first()

        assertAll(
            fun() =
                assertEquals(
                    "Öhman-Testi",
                    oppijaValidationFailure.sukunimi,
                ),
            fun() =
                assertEquals(
                    "Testi",
                    oppijaValidationFailure.kutsumanimi,
                ),
            fun() =
                assertEquals(
                    "Vanja Testi",
                    oppijaValidationFailure.etunimet,
                ),
            fun() =
                assertEquals(
                    EI_LOYTYNYT,
                    oppijaValidationFailure.onrLisatietoja,
                ),
            fun() =
                assertEquals(
                    "Oppijanumeron haku epäonnistui: Henkilöä ei löydy Oppijanumerorekisteristä",
                    oppijaValidationFailure.viesti,
                ),
        )
    }

    @Test
    fun `import with suoritus validation error`(
        @Autowired kielitestiSuoritusErrorRepository: KielitestiSuoritusErrorRepository,
        @Autowired koealustaService: KoealustaService,
    ) {
        // Facade
        mockServer
            .expect(
                requestTo(
                    "https://localhost:8080/dev/koto/webservice/rest/server.php?wstoken=token&wsfunction=local_completion_export_get_completions&moodlewsrestformat=json&from=0",
                ),
            ).andRespond(
                withSuccess(
                    """
                    {
                      "users": [
                        {
                          "userid": 1,
                          "firstnames": "Ranja Testi",
                          "lastname": "Öhman-Testi",
                          "preferredname": "Ranja",
                          "SSN": "010180-9026",
                          "email": "ranja.testi@oph.fi",
                          "completions": [
                            {
                              "courseid": 32,
                              "coursename": "Integraatio testaus",
                              "lang": "fi",
                              "schoolOID": "1.2.246.562.10.42923230015",
                              "results": [
                                {
                                  "name": "luetun ymm\u00e4rt\u00e4minen",
                                  "quiz_grade": "A1"
                                },
                                {
                                  "name": "kuullun ymm\u00e4rt\u00e4minen",
                                  "quiz_grade": "B1"
                                },
                                {
                                  "name": "kirjoittaminen",
                                  "quiz_grade": "B1"
                                }
                              ],
                              "timecompleted": 1728969131,
                              "teacheremail": "opettaja@testi.oph.fi"
                            }
                          ]
                        }
                      ]
                    }
                    """.trimIndent(),
                    MediaType.APPLICATION_JSON,
                ),
            )

        // Test
        val lastSeen = koealustaService.importValmiitSuoritukset(Instant.EPOCH)

        // Verification
        mockServer.verify()

        val errors = kielitestiSuoritusErrorRepository.findAll().toList()

        assertEquals(1, errors.size)

        val missingPuhuminenError = errors[0]
        assertAll(
            fun() = assertEquals("Öhman-Testi Ranja Testi", missingPuhuminenError.nimi),
            fun() = assertEquals("010180-9026", missingPuhuminenError.hetu),
            fun() = assertEquals("1.2.246.562.24.33342764709", missingPuhuminenError.suorittajanOid),
            fun() = assertEquals("1.2.246.562.10.42923230015", missingPuhuminenError.schoolOid.toString()),
            fun() = assertEquals("opettaja@testi.oph.fi", missingPuhuminenError.teacherEmail),
            fun() = assertEquals("puhuminen", missingPuhuminenError.virheellinenKentta),
            fun() =
                assertEquals(
                    "Puuttuva arvosana \"puhuminen\" kurssilla \"Integraatio testaus\" käyttäjälle \"1\"",
                    missingPuhuminenError.viesti,
                ),
        )
    }

    @Test
    fun `import with oppija validation error`(
        @Autowired kielitestiSuoritusErrorRepository: KielitestiSuoritusErrorRepository,
        @Autowired koealustaService: KoealustaService,
    ) {
        // Facade
        mockServer
            .expect(
                requestTo(
                    "https://localhost:8080/dev/koto/webservice/rest/server.php?wstoken=token&wsfunction=local_completion_export_get_completions&moodlewsrestformat=json&from=0",
                ),
            ).andRespond(
                withSuccess(
                    """
                    {
                      "users": [
                        {
                          "userid": 1,
                          "firstnames": "Antero",
                          "lastname": "Testi-Moikka",
                          "preferredname": "Antero",
                          "SSN": "",
                          "email": "ranja.testi@oph.fi",
                          "completions": [
                            {
                              "courseid": 32,
                              "coursename": "Integraatio testaus",
                              "lang": "fi",
                              "schoolOID": "1.2.246.562.10.42923230015",
                              "results": [
                                {
                                  "name": "luetun ymm\u00e4rt\u00e4minen",
                                  "quiz_grade": "A1"
                                },
                                {
                                  "name": "kuullun ymm\u00e4rt\u00e4minen",
                                  "quiz_grade": "B1"
                                },
                                {
                                  "name": "puhuminen",
                                  "quiz_grade": "B1"
                                },
                                {
                                  "name": "kirjoittaminen",
                                  "quiz_grade": "B1"
                                }
                              ],
                              "timecompleted": 1728969131,
                              "teacheremail": "opettaja@testi.oph.fi"
                            }
                          ]
                        }
                      ]
                    }
                    """.trimIndent(),
                    MediaType.APPLICATION_JSON,
                ),
            )

        // Test
        val lastSeen = koealustaService.importValmiitSuoritukset(Instant.EPOCH)

        // Verification
        mockServer.verify()

        val errors = kielitestiSuoritusErrorRepository.findAll().toList()
        assertEquals(1, errors.size)
        val oppijaValidationFailure = errors.first()

        assertAll(
            fun() = assertEquals("1.2.246.562.10.42923230015", oppijaValidationFailure.schoolOid.toString()),
            fun() = assertEquals("opettaja@testi.oph.fi", oppijaValidationFailure.teacherEmail),
            fun() = assertEquals("Puuttuva kenttä \"SSN\" käyttäjälle \"1\"", oppijaValidationFailure.viesti),
            fun() = assertEquals("SSN", oppijaValidationFailure.virheellinenKentta),
        )
    }

    @Test
    fun `import with person information mismatch`(
        @Autowired kielitestiSuoritusErrorRepository: KielitestiSuoritusErrorRepository,
        @Autowired koealustaService: KoealustaService,
    ) {
        // Facade
        mockServer
            .expect(
                requestTo(
                    "https://localhost:8080/dev/koto/webservice/rest/server.php?wstoken=token&wsfunction=local_completion_export_get_completions&moodlewsrestformat=json&from=0",
                ),
            ).andRespond(
                withSuccess(
                    """
                    {
                      "users": [$invalidHetu]
                    }
                    """.trimIndent(),
                    MediaType.APPLICATION_JSON,
                ),
            )

        // Test
        val lastSeen = koealustaService.importValmiitSuoritukset(Instant.EPOCH)

        // Verification
        mockServer.verify()

        assertEquals(
            expected = Instant.parse("1970-01-01T00:00:00Z"),
            actual = lastSeen,
            message = "since an error was encountered, use the previous `from` parameter as the last seen date",
        )

        val errors = kielitestiSuoritusErrorRepository.findAll().toList()
        assertEquals(1, errors.size)
        val onrBadRequestFailure = errors.first()

        assertAll(
            fun() = assertEquals("INVALID_HETU", onrBadRequestFailure.hetu),
            fun() =
                assertEquals(
                    "Oppijanumeron haku epäonnistui: Bad request to oppijanumero-service",
                    onrBadRequestFailure.viesti,
                ),
            fun() =
                assertEquals(
                    EI_LOYTYNYT,
                    onrBadRequestFailure.onrLisatietoja,
                ),
            fun() = assertEquals("Testi-Moikka Antero", onrBadRequestFailure.nimi),
            fun() = assertEquals("1.2.246.562.10.42923230015", onrBadRequestFailure.schoolOid.toString()),
            fun() = assertEquals("opettaja@testi.oph.fi", onrBadRequestFailure.teacherEmail),
        )
    }

    @Test
    fun `import with valid and invalid suoritus should return original from-timestamp and save the suoritus and error`(
        @Autowired kielitestiSuoritusErrorRepository: KielitestiSuoritusErrorRepository,
        @Autowired customKielitestiSuoritusRepository: CustomKielitestiSuoritusRepository,
        @Autowired koealustaService: KoealustaService,
    ) {
        // Facade
        mockServer
            .expect(
                requestTo(
                    "https://localhost:8080/dev/koto/webservice/rest/server.php?wstoken=token&wsfunction=local_completion_export_get_completions&moodlewsrestformat=json&from=0",
                ),
            ).andRespond(
                withSuccess(
                    """
                    {
                      "users": [$validSuoritus, $invalidHetu]
                    }
                    """.trimIndent(),
                    MediaType.APPLICATION_JSON,
                ),
            )

        // Test
        val lastSeen = koealustaService.importValmiitSuoritukset(Instant.EPOCH)

        // Verification
        mockServer.verify()

        val errors = kielitestiSuoritusErrorRepository.findAll()
        val suoritukset = customKielitestiSuoritusRepository.findAll()

        assertAll(
            fun() =
                assertEquals(
                    expected = Instant.EPOCH,
                    actual = lastSeen,
                    message = "Since an error was encountered, the previous `from` parameter should have been returned",
                ),
            fun () = assertEquals(1, errors.count(), "There should be one saved error"),
            fun () = assertEquals(1, suoritukset.count(), "There should be one saved suoritus"),
        )
    }

    @Test
    fun `duplicate suoritus in subsequent imports are not saved`(
        @Autowired kielitestiSuoritusErrorRepository: KielitestiSuoritusErrorRepository,
        @Autowired customKielitestiSuoritusRepository: CustomKielitestiSuoritusRepository,
        @Autowired koealustaService: KoealustaService,
    ) {
        // Facade
        mockServer
            .expect(
                ExpectedCount.manyTimes(),
                requestTo(
                    "https://localhost:8080/dev/koto/webservice/rest/server.php?wstoken=token&wsfunction=local_completion_export_get_completions&moodlewsrestformat=json&from=0",
                ),
            ).andRespond(
                withSuccess(
                    """
                    {
                      "users": [$validSuoritus, $invalidHetu]
                    }
                    """.trimIndent(),
                    MediaType.APPLICATION_JSON,
                ),
            )

        // Test
        val lastSeen = koealustaService.importValmiitSuoritukset(Instant.EPOCH)

        val errors = kielitestiSuoritusErrorRepository.findAll()
        val suoritukset = customKielitestiSuoritusRepository.findAll()

        assertAll(
            fun() =
                assertEquals(
                    expected = Instant.EPOCH,
                    actual = lastSeen,
                    message = "Since an error was encountered, the previous `from` parameter should have been returned",
                ),
            fun () = assertEquals(1, errors.count(), "There should be one saved error"),
            fun () = assertEquals(1, suoritukset.count(), "There should be one saved suoritus"),
        )

        koealustaService.importValmiitSuoritukset(from = lastSeen)
        mockServer.verify()

        val suoritukset2 = customKielitestiSuoritusRepository.findAll()
        val errors2 = kielitestiSuoritusErrorRepository.findAll()
        assertAll(
            fun() = assertEquals(1, suoritukset2.count()),
            fun() = assertEquals(1, errors2.count()),
        )
    }

    @Test
    fun `import removes leading and trailing whitespace from names and ssn`(
        @Autowired customKielitestiSuoritusRepository: CustomKielitestiSuoritusRepository,
        @Autowired kielitestiSuoritusErrorRepository: KielitestiSuoritusErrorRepository,
        @Autowired koealustaService: KoealustaService,
    ) {
        // Facade
        val validSuoritus =
            """
            {
                  "userid": 1,
                  "firstnames": " Ranja Testi ",
                  "lastname": " Öhman-Testi ",
                  "preferredname": " Ranja ", 
                  "SSN": " 010180-9026 ",
                  "email": "ranja.testi@oph.fi",
                  "completions": [
                    {
                      "courseid": 32,
                      "coursename": "Integraatio testaus",
                      "lang": "fi",
                      "schoolOID": "1.2.246.562.10.42923230015",
                      "results": [
                        {
                          "name": "luetun ymm\u00e4rt\u00e4minen",
                          "quiz_grade": "A1"
                        },
                        {
                          "name": "kuullun ymm\u00e4rt\u00e4minen",
                          "quiz_grade": "B1"
                        },
                        {
                          "name": "puhuminen",
                          "quiz_grade": "A1"
                        },
                        {
                          "name": "kirjoittaminen",
                          "quiz_grade": "A1"
                        }
                      ],
                      "timecompleted": 1728969131,
                      "teacheremail": "opettaja@testi.oph.fi"
                    }
                  ]
            }
            """.trimIndent()

        mockServer
            .expect(
                requestTo(
                    "https://localhost:8080/dev/koto/webservice/rest/server.php?wstoken=token&wsfunction=local_completion_export_get_completions&moodlewsrestformat=json&from=0",
                ),
            ).andRespond(
                withSuccess(
                    """
                    {
                      "users": [$validSuoritus]
                    }
                    """.trimIndent(),
                    MediaType.APPLICATION_JSON,
                ),
            )

        // System under test

        // Test
        val lastSeen = koealustaService.importValmiitSuoritukset(Instant.EPOCH)

        // Verification
        mockServer.verify()

        assertAll(
            fun () = assertEquals(emptyList(), kielitestiSuoritusErrorRepository.findAll()),
            fun () = assertEquals(Instant.parse("2024-10-15T05:12:11Z"), lastSeen),
        )

        val ranja = customKielitestiSuoritusRepository.findAll().single()
        assertAll(
            fun () = assertEquals("Ranja Testi", ranja.etunimet),
            fun () = assertEquals("Öhman-Testi", ranja.sukunimi),
            fun () = assertEquals("Ranja", ranja.kutsumanimi),
            fun () = assertEquals(Oid.parse("1.2.246.562.24.33342764709").getOrThrow(), ranja.oppijanumero),
            fun () = assertEquals(0, kielitestiSuoritusErrorRepository.findAll().count()),
        )
    }

    @Test
    fun `import with incorrect or missing arvosana fails`(
        @Autowired kielitestiSuoritusErrorRepository: KielitestiSuoritusErrorRepository,
        @Autowired koealustaService: KoealustaService,
    ) {
        // Facade
        val suoritusJson =
            """
            {
                  "userid": 1,
                  "firstnames": "Ranja Testi",
                  "lastname": "Öhman-Testi",
                  "preferredname": "Ranja",
                  "SSN": "010180-9026",
                  "email": "ranja.testi@oph.fi",
                  "completions": [
                    {
                      "courseid": 123,
                      "coursename": "Integraatio testaus",
                      "lang": "fi",
                      "schoolOID": "1.2.246.562.10.42923230015",
                      "results": [
                        {
                          "name": "luetun ymm\u00e4rt\u00e4minen",
                          "quiz_grade": "B2"
                        },
                        {
                          "name": "kuullun ymm\u00e4rt\u00e4minen",
                          "quiz_grade": "B2"
                        },
                        {
                          "name": "kirjoittaminen",
                          "quiz_grade": null
                        }
                      ],
                      "timecompleted": 1728969131,
                      "teacheremail": "opettaja@testi.oph.fi"
                    }
                  ]
            }
            """.trimIndent()
        mockServer
            .expect(
                requestTo(
                    "https://localhost:8080/dev/koto/webservice/rest/server.php?wstoken=token&wsfunction=local_completion_export_get_completions&moodlewsrestformat=json&from=0",
                ),
            ).andRespond(
                withSuccess(
                    """
                    {
                      "users": [$suoritusJson]
                    }
                    """.trimIndent(),
                    MediaType.APPLICATION_JSON,
                ),
            )

        // System under test

        // Test
        koealustaService.importValmiitSuoritukset(Instant.EPOCH)

        // Verification
        mockServer.verify()

        val errors = kielitestiSuoritusErrorRepository.findAll().toList()
        val luetunYmmartaminenError = errors.find { it.virheellinenKentta == "luetun ymmärtäminen" }
        val kuullunYmmartaminenError = errors.find { it.virheellinenKentta == "kuullun ymmärtäminen" }
        val puheError = errors.find { it.virheellinenKentta == "puhuminen" }
        val kirjoittaminenError = errors.find { it.virheellinenKentta == "kirjoittaminen" }

        assertAll(
            fun() = assertEquals(4, errors.size),
            fun() =
                assertEquals(
                    "Virheellinen arvo \"B2\" kentässä \"luetun ymmärtäminen\" käyttäjälle \"1\"",
                    luetunYmmartaminenError?.viesti,
                ),
            fun() =
                assertEquals(
                    "Virheellinen arvo \"B2\" kentässä \"kuullun ymmärtäminen\" käyttäjälle \"1\"",
                    kuullunYmmartaminenError?.viesti,
                ),
            fun() =
                assertEquals(
                    "Puuttuva arvosana \"puhuminen\" kurssilla \"Integraatio testaus\" käyttäjälle \"1\"",
                    puheError?.viesti,
                ),
            fun() =
                assertEquals(
                    "Puuttuva arvosana \"kirjoittaminen\" kurssilla \"Integraatio testaus\" käyttäjälle \"1\"",
                    kirjoittaminenError?.viesti,
                ),
        )
    }

    @Test
    fun `duplicate suoritus is not saved`(
        @Autowired customKielitestiSuoritusRepository: CustomKielitestiSuoritusRepository,
        @Autowired koealustaService: KoealustaService,
    ) {
        // Facade
        mockServer
            .expect(
                ExpectedCount.times(2),
                requestTo(
                    "https://localhost:8080/dev/koto/webservice/rest/server.php?wstoken=token&wsfunction=local_completion_export_get_completions&moodlewsrestformat=json&from=0",
                ),
            ).andRespond(
                withSuccess(
                    """
                    {
                      "users": [$validSuoritus]
                    }
                    """.trimIndent(),
                    MediaType.APPLICATION_JSON,
                ),
            )

        // System under test

        // Test
        koealustaService.importValmiitSuoritukset(Instant.EPOCH)
        koealustaService.importValmiitSuoritukset(Instant.EPOCH)

        // Verification
        mockServer.verify()

        val suoritukset = customKielitestiSuoritusRepository.findAll()
        assertEquals(1, suoritukset.count())
    }

    @Test
    fun `updated suoritus is saved`(
        @Autowired customKielitestiSuoritusRepository: CustomKielitestiSuoritusRepository,
        @Autowired koealustaService: KoealustaService,
    ) {
        val updatedSuoritus =
            """
            {
                  "userid": 1,
                  "firstnames": "Ranja Testi",
                  "lastname": "Öhman-Testi",
                  "preferredname": "Ranja",
                  "SSN": "010180-9026",
                  "email": "ranja.testi@oph.fi",
                  "completions": [
                    {
                      "courseid": 32,
                      "coursename": "Integraatio testaus",
                      "lang": "fi",
                      "schoolOID": "1.2.246.562.10.42923230015",
                      "results": [
                        {
                          "name": "luetun ymm\u00e4rt\u00e4minen",
                          "quiz_grade": "B1"
                        },
                        {
                          "name": "kuullun ymm\u00e4rt\u00e4minen",
                          "quiz_grade": "B1"
                        },
                        {
                          "name": "puhuminen",
                          "quiz_grade": "A1"
                        },
                        {
                          "name": "kirjoittaminen",
                          "quiz_grade": "A1"
                        }
                      ],
                      "timecompleted": 1728969131,
                      "teacheremail": "opettaja@testi.oph.fi"
                    }
                  ]
            }
            """.trimIndent()
        // Facade
        mockServer
            .expect(
                requestTo(
                    "https://localhost:8080/dev/koto/webservice/rest/server.php?wstoken=token&wsfunction=local_completion_export_get_completions&moodlewsrestformat=json&from=0",
                ),
            ).andRespond(
                withSuccess(
                    """
                    {
                      "users": [$validSuoritus]
                    }
                    """.trimIndent(),
                    MediaType.APPLICATION_JSON,
                ),
            )

        // System under test

        // Test
        val importFrom = koealustaService.importValmiitSuoritukset(Instant.EPOCH)

        // Verification
        mockServer.verify()

        mockServer.reset()
        mockServer
            .expect(
                requestTo(
                    "https://localhost:8080/dev/koto/webservice/rest/server.php?wstoken=token&wsfunction=local_completion_export_get_completions&moodlewsrestformat=json&from=${importFrom.epochSecond}",
                ),
            ).andRespond(
                withSuccess(
                    """
                    {
                      "users": [$updatedSuoritus]
                    }
                    """.trimIndent(),
                    MediaType.APPLICATION_JSON,
                ),
            )

        koealustaService.importValmiitSuoritukset(importFrom)
        mockServer.verify()
        val suoritukset = customKielitestiSuoritusRepository.findAll()
        assertEquals(2, suoritukset.count())
    }

    @Test
    fun `import with invalid language saves an error`(
        @Autowired kielitestiSuoritusErrorRepository: KielitestiSuoritusErrorRepository,
        @Autowired koealustaService: KoealustaService,
    ) {
        // Facade

        val suoritusJson =
            """
            {
                  "userid": 1,
                  "firstnames": "Ranja Testi",
                  "lastname": "Öhman-Testi",
                  "preferredname": "Ranja",
                  "SSN": "010180-9026",
                  "email": "ranja.testi@oph.fi",
                  "completions": [
                    {
                      "courseid": 32,
                      "coursename": "Integraatio testaus",
                      "lang": "asdasd",
                      "schoolOID": "1.2.246.562.10.42923230015",
                      "results": [
                        {
                          "name": "luetun ymm\u00e4rt\u00e4minen",
                          "quiz_grade": "A1"
                        },
                        {
                          "name": "kuullun ymm\u00e4rt\u00e4minen",
                          "quiz_grade": "B1"
                        },
                        {
                          "name": "puhuminen",
                          "quiz_grade": "A1"
                        },
                        {
                          "name": "kirjoittaminen",
                          "quiz_grade": "A1"
                        }
                      ],
                      "timecompleted": 1728969131,
                      "teacheremail": "opettaja@testi.oph.fi"
                    }
                  ]
            }
            """.trimIndent()
        mockServer
            .expect(
                requestTo(
                    "https://localhost:8080/dev/koto/webservice/rest/server.php?wstoken=token&wsfunction=local_completion_export_get_completions&moodlewsrestformat=json&from=0",
                ),
            ).andRespond(
                withSuccess(
                    """
                    {
                      "users": [$suoritusJson]
                    }
                    """.trimIndent(),
                    MediaType.APPLICATION_JSON,
                ),
            )

        // Test
        val lastSeen = koealustaService.importValmiitSuoritukset(Instant.EPOCH)

        // Verification
        mockServer.verify()

        val errors = kielitestiSuoritusErrorRepository.findAll().toList()

        assertEquals(1, errors.size)

        val error = errors[0]
        assertAll(
            fun() = assertEquals("Öhman-Testi Ranja Testi", error.nimi),
            fun() = assertEquals("010180-9026", error.hetu),
            fun() = assertEquals("1.2.246.562.24.33342764709", error.suorittajanOid),
            fun() = assertEquals("1.2.246.562.10.42923230015", error.schoolOid.toString()),
            fun() = assertEquals("opettaja@testi.oph.fi", error.teacherEmail),
            fun() = assertEquals("lang", error.virheellinenKentta),
            fun() =
                assertEquals(
                    "Virheellinen arvo \"asdasd\" kentässä \"lang\" käyttäjälle \"1\"",
                    error.viesti,
                ),
        )
    }

    @Test
    fun `import with no language saves an error`(
        @Autowired kielitestiSuoritusErrorRepository: KielitestiSuoritusErrorRepository,
        @Autowired koealustaService: KoealustaService,
    ) {
        // Facade

        val suoritusJson =
            """
            {
                  "userid": 1,
                  "firstnames": "Ranja Testi",
                  "lastname": "Öhman-Testi",
                  "preferredname": "Ranja",
                  "SSN": "010180-9026",
                  "email": "ranja.testi@oph.fi",
                  "completions": [
                    {
                      "courseid": 32,
                      "coursename": "Integraatio testaus",
                      "schoolOID": "1.2.246.562.10.42923230015",
                      "results": [
                        {
                          "name": "luetun ymm\u00e4rt\u00e4minen",
                          "quiz_grade": "A1"
                        },
                        {
                          "name": "kuullun ymm\u00e4rt\u00e4minen",
                          "quiz_grade": "B1"
                        },
                        {
                          "name": "puhuminen",
                          "quiz_grade": "A1"
                        },
                        {
                          "name": "kirjoittaminen",
                          "quiz_grade": "A1"
                        }
                      ],
                      "timecompleted": 1728969131,
                      "teacheremail": "opettaja@testi.oph.fi"
                    }
                  ]
            }
            """.trimIndent()
        mockServer
            .expect(
                requestTo(
                    "https://localhost:8080/dev/koto/webservice/rest/server.php?wstoken=token&wsfunction=local_completion_export_get_completions&moodlewsrestformat=json&from=0",
                ),
            ).andRespond(
                withSuccess(
                    """
                    {
                      "users": [$suoritusJson]
                    }
                    """.trimIndent(),
                    MediaType.APPLICATION_JSON,
                ),
            )

        // Test
        val lastSeen = koealustaService.importValmiitSuoritukset(Instant.EPOCH)

        // Verification
        mockServer.verify()

        val errors = kielitestiSuoritusErrorRepository.findAll().toList()

        assertEquals(1, errors.size)

        val error = errors[0]
        assertAll(
            fun() = assertEquals("Öhman-Testi Ranja Testi", error.nimi),
            fun() = assertEquals("010180-9026", error.hetu),
            fun() = assertEquals("1.2.246.562.24.33342764709", error.suorittajanOid),
            fun() = assertEquals("1.2.246.562.10.42923230015", error.schoolOid.toString()),
            fun() = assertEquals("opettaja@testi.oph.fi", error.teacherEmail),
            fun() = assertEquals("lang", error.virheellinenKentta),
            fun() =
                assertEquals(
                    "Puuttuva kenttä \"lang\" käyttäjälle \"1\"",
                    error.viesti,
                ),
        )
    }

    private fun keskenerainenUser(
        userid: Int,
        courseid: Int,
        coursename: String,
    ) = """
        {
          "userid": $userid,
          "firstnames": "Kesken Testi",
          "lastname": "Öhman-Testi",
          "preferredname": "Kesken",
          "SSN": "010180-9026",
          "email": "kesken.testi@oph.fi",
          "courses": [
            {
              "courseid": $courseid,
              "coursename": "$coursename",
              "schoolOID": "1.2.246.562.10.42923230015",
              "teacheremail": "opettaja@testi.oph.fi"
            }
          ]
        }
        """.trimIndent()

    private val keskeneraisetNow = Instant.parse("2026-02-01T12:00:00Z")

    // 1768737600 = 2026-01-18T12:00:00Z, eli 14 vrk ennen keskeneraisetNow'ta
    private fun keskeneraisetUrl(enrolledFrom: Long = 1768737600L) =
        "https://localhost:8080/dev/koto/webservice/rest/server.php?wstoken=token" +
            "&wsfunction=local_completion_export_get_incomplete_course_participants" +
            "&moodlewsrestformat=json&enrolledfrom=$enrolledFrom"

    private fun MockRestServiceServer.expectKeskeneraiset(body: String) {
        expect(requestTo(keskeneraisetUrl()))
            .andRespond(withSuccess(body, MediaType.APPLICATION_JSON))
    }

    private fun importKeskeneraiset(
        koealustaService: KoealustaService,
        timeService: TestTimeService,
    ) = timeService.runWithFixedClock(keskeneraisetNow) { koealustaService.importKeskeneraisetSuoritukset() }

    @Test
    fun `keskeneräinen import rajaa haun ilmoittautumisajan perusteella`(
        @Autowired koealustaService: KoealustaService,
        @Autowired timeService: TestTimeService,
    ) {
        mockServer
            .expect(
                requestTo(
                    "https://localhost:8080/dev/koto/webservice/rest/server.php?wstoken=token" +
                        "&wsfunction=local_completion_export_get_incomplete_course_participants" +
                        "&moodlewsrestformat=json&enrolledfrom=1768737600",
                ),
            ).andRespond(withSuccess("""{ "users": [] }""", MediaType.APPLICATION_JSON))

        importKeskeneraiset(koealustaService, timeService)

        mockServer.verify()
    }

    @Test
    fun `keskeneräisten aikaikkuna on konfiguroitavissa`(
        @Autowired koealustaService: KoealustaService,
        @Autowired timeService: TestTimeService,
    ) {
        koealustaService.keskeneraisetEnrolmentWindow = "1d"

        mockServer
            .expect(requestTo(keskeneraisetUrl(1769860800L))) // 2026-01-31T12:00:00Z
            .andRespond(withSuccess("""{ "users": [] }""", MediaType.APPLICATION_JSON))

        importKeskeneraiset(koealustaService, timeService)

        mockServer.verify()
    }

    @Test
    fun `tyhjä aikaikkuna hakee keskeneräiset ilman aikarajaa`(
        @Autowired koealustaService: KoealustaService,
        @Autowired timeService: TestTimeService,
    ) {
        koealustaService.keskeneraisetEnrolmentWindow = ""

        mockServer
            .expect(
                requestTo(
                    "https://localhost:8080/dev/koto/webservice/rest/server.php?wstoken=token" +
                        "&wsfunction=local_completion_export_get_incomplete_course_participants" +
                        "&moodlewsrestformat=json",
                ),
            ).andRespond(withSuccess("""{ "users": [] }""", MediaType.APPLICATION_JSON))

        importKeskeneraiset(koealustaService, timeService)

        mockServer.verify()
    }

    @Test
    fun `keskeneräinen import saves suoritukset with null result fields and completed false`(
        @Autowired customKielitestiSuoritusRepository: CustomKielitestiSuoritusRepository,
        @Autowired koealustaService: KoealustaService,
        @Autowired timeService: TestTimeService,
    ) {
        mockServer.expectKeskeneraiset(
            """
            {
              "users": [${keskenerainenUser(10, 50, "Kotoutumiskoulutus kesken")}]
            }
            """.trimIndent(),
        )

        importKeskeneraiset(koealustaService, timeService)

        mockServer.verify()

        val suoritukset = customKielitestiSuoritusRepository.findAll().toList()
        assertEquals(1, suoritukset.size)

        val keskenerainen = suoritukset.first()
        assertAll(
            fun() = assertEquals(false, keskenerainen.completed),
            fun() = assertEquals(null, keskenerainen.oppijanumero),
            fun() = assertEquals(null, keskenerainen.suoritusaika),
            fun() = assertEquals(null, keskenerainen.luetunYmmartaminen),
            fun() = assertEquals(null, keskenerainen.kuullunYmmartaminen),
            fun() = assertEquals(null, keskenerainen.puhe),
            fun() = assertEquals(null, keskenerainen.kirjoittaminen),
            fun() = assertEquals(null, keskenerainen.testikieli),
            fun() = assertEquals(50, keskenerainen.kurssiId),
            fun() = assertEquals("Kotoutumiskoulutus kesken", keskenerainen.kurssi),
            fun() = assertEquals("Kesken Testi", keskenerainen.etunimet),
            fun() = assertEquals("Kesken", keskenerainen.kutsumanimi),
            fun() = assertEquals(Oid.parse("1.2.246.562.10.42923230015").getOrThrow(), keskenerainen.oppilaitosOid),
        )
    }

    @Test
    fun `subsequent keskeneräinen import replaces the previous incomplete set`(
        @Autowired customKielitestiSuoritusRepository: CustomKielitestiSuoritusRepository,
        @Autowired koealustaService: KoealustaService,
        @Autowired timeService: TestTimeService,
    ) {
        mockServer.expectKeskeneraiset(
            """
            {
              "users": [
                ${keskenerainenUser(10, 50, "Kurssi A")},
                ${keskenerainenUser(11, 51, "Kurssi B")}
              ]
            }
            """.trimIndent(),
        )

        importKeskeneraiset(koealustaService, timeService)
        mockServer.verify()
        assertEquals(2, customKielitestiSuoritusRepository.findAll().count())

        mockServer.reset()
        mockServer.expectKeskeneraiset(
            """
            {
              "users": [${keskenerainenUser(10, 50, "Kurssi A")}]
            }
            """.trimIndent(),
        )

        importKeskeneraiset(koealustaService, timeService)
        mockServer.verify()

        val suoritukset = customKielitestiSuoritusRepository.findAll().toList()
        assertAll(
            fun() = assertEquals(1, suoritukset.size, "Previous incomplete set should have been replaced"),
            fun() = assertEquals(50, suoritukset.first().kurssiId),
        )
    }

    @Test
    fun `keskeneräinen import does not remove valmis suoritukset`(
        @Autowired customKielitestiSuoritusRepository: CustomKielitestiSuoritusRepository,
        @Autowired koealustaService: KoealustaService,
        @Autowired timeService: TestTimeService,
    ) {
        mockServer
            .expect(
                requestTo(
                    "https://localhost:8080/dev/koto/webservice/rest/server.php?wstoken=token&wsfunction=local_completion_export_get_completions&moodlewsrestformat=json&from=0",
                ),
            ).andRespond(
                withSuccess(
                    """
                    {
                      "users": [$validSuoritus]
                    }
                    """.trimIndent(),
                    MediaType.APPLICATION_JSON,
                ),
            )
        mockServer.expectKeskeneraiset(
            """
            {
              "users": [${keskenerainenUser(10, 50, "Kurssi A")}]
            }
            """.trimIndent(),
        )

        koealustaService.importValmiitSuoritukset(Instant.EPOCH)
        importKeskeneraiset(koealustaService, timeService)

        mockServer.verify()

        val suoritukset = customKielitestiSuoritusRepository.findAll().toList()
        val valmis = suoritukset.single { it.completed }
        val keskenerainen = suoritukset.single { !it.completed }

        assertAll(
            fun() = assertEquals(2, suoritukset.size),
            fun() = assertEquals(Oid.parse("1.2.246.562.24.33342764709").getOrThrow(), valmis.oppijanumero),
            fun() = assertEquals(50, keskenerainen.kurssiId),
        )
    }

    @Test
    fun `keskeneräinen import ignores non-verifiable users without wiping valmis errors`(
        @Autowired kielitestiSuoritusErrorRepository: KielitestiSuoritusErrorRepository,
        @Autowired customKielitestiSuoritusRepository: CustomKielitestiSuoritusRepository,
        @Autowired koealustaService: KoealustaService,
        @Autowired timeService: TestTimeService,
    ) {
        val nonVerifiableKeskenerainen =
            """
            {
              "userid": 20,
              "firstnames": "Kesken Testi",
              "lastname": "Öhman-Testi",
              "preferredname": null,
              "SSN": "010180-9026",
              "email": "kesken.testi@oph.fi",
              "courses": [
                {
                  "courseid": 60,
                  "coursename": "Kurssi virhe",
                  "schoolOID": "1.2.246.562.10.42923230015",
                  "teacheremail": "opettaja@testi.oph.fi"
                }
              ]
            }
            """.trimIndent()

        mockServer
            .expect(
                requestTo(
                    "https://localhost:8080/dev/koto/webservice/rest/server.php?wstoken=token&wsfunction=local_completion_export_get_completions&moodlewsrestformat=json&from=0",
                ),
            ).andRespond(
                withSuccess(
                    """
                    {
                      "users": [$invalidHetu]
                    }
                    """.trimIndent(),
                    MediaType.APPLICATION_JSON,
                ),
            )
        mockServer.expectKeskeneraiset(
            """
            {
              "users": [$nonVerifiableKeskenerainen]
            }
            """.trimIndent(),
        )

        koealustaService.importValmiitSuoritukset(Instant.EPOCH)
        importKeskeneraiset(koealustaService, timeService)

        mockServer.verify()

        val errors = kielitestiSuoritusErrorRepository.findAll().toList()

        assertAll(
            fun() =
                assertEquals(
                    1,
                    errors.size,
                    "Non-verifiable keskeneräinen is ignored, so only the valmis error remains",
                ),
            fun() = assertEquals(1, errors.count { it.completed }),
            fun() =
                assertEquals(
                    0,
                    errors.count { !it.completed },
                    "A keskeneräinen that cannot be verified against onr must not be persisted as an error",
                ),
            fun() = assertEquals(0, customKielitestiSuoritusRepository.findAll().count()),
        )
    }

    @Test
    fun `import drops valmis suoritukset that reference testiaineisto`(
        @Autowired customKielitestiSuoritusRepository: CustomKielitestiSuoritusRepository,
        @Autowired kielitestiSuoritusErrorRepository: KielitestiSuoritusErrorRepository,
        @Autowired koealustaService: KoealustaService,
    ) {
        val testiaineistoSuoritus =
            """
            {
                  "userid": 1,
                  "firstnames": "Ranja Testi",
                  "lastname": "Öhman-Testi",
                  "preferredname": "Ranja",
                  "SSN": "010180-9026",
                  "email": "ranja.testi@oph.fi",
                  "completions": [
                    {
                      "courseid": 32,
                      "coursename": "Integraatio testaus",
                      "lang": "fi",
                      "schoolOID": "1.2.246.562.10.1234567890",
                      "results": [
                        { "name": "luetun ymmärtäminen", "quiz_grade": "A1" },
                        { "name": "kuullun ymmärtäminen", "quiz_grade": "B1" },
                        { "name": "puhuminen", "quiz_grade": "A2" },
                        { "name": "kirjoittaminen", "quiz_grade": "B1" }
                      ],
                      "timecompleted": 1728969131,
                      "teacheremail": "opettaja@testi.oph.fi"
                    }
                  ]
            }
            """.trimIndent()

        mockServer
            .expect(
                requestTo(
                    "https://localhost:8080/dev/koto/webservice/rest/server.php?wstoken=token&wsfunction=local_completion_export_get_completions&moodlewsrestformat=json&from=0",
                ),
            ).andRespond(
                withSuccess(
                    """
                    {
                      "users": [$testiaineistoSuoritus]
                    }
                    """.trimIndent(),
                    MediaType.APPLICATION_JSON,
                ),
            )

        koealustaService.importValmiitSuoritukset(Instant.EPOCH)

        mockServer.verify()

        assertAll(
            fun() = assertEquals(0, customKielitestiSuoritusRepository.findAll().count()),
            fun() = assertEquals(emptyList(), kielitestiSuoritusErrorRepository.findAll()),
        )
    }

    @Test
    fun `keskeneräinen import drops users with no courses or testiaineisto`(
        @Autowired customKielitestiSuoritusRepository: CustomKielitestiSuoritusRepository,
        @Autowired kielitestiSuoritusErrorRepository: KielitestiSuoritusErrorRepository,
        @Autowired koealustaService: KoealustaService,
        @Autowired timeService: TestTimeService,
    ) {
        val userWithoutCourses =
            """
            {
              "userid": 30,
              "firstnames": "Tyhjä Testi",
              "lastname": "Öhman-Testi",
              "preferredname": "Tyhjä",
              "SSN": "010180-9026",
              "email": "tyhja.testi@oph.fi",
              "courses": []
            }
            """.trimIndent()
        val testiaineistoUser =
            """
            {
              "userid": 31,
              "firstnames": "Kesken Testi",
              "lastname": "Öhman-Testi",
              "preferredname": "Kesken",
              "SSN": "010180-9026",
              "email": "kesken.testi@oph.fi",
              "courses": [
                {
                  "courseid": 70,
                  "coursename": "Testiaineisto",
                  "schoolOID": "1.2.246.562.10.1234567890",
                  "teacheremail": "opettaja@testi.oph.fi"
                }
              ]
            }
            """.trimIndent()

        mockServer.expectKeskeneraiset(
            """
            {
              "users": [$userWithoutCourses, $testiaineistoUser]
            }
            """.trimIndent(),
        )

        importKeskeneraiset(koealustaService, timeService)

        mockServer.verify()

        assertAll(
            fun() = assertEquals(0, customKielitestiSuoritusRepository.findAll().count()),
            fun() = assertEquals(emptyList(), kielitestiSuoritusErrorRepository.findAll()),
        )
    }
}
