package fi.oph.kitu.kotoutumiskoulutus.koealusta.tehtavapankki

import arrow.core.Either
import fi.oph.kitu.DBContainerConfiguration
import fi.oph.kitu.LocalStackContainerConfiguration
import fi.oph.kitu.LocalStackContainerConfiguration.Companion.TEST_BUCKET
import fi.oph.kitu.tehtavapankki.TehtavapakettiEntity
import fi.oph.kitu.tehtavapankki.TehtavapankkiRepository
import fi.oph.kitu.util.TimeService
import io.awspring.cloud.s3.S3Template
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.core.io.ClassPathResource
import org.springframework.http.MediaType
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.test.context.TestPropertySource
import org.springframework.test.web.client.MockRestServiceServer
import org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo
import org.springframework.test.web.client.response.MockRestResponseCreators.withServerError
import org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess
import org.springframework.web.client.RestClientResponseException
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response
import software.amazon.awssdk.services.s3.model.S3Object
import java.net.URI
import java.time.Instant
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.time.temporal.ChronoUnit
import java.util.Base64
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

@SpringBootTest
@Import(DBContainerConfiguration::class, LocalStackContainerConfiguration::class)
@TestPropertySource(properties = ["spring.cloud.aws.s3.enabled=true"])
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TehtavapankkiServiceTest(
    @param:Autowired private val tehtavapankkiService: TehtavapankkiService,
    @param:Autowired private val importService: TehtavapankkiImportService,
    @param:Autowired private val tehtavapankkiClient: TehtavapankkiClientImpl,
    @param:Autowired private val s3Client: S3Client,
    @param:Autowired private val s3Template: S3Template,
    @param:Autowired private val parser: TehtavapankkiXmlParser,
    @param:Autowired private val timeService: TimeService,
    @param:Autowired private val repository: TehtavapankkiRepository,
    @param:Autowired private val jdbc: JdbcTemplate,
) {
    // Yksi MockRestServiceServer per jaettu Spring-konteksti: clientin restClient
    // rakennetaan laiskasti vain kerran, joten sidonta on tehtävä ennen ensimmäistä
    // client-kutsua ja jaettava kaikkien testien kesken reset()-pohjaisesti.
    private val mockServer: MockRestServiceServer by lazy {
        MockRestServiceServer.bindTo(tehtavapankkiClient.restClientBuilder).build()
    }

    @BeforeEach
    fun reset() {
        s3Client
            .listObjectsV2 { it.bucket(TEST_BUCKET) }
            .contents()
            .forEach { obj ->
                s3Client.deleteObject { it.bucket(TEST_BUCKET).key(obj.key()) }
            }
        jdbc.execute("DELETE FROM tehtavapaketti")
        tehtavapankkiClient.koealustaToken = "testitoken"
        tehtavapankkiClient.koealustaBaseUrl = "https://localhost:8080/dev/koto"
        mockServer.reset()
    }

    private val listResponse =
        """
        {
          "questionbanks": [
            {
              "courseid": 7,
              "coursename": "Suomi 2",
              "coursestartdate": 0,
              "filegenerated": 1733400000,
              "questionbankversion": "v1",
              "language": "fin",
              "downloadurl": "https://localhost:8080/dev/koto/pluginfile.php/7/qb.xml"
            },
            {
              "courseid": 8,
              "coursename": "Suomi 3",
              "coursestartdate": 0,
              "filegenerated": 1733400001,
              "questionbankversion": "v1",
              "language": "fin",
              "downloadurl": "https://localhost:8080/dev/koto/pluginfile.php/8/qb.xml"
            }
          ]
        }
        """.trimIndent()

    @Test
    fun `uploadTehtavapankki kirjoittaa tiedostot S3-buckettiin filegenerated-suffiksilla ja user metadatan`() {
        val downloads =
            listOf(
                QuestionBankDownload(
                    metadata =
                        QuestionBankMetadata(
                            courseId = 42,
                            courseName = "Suomi alkeet",
                            published = Instant.ofEpochSecond(1672531200),
                            generated = Instant.ofEpochSecond(1733400000),
                            version = "test",
                            language = "FIN",
                            downloadUrl = "ignored",
                        ),
                    xml = StringXmlSource("<questions><q id=\"1\"/></questions>"),
                ),
            )

        tehtavapankkiService.uploadTehtavapankki(downloads)

        val objects = s3Client.listObjectsV2 { it.bucket(TEST_BUCKET) }
        assertEquals(1, objects.keyCount(), "Yksi tiedosto pitäisi olla ladattu")

        val key = objects.contents().single().key()
        assertTrue(
            key.startsWith("42-Suomi_alkeet/"),
            "Avaimen pitäisi olla courseid-coursename/-prefiksillä, oli: $key",
        )
        assertTrue(
            key.contains("-fg1733400000-"),
            "Avaimessa pitäisi olla filegenerated epoch-sekunteina, oli: $key",
        )

        val content =
            s3Client
                .getObject { it.bucket(TEST_BUCKET).key(key) }
                .readAllBytes()
                .toString(Charsets.UTF_8)
        assertEquals("<questions><q id=\"1\"/></questions>", content)

        val head = s3Client.headObject { it.bucket(TEST_BUCKET).key(key) }
        assertEquals("1672531200", head.metadata()[TehtavapankkiService.S3_META_PUBLISHED])
        assertEquals("test", head.metadata()[TehtavapankkiService.S3_META_VERSION])
        assertEquals("FIN", head.metadata()[TehtavapankkiService.S3_META_LANGUAGE])
    }

    @Test
    fun `fetchS3UserMetadata palauttaa objektin user metadatan`() {
        val key = "42-Suomi_alkeet/2026-01-01T00-00-00-fg5-0.xml"
        s3Client.putObject(
            { req ->
                req
                    .bucket(TEST_BUCKET)
                    .key(key)
                    .metadata(
                        mapOf(
                            TehtavapankkiService.S3_META_VERSION to "v9",
                            TehtavapankkiService.S3_META_LANGUAGE to "ENG",
                        ),
                    )
            },
            RequestBody.fromString("<x/>"),
        )

        val metadata = tehtavapankkiService.fetchS3UserMetadata(key)
        assertEquals("v9", metadata[TehtavapankkiService.S3_META_VERSION])
        assertEquals("ENG", metadata[TehtavapankkiService.S3_META_LANGUAGE])
    }

    @Test
    fun `fetchS3UserMetadata palauttaa tyhjan mapin tuntemattomalle avaimelle`() {
        val metadata = tehtavapankkiService.fetchS3UserMetadata("ei-olemassa.xml")
        assertEquals(emptyMap<String, String>(), metadata)
    }

    @Test
    fun `importTehtavapankki lataa muuttuneet ja ohittaa skipattavat`() {
        // Edellisellä ajolla on jo tuotu courseid=7 filegenerated=1733400000:lla,
        // joten se pitäisi skipata. Courseid=8:lla ei ole vastaavaa riviä, joten se
        // pitäisi ladata.
        repository.insertPaketti(
            TehtavapakettiEntity(
                lahdejarjestelma = "moodle.koealusta",
                lahdeId = "7",
                nimi = "Suomi 2",
                versioHash = "esim-hash",
                s3Avain = null,
                lahdeFilegenerated =
                    OffsetDateTime.ofInstant(Instant.ofEpochSecond(1733400000), ZoneOffset.UTC),
            ),
        )

        mockServer
            .expect(
                requestTo(
                    "https://localhost:8080/dev/koto/webservice/rest/server.php?" +
                        "wstoken=testitoken&moodlewsrestformat=json&" +
                        "wsfunction=local_completion_export_export_question_bank",
                ),
            ).andRespond(withSuccess(listResponse, MediaType.APPLICATION_JSON))
        // Vain courseid=8:n latauksen pitäisi tapahtua — courseid=7 skipataan
        // ennen pluginfile.php-kutsua.
        mockServer
            .expect(requestTo("https://localhost:8080/dev/koto/pluginfile.php/8/qb.xml?token=testitoken"))
            .andRespond(
                withSuccess("<questions><q id=\"b\"/></questions>", MediaType.APPLICATION_XML),
            )

        tehtavapankkiService.importTehtavapankki()

        mockServer.verify()

        val keys =
            s3Client
                .listObjectsV2 { it.bucket(TEST_BUCKET) }
                .contents()
                .map { it.key() }

        assertEquals(1, keys.size, "Vain courseid=8:n pitäisi olla ladattu, oli: $keys")
        val suomi3Key = keys.single()
        assertTrue(
            suomi3Key.startsWith("8-Suomi_3/"),
            "Ladatun avaimen pitäisi alkaa courseid=8:n kansiopolulla, oli: $suomi3Key",
        )
        assertTrue(
            suomi3Key.contains("-fg1733400001-"),
            "Avaimessa pitäisi olla filegenerated epoch-sekunteina, oli: $suomi3Key",
        )

        val suomi3Content =
            s3Client
                .getObject { it.bucket(TEST_BUCKET).key(suomi3Key) }
                .readAllBytes()
                .toString(Charsets.UTF_8)
        assertEquals("<questions><q id=\"b\"/></questions>", suomi3Content)
    }

    @Test
    fun `importTehtavapankki ohittaa ei-XML-vastauksen mutta vie terveen kurssin S3-ageen`() {
        mockServer
            .expect(
                requestTo(
                    "https://localhost:8080/dev/koto/webservice/rest/server.php?" +
                        "wstoken=testitoken&moodlewsrestformat=json&" +
                        "wsfunction=local_completion_export_export_question_bank",
                ),
            ).andRespond(withSuccess(listResponse, MediaType.APPLICATION_JSON))
        mockServer
            .expect(requestTo("https://localhost:8080/dev/koto/pluginfile.php/7/qb.xml?token=testitoken"))
            .andRespond(withSuccess("<questions><q id=\"a\"/></questions>", MediaType.APPLICATION_XML))
        mockServer
            .expect(requestTo("https://localhost:8080/dev/koto/pluginfile.php/8/qb.xml?token=testitoken"))
            .andRespond(
                withSuccess(
                    """{"error":"Pääsyn hallinnan poikkeus","errorcode":"accessexception","stacktrace":null,"debuginfo":null,"reproductionlink":null}""",
                    MediaType.APPLICATION_JSON,
                ),
            )

        val failures = tehtavapankkiService.importTehtavapankki()

        mockServer.verify()

        assertEquals(1, failures.size, "Vain courseid=8:n pitäisi epäonnistua")
        assertEquals(8, failures.single().courseId)
        assertContains(failures.single().reason, "accessexception")

        val keys =
            s3Client
                .listObjectsV2 { it.bucket(TEST_BUCKET) }
                .contents()
                .map { it.key() }
        assertEquals(1, keys.size, "Vain terveen courseid=7:n pitäisi olla S3:ssa, oli: $keys")
        assertTrue(keys.single().startsWith("7-Suomi_2/"), "Avaimen pitäisi olla courseid=7, oli: ${keys.single()}")
    }

    @Test
    fun `importTehtavapankki kaatuu valittomasti eika vie mitaan kun lataus-URL on virheellinen`() {
        mockServer
            .expect(
                requestTo(
                    "https://localhost:8080/dev/koto/webservice/rest/server.php?" +
                        "wstoken=testitoken&moodlewsrestformat=json&" +
                        "wsfunction=local_completion_export_export_question_bank",
                ),
            ).andRespond(withSuccess(listResponse, MediaType.APPLICATION_JSON))
        mockServer
            .expect(requestTo("https://localhost:8080/dev/koto/pluginfile.php/7/qb.xml?token=testitoken"))
            .andRespond(withSuccess("<questions><q id=\"a\"/></questions>", MediaType.APPLICATION_XML))
        mockServer
            .expect(requestTo("https://localhost:8080/dev/koto/pluginfile.php/8/qb.xml?token=testitoken"))
            .andRespond(withServerError())

        assertFailsWith<RestClientResponseException> {
            tehtavapankkiService.importTehtavapankki()
        }

        val keys =
            s3Client
                .listObjectsV2 { it.bucket(TEST_BUCKET) }
                .contents()
                .map { it.key() }
        assertEquals(0, keys.size, "Fail-fast: mitään ei pitäisi viedä S3:een, oli: $keys")
    }

    @Test
    fun `importAndIngest ingestoi terveen kurssin mutta heittaa kun toinen kurssi epaonnistuu`() {
        val validQuiz =
            """
            <?xml version="1.0" encoding="UTF-8"?>
            <quiz>
              <question type="category">
                <category><text>${'$'}course${'$'}/top/A1</text></category>
                <info format="html"><text/></info>
                <idnumber/>
              </question>
            </quiz>
            """.trimIndent()
        mockServer
            .expect(
                requestTo(
                    "https://localhost:8080/dev/koto/webservice/rest/server.php?" +
                        "wstoken=testitoken&moodlewsrestformat=json&" +
                        "wsfunction=local_completion_export_export_question_bank",
                ),
            ).andRespond(withSuccess(listResponse, MediaType.APPLICATION_JSON))
        mockServer
            .expect(requestTo("https://localhost:8080/dev/koto/pluginfile.php/7/qb.xml?token=testitoken"))
            .andRespond(withSuccess(validQuiz, MediaType.APPLICATION_XML))
        mockServer
            .expect(requestTo("https://localhost:8080/dev/koto/pluginfile.php/8/qb.xml?token=testitoken"))
            .andRespond(
                withSuccess(
                    """{"error":"Pääsyn hallinnan poikkeus","errorcode":"accessexception","stacktrace":null,"debuginfo":null,"reproductionlink":null}""",
                    MediaType.APPLICATION_JSON,
                ),
            )

        val ex =
            assertFailsWith<TehtavapankkiImportException> {
                importService.importAndIngest()
            }
        assertContains(ex.message!!, "accessexception")

        assertNotNull(
            repository.findLatestPakettiBySource(TehtavapankkiIngestService.LAHDEJARJESTELMA, "7"),
            "Terveen courseid=7:n pitäisi olla ingestoitu vaikka courseid=8 epäonnistui",
        )
        assertNull(
            repository.findLatestPakettiBySource(TehtavapankkiIngestService.LAHDEJARJESTELMA, "8"),
            "Virheellistä courseid=8:aa ei pitäisi ingestoida",
        )
    }

    @Test
    fun `importAndIngest ingestoi terveen paketin vaikka toisen paketin ingest epaonnistuu`() {
        val validQuiz =
            """
            <?xml version="1.0" encoding="UTF-8"?>
            <quiz>
              <question type="category">
                <category><text>${'$'}course${'$'}/top/A1</text></category>
                <info format="html"><text/></info>
                <idnumber/>
              </question>
            </quiz>
            """.trimIndent()
        // courseid=3 ladataan ja viedään S3:een (alkaa <:lla), mutta sen XML on
        // viallinen → ingest palauttaa Left. courseid=7 on validi. Avain "3-..."
        // järjestyy ennen "7-..." joten viallinen ingestoidaan ensin.
        val listResponseTwoValid =
            """
            {
              "questionbanks": [
                {
                  "courseid": 3,
                  "coursename": "Suomi 3",
                  "coursestartdate": 0,
                  "filegenerated": 1733400000,
                  "questionbankversion": "v1",
                  "language": "fin",
                  "downloadurl": "https://localhost:8080/dev/koto/pluginfile.php/3/qb.xml"
                },
                {
                  "courseid": 7,
                  "coursename": "Suomi 7",
                  "coursestartdate": 0,
                  "filegenerated": 1733400001,
                  "questionbankversion": "v1",
                  "language": "fin",
                  "downloadurl": "https://localhost:8080/dev/koto/pluginfile.php/7/qb.xml"
                }
              ]
            }
            """.trimIndent()
        mockServer
            .expect(
                requestTo(
                    "https://localhost:8080/dev/koto/webservice/rest/server.php?" +
                        "wstoken=testitoken&moodlewsrestformat=json&" +
                        "wsfunction=local_completion_export_export_question_bank",
                ),
            ).andRespond(withSuccess(listResponseTwoValid, MediaType.APPLICATION_JSON))
        mockServer
            .expect(requestTo("https://localhost:8080/dev/koto/pluginfile.php/3/qb.xml?token=testitoken"))
            .andRespond(withSuccess("<rikki>", MediaType.APPLICATION_XML))
        mockServer
            .expect(requestTo("https://localhost:8080/dev/koto/pluginfile.php/7/qb.xml?token=testitoken"))
            .andRespond(withSuccess(validQuiz, MediaType.APPLICATION_XML))

        assertFailsWith<TehtavapankkiImportException> {
            importService.importAndIngest()
        }

        assertNotNull(
            repository.findLatestPakettiBySource(TehtavapankkiIngestService.LAHDEJARJESTELMA, "7"),
            "Terveen courseid=7:n pitäisi olla ingestoitu vaikka courseid=3:n ingest epäonnistui",
        )
        assertNull(
            repository.findLatestPakettiBySource(TehtavapankkiIngestService.LAHDEJARJESTELMA, "3"),
            "Viallista courseid=3:a ei pitäisi ingestoida",
        )
    }

    @Test
    fun `importTehtavapankki ohittaa kurssin jonka lataustiedoston nimi on tyhja`() {
        val listResponseEmptyFilename =
            """
            {
              "questionbanks": [
                {
                  "courseid": 7,
                  "coursename": "Suomi 2",
                  "coursestartdate": 0,
                  "filegenerated": 1733400000,
                  "questionbankversion": "v1",
                  "language": "fin",
                  "downloadurl": "https://localhost:8080/dev/koto/pluginfile.php/7/qb.xml"
                },
                {
                  "courseid": 9,
                  "coursename": "Suomi tyhja",
                  "coursestartdate": 0,
                  "filegenerated": 1733400002,
                  "questionbankversion": "v1",
                  "language": "fin",
                  "downloadurl": "https://localhost:8080/dev/koto/pluginfile.php/9/"
                }
              ]
            }
            """.trimIndent()
        mockServer
            .expect(
                requestTo(
                    "https://localhost:8080/dev/koto/webservice/rest/server.php?" +
                        "wstoken=testitoken&moodlewsrestformat=json&" +
                        "wsfunction=local_completion_export_export_question_bank",
                ),
            ).andRespond(withSuccess(listResponseEmptyFilename, MediaType.APPLICATION_JSON))
        // Vain courseid=7:n lataus odotetaan — courseid=9 ohitetaan tyhjän
        // tiedostonimen takia ilman pluginfile.php-kutsua.
        mockServer
            .expect(requestTo("https://localhost:8080/dev/koto/pluginfile.php/7/qb.xml?token=testitoken"))
            .andRespond(withSuccess("<questions><q id=\"a\"/></questions>", MediaType.APPLICATION_XML))

        val failures = tehtavapankkiService.importTehtavapankki()

        mockServer.verify()

        assertTrue(failures.isEmpty(), "Tyhjän tiedostonimen ei pitäisi tuottaa virhettä, oli: $failures")
        val keys =
            s3Client
                .listObjectsV2 { it.bucket(TEST_BUCKET) }
                .contents()
                .map { it.key() }
        assertEquals(1, keys.size, "Vain courseid=7:n pitäisi olla S3:ssa, oli: $keys")
        assertTrue(keys.single().startsWith("7-Suomi_2/"), "Avaimen pitäisi olla courseid=7, oli: ${keys.single()}")
    }

    @Test
    fun `listTehtavapaketit palauttaa bucketin objektit`() {
        val startTime = Instant.now()
        s3Client.putObject(
            { it.bucket(TEST_BUCKET).key("42-Suomi_alkeet/2026-01-01T00:00:00-0.xml") },
            RequestBody.fromString("<questions/>"),
        )
        s3Client.putObject(
            { it.bucket(TEST_BUCKET).key("7-Suomi_2/2026-02-02T00:00:00-0.xml") },
            RequestBody.fromString("<questions/>"),
        )
        val endTime = Instant.now()

        val tehtavapaketit = tehtavapankkiService.listAllObjects()

        assertEquals(2, tehtavapaketit.size, "Molempien objektien pitäisi näkyä listauksessa")
        assertEquals(
            setOf(
                "42-Suomi_alkeet/2026-01-01T00:00:00-0.xml",
                "7-Suomi_2/2026-02-02T00:00:00-0.xml",
            ),
            tehtavapaketit.map { it.key }.toSet(),
        )
        tehtavapaketit.forEach { tp ->
            assertEquals("<questions/>".length.toLong(), tp.size, "Koko, avain: ${tp.key}")
            assertTrue(
                !tp.timestamp.isBefore(startTime.truncatedTo(ChronoUnit.SECONDS)) &&
                    !tp.timestamp.isAfter(endTime.plusSeconds(1)),
                "Aikaleiman ${tp.timestamp} pitäisi olla välillä $startTime..$endTime, avain: ${tp.key}",
            )
        }
    }

    @Test
    fun `listTehtavapaketit palauttaa tyhjan listan kun bucketissa ei ole objekteja`() {
        assertEquals(emptyList(), tehtavapankkiService.listAllObjects())
    }

    @Test
    fun `listAllObjects lukee kaikki sivut ja ottaa koon ja ajan listauksesta ilman HEAD-kutsuja`() {
        val aika = Instant.parse("2026-01-01T00:00:00Z")
        val sivut =
            mapOf(
                null to
                    ListObjectsV2Response
                        .builder()
                        .contents(
                            S3Object
                                .builder()
                                .key("1-A/a.xml")
                                .size(11L)
                                .lastModified(aika)
                                .build(),
                        ).isTruncated(true)
                        .nextContinuationToken("sivu2")
                        .build(),
                "sivu2" to
                    ListObjectsV2Response
                        .builder()
                        .contents(
                            S3Object
                                .builder()
                                .key("2-B/b.xml")
                                .size(22L)
                                .lastModified(aika.plusSeconds(1))
                                .build(),
                        ).isTruncated(false)
                        .build(),
            )
        val vainListaavaS3 =
            object : S3Client {
                override fun serviceName(): String = S3Client.SERVICE_NAME

                override fun close() {}

                override fun listObjectsV2(request: ListObjectsV2Request): ListObjectsV2Response =
                    sivut.getValue(request.continuationToken())
            }
        val service =
            TehtavapankkiService(tehtavapankkiClient, s3Template, vainListaavaS3, parser, repository, timeService)
                .apply { bucket = TEST_BUCKET }

        assertEquals(
            listOf(
                TehtavapakettiObject("1-A/a.xml", "1-A/a.xml", 11L, aika),
                TehtavapakettiObject("2-B/b.xml", "2-B/b.xml", 22L, aika.plusSeconds(1)),
            ),
            service.listAllObjects(),
        )
    }

    @Test
    fun `groupTehtavapaketit jarjestaa ryhmat uusin ensin ja versiot ryhman sisalla uusin ensin`() {
        val base = Instant.parse("2026-01-01T00:00:00Z")

        fun obj(
            folder: String,
            offsetSeconds: Long,
        ) = TehtavapakettiObject(
            key = "$folder/$offsetSeconds.xml",
            filename = "$folder/$offsetSeconds.xml",
            size = 1,
            timestamp = base.plusSeconds(offsetSeconds),
        )

        val result =
            groupTehtavapaketit(
                listOf(
                    obj("1-Vanha", 5),
                    obj("3-Uusin", 30),
                    obj("2-Keski", 20),
                    obj("1-Vanha", 10),
                    obj("3-Uusin", 25),
                    TehtavapakettiObject("muu/note.txt", "muu/note.txt", 1, base.plusSeconds(99)),
                ),
            )

        assertEquals(listOf("3-Uusin", "2-Keski", "1-Vanha"), result.keys.toList())
        assertEquals(
            listOf(base.plusSeconds(10), base.plusSeconds(5)),
            result.getValue("1-Vanha").map { it.timestamp },
        )
    }

    @Test
    fun `countTehtavapaketit laskee jokaisen S3-kansion vaikka courseid on sama`() {
        s3Client.putObject(
            { it.bucket(TEST_BUCKET).key("17-Kielitesti_esimerkki/2026-01-01T00:00:00-0.xml") },
            RequestBody.fromString("<questions/>"),
        )
        s3Client.putObject(
            { it.bucket(TEST_BUCKET).key("17-Kielitesti_opettaja_pilotointi/2026-02-02T00:00:00-0.xml") },
            RequestBody.fromString("<questions/>"),
        )

        assertEquals(
            2,
            tehtavapankkiService.countTehtavapaketit(),
            "Saman courseid:n eri kansioiden pitäisi laskeutua erikseen, kuten listausnäkymässä",
        )
        assertEquals(
            tehtavapankkiService.listTehtavapaketit().size.toLong(),
            tehtavapankkiService.countTehtavapaketit(),
        )
    }

    @Test
    fun `getTemporaryDownloadUrl palauttaa toimivan signed URL_n`() {
        val key = "42-Suomi_alkeet/2026-01-01T00:00:00-0.xml"
        val content = "<questions><q id=\"1\"/></questions>"
        s3Client.putObject(
            { it.bucket(TEST_BUCKET).key(key) },
            RequestBody.fromString(content),
        )

        val url = tehtavapankkiService.getTemporaryDownloadUrl(key)

        assertNotNull(url, "Signed URL:n pitäisi palautua kun bucket on määritelty")
        val downloaded =
            URI
                .create(url.toString())
                .toURL()
                .openStream()
                .use { it.readBytes().toString(Charsets.UTF_8) }
        assertEquals(content, downloaded)
    }

    @Test
    fun `getTemporaryDownloadUrl palauttaa null, jos objektiavain on tuntematon`() {
        val url = tehtavapankkiService.getTemporaryDownloadUrl("42-Suomi_alkeet/2026-01-01T00:00:00-0.xml")
        assertNull(url, "Signed URL:n pitäisi olla null, kun objektia ei löydy")
    }

    @Test
    fun `fetchAndParseFromS3 lataa ja parsii bucketissa olevan xml-tiedoston`() {
        val key = "42-Suomi_alkeet/2026-01-01T00:00:00-0.xml"
        val xml =
            """
            <?xml version="1.0" encoding="UTF-8"?>
            <quiz>
              <question type="category">
                <category><text>${'$'}course${'$'}/top/A1</text></category>
                <info format="html"><text/></info>
                <idnumber/>
              </question>
            </quiz>
            """.trimIndent()
        s3Client.putObject(
            { it.bucket(TEST_BUCKET).key(key) },
            RequestBody.fromString(xml),
        )

        val result = tehtavapankkiService.fetchAndParseFromS3(key)

        assertIs<Either.Right<TehtavapankkiQuiz>>(result)
        assertEquals(1, result.value.questions.size)
        assertIs<CategoryQuestion>(result.value.questions.single())
    }

    @Test
    fun `fetchAndParseFromS3 palauttaa NotFound-virheen tuntemattomalle avaimelle`() {
        val result = tehtavapankkiService.fetchAndParseFromS3("ei-olemassa.xml")

        assertIs<Either.Left<TehtavapankkiParseError>>(result)
        assertEquals(TehtavapankkiParseError.NotFound, result.value)
    }

    @Test
    fun `removeDuplicates poistaa kustakin kansiosta saman sisallon duplikaatit ja jattaa vanhimman`() {
        val sameContent = "<quiz><q id=\"1\"/></quiz>"
        val otherContent = "<quiz><q id=\"2\"/></quiz>"

        // 42-Suomi_alkeet/: 3 duplikaattia + 1 erilainen → poistettava 2
        s3Client.putObject(
            { it.bucket(TEST_BUCKET).key("42-Suomi_alkeet/2026-01-01T00-00-00-0.xml") },
            RequestBody.fromString(sameContent),
        )
        s3Client.putObject(
            { it.bucket(TEST_BUCKET).key("42-Suomi_alkeet/2026-02-01T00-00-00-0.xml") },
            RequestBody.fromString(sameContent),
        )
        s3Client.putObject(
            { it.bucket(TEST_BUCKET).key("42-Suomi_alkeet/2026-03-01T00-00-00-0.xml") },
            RequestBody.fromString(sameContent),
        )
        s3Client.putObject(
            { it.bucket(TEST_BUCKET).key("42-Suomi_alkeet/2026-04-01T00-00-00-0.xml") },
            RequestBody.fromString(otherContent),
        )

        // 7-Suomi_2/: 2 saman sisältöistä → poistettava 1
        s3Client.putObject(
            { it.bucket(TEST_BUCKET).key("7-Suomi_2/2026-01-01T00-00-00-0.xml") },
            RequestBody.fromString(sameContent),
        )
        s3Client.putObject(
            { it.bucket(TEST_BUCKET).key("7-Suomi_2/2026-02-01T00-00-00-0.xml") },
            RequestBody.fromString(sameContent),
        )

        val result = tehtavapankkiService.removeDuplicates()

        assertNotNull(result)
        assertEquals(6, result.scanned)
        assertEquals(3, result.deleted.size)

        val remaining =
            s3Client
                .listObjectsV2 { it.bucket(TEST_BUCKET) }
                .contents()
                .map { it.key() }
        assertEquals(3, remaining.size)
        // 42-Suomi_alkeet/: yksi sameContent + yksi otherContent
        val alkeetKeys = remaining.filter { it.startsWith("42-Suomi_alkeet/") }
        assertEquals(2, alkeetKeys.size)
        assertTrue(alkeetKeys.any { it.endsWith("2026-04-01T00-00-00-0.xml") })
        // 7-Suomi_2/: tasan yksi tiedosto
        assertEquals(1, remaining.count { it.startsWith("7-Suomi_2/") })
    }

    @Test
    fun `removeDuplicates ei poista XML-versioiden yhteisia assetteja`() {
        s3Client.putObject(
            { it.bucket(TEST_BUCKET).key("42-Suomi_alkeet/2026-01-01T00-00-00-0 assets/a.mp3") },
            RequestBody.fromString("aanitiedosto"),
        )
        s3Client.putObject(
            { it.bucket(TEST_BUCKET).key("42-Suomi_alkeet/2026-02-01T00-00-00-0 assets/a.mp3") },
            RequestBody.fromString("aanitiedosto"),
        )

        val result = tehtavapankkiService.removeDuplicates()

        assertEquals(emptyList(), result!!.deleted)
        assertEquals(
            2,
            s3Client
                .listObjectsV2 { it.bucket(TEST_BUCKET) }
                .contents()
                .size,
        )
    }

    @Test
    fun `removeDuplicates ei poista mitaan kun bucket on tyhja`() {
        val result = tehtavapankkiService.removeDuplicates()

        assertNotNull(result)
        assertEquals(0, result.scanned)
        assertEquals(emptyList(), result.deleted)
    }

    @Test
    fun `uploadAssets purkaa upotetut tiedostot omiksi S3-objekteiksi`() {
        val xmlBytes =
            ClassPathResource("kotoutumiskoulutus/tehtavapankki/tehtavapankki-fixture.xml")
                .inputStream
                .use { it.readBytes() }
        val xmlKey = "42-Suomi/2026-01-01.xml"
        s3Client.putObject(
            { it.bucket(TEST_BUCKET).key(xmlKey) },
            RequestBody.fromBytes(xmlBytes),
        )

        // Lasketaan odotettu sisältö parsimalla XML samalla parserilla.
        val parsedFiles =
            TehtavapankkiXmlParser()
                .parse(xmlBytes.inputStream())
                .let { (it as Either.Right).value }
                .questions
                .filterIsInstance<DescriptionQuestion>()
                .flatMap { it.questiontext?.embeddedFiles.orEmpty() } +
                TehtavapankkiXmlParser()
                    .parse(xmlBytes.inputStream())
                    .let { (it as Either.Right).value }
                    .questions
                    .filterIsInstance<MultichoiceQuestion>()
                    .flatMap { it.questiontext?.embeddedFiles.orEmpty() }
        val expectedByName = parsedFiles.associate { it.name to Base64.getMimeDecoder().decode(it.content) }
        assertTrue(expectedByName.keys.any { it.endsWith(".png") })
        assertTrue(expectedByName.keys.any { it.endsWith(".mp3") })

        val result =
            tehtavapankkiService
                .fetchAndParseFromS3(xmlKey)
                .map { quiz -> tehtavapankkiService.uploadAssets(xmlKey, quiz) }

        assertIs<Either.Right<AssetExtractResult>>(result)
        assertEquals(emptyList(), result.value.failed)
        assertEquals(expectedByName.size, result.value.uploadedAssets.size)

        val expectedKeys = expectedByName.keys.map { "42-Suomi/2026-01-01 assets/$it" }.toSet()
        assertEquals(expectedKeys, result.value.uploadedAssets.toSet())

        // Verify a couple of the assets actually contain the decoded bytes.
        expectedByName.forEach { (name, expectedBytes) ->
            val downloaded =
                s3Client
                    .getObject { it.bucket(TEST_BUCKET).key("42-Suomi/2026-01-01 assets/$name") }
                    .readAllBytes()
            assertEquals(
                expectedBytes.size,
                downloaded.size,
                "Asset $name byte length mismatch",
            )
            assertTrue(expectedBytes.contentEquals(downloaded), "Asset $name content mismatch")
        }
    }

    @Test
    fun `fetchAndParseFromS3 palauttaa NotFound-virheen tuntemattomalle xml-avaimelle`() {
        val result = tehtavapankkiService.fetchAndParseFromS3("ei-olemassa.xml")

        assertIs<Either.Left<TehtavapankkiParseError>>(result)
        assertEquals(TehtavapankkiParseError.NotFound, result.value)
    }

    @Test
    fun `uploadAssets ylikirjoittaa olemassaolevan asset-objektin`() {
        val xmlBytes =
            ClassPathResource("kotoutumiskoulutus/tehtavapankki/tehtavapankki-fixture.xml")
                .inputStream
                .use { it.readBytes() }
        val xmlKey = "42-Suomi/2026-01-01.xml"
        val pngAssetKey = "42-Suomi/2026-01-01 assets/image.png"
        s3Client.putObject(
            { it.bucket(TEST_BUCKET).key(xmlKey) },
            RequestBody.fromBytes(xmlBytes),
        )
        s3Client.putObject(
            { it.bucket(TEST_BUCKET).key(pngAssetKey) },
            RequestBody.fromString("vanhaa-roskaa"),
        )

        val result =
            tehtavapankkiService
                .fetchAndParseFromS3(xmlKey)
                .map { quiz -> tehtavapankkiService.uploadAssets(xmlKey, quiz) }

        assertIs<Either.Right<AssetExtractResult>>(result)
        val downloaded =
            s3Client
                .getObject { it.bucket(TEST_BUCKET).key(pngAssetKey) }
                .readAllBytes()
        assertTrue(
            downloaded.size > 1000,
            "Asset olisi pitänyt ylikirjoittaa oikealla png-sisällöllä, oli ${downloaded.size} tavua",
        )
        assertTrue(
            !"vanhaa-roskaa".toByteArray().contentEquals(downloaded),
            "Roskasisällön ei pitäisi enää olla S3:ssa",
        )
    }

    @Test
    fun `removeDuplicates ei vertaa eri kansioiden valilla`() {
        val sameContent = "<quiz><q id=\"1\"/></quiz>"

        s3Client.putObject(
            { it.bucket(TEST_BUCKET).key("42-Suomi_alkeet/2026-01-01T00-00-00-0.xml") },
            RequestBody.fromString(sameContent),
        )
        s3Client.putObject(
            { it.bucket(TEST_BUCKET).key("7-Suomi_2/2026-01-01T00-00-00-0.xml") },
            RequestBody.fromString(sameContent),
        )

        val result = tehtavapankkiService.removeDuplicates()

        assertNotNull(result)
        assertEquals(2, result.scanned)
        assertEquals(emptyList(), result.deleted, "Eri kansioiden samansisältöisiä ei pitäisi poistaa")
    }

    @Test
    fun `uploadAssets vapauttaa upotettujen tiedostojen base64-sisallon`() {
        val xmlBytes =
            ClassPathResource("kotoutumiskoulutus/tehtavapankki/tehtavapankki-fixture.xml")
                .inputStream
                .use { it.readBytes() }
        val quiz =
            TehtavapankkiXmlParser()
                .parse(xmlBytes.inputStream())
                .let { (it as Either.Right).value }
        val files = quiz.allEmbeddedFiles()
        assertTrue(files.isNotEmpty(), "Fixturissa pitäisi olla upotettuja tiedostoja")
        assertTrue(
            files.all { it.content.isNotEmpty() },
            "Parsitussa quizissa pitäisi olla base64-sisältö ennen vientiä",
        )

        val result = tehtavapankkiService.uploadAssets("42-Suomi/2026-01-01.xml", quiz)

        assertEquals(emptyList(), result.failed)
        assertTrue(
            files.all { it.content.isEmpty() },
            "Base64-sisällön pitäisi vapautua heti S3-viennin jälkeen",
        )
    }
}
