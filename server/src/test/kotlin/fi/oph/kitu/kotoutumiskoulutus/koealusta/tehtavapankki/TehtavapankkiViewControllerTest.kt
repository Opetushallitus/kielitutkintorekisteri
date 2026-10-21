package fi.oph.kitu.kotoutumiskoulutus.koealusta.tehtavapankki

import arrow.core.Either
import fi.oph.kitu.DBContainerConfiguration
import fi.oph.kitu.LocalStackContainerConfiguration
import fi.oph.kitu.LocalStackContainerConfiguration.Companion.TEST_BUCKET
import fi.oph.kitu.tehtavapankki.TehtavapakettiEntity
import fi.oph.kitu.tehtavapankki.TehtavapankkiRepository
import fi.oph.kitu.virkailijaSession
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.core.io.ClassPathResource
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.security.test.web.servlet.setup.SecurityMockMvcConfigurers.springSecurity
import org.springframework.test.context.TestPropertySource
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.header
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.status
import org.springframework.test.web.servlet.setup.DefaultMockMvcBuilder
import org.springframework.test.web.servlet.setup.MockMvcBuilders
import org.springframework.web.context.WebApplicationContext
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.S3Client
import kotlin.test.assertContains
import kotlin.test.assertFalse
import kotlin.test.assertNotNull

@SpringBootTest
@Import(DBContainerConfiguration::class, LocalStackContainerConfiguration::class)
@TestPropertySource(properties = ["spring.cloud.aws.s3.enabled=true"])
class TehtavapankkiViewControllerTest(
    @param:Autowired private val context: WebApplicationContext,
    @param:Autowired private val s3Client: S3Client,
    @param:Autowired private val ingestService: TehtavapankkiIngestService,
    @param:Autowired private val repository: TehtavapankkiRepository,
    @param:Autowired private val jdbc: JdbcTemplate,
) {
    private lateinit var mockMvc: MockMvc

    @BeforeEach
    fun setup() {
        s3Client
            .listObjectsV2 { it.bucket(TEST_BUCKET) }
            .contents()
            .forEach { obj ->
                s3Client.deleteObject { it.bucket(TEST_BUCKET).key(obj.key()) }
            }
        jdbc.execute("DELETE FROM tehtavapaketti")

        mockMvc =
            MockMvcBuilders
                .webAppContextSetup(context)
                .apply<DefaultMockMvcBuilder>(springSecurity())
                .build()
    }

    @Test
    fun `listView nayttaa bucketin tehtavapaketit`() {
        s3Client.putObject(
            { it.bucket(TEST_BUCKET).key("42-Suomi_alkeet/2026-01-01T00:00:00-0.xml") },
            RequestBody.fromString("<questions/>"),
        )
        s3Client.putObject(
            { it.bucket(TEST_BUCKET).key("7-Suomi_2/2026-02-02T00:00:00-0.xml") },
            RequestBody.fromString("<questions/>"),
        )

        val response =
            mockMvc
                .perform(get("/koto-tehtavapankki").session(virkailijaSession()))
                .andExpect(status().isOk)
                .andReturn()
                .response.contentAsString

        assertContains(response, "42-Suomi_alkeet")
        assertContains(response, "7-Suomi_2")
    }

    @Test
    fun `listView nayttaa tyhjan listan kun bucketissa ei ole objekteja`() {
        val response =
            mockMvc
                .perform(get("/koto-tehtavapankki").session(virkailijaSession()))
                .andExpect(status().isOk)
                .andReturn()
                .response.contentAsString

        assertContains(response, "Ei testipaketteja.")
    }

    @Test
    fun `downloadRedirect ohjaa signed URLiin`() {
        val key = "42-Suomi_alkeet/2026-01-01T00:00:00-0.xml"
        s3Client.putObject(
            { it.bucket(TEST_BUCKET).key(key) },
            RequestBody.fromString("<questions/>"),
        )

        val location =
            mockMvc
                .perform(get("/koto-tehtavapankki/lataa").param("key", key).session(virkailijaSession()))
                .andExpect(status().isFound)
                .andExpect(header().exists("Location"))
                .andReturn()
                .response
                .getHeader("Location")

        assertNotNull(location)
        assertContains(location, TEST_BUCKET)
    }

    @Test
    fun `downloadRedirect palauttaa 404 kun avainta ei loydy`() {
        val response =
            mockMvc
                .perform(get("/koto-tehtavapankki/lataa").param("key", "ei-olemassa.xml").session(virkailijaSession()))
                .andExpect(status().isNotFound)
                .andReturn()
                .response.contentAsString

        assertContains(response, "Sivua ei löydy")
    }

    @Test
    fun `pakettiView nayttaa paketin sisallon ja kirjoittaa PLUGINFILE-viittaukset uudelleen`() {
        val pakettiId = ingestFixture()

        val response =
            mockMvc
                .perform(get("/koto-tehtavapankki/paketti/$pakettiId").session(virkailijaSession()))
                .andExpect(status().isOk)
                .andReturn()
                .response.contentAsString

        // Header
        assertContains(response, "Suomi alkeet")
        assertContains(response, "moodle.koealusta")
        // Both ryhmät from the fixture
        assertContains(response, "(tyhjä nimi)")
        assertContains(response, "Suomen kieli A2 - esimerkkitehtävät")
        // At least one tehtävä body content survived
        assertContains(response, "Mikä on Suomen pääkaupunki?")
        // Tehtävätyypit suomennettuina
        assertContains(response, "Monivalinta")
        assertContains(response, "Ääninauhoitus")
        // Asset filename appears as a download link
        assertContains(response, "image.png")
        assertContains(response, "A2 Tietokonetuki äänitiedosto.mp3")
        // @@PLUGINFILE@@ references rewritten to /lataa endpoints
        assertFalse(
            response.contains("@@PLUGINFILE@@"),
            "Sivulla ei pitäisi olla raakoja @@PLUGINFILE@@-viittauksia, oli: ${response.lines().filter {
                it.contains(
                    "@@PLUGINFILE@@",
                )
            }}",
        )
        assertContains(response, "/koto-tehtavapankki/lataa")
    }

    @Test
    fun `pakettiView sanitoi Moodlen HTML-sisallon`() {
        val pakettiId =
            ingest("kotoutumiskoulutus/tehtavapankki/tehtavapankki-xss-fixture.xml", "99-XSS_testi/2026-01-01.xml")

        val response =
            mockMvc
                .perform(get("/koto-tehtavapankki/paketti/$pakettiId").session(virkailijaSession()))
                .andExpect(status().isOk)
                .andReturn()
                .response.contentAsString

        assertContains(response, "Turvallinen teksti alkaa tästä.")
        assertFalse(response.contains("onerror"), "onerror-attribuutin olisi pitänyt poistua")
        assertFalse(response.contains("window.alert"), "script-sisällön olisi pitänyt poistua")
    }

    @Test
    fun `pakettiView palauttaa 404 kun id ta ei loydy`() {
        val response =
            mockMvc
                .perform(get("/koto-tehtavapankki/paketti/9999").session(virkailijaSession()))
                .andExpect(status().isNotFound)
                .andReturn()
                .response.contentAsString

        assertContains(response, "Sivua ei löydy")
    }

    @Test
    fun `listView nayttaa Nayta sisalto -linkin paketeille jotka on tallessa tietokannassa`() {
        val pakettiId = ingestFixture()
        val xmlKey = repository.findPakettiById(pakettiId)!!.s3Avain!!

        val response =
            mockMvc
                .perform(get("/koto-tehtavapankki").session(virkailijaSession()))
                .andExpect(status().isOk)
                .andReturn()
                .response.contentAsString

        assertContains(response, xmlKey.substringBefore("/"))
        assertContains(response, "Näytä sisältö")
        assertContains(response, "/koto-tehtavapankki/paketti/$pakettiId")
    }

    @Test
    fun `listView nayttaa kullekin saman courseid_n kansiolle sen oman lahdeversion`() {
        val esimerkkiKey = "17-Kielitesti_esimerkki/2026-01-01T00:00:00-0.xml"
        val pilotointiKey = "17-Kielitesti_opettaja_pilotointi/2026-02-02T00:00:00-0.xml"
        s3Client.putObject(
            { it.bucket(TEST_BUCKET).key(esimerkkiKey) },
            RequestBody.fromString("<questions/>"),
        )
        s3Client.putObject(
            { it.bucket(TEST_BUCKET).key(pilotointiKey) },
            RequestBody.fromString("<questions/>"),
        )
        repository.insertPaketti(
            TehtavapakettiEntity(
                lahdejarjestelma = TehtavapankkiIngestService.LAHDEJARJESTELMA,
                lahdeId = "17",
                nimi = "Kielitesti esimerkki",
                versioHash = "hash-esimerkki",
                s3Avain = esimerkkiKey,
                lahdeVersion = "vEsimerkki",
            ),
        )
        repository.insertPaketti(
            TehtavapakettiEntity(
                lahdejarjestelma = TehtavapankkiIngestService.LAHDEJARJESTELMA,
                lahdeId = "17",
                nimi = "Kielitesti opettaja pilotointi",
                versioHash = "hash-pilotointi",
                s3Avain = pilotointiKey,
                lahdeVersion = "vPilotointi",
            ),
        )

        val response =
            mockMvc
                .perform(get("/koto-tehtavapankki").session(virkailijaSession()))
                .andExpect(status().isOk)
                .andReturn()
                .response.contentAsString

        // Saman courseid:n kummankin kansion tiivistelmä näyttää oman versionsa
        // eikä courseid:n viimeisintä.
        assertContains(response, "versio vEsimerkki")
        assertContains(response, "versio vPilotointi")
    }

    private fun ingestFixture(): Int =
        ingest("kotoutumiskoulutus/tehtavapankki/tehtavapankki-fixture.xml", "42-Suomi_alkeet/2026-01-01.xml")

    private fun ingest(
        resource: String,
        xmlKey: String,
    ): Int {
        val xmlBytes = ClassPathResource(resource).inputStream.use { it.readBytes() }
        s3Client.putObject(
            { it.bucket(TEST_BUCKET).key(xmlKey) },
            RequestBody.fromBytes(xmlBytes),
        )
        val result = ingestService.ingestFromS3(xmlKey)
        return (result as Either.Right).value.id!!
    }
}
