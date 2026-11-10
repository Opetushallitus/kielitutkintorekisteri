package fi.oph.kitu.kotoutumiskoulutus.koealusta.tehtavapankki

import org.hamcrest.Matchers.startsWith
import org.junit.jupiter.api.Test
import org.springframework.http.MediaType
import org.springframework.test.web.client.MockRestServiceServer
import org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo
import org.springframework.test.web.client.response.MockRestResponseCreators.withException
import org.springframework.test.web.client.response.MockRestResponseCreators.withServerError
import org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess
import org.springframework.web.client.ResourceAccessException
import org.springframework.web.client.RestClient
import org.springframework.web.client.RestClientResponseException
import java.net.SocketTimeoutException
import java.time.Instant
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse

class TehtavapankkiClientImplTest {
    @Test
    fun `listQuestionBanks mappaa Moodlen vastauksen metadataksi`() {
        val builder = RestClient.builder()
        val mockServer = MockRestServiceServer.bindTo(builder).build()
        mockServer
            .expect(
                requestTo(
                    "https://example.test/koto/webservice/rest/server.php?" +
                        "wstoken=testitoken&moodlewsrestformat=json&" +
                        "wsfunction=local_completion_export_export_question_bank",
                ),
            ).andRespond(
                withSuccess(
                    """
                    {
                      "questionbanks": [
                        {
                          "courseid": 9,
                          "coursename": "Suomi 9",
                          "coursestartdate": 0,
                          "filegenerated": 1733400000,
                          "questionbankversion": "v1",
                          "language": "fin",
                          "downloadurl": "https://example.test/koto/pluginfile.php/9/qb.xml"
                        }
                      ]
                    }
                    """.trimIndent(),
                    MediaType.APPLICATION_JSON,
                ),
            )

        val client =
            TehtavapankkiClientImpl(builder).apply {
                koealustaToken = "testitoken"
                koealustaBaseUrl = "https://example.test/koto"
            }

        val metas = client.listQuestionBanks()
        assertEquals(1, metas.size)
        val meta = metas.single()
        assertEquals(9, meta.courseId)
        assertEquals("Suomi 9", meta.courseName)
        assertEquals("fin", meta.language)
        assertEquals(Instant.ofEpochSecond(1733400000), meta.generated)
        assertEquals(Instant.EPOCH, meta.published)
        assertEquals("https://example.test/koto/pluginfile.php/9/qb.xml", meta.downloadUrl)
    }

    @Test
    fun `downloadXml heittaa kun XML-latausvastaus on virhe`() {
        val builder = RestClient.builder()
        val mockServer = MockRestServiceServer.bindTo(builder).build()
        mockServer
            .expect(requestTo("https://example.test/koto/pluginfile.php/9/qb.xml?token=testitoken"))
            .andRespond(withServerError())

        val client =
            TehtavapankkiClientImpl(builder).apply {
                koealustaToken = "testitoken"
                koealustaBaseUrl = "https://example.test/koto"
            }

        val meta =
            QuestionBankMetadata(
                courseId = 9,
                courseName = "Suomi virheellinen",
                published = Instant.ofEpochMilli(0),
                generated = Instant.ofEpochMilli(0),
                version = "v1",
                language = "fin",
                downloadUrl = "https://example.test/koto/pluginfile.php/9/qb.xml",
            )

        val ex =
            assertFailsWith<RestClientResponseException> {
                client.downloadXml(meta)
            }
        assertFalse(ex.message.orEmpty().contains("testitoken"), ex.message)
    }

    @Test
    fun `yhteysvirheen viestissa ei ole tokenia`() {
        val builder = RestClient.builder()
        val mockServer = MockRestServiceServer.bindTo(builder).build()
        mockServer
            .expect(requestTo("https://example.test/koto/pluginfile.php/9/qb.xml?token=testitoken"))
            .andRespond(withException(SocketTimeoutException("Read timed out")))
        mockServer
            .expect(requestTo(startsWith("https://example.test/koto/webservice/rest/server.php?wstoken=testitoken")))
            .andRespond(withException(SocketTimeoutException("Read timed out")))

        val client =
            TehtavapankkiClientImpl(builder).apply {
                koealustaToken = "testitoken"
                koealustaBaseUrl = "https://example.test/koto"
            }
        val meta =
            QuestionBankMetadata(
                courseId = 9,
                courseName = "Suomi",
                published = Instant.ofEpochMilli(0),
                generated = Instant.ofEpochMilli(0),
                version = "v1",
                language = "fin",
                downloadUrl = "https://example.test/koto/pluginfile.php/9/qb.xml",
            )

        val lataus = assertFailsWith<ResourceAccessException> { client.downloadXml(meta) }
        val listaus = assertFailsWith<ResourceAccessException> { client.listQuestionBanks() }

        for (ex in listOf(lataus, listaus)) {
            assertFalse(ex.stackTraceToString().contains("testitoken"), ex.stackTraceToString())
            assertContains(ex.message.orEmpty(), "Read timed out")
        }
    }

    @Test
    fun `downloadXml heittaa kun XML-latausvastaus on Moodlen JSON-virhe`() {
        val builder = RestClient.builder()
        val mockServer = MockRestServiceServer.bindTo(builder).build()
        mockServer
            .expect(requestTo("https://example.test/koto/pluginfile.php/9/qb.xml?token=testitoken"))
            .andRespond(
                withSuccess(
                    """{"error":"Pääsyn hallinnan poikkeus","errorcode":"accessexception","stacktrace":null,"debuginfo":null,"reproductionlink":null}""",
                    MediaType.APPLICATION_JSON,
                ),
            )

        val client =
            TehtavapankkiClientImpl(builder).apply {
                koealustaToken = "testitoken"
                koealustaBaseUrl = "https://example.test/koto"
            }

        val meta =
            QuestionBankMetadata(
                courseId = 9,
                courseName = "Suomi virheellinen",
                published = Instant.ofEpochMilli(0),
                generated = Instant.ofEpochMilli(0),
                version = "v1",
                language = "fin",
                downloadUrl = "https://example.test/koto/pluginfile.php/9/qb.xml",
            )

        val ex =
            assertFailsWith<TehtavapankkiDownloadException> {
                client.downloadXml(meta)
            }
        assertContains(ex.message!!, "accessexception")
    }

    @Test
    fun `downloadXml onnistuu kun runko on XML BOMilla ja alku-whitespacella`() {
        val builder = RestClient.builder()
        val mockServer = MockRestServiceServer.bindTo(builder).build()
        val xml = "\uFEFF\n  <?xml version=\"1.0\"?><questions><q id=\"a\"/></questions>"
        mockServer
            .expect(requestTo("https://example.test/koto/pluginfile.php/9/qb.xml?token=testitoken"))
            .andRespond(withSuccess(xml, MediaType.APPLICATION_XML))

        val client =
            TehtavapankkiClientImpl(builder).apply {
                koealustaToken = "testitoken"
                koealustaBaseUrl = "https://example.test/koto"
            }

        val meta =
            QuestionBankMetadata(
                courseId = 9,
                courseName = "Suomi",
                published = Instant.ofEpochMilli(0),
                generated = Instant.ofEpochMilli(0),
                version = "v1",
                language = "fin",
                downloadUrl = "https://example.test/koto/pluginfile.php/9/qb.xml",
            )

        client.downloadXml(meta).use { source ->
            val content = source.openStream().use { it.readBytes().toString(Charsets.UTF_8) }
            assertEquals(xml, content)
        }
    }

    @Test
    fun `listQuestionBanks heittaa kun vastaus on Moodlen JSON-virhe`() {
        val builder = RestClient.builder()
        val mockServer = MockRestServiceServer.bindTo(builder).build()
        mockServer
            .expect(
                requestTo(
                    "https://example.test/koto/webservice/rest/server.php?" +
                        "wstoken=testitoken&moodlewsrestformat=json&" +
                        "wsfunction=local_completion_export_export_question_bank",
                ),
            ).andRespond(
                withSuccess(
                    """{"error":"Pääsyn hallinnan poikkeus","errorcode":"accessexception","stacktrace":null,"debuginfo":null,"reproductionlink":null}""",
                    MediaType.APPLICATION_JSON,
                ),
            )

        val client =
            TehtavapankkiClientImpl(builder).apply {
                koealustaToken = "testitoken"
                koealustaBaseUrl = "https://example.test/koto"
            }

        val ex =
            assertFailsWith<TehtavapankkiDownloadException> {
                client.listQuestionBanks()
            }
        assertContains(ex.message!!, "accessexception")
    }
}
