package fi.oph.kitu.kotoutumiskoulutus.koealusta.tehtavapankki

import com.fasterxml.jackson.annotation.JsonProperty
import fi.oph.kitu.restclient.withJacksonStreamMaxStringLength
import fi.oph.kitu.util.defaultObjectMapper
import fi.oph.kitu.util.toJsonNode
import io.opentelemetry.api.trace.Span
import io.opentelemetry.instrumentation.annotations.WithSpan
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Profile
import org.springframework.http.MediaType
import org.springframework.stereotype.Service
import org.springframework.web.client.RestClient
import org.springframework.web.client.RestClientResponseException
import org.springframework.web.client.toEntity
import org.springframework.web.util.UriComponentsBuilder
import tools.jackson.databind.JsonNode
import java.io.BufferedInputStream
import java.io.InputStream
import java.net.URI
import java.nio.file.Files
import java.time.Instant

@Service
@Profile("!local-opintopolku")
class TehtavapankkiClientImpl(
    val restClientBuilder: RestClient.Builder,
) : TehtavapankkiClient {
    @Value($$"${kitu.kotoutumiskoulutus.koealusta.wstoken}")
    lateinit var koealustaToken: String

    @Value($$"${kitu.kotoutumiskoulutus.koealusta.baseurl}")
    lateinit var koealustaBaseUrl: String

    private val restClient by lazy {
        restClientBuilder
            .baseUrl(koealustaBaseUrl)
            .withJacksonStreamMaxStringLength(200_000_000)
            .build()
    }

    @WithSpan
    override fun listQuestionBanks(): List<QuestionBankMetadata> {
        Span.current().setAttribute("function", "local_completion_export_export_question_bank")
        val tree =
            restClient
                .get()
                .uri(
                    "/webservice/rest/server.php?wstoken={token}&moodlewsrestformat=json&wsfunction={function}",
                    mapOf<String, Any>(
                        "token" to koealustaToken,
                        "function" to "local_completion_export_export_question_bank",
                    ),
                ).accept(MediaType.APPLICATION_JSON)
                .retrieve()
                .toEntity<JsonNode>()
                .body
                ?: throw TehtavapankkiDownloadException("Tehtäväpankkilistan haku palautti tyhjän vastauksen")
        if (!tree.has("questionbanks")) {
            val detail = moodleErrorDetail(tree) ?: tree.toString().take(200)
            throw TehtavapankkiDownloadException("Tehtäväpankkilistan haku epäonnistui: $detail")
        }
        val raw = defaultObjectMapper.treeToValue(tree, RawTehtavapankkiResponse::class.java)
        return raw.questionbanks.map { qb ->
            QuestionBankMetadata(
                courseId = qb.courseid,
                courseName = qb.coursename,
                published = Instant.ofEpochSecond(qb.coursestartdate),
                generated = Instant.ofEpochSecond(qb.filegenerated),
                version = qb.questionbankversion,
                language = qb.language,
                downloadUrl = qb.downloadurl,
            )
        }
    }

    @WithSpan
    override fun downloadXml(meta: QuestionBankMetadata): XmlSource {
        val uri =
            UriComponentsBuilder
                .fromUriString(meta.downloadUrl)
                .queryParam("token", koealustaToken)
                .build()
                .toUri()
        return FileXmlSource(spoolToTempFile(uri, meta.downloadUrl), deleteOnClose = true)
    }

    private fun spoolToTempFile(
        uri: URI,
        naytettavaOsoite: String,
    ) = Files.createTempFile("tehtavapankki-", ".xml").apply {
        try {
            restClient.get().uri(uri).exchange { _, response ->
                if (!response.statusCode.is2xxSuccessful) {
                    throw RestClientResponseException(
                        "Tehtäväpankin XML-lataus epäonnistui ($naytettavaOsoite): ${response.statusCode}",
                        response.statusCode,
                        response.statusText,
                        response.headers,
                        null,
                        null,
                    )
                }
                response.body.buffered().use { body ->
                    if (!startsWithXml(body)) {
                        throw nonXmlDownloadException(naytettavaOsoite, body)
                    }
                    Files.newOutputStream(this).use { out -> body.copyTo(out) }
                }
            }
        } catch (e: Throwable) {
            Files.deleteIfExists(this)
            throw e
        }
    }

    private fun startsWithXml(input: BufferedInputStream): Boolean {
        val limit = 64
        input.mark(limit)
        try {
            var b = input.read()
            var read = 1
            if (b == 0xEF) {
                val b2 = input.read()
                val b3 = input.read()
                read += 2
                if (b2 != 0xBB || b3 != 0xBF) return false
                b = input.read()
                read++
            }
            while (read < limit && (b == ' '.code || b == '\t'.code || b == '\n'.code || b == '\r'.code)) {
                b = input.read()
                read++
            }
            return b == '<'.code
        } finally {
            input.reset()
        }
    }

    private fun nonXmlDownloadException(
        naytettavaOsoite: String,
        body: InputStream,
    ): TehtavapankkiDownloadException {
        val snippet = body.readNBytes(2_000).toString(Charsets.UTF_8)
        val detail = moodleErrorDetail(snippet.toJsonNode()) ?: snippet.take(200)
        return TehtavapankkiDownloadException(
            "Tehtäväpankin XML-lataus ($naytettavaOsoite) palautti ei-XML-sisältöä: $detail",
        )
    }

    private fun moodleErrorDetail(node: JsonNode): String? {
        val errorcode = node.get("errorcode")?.takeIf { it.isValueNode }?.asString()
        val message =
            node.get("error")?.takeIf { it.isValueNode }?.asString()
                ?: node.get("message")?.takeIf { it.isValueNode }?.asString()
        return listOfNotNull(errorcode?.let { "errorcode=$it" }, message)
            .joinToString(" ")
            .ifBlank { null }
    }

    private data class RawTehtavapankkiResponse(
        @param:JsonProperty("questionbanks")
        val questionbanks: List<RawQuestionbank>,
    )

    private data class RawQuestionbank(
        @param:JsonProperty("courseid")
        val courseid: Int,
        @param:JsonProperty("coursename")
        val coursename: String,
        @param:JsonProperty("coursestartdate")
        val coursestartdate: Long,
        @param:JsonProperty("questionbankversion")
        val questionbankversion: String,
        @param:JsonProperty("language")
        val language: String,
        @param:JsonProperty("downloadurl")
        val downloadurl: String,
        @param:JsonProperty("filegenerated")
        val filegenerated: Long,
    )
}
