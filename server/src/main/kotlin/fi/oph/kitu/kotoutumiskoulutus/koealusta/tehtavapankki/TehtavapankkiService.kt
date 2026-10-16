package fi.oph.kitu.kotoutumiskoulutus.koealusta.tehtavapankki

import arrow.core.Either
import arrow.core.left
import arrow.core.right
import fi.oph.kitu.tehtavapankki.TehtavapankkiRepository
import fi.oph.kitu.util.TimeService
import io.awspring.cloud.s3.ObjectMetadata
import io.awspring.cloud.s3.S3Template
import io.opentelemetry.api.trace.Span
import io.opentelemetry.api.trace.Tracer
import io.opentelemetry.instrumentation.annotations.WithSpan
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.stereotype.Service
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.NoSuchKeyException
import java.io.ByteArrayInputStream
import java.net.URL
import java.nio.file.Files
import java.nio.file.Path
import java.time.Instant
import java.time.format.DateTimeFormatter
import java.util.Base64

// Wired ellei spring.cloud.aws.s3.enabled ole false. Bean luodaan
// myös testeissä ja e2e:ssä kun ne osoittavat LocalStackiin; ajastettu
// import gateataan erikseen TehtavapankkiScheduledTasksissa.
@Service
@ConditionalOnProperty(
    name = ["spring.cloud.aws.s3.enabled"],
    havingValue = "true",
    matchIfMissing = true,
)
class TehtavapankkiService(
    private val client: TehtavapankkiClient,
    private val s3Template: S3Template,
    private val s3Client: S3Client,
    private val parser: TehtavapankkiXmlParser,
    private val repository: TehtavapankkiRepository,
    private val timeService: TimeService,
) {
    @Value($$"${kitu.kotoutumiskoulutus.tehtavapankki.bucket:#{null}}")
    var bucket: String? = null

    companion object {
        // S3 user metadata -avaimet — tallennetaan x-amz-meta-* otsikoiksi.
        // AWS lower-case:ttaa avaimet header-tasolla, joten pidetään
        // pikkukirjaimisina jotta vertailu toimii ilman normalisointia.
        // Arvo on epoch-sekunteja; otsikon nimen -ms-pääte on historiallinen
        // eikä sitä voi vaihtaa rikkomatta jo ladattujen objektien lukua.
        const val S3_META_PUBLISHED: String = "lahde-published-ms"
        const val S3_META_VERSION: String = "lahde-version"
        const val S3_META_LANGUAGE: String = "lahde-language"
    }

    fun sanitizeFilename(string: String) =
        string
            .replace(' ', '_')
            .replace(Regex("\\W+"), "")
            .take(128)

    @WithSpan
    fun uploadTehtavapankki(downloads: List<QuestionBankDownload>) {
        val now =
            timeService
                .now()
                .atZone(TimeService.zoneId)
                .toLocalDateTime()
                .format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
        downloads.forEachIndexed { index, download ->
            val meta = download.metadata
            val sanitizedCoursename = sanitizeFilename(meta.courseName)
            // Avain sisältää sekä lataushetken aikaleiman (lukukelpoinen) että
            // Koealustan filegenerated epoch-sekunnit (parsittava), jotta ingest
            // voi tallentaa lähdejärjestelmän version per paketti.
            val filename =
                "${meta.courseId}-$sanitizedCoursename/$now-fg${meta.generated.epochSecond}-$index.xml"
            // Lähdejärjestelmän muu metadata kulkee S3-objektin
            // user metadata -otsikoissa, jotta ingest näkee sen
            // tarvitsematta erillistä tilaa tai sidecar-tiedostoa.
            val objectMetadata =
                ObjectMetadata
                    .builder()
                    .metadata(S3_META_PUBLISHED, meta.published.epochSecond.toString())
                    .metadata(S3_META_VERSION, meta.version)
                    .metadata(S3_META_LANGUAGE, meta.language)
                    .build()

            download.use { d ->
                d.xml.openStream().use { stream ->
                    useS3 { bucketName ->
                        upload(bucketName, filename, stream, objectMetadata)
                    }
                }
            }
        }
    }

    @WithSpan
    fun fetchS3UserMetadata(key: String): Map<String, String> {
        Span.current().setAttribute("s3.key", key)
        return useS3 { bucket ->
            try {
                s3Client.headObject { it.bucket(bucket).key(key) }.metadata()
            } catch (_: NoSuchKeyException) {
                emptyMap()
            }
        }.orEmpty()
    }

    @WithSpan
    fun importTehtavapankki(): List<TehtavapankkiDownloadFailure> {
        val metas = client.listQuestionBanks()
        var skipped = 0
        var skippedEmptyFilename = 0
        val downloads = mutableListOf<QuestionBankDownload>()
        val failures = mutableListOf<TehtavapankkiDownloadFailure>()
        try {
            metas.forEach { meta ->
                if (meta.filename().isBlank()) {
                    skippedEmptyFilename++
                    return@forEach
                }
                val prev =
                    repository.findLatestFilegeneratedBySource(
                        TehtavapankkiIngestService.LAHDEJARJESTELMA,
                        meta.courseId.toString(),
                    )
                if (prev != null && prev.toInstant() == meta.generated) {
                    skipped++
                } else {
                    try {
                        downloads += QuestionBankDownload(meta, client.downloadXml(meta))
                    } catch (e: TehtavapankkiDownloadException) {
                        failures += TehtavapankkiDownloadFailure(meta.courseId, e.message ?: "lataus epäonnistui")
                    }
                }
            }
        } catch (e: Throwable) {
            downloads.forEach { runCatching { it.close() } }
            throw e
        }
        Span.current().setAttribute("tehtavapankki.metas.count", metas.size.toLong())
        Span.current().setAttribute("tehtavapankki.skipped.count", skipped.toLong())
        Span.current().setAttribute("tehtavapankki.skipped_empty_filename.count", skippedEmptyFilename.toLong())
        Span.current().setAttribute("tehtavapankki.downloaded.count", downloads.size.toLong())
        Span.current().setAttribute("tehtavapankki.failed.count", failures.size.toLong())
        uploadTehtavapankki(downloads)
        return failures
    }

    @WithSpan
    fun listTehtavapaketit(): Map<String, List<TehtavapakettiObject>> = groupTehtavapaketit(listAllObjects())

    @WithSpan
    fun countTehtavapaketit(): Long = listTehtavapaketit().size.toLong()

    @WithSpan
    fun listAllObjects(): List<TehtavapakettiObject> =
        useS3 { bucket ->
            listAllObjects(bucket).map {
                TehtavapakettiObject(
                    key = it.location.`object`,
                    filename = it.filename,
                    size = it.contentLength(),
                    timestamp = Instant.ofEpochMilli(it.lastModified()),
                )
            }
        }.orEmpty()

    @WithSpan
    fun getTemporaryDownloadUrl(key: String): URL? =
        useS3 { bucket ->
            if (objectExists(bucket, key)) {
                createSignedGetURL(bucket, key, java.time.Duration.ofMinutes(10))
            } else {
                null
            }
        }

    /**
     * Poistaa kustakin "kansiosta" (avaimen `/`-prefiksi) duplikaatti-objektit:
     * objektit, joilla on sama koko ja ETag (eli sama sisältö). Vanhin pidetään,
     * loput poistetaan. Eri kansioiden välillä ei verrata.
     */
    @WithSpan
    fun removeDuplicates(): CleanupResult? =
        useS3 { bucket ->
            val objects =
                s3Client
                    .listObjectsV2Paginator { it.bucket(bucket) }
                    .contents()
                    .toList()

            val toDelete =
                objects
                    .filter { it.key().contains("/") }
                    .groupBy { it.key().substringBefore("/") }
                    .flatMap { (_, folderObjects) ->
                        folderObjects
                            .groupBy { it.size() to it.eTag().normalizeETag() }
                            .filter { (_, dupes) -> dupes.size > 1 }
                            .flatMap { (_, dupes) ->
                                dupes.sortedBy { it.lastModified() }.drop(1)
                            }
                    }

            toDelete.forEach { obj ->
                s3Client.deleteObject { it.bucket(bucket).key(obj.key()) }
            }

            Span.current().setAttribute("scanned", objects.size.toLong())
            Span.current().setAttribute("deleted", toDelete.size.toLong())

            CleanupResult(
                scanned = objects.size,
                deleted = toDelete.map { it.key() },
            )
        }

    /**
     * Kirjoittaa XML:n sisällä olevat upotetut <file>-blobit erillisinä S3-objekteina.
     * Avain muodostetaan XML:n sijainnista:
     * `{tehtäväpankki-kansio}/{xml-tiedoston basename} assets/{tiedoston nimi}`.
     * Esim. `42-Suomi/2026-01-01.xml` → `42-Suomi/2026-01-01 assets/audio.mp3`.
     *
     * Olemassaolevat assetit ylikirjoitetaan, jotta XML ja sen assetit pysyvät
     * synkassa. Yksittäisen tiedoston dekoodausvirhe ei keskeytä koko ajoa
     * vaan kirjataan tulokseen `failed`-listaan.
     *
     * Huom: [quiz] mutatoituu — kunkin tiedoston base64-sisältö vapautetaan
     * heti käsittelyn jälkeen, jottei koko mediamassa jää kekoon myös
     * tietokantatallennuksen ajaksi.
     */
    fun uploadAssets(
        xmlKey: String,
        quiz: TehtavapankkiQuiz,
    ): AssetExtractResult {
        val prefix = "${xmlKey.removeSuffix(".xml")} assets/"
        val uploaded = mutableListOf<String>()
        val failed = mutableListOf<FailedAsset>()
        val decoder = Base64.getMimeDecoder()

        quiz.allEmbeddedFiles().forEach { file ->
            try {
                if (file.name.isBlank()) return@forEach
                val key = "$prefix${file.name}"
                val bytes =
                    try {
                        decoder.decode(file.content)
                    } catch (e: IllegalArgumentException) {
                        failed += FailedAsset(file.name, e.message ?: "base64 decode failed")
                        return@forEach
                    }
                useS3 { bucket -> upload(bucket, key, ByteArrayInputStream(bytes)) }
                uploaded += key
            } finally {
                file.releaseContent()
            }
        }

        Span.current().setAttribute("assets.uploaded", uploaded.size.toLong())
        Span.current().setAttribute("assets.failed", failed.size.toLong())

        return AssetExtractResult(xmlKey, uploaded, failed)
    }

    /**
     * Lataa S3-objektin väliaikaistiedostoon streamaten, jottei isoa tehtäväpankkia
     * tarvitse materialisoida muistiin. Kutsujan vastuulla on poistaa tiedosto.
     */
    @WithSpan
    fun fetchToTempFile(key: String): Either<TehtavapankkiParseError, Path> {
        Span.current().setAttribute("s3.key", key)
        val resource =
            useS3 { bucket ->
                if (objectExists(bucket, key)) download(bucket, key) else null
            } ?: return TehtavapankkiParseError.NotFound.left()
        val tempFile = Files.createTempFile("tehtavapankki-ingest-", ".xml")
        return try {
            resource.inputStream.use { input ->
                Files.newOutputStream(tempFile).use { output -> input.copyTo(output) }
            }
            tempFile.right()
        } catch (e: Throwable) {
            Files.deleteIfExists(tempFile)
            TehtavapankkiParseError.IO(e).left()
        }
    }

    @WithSpan
    fun fetchAndParseFromS3(key: String): Either<TehtavapankkiParseError, TehtavapankkiQuiz> =
        when (val tempFile = fetchToTempFile(key)) {
            is Either.Right -> {
                try {
                    Files.newInputStream(tempFile.value).use { parser.parse(it) }
                } finally {
                    Files.deleteIfExists(tempFile.value)
                }
            }

            is Either.Left -> {
                tempFile.value.left()
            }
        }

    @WithSpan
    private fun <T> useS3(f: S3Template.(bucketName: String) -> T): T? {
        val bucketName = bucket?.trim()
        Span.current().setAttribute("dryRun", bucketName.isNullOrBlank())
        return bucketName?.let {
            f(s3Template, bucketName)
        }
    }
}

/**
 * Service-tason kapseli, joka pitää metadatan ja avoinna olevan XmlSourcen
 * yhdessä. Sulkemalla tämä sulkee XmlSourcen (esim. poistaa väliaikaistiedoston).
 */
data class QuestionBankDownload(
    val metadata: QuestionBankMetadata,
    val xml: XmlSource,
) : AutoCloseable {
    override fun close() = xml.close()
}

data class TehtavapankkiDownloadFailure(
    val courseId: Int,
    val reason: String,
)

data class TehtavapakettiObject(
    val key: String,
    val filename: String,
    val size: Long,
    val timestamp: Instant,
)

data class CleanupResult(
    val scanned: Int,
    val deleted: List<String>,
)

data class AssetExtractResult(
    val xmlKey: String,
    val uploadedAssets: List<String>,
    val failed: List<FailedAsset>,
)

data class FailedAsset(
    val filename: String,
    val reason: String,
)

internal fun groupTehtavapaketit(objects: List<TehtavapakettiObject>): Map<String, List<TehtavapakettiObject>> =
    objects
        .filter { it.filename.lowercase().endsWith(".xml") }
        .groupBy { it.filename.substringBefore("/") }
        .mapValues { (_, versions) -> versions.sortedByDescending { it.timestamp } }
        .entries
        .sortedByDescending { (_, versions) -> versions.first().timestamp }
        .associate { it.toPair() }

// AWS palauttaa ETagin lainausmerkeissä; karsitaan ne ennen vertailua.
private fun String.normalizeETag(): String = this.trim('"')

private fun QuestionBankMetadata.filename(): String = downloadUrl.substringBefore('?').substringAfterLast('/')

internal fun TehtavapankkiQuiz.allEmbeddedFiles(): List<EmbeddedFile> =
    questions.flatMap { question ->
        when (question) {
            is DescriptionQuestion -> question.questiontext?.embeddedFiles.orEmpty()
            is MultichoiceQuestion -> question.questiontext?.embeddedFiles.orEmpty()
            is ShortanswerQuestion -> question.questiontext?.embeddedFiles.orEmpty()
            is EssayQuestion -> question.questiontext?.embeddedFiles.orEmpty()
            is CloudpoodllQuestion -> question.questiontext?.embeddedFiles.orEmpty()
            is CategoryQuestion, is UnknownQuestion -> emptyList()
        }
    }
