package fi.oph.kitu.kotoutumiskoulutus.koealusta.tehtavapankki

import io.opentelemetry.api.trace.Span
import io.opentelemetry.instrumentation.annotations.WithSpan
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.stereotype.Service

@Service
@ConditionalOnProperty(
    name = ["spring.cloud.aws.s3.enabled"],
    havingValue = "true",
    matchIfMissing = true,
)
class TehtavapankkiImportService(
    private val tehtavapankkiService: TehtavapankkiService,
    private val ingestService: TehtavapankkiIngestService,
) {
    /**
     * Ajastetun tehtävän kokonaisuus: lataa tehtäväpankit S3:een, siivoaa
     * duplikaatit ja ingestoi kunkin kurssin uusimman XML:n tietokantaan.
     * Yhden paketin epäonnistuminen ei estä muiden ingestiä; jos yksikin
     * lataus tai ingest epäonnistui, heitetään lopuksi
     * [TehtavapankkiImportException] jotta tehtävä menee FAILED-tilaan.
     */
    @WithSpan
    fun importAndIngest() {
        val downloadFailures = tehtavapankkiService.importTehtavapankki()
        // Jokainen siirto saa uuden avaimen, vaikka sisältö olisi ennallaan:
        // poistetaan kunkin kurssin sisällä saman sisältöiset objektit,
        // jotta bucket ei kasva turhaan.
        tehtavapankkiService.removeDuplicates()
        // Käydään kunkin kurssin uusimmat XML:t läpi: puretaan upotetut
        // <file>-blobit erillisiksi S3-objekteiksi (mp3-/png-assetit) ja
        // tallennetaan parsittu sisältö yleiseen tehtäväpankki-skeemaan.
        val ingestFailures =
            tehtavapankkiService
                .listTehtavapaketit()
                .values
                .mapNotNull { it.firstOrNull() }
                .mapNotNull { obj -> ingestOne(obj.key) }

        Span.current().setAttribute("import.download_failures", downloadFailures.size.toLong())
        Span.current().setAttribute("import.ingest_failures", ingestFailures.size.toLong())

        if (downloadFailures.isNotEmpty() || ingestFailures.isNotEmpty()) {
            throw TehtavapankkiImportException(failureMessage(downloadFailures, ingestFailures))
        }
    }

    private fun ingestOne(key: String): Pair<String, String>? =
        try {
            ingestService.ingestFromS3(key).leftOrNull()?.let { key to it.describe() }
        } catch (e: Exception) {
            Span.current().setAttribute("ingest.threw", true)
            key to (e.message ?: e::class.simpleName ?: "tuntematon virhe")
        }

    private fun failureMessage(
        downloadFailures: List<TehtavapankkiDownloadFailure>,
        ingestFailures: List<Pair<String, String>>,
    ): String {
        val parts = mutableListOf<String>()
        if (downloadFailures.isNotEmpty()) {
            parts += "latausvirheet=" + downloadFailures.joinToString("; ") { "kurssi ${it.courseId}: ${it.reason}" }
        }
        if (ingestFailures.isNotEmpty()) {
            parts += "ingest-virheet=" + ingestFailures.joinToString("; ") { (key, reason) -> "$key: $reason" }
        }
        return "Tehtäväpankin tuonti epäonnistui (${parts.joinToString(", ")})"
    }

    private fun TehtavapankkiParseError.describe(): String =
        when (this) {
            is TehtavapankkiParseError.NotFound -> "ei löytynyt"
            is TehtavapankkiParseError.InvalidXml -> "virheellinen XML: ${cause.message ?: cause::class.simpleName}"
            is TehtavapankkiParseError.IO -> "IO-virhe: ${cause.message ?: cause::class.simpleName}"
        }
}
