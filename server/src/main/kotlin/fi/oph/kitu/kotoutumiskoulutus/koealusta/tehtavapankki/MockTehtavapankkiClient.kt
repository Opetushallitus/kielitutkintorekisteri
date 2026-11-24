package fi.oph.kitu.kotoutumiskoulutus.koealusta.tehtavapankki

import org.springframework.context.annotation.Profile
import org.springframework.core.io.ClassPathResource
import org.springframework.stereotype.Service
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path
import java.time.Instant
import java.time.temporal.ChronoUnit

/**
 * Paikallisen kehityksen mock-klientti: palauttaa testidatana käytetyn
 * tehtäväpankki-fikstuurin sellaisenaan, jotta koko pipeline (S3 → dedup →
 * asset-purku → tietokannan ingest → näkymä) voi toimia ilman verkkoa
 * tai oikean Moodle-tunnuksen olemassaoloa.
 *
 * Asettamalla ympäristömuuttujan `MOCK_TEHTAVAPAKETTI_XML` voi käyttää
 * mielivaltaista paikallista XML-tiedostoa fikstuurin sijaan. Kurssin
 * nimi johdetaan tällöin tiedostonimestä ja sisältö luetaan streaminä —
 * tiedosto ei missään vaiheessa materialisoidu Java-merkkijonoksi, joten
 * useamman gigatavun XML-tiedostotkin toimivat.
 *
 * `filegenerated`-arvona käytetään fixture- tai custom-tiedoston
 * `lastModified`-aikaa, jotta importTehtavapankki-pipelinen skip-haara
 * (sama filegenerated → ei latausta) aktivoituu myös paikallisesti.
 */
@Service
@Profile("local-opintopolku")
class MockTehtavapankkiClient : TehtavapankkiClient {
    override fun listQuestionBanks(): List<QuestionBankMetadata> {
        val customXmlPath = System.getenv("MOCK_TEHTAVAPAKETTI_XML")
        val meta =
            if (customXmlPath.isNullOrBlank()) {
                QuestionBankMetadata(
                    courseId = 42,
                    courseName = "Suomi alkeet",
                    published = Instant.ofEpochMilli(0),
                    generated = ClassPathResource(FIXTURE_PATH).lastModifiedInstant().truncatedTo(ChronoUnit.SECONDS),
                    version = "example",
                    language = "FIN",
                    downloadUrl = "$CLASSPATH_PREFIX$FIXTURE_PATH",
                )
            } else {
                val path = Path.of(customXmlPath)
                QuestionBankMetadata(
                    courseId = customXmlPath.fold(0) { acc, c -> acc + c.code } % 10000,
                    courseName = path.fileName.toString().substringBeforeLast('.'),
                    published = Instant.ofEpochMilli(0),
                    generated = Files.getLastModifiedTime(path).toInstant().truncatedTo(ChronoUnit.SECONDS),
                    version = "custom",
                    language = "FIN",
                    downloadUrl = "$FILE_PREFIX${path.toAbsolutePath()}",
                )
            }
        return listOf(meta)
    }

    override fun downloadXml(meta: QuestionBankMetadata): XmlSource =
        when {
            meta.downloadUrl.startsWith(CLASSPATH_PREFIX) -> {
                ClassPathXmlSource(meta.downloadUrl.removePrefix(CLASSPATH_PREFIX))
            }

            meta.downloadUrl.startsWith(FILE_PREFIX) -> {
                FileXmlSource(Path.of(meta.downloadUrl.removePrefix(FILE_PREFIX)))
            }

            else -> {
                error("Tuntematon mock-downloadUrl: ${meta.downloadUrl}")
            }
        }

    companion object {
        private const val FIXTURE_PATH = "kotoutumiskoulutus/tehtavapankki/tehtavapankki-fixture.xml"
        private const val CLASSPATH_PREFIX = "mock-classpath:"
        private const val FILE_PREFIX = "mock-file:"
    }
}

private fun ClassPathResource.lastModifiedInstant(): Instant =
    try {
        Instant.ofEpochMilli(lastModified())
    } catch (e: IOException) {
        Instant.EPOCH
    }
