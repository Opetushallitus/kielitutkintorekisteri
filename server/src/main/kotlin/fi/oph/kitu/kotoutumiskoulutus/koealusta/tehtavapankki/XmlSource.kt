package fi.oph.kitu.kotoutumiskoulutus.koealusta.tehtavapankki

import org.springframework.core.io.ClassPathResource
import java.io.InputStream
import java.nio.file.Files
import java.nio.file.Path

/**
 * XML-sisällön lähde, joka palauttaa sen tarvittaessa streaminä eikä
 * materialisoi koko sisältöä Java-merkkijonoksi.
 *
 * Huom: vain lataus, hashays ja S3-vienti streamaavat. [TehtavapankkiXmlParser]
 * lukee sisällön kokonaan muistiin, joten tiedoston koko rajoittaa yhä kekoa.
 */
sealed interface XmlSource : AutoCloseable {
    fun openStream(): InputStream

    override fun close() {}
}

/** Pieni testidata-XML pidetään muistissa merkkijonona. */
class StringXmlSource(
    private val content: String,
) : XmlSource {
    override fun openStream(): InputStream = content.byteInputStream(Charsets.UTF_8)
}

class ClassPathXmlSource(
    private val resourcePath: String,
) : XmlSource {
    override fun openStream(): InputStream = ClassPathResource(resourcePath).inputStream
}

/**
 * Levyllä oleva XML-tiedosto. Asetettaessa [deleteOnClose] tiedosto
 * poistetaan suljettaessa — käytetään väliaikaistiedostoille, jotka
 * Koealusta-klientti kirjoittaa HTTP-vastauksesta.
 */
class FileXmlSource(
    private val path: Path,
    private val deleteOnClose: Boolean = false,
) : XmlSource {
    override fun openStream(): InputStream = Files.newInputStream(path)

    override fun close() {
        if (deleteOnClose) {
            Files.deleteIfExists(path)
        }
    }
}
