package fi.oph.kitu.observability

import fi.oph.kitu.DBContainerConfiguration
import fi.oph.kitu.util.defaultObjectMapper
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.slf4j.LoggerFactory
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.system.CapturedOutput
import org.springframework.boot.test.system.OutputCaptureExtension
import org.springframework.context.annotation.Import
import kotlin.test.assertEquals
import kotlin.test.assertTrue

@SpringBootTest
@Import(DBContainerConfiguration::class)
@ExtendWith(OutputCaptureExtension::class)
class StructuredLogLineSizeTest {
    private val logger = LoggerFactory.getLogger(javaClass)

    @Test
    fun `ylipitkä virheloki mahtuu yhdelle alle 16 kilotavun JSON-riville`(output: CapturedOutput) {
        val juurisyy = IllegalStateException("juurisyy " + "ä😀".repeat(2_000))
        val keskimmainen = IllegalArgumentException("keskimmäinen " + "y".repeat(5_000), juurisyy)
        val virhe = RuntimeException("ulompi " + "x".repeat(5_000), keskimmainen)

        logger
            .atError()
            .setCause(virhe)
            .addKeyValue("pitka.arvo", "z".repeat(10_000))
            .log("pitkä-lokirivi-testi " + "ö".repeat(50_000))

        val rivi = output.out.lines().single { it.contains("pitkä-lokirivi-testi") }
        val json = defaultObjectMapper.readTree(rivi)
        val stackTrace = json.path("error").path("stack_trace").asString()

        assertTrue(rivi.toByteArray().size < 16_384, "${rivi.toByteArray().size} tavua")
        assertEquals("ERROR", json.path("log").path("level").asString())
        assertTrue(json.path("message").asString().endsWith(" bytes]"))
        assertTrue(
            json
                .path("pitka")
                .path("arvo")
                .asString()
                .endsWith(" bytes]"),
        )
        assertTrue(stackTrace.startsWith("java.lang.IllegalStateException: juurisyy"), stackTrace.take(200))
    }
}
