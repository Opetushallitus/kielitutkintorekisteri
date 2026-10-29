package fi.oph.kitu.observability

import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.springframework.boot.json.JsonWriter
import kotlin.test.assertEquals
import kotlin.test.assertSame
import kotlin.test.assertTrue

class LogLineSizeLimitsTest {
    private fun bootinJsonTavut(value: String): Int =
        JsonWriter
            .standard<String>()
            .writeToString(value)
            .toByteArray()
            .size - 2

    @ParameterizedTest
    @ValueSource(
        strings = [
            "a",
            "\"",
            "\\",
            "\b\u000C\n\r\t",
            "\u0000\u0001\u001F",
            "\u007F\u0085\u009F",
            "ä",
            "€",
            "😀",
            "Äijä € 😀 \"lainaus\"\n\tsisennys \u0000",
        ],
    )
    fun `jsonUtf8Length vastaa Bootin JSON-kirjoittimen tavumäärää`(value: String) {
        assertEquals(bootinJsonTavut(value), value.jsonUtf8Length())
    }

    @Test
    fun `budjettiin mahtuva merkkijono säilyy ennallaan`() {
        val value = "ä".repeat(100)

        assertSame(value, value.truncateToJsonUtf8Bytes(200))
    }

    @Test
    fun `katkaistu merkkijono pysyy budjetissa eikä halkaise surrogaattiparia`() {
        val value = "😀".repeat(1_000)

        for (maxBytes in 40..80) {
            val katkaistu = value.truncateToJsonUtf8Bytes(maxBytes)
            val alku = katkaistu.substringBefore("...[truncated")

            assertTrue(katkaistu.jsonUtf8Length() <= maxBytes, "$maxBytes: ${katkaistu.jsonUtf8Length()}")
            assertTrue(katkaistu.endsWith("...[truncated from 4000 bytes]"), katkaistu)
            assertEquals("😀".repeat(alku.length / 2), alku)
        }
    }

    @Test
    fun `katkaisu laskee escapet budjettiin`() {
        val katkaistu = "\n".repeat(1_000).truncateToJsonUtf8Bytes(100)

        assertTrue(bootinJsonTavut(katkaistu) <= 100, katkaistu)
    }
}
