package fi.oph.kitu.webmvc

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.io.ByteArrayInputStream
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals

class Utf8ValidatingInputStreamTest {
    private fun bytes(hex: String): ByteArray = hex.split(" ").map { it.toInt(16).toByte() }.toByteArray()

    private fun readAll(input: ByteArray): ByteArray =
        Utf8ValidatingInputStream(ByteArrayInputStream(input)).readAllBytes()

    private fun readByteByByte(input: ByteArray): ByteArray {
        val stream = Utf8ValidatingInputStream(ByteArrayInputStream(input))
        return generateSequence { stream.read().takeIf { it != -1 } }.map { it.toByte() }.toList().toByteArray()
    }

    @ParameterizedTest
    @ValueSource(
        strings = [
            "C0 BC",
            "C1 BF",
            "C0 80",
            "E0 80 AF",
            "E0 9F BF",
            "F0 80 80 AF",
            "F0 8F BF BF",
            "ED A0 80",
            "ED BF BF",
            "F4 90 80 80",
            "F5 80 80 80",
            "FF",
            "80",
            "C3",
            "E2 82",
            "F0 9F 98",
        ],
    )
    fun `virheellinen tai ylipitkä UTF-8 hylätään`(hex: String) {
        val input = "{\"nimi\":\"".toByteArray() + bytes(hex) + "\"}".toByteArray()

        assertEquals(9, assertThrows<InvalidUtf8Exception> { readAll(input) }.offset)
        assertEquals(9, assertThrows<InvalidUtf8Exception> { readByteByByte(input) }.offset)
    }

    @Test
    fun `katkennut monitavuinen merkki rungon lopussa hylätään`() {
        val input = "{\"nimi\":\"ä\"}".toByteArray() + bytes("E2 82")

        assertThrows<InvalidUtf8Exception> { readAll(input) }
        assertThrows<InvalidUtf8Exception> { readByteByByte(input) }
    }

    @Test
    fun `kelvollinen UTF-8 kulkee läpi muuttumattomana`() {
        val input = "{\"nimi\":\"Äijä € 😀 \\u00e4\",\"x\":\"${"ö".repeat(10_000)}\"}".toByteArray()

        assertContentEquals(input, readAll(input))
        assertContentEquals(input, readByteByByte(input))
    }

    @Test
    fun `virheilmoitus kertoo virheellisen tavun sijainnin`() {
        val virhe = assertThrows<InvalidUtf8Exception> { readAll("abc".toByteArray() + bytes("C0 BC")) }

        assertEquals("Pyynnön runko ei ole kelvollista UTF-8:aa (tavu 3)", virhe.message)
    }
}
