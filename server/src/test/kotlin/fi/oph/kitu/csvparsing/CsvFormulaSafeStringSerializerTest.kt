package fi.oph.kitu.csvparsing

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class CsvFormulaSafeStringSerializerTest {
    @Test
    fun `prepends a tick when the value starts with a spreadsheet-formula trigger`() {
        assertEquals("'=cmd|'/c calc'!A0", CsvFormulaSafeStringSerializer.sanitize("=cmd|'/c calc'!A0"))
        assertEquals("'+SUM(A1:A2)", CsvFormulaSafeStringSerializer.sanitize("+SUM(A1:A2)"))
        assertEquals("'-2+3", CsvFormulaSafeStringSerializer.sanitize("-2+3"))
        assertEquals("'@SUM(A1)", CsvFormulaSafeStringSerializer.sanitize("@SUM(A1)"))
        assertEquals("'\tinjection", CsvFormulaSafeStringSerializer.sanitize("\tinjection"))
        assertEquals("'\rinjection", CsvFormulaSafeStringSerializer.sanitize("\rinjection"))
    }

    @Test
    fun `leaves safe values untouched`() {
        assertEquals("Öhman-Testi", CsvFormulaSafeStringSerializer.sanitize("Öhman-Testi"))
        assertEquals(
            "matti.meikalainen@example.com",
            CsvFormulaSafeStringSerializer.sanitize("matti.meikalainen@example.com"),
        )
        assertEquals("123", CsvFormulaSafeStringSerializer.sanitize("123"))
        assertEquals("-", CsvFormulaSafeStringSerializer.sanitize("-"))
        assertEquals("", CsvFormulaSafeStringSerializer.sanitize(""))
    }
}
