package fi.oph.kitu.html.table

import fi.oph.kitu.i18n.LocalizedString
import kotlinx.html.FlowContent
import org.junit.jupiter.api.Test
import java.io.ByteArrayOutputStream
import kotlin.test.assertEquals

class DisplayTableCsvRendererTest {
    enum class TestiSarake(
        override val entityName: String?,
        override val uiHeaderValue: LocalizedString,
        override val urlParam: String,
        override val getValue: (String) -> String,
        override val renderHtml: ((FlowContent, String) -> Unit)? = null,
    ) : RenderableDisplayTableEnum<String> {
        @ColumnTags(ColumnTag.CSV_EXPORT)
        Arvo("arvo", LocalizedString(fi = "Arvo"), "arvo", { it }),
    }

    private fun renderoi(vararg arvot: String): List<String> {
        val output = ByteArrayOutputStream()
        DisplayTableCsvRenderer.renderCsv<TestiSarake, _>(output, arvot.toList())
        return output
            .toString(Charsets.UTF_8)
            .lines()
            .drop(1)
            .dropLast(1)
    }

    @Test
    fun `kaavaksi tulkittavat arvot neutraloidaan heittomerkilla`() {
        assertEquals(
            listOf("'=cmd|'/c calc'!A0", "'+SUM(A1:A2)", "'-2+3", "'@SUM(A1)", "'\tinjektio"),
            renderoi("=cmd|'/c calc'!A0", "+SUM(A1:A2)", "-2+3", "@SUM(A1)", "\tinjektio"),
        )
    }

    @Test
    fun `tyhjan arvon viiva ja turvalliset arvot sailyvat ennallaan`() {
        assertEquals(
            listOf("-", "Öhman-Testi", "matti.meikalainen@example.com", "123", ""),
            renderoi("-", "Öhman-Testi", "matti.meikalainen@example.com", "123", ""),
        )
    }

    @Test
    fun `rivinvaihtoja ja erottimia sisaltavat arvot lainataan`() {
        assertEquals("\"'\rx\"", DisplayTableCsvRenderer.escape("\rx"))
        assertEquals("\"a\rb\"", DisplayTableCsvRenderer.escape("a\rb"))
        assertEquals("\"a;b\"", DisplayTableCsvRenderer.escape("a;b"))
        assertEquals("\"sanoi \"\"hei\"\"\"", DisplayTableCsvRenderer.escape("sanoi \"hei\""))
    }
}
