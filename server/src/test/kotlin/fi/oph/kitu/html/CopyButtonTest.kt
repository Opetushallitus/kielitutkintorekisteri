package fi.oph.kitu.html

import fi.oph.kitu.i18n.LocalizedString
import fi.oph.kitu.i18n.UiText
import kotlinx.html.DIV
import kotlinx.html.div
import kotlinx.html.stream.createHTML
import org.junit.jupiter.api.Test
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertFalse

class CopyButtonTest {
    private val oid = "1.2.246.562.24.20281155246"

    private fun render(block: DIV.() -> Unit) = createHTML().div { block() }.replace(Regex(">\\s+<"), "><")

    @Test
    fun `copyButton renders an icon-only button that copies the value`() {
        val html = render { copyButton(oid) }

        assertContains(html, """type="button"""")
        assertContains(html, """data-copy="$oid"""")
        assertContains(html, """aria-label="${UiText.Toiminto.kopioi}"""")
        assertContains(html, """data-copied="${UiText.Toiminto.kopioitu}"""")
        assertEquals(1, Regex(Regex.escape(oid)).findAll(html).count(), "value should appear only in data-copy:\n$html")
    }

    @Test
    fun `copyable shows the value once as text next to the button`() {
        val html = render { copyable(oid) }

        assertContains(html, """<span class="copyable">$oid<button""")
        assertEquals(2, Regex(Regex.escape(oid)).findAll(html).count(), html)
    }

    @Test
    fun `copyable renders custom content in place of the plain value`() {
        val html = render { copyable(oid) { +"($oid)" } }

        assertContains(html, """<span class="copyable">($oid)<button""")
        assertContains(html, """data-copy="$oid"""")
    }

    @Test
    fun `copyableOrDash renders a dash without a button for a missing value`() {
        val html = render { copyableOrDash(null) }

        assertContains(html, "–")
        assertFalse(html.contains("<button"), html)
    }

    @Test
    fun `ofCopyable comparison rows are highlighted only when the values differ`() {
        val html =
            render {
                comparisonTable(
                    LocalizedString(fi = "Kitu"),
                    LocalizedString(fi = "ONR"),
                    Comparison.ofCopyable("Sama", oid, oid),
                    Comparison.ofCopyable("Eri", oid, "1.2.246.562.24.33342764709"),
                )
            }

        assertContains(html, "<tr><th>Sama</th>")
        assertContains(html, """<tr class="diff"><th>Eri</th>""")
    }
}
