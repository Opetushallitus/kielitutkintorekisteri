package fi.oph.kitu.etusivu

import fi.oph.kitu.i18n.tolgee.TolgeeSyncResult
import fi.oph.kitu.i18n.tolgee.TolgeeSyncStatus
import kotlinx.html.div
import kotlinx.html.stream.createHTML
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import kotlin.test.assertContains
import kotlin.test.assertFalse

class TolgeeSyncWarningTest {
    @AfterEach
    fun clearStatus() {
        TolgeeSyncStatus.last = null
    }

    private fun render(result: TolgeeSyncResult?): String {
        TolgeeSyncStatus.last = result
        return createHTML().div { tolgeeSyncWarning() }
    }

    @Test
    fun `varoittaa kun poistot ohitettiin turvarajan takia`() {
        val html = render(TolgeeSyncResult.PoistorajaYlittyi(poistettavia = 120, raja = 47))

        assertContains(html, "warning-text")
        assertContains(html, "120")
        assertContains(html, "47")
    }

    @Test
    fun `varoittaa kun avainrekisteri oli tyhja`() {
        assertContains(render(TolgeeSyncResult.RekisteriTyhja), "warning-text")
    }

    @Test
    fun `varoittaa kun synkronointi epaonnistui`() {
        val html = render(TolgeeSyncResult.Virhe("401 Unauthorized"))

        assertContains(html, "warning-text")
        assertContains(html, "401 Unauthorized")
    }

    @Test
    fun `ei varoita onnistuneesta synkronoinnista`() {
        assertFalse(render(TolgeeSyncResult.Ok(lisatty = 7, poistettu = 12)).contains("warning-text"))
    }

    @Test
    fun `ei varoita kun synkronointia ei ole ajettu`() {
        assertFalse(render(null).contains("warning-text"))
    }
}
