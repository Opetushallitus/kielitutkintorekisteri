package fi.oph.kitu.html

import fi.oph.kitu.i18n.CurrentLanguage
import fi.oph.kitu.i18n.Language
import fi.oph.kitu.i18n.LocalizedString
import fi.oph.kitu.i18n.TolgeeMessages
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

private const val SIVU = "test.ohje.sivu"
private const val OLETUS = "https://wiki.example/oletus"
private const val SIVUN_OHJE = "https://wiki.example/sivu"

class OhjeTest {
    private val sivu = LocalizedString.withTolgeeKey(SIVU)

    @BeforeEach
    fun asetaOletus() {
        TolgeeMessages.setSourceTexts(mapOf("ohje.oletus" to LocalizedString(fi = OLETUS)))
    }

    @AfterEach
    fun palautaKaannokset() {
        TolgeeMessages.clear()
        TolgeeMessages.reloadSourceTexts()
    }

    private fun sivunOhje(ohje: LocalizedString) =
        TolgeeMessages.setSourceTexts(
            mapOf("ohje.oletus" to LocalizedString(fi = OLETUS), SIVU to ohje),
        )

    @Test
    fun `sivukohtainen ohje voittaa oletuksen`() {
        sivunOhje(LocalizedString(fi = SIVUN_OHJE))

        assertEquals(SIVUN_OHJE, ohjeUrl(sivu))
    }

    @Test
    fun `ilman sivua kaytetaan oletusta`() {
        assertEquals(OLETUS, ohjeUrl(null))
    }

    @Test
    fun `tyhja sivukohtainen ohje palaa oletukseen`() {
        sivunOhje(LocalizedString(fi = ""))

        assertEquals(OLETUS, ohjeUrl(sivu))
    }

    @Test
    fun `puuttuva avain palaa oletukseen eika renderoi avainta`() {
        assertEquals(OLETUS, ohjeUrl(sivu))
    }

    @Test
    fun `muu kuin http-osoite palaa oletukseen`() {
        sivunOhje(LocalizedString(fi = "wiki.example/sivu"))

        assertEquals(OLETUS, ohjeUrl(sivu))
    }

    @Test
    fun `elava kaannos voittaa lahdetekstin`() {
        sivunOhje(LocalizedString(fi = SIVUN_OHJE))
        TolgeeMessages.set(mapOf(SIVU to LocalizedString(fi = "https://wiki.example/muokattu")))

        assertEquals("https://wiki.example/muokattu", ohjeUrl(sivu))
    }

    @Test
    fun `kieliversiota kaytetaan kun se on olemassa, muuten suomenkielista`() {
        sivunOhje(LocalizedString(fi = SIVUN_OHJE, sv = "https://wiki.example/sida"))

        assertEquals("https://wiki.example/sida", CurrentLanguage.withLanguage(Language.SV) { ohjeUrl(sivu) })
        assertEquals(SIVUN_OHJE, CurrentLanguage.withLanguage(Language.EN) { ohjeUrl(sivu) })
    }
}
