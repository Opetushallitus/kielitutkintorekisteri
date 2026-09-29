package fi.oph.kitu.i18n

import fi.oph.kitu.yki.suoritukset.YkiSuoritusColumn
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

private const val AVAIN = "test.avain"

class LocalizedStringTest {
    @AfterEach
    fun palautaKaannokset() {
        TolgeeMessages.clear()
        TolgeeMessages.reloadSourceTexts()
    }

    private fun lahdeteksti(vararg parit: Pair<String, LocalizedString>) = TolgeeMessages.setSourceTexts(mapOf(*parit))

    @Test
    fun `withTolgeeKey resolvoi kaannokset laiskasti eika jaady`() {
        lahdeteksti(AVAIN to LocalizedString(fi = "Suomeksi"))
        val teksti = LocalizedString.withTolgeeKey(AVAIN)

        assertEquals("Suomeksi", teksti.get(Language.SV), "Ilman elävää käännöstä varakielenä suomi")

        TolgeeMessages.set(mapOf(AVAIN to LocalizedString(sv = "På svenska")))
        assertEquals("På svenska", teksti.get(Language.SV))

        TolgeeMessages.set(mapOf(AVAIN to LocalizedString(sv = "Ändrad")))
        assertEquals("Ändrad", teksti.get(Language.SV), "Sama instanssi heijastaa myöhemmän päivityksen")
    }

    @Test
    fun `elava suomi voittaa lahdetekstin`() {
        lahdeteksti(AVAIN to LocalizedString(fi = "Lähteestä"))
        val teksti = LocalizedString.withTolgeeKey(AVAIN)

        assertEquals("Lähteestä", teksti.get(Language.FI), "Ilman elävää dataa näytetään lähdeteksti")

        TolgeeMessages.set(mapOf(AVAIN to LocalizedString(fi = "Elävästä")))

        assertEquals("Elävästä", teksti.get(Language.FI))
        assertEquals("Elävästä", teksti.toString(), "Ambientti oletuskieli on suomi")
        assertEquals("Elävästä", teksti.get(Language.SV), "Kääntämätön kieli varautuu elävään suomeen")
    }

    @Test
    fun `elava data ei pyyhi lahdetekstin muita kielia`() {
        lahdeteksti(AVAIN to LocalizedString(fi = "Lähteestä", sv = "Från källan"))
        val teksti = LocalizedString.withTolgeeKey(AVAIN)

        TolgeeMessages.set(mapOf(AVAIN to LocalizedString(fi = "Elävästä")))

        assertEquals("Elävästä", teksti.get(Language.FI), "Elävä voittaa kielikohtaisesti")
        assertEquals(
            "Från källan",
            teksti.get(Language.SV),
            "Pelkän suomen sisältävä elävä rivi ei saa pyyhkiä lähdetekstin ruotsia",
        )
    }

    @Test
    fun `lahdeteksti jaa voimaan kun elavassa ei ole suomea`() {
        lahdeteksti(AVAIN to LocalizedString(fi = "Lähteestä"))
        val teksti = LocalizedString.withTolgeeKey(AVAIN)

        TolgeeMessages.set(mapOf(AVAIN to LocalizedString(sv = "Från källan")))

        assertEquals("Lähteestä", teksti.get(Language.FI))
        assertEquals("Från källan", teksti.get(Language.SV))
    }

    @Test
    fun `valimuistin tyhjennys ei vie lahdetekstia`() {
        lahdeteksti(AVAIN to LocalizedString(fi = "Lähteestä"))
        val teksti = LocalizedString.withTolgeeKey(AVAIN)
        TolgeeMessages.set(mapOf(AVAIN to LocalizedString(fi = "Elävästä")))

        TolgeeMessages.clear()

        assertEquals("Lähteestä", teksti.get(Language.FI), "clear() tyhjentää vain elävän kerroksen")
    }

    @Test
    fun `tuntematon avain palauttaa avaimen itsensa`() {
        val teksti = LocalizedString.withTolgeeKey("test.eiMissaan")

        assertEquals("test.eiMissaan", teksti.get(Language.FI), "Avain on diagnosoitavampi kuin tyhjä")
    }

    @Test
    fun `haku osuu elavaan suomenkieliseen tekstiin`() {
        lahdeteksti(AVAIN to LocalizedString(fi = "Lähteestä"))
        val teksti = LocalizedString.withTolgeeKey(AVAIN)

        TolgeeMessages.set(mapOf(AVAIN to LocalizedString(fi = "Testipaketit")))

        assertTrue(teksti.contains("Testi"), "Suodatus näkee saman tekstin kuin käyttäjä")
    }

    @Test
    fun `interpolate kayttaa elavaa suomea`() {
        TolgeeMessages.set(mapOf("error.jarjestelmassaVirheita" to LocalizedString(fi = "Virheitä: {count}")))

        assertEquals("Virheitä: 3", UiText.Error.jarjestelmassaVirheita(3L).get(Language.FI))
    }

    @Test
    fun `interpolate sailyttaa muut kielet`() {
        TolgeeMessages.set(mapOf("error.jarjestelmassaVirheita" to LocalizedString(sv = "{count} fel i systemet.")))

        val interpolated = UiText.Error.jarjestelmassaVirheita(3L)

        assertEquals("3 fel i systemet.", interpolated.get(Language.SV))
        assertTrue(interpolated.get(Language.FI).contains("3"), "Suomi tulee lähdetekstistä ja interpoloituu")
    }

    @Test
    fun `taulukon sarakeotsikko ei jaady luokan latausaikaan`() {
        val header = YkiSuoritusColumn.SuorittajanOid.uiHeaderValue

        TolgeeMessages.set(mapOf("yki.sarake.oppijanumero" to LocalizedString(sv = "Studentnummer")))
        assertEquals("Studentnummer", header.get(Language.SV))

        TolgeeMessages.set(mapOf("yki.sarake.oppijanumero" to LocalizedString(sv = "Elevnummer")))
        assertEquals(
            "Elevnummer",
            header.get(Language.SV),
            "Sarakeotsikko heijastaa Tolgee-päivityksen ilman uudelleenrakennusta",
        )
    }
}
