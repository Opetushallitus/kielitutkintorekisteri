package fi.oph.kitu.i18n

import fi.oph.kitu.yki.suoritukset.YkiSuoritusColumn
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class LocalizedStringTest {
    @AfterEach
    fun clearTolgee() = TolgeeMessages.set(emptyMap())

    @Test
    fun `withTolgeeKey resolvoi kaannokset laiskasti eika jaady`() {
        val frozen = LocalizedString.withTolgeeKey("test.avain", "Suomeksi")

        assertEquals("Suomeksi", frozen.get(Language.SV), "Ilman Tolgeeta varakielenä suomi")

        TolgeeMessages.set(mapOf("test.avain" to LocalizedString(sv = "På svenska")))
        assertEquals("På svenska", frozen.get(Language.SV))

        TolgeeMessages.set(mapOf("test.avain" to LocalizedString(sv = "Ändrad")))
        assertEquals("Ändrad", frozen.get(Language.SV), "Sama instanssi heijastaa myöhemmän päivityksen")
    }

    @Test
    fun `Tolgeen suomi voittaa koodin oletustekstin`() {
        val teksti = LocalizedString.withTolgeeKey("nav.tehtavapaketit", "Tehtäväpaketit")

        assertEquals("Tehtäväpaketit", teksti.get(Language.FI), "Ilman Tolgeeta näytetään koodin oletus")

        TolgeeMessages.set(mapOf("nav.tehtavapaketit" to LocalizedString(fi = "Testipaketit")))

        assertEquals("Testipaketit", teksti.get(Language.FI))
        assertEquals("Testipaketit", teksti.toString(), "Ambientti oletuskieli on suomi")
        assertEquals("Testipaketit", teksti.get(Language.SV), "Kääntämätön kieli varautuu Tolgeen suomeen")
    }

    @Test
    fun `koodin oletusteksti jaa voimaan kun Tolgeessa ei ole suomea`() {
        val teksti = LocalizedString.withTolgeeKey("nav.tehtavapaketit", "Tehtäväpaketit")

        TolgeeMessages.set(mapOf("nav.tehtavapaketit" to LocalizedString(sv = "Uppgiftspaket")))

        assertEquals("Tehtäväpaketit", teksti.get(Language.FI))
        assertEquals("Uppgiftspaket", teksti.get(Language.SV))
    }

    @Test
    fun `haku osuu Tolgeen suomenkieliseen tekstiin`() {
        val teksti = LocalizedString.withTolgeeKey("nav.tehtavapaketit", "Tehtäväpaketit")

        TolgeeMessages.set(mapOf("nav.tehtavapaketit" to LocalizedString(fi = "Testipaketit")))

        assertTrue(teksti.contains("Testi"), "Suodatus näkee saman tekstin kuin käyttäjä")
    }

    @Test
    fun `interpolate kayttaa Tolgeen suomea`() {
        TolgeeMessages.set(mapOf("error.jarjestelmassaVirheita" to LocalizedString(fi = "Virheitä: {count}")))

        assertEquals("Virheitä: 3", UiText.Error.jarjestelmassaVirheita(3L).get(Language.FI))
    }

    @Test
    fun `interpolate sailyttaa Tolgee-kaannokset`() {
        TolgeeMessages.set(mapOf("error.jarjestelmassaVirheita" to LocalizedString(sv = "{count} fel i systemet.")))

        val interpolated = UiText.Error.jarjestelmassaVirheita(3L)

        assertEquals("Järjestelmässä on 3 virhettä.", interpolated.get(Language.FI))
        assertEquals("3 fel i systemet.", interpolated.get(Language.SV))
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
