package fi.oph.kitu.i18n

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

/**
 * Lähdetekstit ovat `lokalisointi/fi.json`issa ja [UiText] sisältää vain avaimet, joten mikään
 * kääntäjä ei enää valvo niiden vastaavuutta. Tämä testi on se valvonta: kumpikin suunta on
 * tarkistettava, koska puuttuva lähdeteksti renderöisi avainmerkkijonon ja ylimääräinen
 * lähdeteksti jäisi ikuisesti roikkumaan Tolgeeseen.
 */
class UiTextTest {
    @Test
    fun `UiTextin ja lahdetekstien avaimet vastaavat toisiaan`() {
        UiTextWarmup().warmUp(UiText)

        val koodissa = UiTextRegistry.all()
        val lahteessa = TolgeeMessages.sourceKeys()

        assertEquals(
            emptySet(),
            koodissa - lahteessa,
            "UiTextissä on avaimia joille ei ole suomenkielistä lähdetekstiä lokalisointi/fi.json:ssa",
        )
        assertEquals(
            emptySet(),
            lahteessa - koodissa,
            "lokalisointi/fi.json:ssa on avaimia joita UiText ei käytä — poista ne",
        )
    }
}
