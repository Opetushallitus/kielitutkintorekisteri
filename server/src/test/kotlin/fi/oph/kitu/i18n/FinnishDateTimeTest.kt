package fi.oph.kitu.i18n

import org.junit.jupiter.api.Test
import java.time.Instant
import kotlin.test.assertEquals

class FinnishDateTimeTest {
    // 21:30 UTC on kesäaikaan jo seuraavaa päivää Helsingissä. CI ajaa UTC:ssä ja
    // kehityskoneet Helsingissä, joten JVM:n oletusvyöhykkeeseen nojaava muotoilu
    // antaisi näille eri tuloksen.
    private val illallaUtc: Instant = Instant.parse("2026-05-29T21:30:00Z")

    @Test
    fun `finnishDateTime muotoilee Suomen aikaan JVM-n oletusvyohykkeesta riippumatta`() {
        assertEquals("30.05.2026 00:30:00", illallaUtc.finnishDateTime(includeTimeZone = false))
    }

    @Test
    fun `finnishDateTime kertoo Suomen vyohykesiirtyman`() {
        assertEquals("30.05.2026 00:30:00+03", illallaUtc.finnishDateTime())
    }

    @Test
    fun `talviaikaan siirtyma on kaksi tuntia`() {
        val talvella = Instant.parse("2026-01-15T22:30:00Z")
        assertEquals("16.01.2026 00:30:00+02", talvella.finnishDateTime())
    }

    @Test
    fun `yli viikon vanha suhteellinen aika kayttaa Suomen paivaa`() {
        val now = illallaUtc.plusSeconds(30L * 24 * 60 * 60)
        assertEquals("30.5.2026", formatRelativeTime(illallaUtc, now))
    }
}
