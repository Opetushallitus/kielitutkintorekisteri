package fi.oph.kitu.yki

import fi.oph.kitu.yki.arvioijat.Arviointikausi
import org.junit.jupiter.api.Test
import java.time.LocalDate
import kotlin.test.assertEquals

class ArviointikausiTest {
    @Test
    fun `kausi paattyy viiden vuoden vuosipaivaa edeltavana paivana`() {
        assertEquals(
            LocalDate.of(2020, 12, 6),
            Arviointikausi.paattymispaiva(LocalDate.of(2015, 12, 7)),
        )
    }

    @Test
    fun `karkauspaivalta alkava kausi paattyy helmikuun toiseksi viimeisena`() {
        assertEquals(
            LocalDate.of(2029, 2, 27),
            Arviointikausi.paattymispaiva(LocalDate.of(2024, 2, 29)),
        )
    }

    @Test
    fun `jatkokausi voi alkaa paattymispaivaa seuraavana paivana`() {
        val alkupaiva = LocalDate.of(2026, 1, 1)

        assertEquals(
            alkupaiva.plusYears(Arviointikausi.KAUDEN_PITUUS_VUOSINA),
            Arviointikausi.paattymispaiva(alkupaiva).plusDays(1),
        )
    }

    @Test
    fun `kauden pituus on viisi vuotta`() {
        assertEquals(5L, Arviointikausi.KAUDEN_PITUUS_VUOSINA)
    }
}
