package fi.oph.kitu.yki.arvioijat

import java.time.LocalDate

object Arviointikausi {
    const val KAUDEN_PITUUS_VUOSINA = 5L

    /** Paattymispaiva on inklusiivinen, joten viisi vuotta paattyy vuosipaivaa edeltavana paivana. */
    fun paattymispaiva(alkupaiva: LocalDate): LocalDate = alkupaiva.plusYears(KAUDEN_PITUUS_VUOSINA).minusDays(1)
}
