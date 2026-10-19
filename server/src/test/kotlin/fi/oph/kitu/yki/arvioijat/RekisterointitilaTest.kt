package fi.oph.kitu.yki.arvioijat

import fi.oph.kitu.yki.arvioijat.Rekisterointitila
import fi.oph.kitu.yki.arvioijat.YkiArvioijaTila
import java.time.LocalDate
import kotlin.test.Test
import kotlin.test.assertEquals

class RekisterointitilaTest {
    private val tanaan = LocalDate.of(2026, 6, 1)

    private fun laske(
        alku: LocalDate?,
        loppu: LocalDate?,
        tallennettu: YkiArvioijaTila? = null,
    ) = Rekisterointitila.laske(tallennettu, alku, loppu, tanaan)

    @Test
    fun `paattynyt kausi on passivoitu`() {
        assertEquals(
            Rekisterointitila.PASSIVOITU,
            laske(LocalDate.of(2021, 1, 1), tanaan.minusDays(1)),
        )
    }

    @Test
    fun `kauden viimeinen paiva on viela aktiivinen`() {
        assertEquals(
            Rekisterointitila.AKTIIVINEN,
            laske(LocalDate.of(2021, 1, 1), tanaan),
            "paattymispaiva on inklusiivinen (OPH kys. 2)",
        )
    }

    @Test
    fun `alkava kausi on tulevaisuudessa`() {
        assertEquals(
            Rekisterointitila.TULEVAISUUDESSA,
            laske(tanaan.plusDays(1), tanaan.plusYears(5)),
        )
    }

    @Test
    fun `kausi joka alkaa tanaan on aktiivinen`() {
        assertEquals(Rekisterointitila.AKTIIVINEN, laske(tanaan, tanaan.plusYears(5)))
    }

    @Test
    fun `tyhja paattymispaiva on avoin kausi`() {
        assertEquals(Rekisterointitila.AKTIIVINEN, laske(LocalDate.of(2021, 1, 1), null))
        assertEquals(Rekisterointitila.AKTIIVINEN, laske(null, null))
    }

    @Test
    fun `tallennettu passivointi ohittaa voimassa olevan kauden`() {
        assertEquals(
            Rekisterointitila.PASSIVOITU,
            laske(LocalDate.of(2021, 1, 1), tanaan.plusYears(1), YkiArvioijaTila.PASSIVOITU),
            "Solkin nimenomainen passivointi on kannanotto, jota paivamaarat eivat kumoa",
        )
    }

    @Test
    fun `tallennettu aktiivinen ei elvyta paattynytta kautta`() {
        assertEquals(
            Rekisterointitila.PASSIVOITU,
            laske(LocalDate.of(2021, 1, 1), tanaan.minusDays(1), YkiArvioijaTila.AKTIIVINEN),
            "V117 taytti sarakkeen AKTIIVINENilla, joten se ei kanna tietoa",
        )
    }

    @Test
    fun `tallennettu passivointi ei tee tulevasta kaudesta tulevaisuutta`() {
        assertEquals(
            Rekisterointitila.PASSIVOITU,
            laske(tanaan.plusMonths(1), tanaan.plusYears(5), YkiArvioijaTila.PASSIVOITU),
        )
    }
}
