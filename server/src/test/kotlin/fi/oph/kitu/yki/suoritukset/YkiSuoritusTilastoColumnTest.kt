package fi.oph.kitu.yki.suoritukset

import fi.oph.kitu.jdbc.SortDirection
import fi.oph.kitu.yki.Arviointitila
import fi.oph.kitu.yki.Tutkintokieli
import fi.oph.kitu.yki.Tutkintotaso
import org.junit.jupiter.api.Test
import java.time.LocalDate
import kotlin.test.assertEquals

class YkiSuoritusTilastoColumnTest {
    private fun rivi(
        paiva: String,
        kieli: Tutkintokieli,
        taso: Tutkintotaso = Tutkintotaso.KT,
        tila: Arviointitila = Arviointitila.ARVIOITU,
        lukumaara: Long = 1,
    ) = YkiSuoritusTilastoRivi(LocalDate.parse(paiva), kieli, taso, tila, lukumaara)

    private val suomiKevat = rivi("2026-03-01", Tutkintokieli.FIN, lukumaara = 5)
    private val ruotsiKevat = rivi("2026-03-01", Tutkintokieli.SWE, lukumaara = 2)
    private val englantiSyksy = rivi("2025-10-01", Tutkintokieli.ENG, Tutkintotaso.YT, lukumaara = 9)
    private val suomiKevatArvioitava =
        rivi("2026-03-01", Tutkintokieli.FIN, tila = Arviointitila.ARVIOITAVA, lukumaara = 2)
    private val rivit = listOf(ruotsiKevat, englantiSyksy, suomiKevatArvioitava, suomiKevat)

    @Test
    fun `oletusjärjestys on uusin tutkintopäivä ensin, sitten kieli, taso ja arviointitila`() {
        assertEquals(
            listOf(ruotsiKevat, suomiKevatArvioitava, suomiKevat, englantiSyksy),
            YkiSuoritusTilastoColumn.jarjesta(rivit, YkiSuoritusTilastoColumn.Tutkintopaiva, SortDirection.DESC),
        )
    }

    @Test
    fun `lukumäärän mukaan järjestettäessä tasapelit ratkaistaan oletusjärjestyksellä`() {
        assertEquals(
            listOf(ruotsiKevat, suomiKevatArvioitava, suomiKevat, englantiSyksy),
            YkiSuoritusTilastoColumn.jarjesta(rivit, YkiSuoritusTilastoColumn.Lukumaara, SortDirection.ASC),
        )
        assertEquals(
            listOf(englantiSyksy, suomiKevat, ruotsiKevat, suomiKevatArvioitava),
            YkiSuoritusTilastoColumn.jarjesta(rivit, YkiSuoritusTilastoColumn.Lukumaara, SortDirection.DESC),
        )
    }

    @Test
    fun `kieli järjestetään nimen mukaan`() {
        assertEquals(
            listOf(englantiSyksy, ruotsiKevat, suomiKevatArvioitava, suomiKevat),
            YkiSuoritusTilastoColumn.jarjesta(rivit, YkiSuoritusTilastoColumn.Tutkintokieli, SortDirection.ASC),
        )
    }

    @Test
    fun `taso ja arviointitila järjestetään enumin järjestyksessä`() {
        assertEquals(
            englantiSyksy,
            YkiSuoritusTilastoColumn
                .jarjesta(rivit, YkiSuoritusTilastoColumn.Tutkintotaso, SortDirection.DESC)
                .first(),
        )
        assertEquals(
            suomiKevatArvioitava,
            YkiSuoritusTilastoColumn
                .jarjesta(rivit, YkiSuoritusTilastoColumn.Arviointitila, SortDirection.ASC)
                .first(),
        )
    }
}
