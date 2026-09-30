package fi.oph.kitu.yki.suoritukset

import fi.oph.kitu.yki.Arviointitila
import fi.oph.kitu.yki.laskeArviointitila
import java.time.LocalDate
import kotlin.test.Test
import kotlin.test.assertEquals

class YkiArviointitilaLaskentaTest {
    private fun laske(
        nykyinen: Arviointitila,
        osakoeCount: Int = 0,
        arvosanaPuuttuu: Int = 0,
        oikeitaArvosanoja: Int = 0,
        onTarkistusarviointi: Boolean = false,
        tarkistuksenKasittelypaiva: LocalDate? = null,
    ) = laskeArviointitila(
        nykyinen = nykyinen,
        osakoeCount = osakoeCount,
        arvosanaPuuttuu = arvosanaPuuttuu,
        oikeitaArvosanoja = oikeitaArvosanoja,
        onTarkistusarviointi = onTarkistusarviointi,
        tarkistuksenKasittelypaiva = tarkistuksenKasittelypaiva,
    )

    @Test
    fun `ARVIOITU ilman oikeita arvosanoja muuttuu EI_SUORITUSTA-tilaan`() {
        assertEquals(
            Arviointitila.EI_SUORITUSTA,
            laske(Arviointitila.ARVIOITU, osakoeCount = 4, oikeitaArvosanoja = 0),
        )
    }

    @Test
    fun `ARVIOITU jossa on oikea arvosana sailyy`() {
        assertEquals(
            Arviointitila.ARVIOITU,
            laske(Arviointitila.ARVIOITU, osakoeCount = 4, oikeitaArvosanoja = 2),
        )
    }

    @Test
    fun `osakoe ilman arvosanaa tuottaa ARVIOITAVA-tilan`() {
        assertEquals(
            Arviointitila.ARVIOITAVA,
            laske(Arviointitila.ARVIOITU, osakoeCount = 4, arvosanaPuuttuu = 1, oikeitaArvosanoja = 2),
        )
    }

    @Test
    fun `ilman osakokeita tila sailyy ennallaan`() {
        assertEquals(
            Arviointitila.ARVIOITAVA,
            laske(Arviointitila.ARVIOITAVA, osakoeCount = 0),
        )
    }

    @Test
    fun `tarkistusarviointi ilman kasittelypaivaa tuottaa TARKISTUSARVIOITAVA-tilan`() {
        assertEquals(
            Arviointitila.TARKISTUSARVIOITAVA,
            laske(Arviointitila.ARVIOITU, osakoeCount = 3, oikeitaArvosanoja = 1, onTarkistusarviointi = true),
        )
    }

    @Test
    fun `tarkistusarviointi kasittelypaivalla tuottaa TARKISTUSARVIOITU-tilan`() {
        assertEquals(
            Arviointitila.TARKISTUSARVIOITU,
            laske(
                Arviointitila.ARVIOITU,
                osakoeCount = 3,
                oikeitaArvosanoja = 1,
                onTarkistusarviointi = true,
                tarkistuksenKasittelypaiva = LocalDate.of(2025, 1, 1),
            ),
        )
    }

    @Test
    fun `ILMOITTAUTUNUT sailyy vaikka osakokeilla ei ole arvosanaa`() {
        assertEquals(
            Arviointitila.ILMOITTAUTUNUT,
            laske(Arviointitila.ILMOITTAUTUNUT, osakoeCount = 3, arvosanaPuuttuu = 3),
        )
    }

    @Test
    fun `PERUTTU sailyy`() {
        assertEquals(
            Arviointitila.PERUTTU,
            laske(Arviointitila.PERUTTU, osakoeCount = 0),
        )
    }

    @Test
    fun `TARKISTUSARVIOINTI_HYVAKSYTTY sailyy vaikka tarkistustietoja olisi`() {
        assertEquals(
            Arviointitila.TARKISTUSARVIOINTI_HYVAKSYTTY,
            laske(
                Arviointitila.TARKISTUSARVIOINTI_HYVAKSYTTY,
                osakoeCount = 3,
                oikeitaArvosanoja = 1,
                onTarkistusarviointi = true,
                tarkistuksenKasittelypaiva = LocalDate.of(2025, 1, 1),
            ),
        )
    }
}
