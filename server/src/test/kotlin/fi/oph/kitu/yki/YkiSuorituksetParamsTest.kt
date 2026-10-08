package fi.oph.kitu.yki

import fi.oph.kitu.jdbc.SortDirection
import fi.oph.kitu.yki.suoritukset.YkiSuoritusTilastoColumn
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

class YkiSuorituksetParamsTest {
    @Test
    fun `oletuksena ryhmitellään kaikilla neljällä sarakkeella eikä ryhmittelyä viedä urliin`() {
        val params = YkiSuorituksetParams()

        assertEquals(YkiSuoritusTilastoColumn.ryhmittelyt, params.valittuRyhmittely())
        assertNull(params.toMap()["ryhmittely"])
    }

    @Test
    fun `tyhjä ryhmittely ja lukumäärä tulkitaan oletusryhmittelyksi`() {
        assertEquals(
            YkiSuoritusTilastoColumn.ryhmittelyt,
            YkiSuorituksetParams(ryhmittely = emptyList()).valittuRyhmittely(),
        )
        assertEquals(
            YkiSuoritusTilastoColumn.ryhmittelyt,
            YkiSuorituksetParams(ryhmittely = listOf(YkiSuoritusTilastoColumn.Lukumaara)).valittuRyhmittely(),
        )
    }

    @Test
    fun `valittu ryhmittely järjestetään sarakejärjestykseen ja viedään urliin pilkkueroteltuna`() {
        val params =
            YkiSuorituksetParams(
                ryhmittely = listOf(YkiSuoritusTilastoColumn.Arviointitila, YkiSuoritusTilastoColumn.Tutkintokieli),
            )

        assertEquals(
            listOf(YkiSuoritusTilastoColumn.Tutkintokieli, YkiSuoritusTilastoColumn.Arviointitila),
            params.valittuRyhmittely(),
        )
        assertEquals("tutkintokieli,arviointitila", params.toMap()["ryhmittely"])
    }

    @Test
    fun `lajittelu palaa ensimmäiseen ryhmittelysarakkeeseen jos lajittelusarake ei ole ryhmittelyssä`() {
        val params =
            YkiSuorituksetParams(
                ryhmittely = listOf(YkiSuoritusTilastoColumn.Tutkintokieli),
                tilastoSortColumn = YkiSuoritusTilastoColumn.Tutkintopaiva,
                tilastoSortDirection = SortDirection.DESC,
            )

        assertEquals(YkiSuoritusTilastoColumn.Tutkintokieli to SortDirection.ASC, params.tilastoJarjestys())
        assertEquals(
            YkiSuoritusTilastoColumn.Lukumaara to SortDirection.DESC,
            params.copy(tilastoSortColumn = YkiSuoritusTilastoColumn.Lukumaara).tilastoJarjestys(),
        )
    }
}
