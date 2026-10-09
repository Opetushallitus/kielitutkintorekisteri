package fi.oph.kitu.yki

import fi.oph.kitu.i18n.UiText
import fi.oph.kitu.jdbc.SortDirection
import fi.oph.kitu.yki.suoritukset.YkiSuoritusAikaryhmittely
import fi.oph.kitu.yki.suoritukset.YkiSuoritusTilastoColumn
import org.junit.jupiter.api.Test
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

class YkiSuorituksetParamsTest {
    @Test
    fun `oletuksena ryhmitellään tutkintopäivällä ja kaikilla muilla sarakkeilla eikä ryhmittelyä viedä urliin`() {
        val params = YkiSuorituksetParams()

        assertEquals(YkiSuoritusTilastoColumn.oletusryhmittely, params.valittuRyhmittely())
        assertNull(params.toMap()["aikaryhmittely"])
        assertNull(params.toMap()["ryhmittely"])
        assertEquals(
            YkiSuoritusTilastoColumn.oletusryhmittely,
            YkiSuorituksetParams(
                aikaryhmittely = YkiSuoritusAikaryhmittely.Tutkintopaiva,
                ryhmittely = YkiSuoritusTilastoColumn.muutRyhmittelyt,
            ).valittuRyhmittely(),
        )
    }

    @Test
    fun `pelkkä ryhmittely ilman aikaryhmittelyä ryhmitellään myös tutkintopäivällä`() {
        val params =
            YkiSuorituksetParams(
                ryhmittely = listOf(YkiSuoritusTilastoColumn.Arviointitila, YkiSuoritusTilastoColumn.Tutkintokieli),
            )

        assertEquals(
            listOf(
                YkiSuoritusTilastoColumn.Tutkintopaiva,
                YkiSuoritusTilastoColumn.Tutkintokieli,
                YkiSuoritusTilastoColumn.Arviointitila,
            ),
            params.valittuRyhmittely(),
        )
        assertEquals("Tutkintopaiva", params.toMap()["aikaryhmittely"])
        assertEquals("tutkintokieli,arviointitila", params.toMap()["ryhmittely"])
    }

    @Test
    fun `aikasarakkeet ja lukumäärä eivät kelpaa muiksi ryhmittelyiksi`() {
        assertEquals(
            listOf(YkiSuoritusTilastoColumn.Tutkintokieli),
            YkiSuorituksetParams(
                aikaryhmittely = YkiSuoritusAikaryhmittely.Ei,
                ryhmittely =
                    listOf(
                        YkiSuoritusTilastoColumn.Tutkintovuosi,
                        YkiSuoritusTilastoColumn.Tutkintopaiva,
                        YkiSuoritusTilastoColumn.Lukumaara,
                        YkiSuoritusTilastoColumn.Tutkintokieli,
                    ),
            ).valittuRyhmittely(),
        )
    }

    @Test
    fun `pelkkä tutkintovuosi ryhmitellään vain vuosittain`() {
        val params = YkiSuorituksetParams(aikaryhmittely = YkiSuoritusAikaryhmittely.Tutkintovuosi)

        assertEquals(listOf(YkiSuoritusTilastoColumn.Tutkintovuosi), params.valittuRyhmittely())
        assertEquals("Tutkintovuosi", params.toMap()["aikaryhmittely"])
        assertNull(params.toMap()["ryhmittely"])
        assertEquals(YkiSuoritusTilastoColumn.Tutkintovuosi to SortDirection.DESC, params.tilastoJarjestys())
        assertTrue(params.tilastoCsvFileName().contains("ryhmittely_tutkintovuosi"))
    }

    @Test
    fun `ilman ryhmittelyä lasketaan kaikki yhteen ja lajitellaan lukumäärällä`() {
        val params = YkiSuorituksetParams(aikaryhmittely = YkiSuoritusAikaryhmittely.Ei, ryhmittely = emptyList())

        assertEquals(emptyList(), params.valittuRyhmittely())
        assertEquals("Ei", params.toMap()["aikaryhmittely"])
        assertNull(params.toMap()["ryhmittely"])
        assertEquals(YkiSuoritusTilastoColumn.Lukumaara to SortDirection.DESC, params.tilastoJarjestys())
        assertTrue(params.tilastoCsvFileName().contains("ryhmittely_ei"))
    }

    @Test
    fun `lajittelu palaa ensimmäiseen ryhmittelysarakkeeseen jos lajittelusarake ei ole ryhmittelyssä`() {
        val params =
            YkiSuorituksetParams(
                aikaryhmittely = YkiSuoritusAikaryhmittely.Ei,
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

    @Test
    fun `rajauskuvaus ja CSV-tiedostonimi sisältävät arviointitilan`() {
        val params = YkiSuorituksetParams(arviointitila = Arviointitila.TARKISTUSARVIOITU)

        assertContains(
            params.filterDescriptions(),
            "${UiText.Yki.Sarake.arviointitila}: ${Arviointitila.TARKISTUSARVIOITU.viewText}",
        )
        assertContains(params.csvFileName(), "TARKISTUSARVIOITU")
        assertContains(params.tilastoCsvFileName(), "TARKISTUSARVIOITU")
    }
}
