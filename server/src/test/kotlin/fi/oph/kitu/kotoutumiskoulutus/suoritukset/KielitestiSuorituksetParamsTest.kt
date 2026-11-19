package fi.oph.kitu.kotoutumiskoulutus.suoritukset

import fi.oph.kitu.html.table.rajaustenPoistoHref
import fi.oph.kitu.jdbc.SortDirection
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class KielitestiSuorituksetParamsTest {
    @Test
    fun `valmiiden suoritusten oletusnakyma ei ole rajattu`() {
        assertFalse(KielitestiSuorituksetParams().onRajattu())
        assertFalse(KielitestiSuorituksetParams(page = 2, sortDirection = SortDirection.ASC).onRajattu())
    }

    @Test
    fun `keskeneraisten nayttaminen ja haku tekevat nakymasta rajatun`() {
        assertTrue(
            KielitestiSuorituksetParams(naytettavatSuoritukset = NaytettavatSuoritukset.KESKENERAISET).onRajattu(),
        )
        assertTrue(KielitestiSuorituksetParams(search = "virtanen").onRajattu())
    }

    @Test
    fun `rajausten poisto palaa valmiisiin ja sailyttaa jarjestyksen`() {
        val params =
            KielitestiSuorituksetParams(
                search = "virtanen",
                page = 2,
                sortDirection = SortDirection.ASC,
                naytettavatSuoritukset = NaytettavatSuoritukset.KESKENERAISET,
            )

        assertEquals(
            "?sortColumn=${KielitestiSuoritusColumn.Suoritusaika.urlParam}&sortDirection=ASC" +
                "&naytettavatSuoritukset=VALMIIT",
            rajaustenPoistoHref(params.onRajattu(), params.ilmanRajauksia().toMap()),
        )
    }
}
