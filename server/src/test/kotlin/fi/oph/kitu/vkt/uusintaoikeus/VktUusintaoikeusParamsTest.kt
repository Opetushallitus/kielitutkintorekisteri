package fi.oph.kitu.vkt.uusintaoikeus

import fi.oph.kitu.jdbc.SortDirection
import fi.oph.kitu.koodisto.Koodisto
import org.junit.jupiter.api.Test
import java.time.LocalDate
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class VktUusintaoikeusParamsTest {
    private val tanaan = LocalDate.of(2026, 10, 21)

    @Test
    fun `taman paivan tarkastelupaiva ei ole rajaus`() {
        assertFalse(VktUusintaoikeusParams().onRajattu(tanaan))
        assertFalse(
            VktUusintaoikeusParams(tarkastelupaiva = tanaan, sortDirection = SortDirection.DESC).onRajattu(tanaan),
        )
    }

    @Test
    fun `muu tarkastelupaiva ja rajaukset tekevat nakymasta rajatun`() {
        assertTrue(VktUusintaoikeusParams(tarkastelupaiva = tanaan.minusDays(1)).onRajattu(tanaan))
        assertTrue(VktUusintaoikeusParams(tutkintokieli = listOf(Koodisto.Tutkintokieli.FIN)).onRajattu(tanaan))
        assertTrue(VktUusintaoikeusParams(piilotaHenkilotiedot = true).onRajattu(tanaan))
    }

    @Test
    fun `rajausten poisto sailyttaa jarjestyksen`() {
        assertEquals(
            VktUusintaoikeusParams(sortDirection = SortDirection.DESC),
            VktUusintaoikeusParams(
                tarkastelupaiva = tanaan.minusDays(1),
                piilotaHenkilotiedot = true,
                sortDirection = SortDirection.DESC,
            ).ilmanRajauksia(),
        )
    }
}
