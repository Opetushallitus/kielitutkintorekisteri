package fi.oph.kitu.vkt

import fi.oph.kitu.koodisto.Koodisto
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

class VktSuoritusFilterTest {
    @Test
    fun `monisanainen haku sitoo jokaisen sanan omaksi parametrikseen`() {
        val filter = VktSuoritusFilter(search = "fiona eriksson")

        assertEquals("%fiona%", filter.params()["filter_search_0"])
        assertEquals("%eriksson%", filter.params()["filter_search_1"])
        assertTrue(filter.whereSql()!!.contains("vkt_suoritus.sukunimi ILIKE :filter_search_1"))
    }

    @Test
    fun `tyhja haku ei rajaa`() {
        assertNull(VktSuoritusFilter(search = "  ").whereSql())
    }

    @Test
    fun `CSV-tiedoston nimessa on taitotason koodiarvo`() {
        val filter =
            VktSuoritusFilter(
                tutkintokieli = Koodisto.Tutkintokieli.FIN,
                taitotaso = Koodisto.VktTaitotaso.HyväJaTyydyttävä,
            )

        assertEquals("vkt_suoritukset_henkilotiedot_FIN_hyvajatyydyttava.csv", filter.csvFileName())
    }
}
