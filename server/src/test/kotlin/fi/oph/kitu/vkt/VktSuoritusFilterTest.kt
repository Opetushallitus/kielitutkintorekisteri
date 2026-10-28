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
                tutkintokieli = listOf(Koodisto.Tutkintokieli.FIN),
                taitotaso = listOf(Koodisto.VktTaitotaso.HyväJaTyydyttävä),
            )

        assertEquals("vkt_suoritukset_henkilotiedot_FIN_hyvajatyydyttava.csv", filter.csvFileName())
    }

    @Test
    fun `usean arvon valinta rajaa mihin tahansa valituista`() {
        val filter =
            VktSuoritusFilter(
                tutkintokieli = listOf(Koodisto.Tutkintokieli.FIN, Koodisto.Tutkintokieli.SWE),
                taitotaso = listOf(Koodisto.VktTaitotaso.Erinomainen, Koodisto.VktTaitotaso.HyväJaTyydyttävä),
            )
        val where = filter.whereSql()!!

        assertTrue(where.contains("(tutkintokieli IN (:filter_kieli))"), "Got: $where")
        assertTrue(where.contains("(taitotaso IN (:filter_taso))"), "Got: $where")
        assertEquals(listOf("FIN", "SWE"), filter.params()["filter_kieli"])
        assertEquals(
            "vkt_suoritukset_henkilotiedot_FIN-SWE_erinomainen-hyvajatyydyttava.csv",
            filter.csvFileName(),
        )
    }

    @Test
    fun `arvioinnin tilat yhdistetään TAI-ehdolla`() {
        val where =
            VktSuoritusFilter(
                arvioitu = listOf(VktArvioinninTila.ArviointejaPuuttuu, VktArvioinninTila.ArvioituOsittainTaiKokonaan),
            ).whereSql()!!

        assertTrue(
            where.startsWith("WHERE ((osakokeet.arviointeja > 0) OR (vkt_osakoe.arvosana IS NULL"),
            "Got: $where",
        )
    }
}
