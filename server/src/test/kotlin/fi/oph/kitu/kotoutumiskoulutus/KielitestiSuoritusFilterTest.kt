package fi.oph.kitu.kotoutumiskoulutus

import fi.oph.kitu.kotoutumiskoulutus.suoritukset.KielitestiSuoritusFilter
import fi.oph.kitu.kotoutumiskoulutus.suoritukset.NaytettavatSuoritukset
import fi.oph.kitu.oid.Oid
import fi.oph.kitu.util.result.getOrThrow
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

class KielitestiSuoritusFilterTest {
    private val oid1 = Oid.parse("1.2.246.562.10.14893989377").getOrThrow()
    private val oid2 = Oid.parse("1.2.246.562.10.99999999999").getOrThrow()

    @Test
    fun `organisaatio-OIDeja ei interpoloida raakana SQL-lauseeseen`() {
        val filter = KielitestiSuoritusFilter().withOrgOids(listOf(oid1, oid2))

        val where = filter.whereSql()
        val params = filter.params()

        assertTrue(where!!.contains("oppilaitos_oid IN (:filter_org_oids)"), "Got: $where")
        assertFalse(where.contains("'$oid1'"), "OID must not appear inline-quoted in SQL: $where")
        assertEquals(
            listOf(oid1.toString(), oid2.toString()),
            params["filter_org_oids"],
            "OIDs must be bound as a named parameter, not concatenated.",
        )
    }

    @Test
    fun `tyhjä organisaatio-OID-lista jättää suodattimen kokonaan pois`() {
        val filter =
            KielitestiSuoritusFilter(
                naytettavatSuoritukset = NaytettavatSuoritukset.KAIKKI,
            ).withOrgOids(emptyList())

        assertNull(filter.whereSql())
        assertFalse(filter.params().containsKey("filter_org_oids"))
    }

    @Test
    fun `yli yhdeksän organisaatio-OIDin lista pudottaa suodattimen pois (säilytetty käytös)`() {
        val tenOids =
            (0 until 10).map { Oid.parse("1.2.246.562.10.${1_000_000 + it}").getOrThrow() }
        val filter =
            KielitestiSuoritusFilter(
                naytettavatSuoritukset = NaytettavatSuoritukset.KAIKKI,
            ).withOrgOids(tenOids)

        assertNull(filter.whereSql())
        assertFalse(filter.params().containsKey("filter_org_oids"))
    }

    @Test
    fun `oletuksena haetaan vain valmiit suoritukset`() {
        val filter = KielitestiSuoritusFilter()
        assertEquals("WHERE (completed)", filter.whereSql())
    }

    @Test
    fun `keskeneräiset-rajaus hakee vain keskeneräiset suoritukset`() {
        val filter = KielitestiSuoritusFilter(naytettavatSuoritukset = NaytettavatSuoritukset.KESKENERAISET)
        assertEquals("WHERE (NOT completed)", filter.whereSql())
    }

    @Test
    fun `kaikki-rajaus ei rajaa valmiusasteen mukaan`() {
        val filter = KielitestiSuoritusFilter(naytettavatSuoritukset = NaytettavatSuoritukset.KAIKKI)
        assertNull(filter.whereSql())
    }
}
