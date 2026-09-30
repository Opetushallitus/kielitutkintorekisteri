package fi.oph.kitu.yki.suoritukset

import arrow.core.Either
import arrow.core.left
import arrow.core.right
import fi.oph.kitu.oid.Oid
import fi.oph.kitu.oppijanumero.EmptyRequest
import fi.oph.kitu.oppijanumero.Oppija
import fi.oph.kitu.oppijanumero.OppijanumeroException
import fi.oph.kitu.oppijanumero.OppijanumeroService
import fi.oph.kitu.oppijanumero.OppijanumerorekisteriHenkilo
import fi.oph.kitu.util.result.getOrThrow
import org.junit.jupiter.api.Test
import org.springframework.http.ResponseEntity
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

class YkiSuoritusFilterTest {
    private val oppijaOid = "1.2.246.562.24.12345678901"
    private val orgOid = "1.2.246.562.100.12345678901"

    @Test
    fun `vapaasanahaku tuottaa ILIKE-ehdon parametrilla`() {
        val filter = YkiSuoritusFilter.from(search = "matti")

        assertTrue(filter.whereSql()!!.contains("filter_search_0"), "Got: ${filter.whereSql()}")
        assertEquals("%matti%", filter.params()["filter_search_0"])
    }

    @Test
    fun `useampi vapaasanatermi saa omat parametrinsa`() {
        val params = YkiSuoritusFilter.from(search = "matti meikäläinen").params()

        assertEquals("%matti%", params["filter_search_0"])
        assertEquals("%meikäläinen%", params["filter_search_1"])
    }

    @Test
    fun `henkilö-oid tuottaa suorittajan_oid IN -ehdon eikä interpoloi oidia raakana`() {
        val filter = YkiSuoritusFilter.from(search = oppijaOid)
        val where = filter.whereSql()!!

        assertTrue(where.contains("suorittajan_oid IN (:henkilo_oids)"), "Got: $where")
        assertFalse(where.contains(oppijaOid), "OID must not appear inline in SQL: $where")
        assertEquals(setOf(oppijaOid), filter.params().collectionAt("henkilo_oids"))
    }

    @Test
    fun `organisaatio-oid tuottaa jarjestajan_tunnus_oid IN -ehdon`() {
        val filter = YkiSuoritusFilter.from(search = orgOid)
        val where = filter.whereSql()!!

        assertTrue(where.contains("jarjestajan_tunnus_oid IN (:org_oids)"), "Got: $where")
        assertEquals(setOf(orgOid), filter.params().collectionAt("org_oids"))
    }

    @Test
    fun `numero tuottaa solki_id-ehdon kokonaislukuparametrilla`() {
        val filter = YkiSuoritusFilter.from(search = "12345")
        val where = filter.whereSql()!!

        assertTrue(where.contains("yki_suoritus.solki_id IN (:solki_ids)"), "Got: $where")
        assertEquals(setOf(12345), filter.params().collectionAt("solki_ids"))
    }

    @Test
    fun `sekahaku tuottaa kaikki ehdot yhtä aikaa`() {
        val filter = YkiSuoritusFilter.from(search = "$oppijaOid $orgOid 42 matti")
        val where = filter.whereSql()!!
        val params = filter.params()

        assertTrue(where.contains("suorittajan_oid IN (:henkilo_oids)"), "Got: $where")
        assertTrue(where.contains("jarjestajan_tunnus_oid IN (:org_oids)"), "Got: $where")
        assertTrue(where.contains("yki_suoritus.solki_id IN (:solki_ids)"), "Got: $where")
        assertTrue(where.contains("filter_search_0"), "Got: $where")
        assertEquals(setOf(oppijaOid), params.collectionAt("henkilo_oids"))
        assertEquals(setOf(orgOid), params.collectionAt("org_oids"))
        assertEquals(setOf(42), params.collectionAt("solki_ids"))
        assertEquals("%matti%", params["filter_search_0"])
    }

    @Test
    fun `tyhjä haku ei tuota where-lausetta`() {
        assertNull(YkiSuoritusFilter().whereSql())
        assertNull(YkiSuoritusFilter.from(search = "   ").whereSql())
    }

    @Test
    fun `extendHenkiloOids ilman henkilö-oideja palauttaa saman filtterin`() {
        val filter = YkiSuoritusFilter.from(search = "matti")

        val result = filter.extendHenkiloOids(onrFailing(unexpectedError()))

        assertEquals(filter, result.getOrThrow())
    }

    @Test
    fun `extendHenkiloOids laajentaa haun kaikilla linkitetyillä oideilla`() {
        val slaveA = "1.2.246.562.24.99999999999"
        val slaveB = "1.2.246.562.24.88888888888"
        val filter = YkiSuoritusFilter.from(search = oppijaOid)

        val extended = filter.extendHenkiloOids(onrReturningLinked(oppijaOid to setOf(slaveA, slaveB))).getOrThrow()

        assertEquals(setOf(oppijaOid, slaveA, slaveB), extended.params().collectionAt("henkilo_oids"))
    }

    @Test
    fun `extendHenkiloOids säilyttää alkuperäisen oidin kun oppijaa ei löydy`() {
        val filter = YkiSuoritusFilter.from(search = oppijaOid)

        val extended = filter.extendHenkiloOids(onrFailing(oppijaNotFound())).getOrThrow()

        assertEquals(setOf(oppijaOid), extended.params().collectionAt("henkilo_oids"))
    }

    @Test
    fun `extendHenkiloOids palauttaa virheen kun oppijanumeropalvelu ei vastaa`() {
        val filter = YkiSuoritusFilter.from(search = oppijaOid)

        val result = filter.extendHenkiloOids(onrFailing(unexpectedError()))

        assertTrue(result.isLeft())
    }

    private fun Map<String, Any?>.collectionAt(key: String): Set<*> = (this[key] as Collection<*>).toSet()

    private fun oppijaNotFound() =
        OppijanumeroException.OppijaNotFoundException(EmptyRequest(), ResponseEntity.notFound().build())

    private fun unexpectedError() =
        OppijanumeroException.UnexpectedError(EmptyRequest(), ResponseEntity.internalServerError().build())

    private fun onrReturningLinked(vararg links: Pair<String, Set<String>>): OppijanumeroService {
        val linked = links.toMap()
        return object : OppijanumeroService {
            override fun getMasterOid(oppija: Oppija): Either<OppijanumeroException, Oid> = throw NotImplementedError()

            override fun getMasterOid(henkiloOid: Oid): Either<OppijanumeroException, Oid> = throw NotImplementedError()

            override fun getOppijanumero(henkiloOid: Oid): Either<OppijanumeroException, Oid> =
                throw NotImplementedError()

            override fun getHenkiloByMasterOid(
                masterOid: Oid,
            ): Either<OppijanumeroException, OppijanumerorekisteriHenkilo> = throw NotImplementedError()

            override fun getLinkedOids(henkiloOid: Oid): Either<OppijanumeroException, Set<Oid>> =
                (linked[henkiloOid.toString()].orEmpty() + henkiloOid.toString())
                    .map { Oid.parse(it).getOrThrow() }
                    .toSet()
                    .right()
        }
    }

    private fun onrFailing(error: OppijanumeroException): OppijanumeroService =
        object : OppijanumeroService {
            override fun getMasterOid(oppija: Oppija): Either<OppijanumeroException, Oid> = throw NotImplementedError()

            override fun getMasterOid(henkiloOid: Oid): Either<OppijanumeroException, Oid> = throw NotImplementedError()

            override fun getOppijanumero(henkiloOid: Oid): Either<OppijanumeroException, Oid> =
                throw NotImplementedError()

            override fun getHenkiloByMasterOid(
                masterOid: Oid,
            ): Either<OppijanumeroException, OppijanumerorekisteriHenkilo> = throw NotImplementedError()

            override fun getLinkedOids(henkiloOid: Oid): Either<OppijanumeroException, Set<Oid>> = error.left()
        }
}
