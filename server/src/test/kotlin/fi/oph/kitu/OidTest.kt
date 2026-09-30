package fi.oph.kitu

import fi.oph.kitu.oid.Oid
import fi.oph.kitu.oid.Oid.Companion.isOidOfClass
import fi.oph.kitu.oid.OidClass
import fi.oph.kitu.util.defaultObjectMapper
import fi.oph.kitu.util.result.getOrThrow
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class OidTest {
    private val validOidString = "1.2.246.562.10.1234567890"
    private val nonValidOidString = "definitely.not.a.valid.oid.string"
    private val oppijaOidString = "1.2.246.562.24.12345678901"
    private val orgOidString = "1.2.246.562.100.12345678901"

    @Test
    fun `parsing correctly formatted string as OID succeeds`() {
        assertTrue(Oid.parse(validOidString).isRight())
    }

    @Test
    fun `parsing correctly formatted string as OID converts to json correctly`() {
        val oid = Oid.parse(validOidString).getOrThrow()
        val result = defaultObjectMapper.writeValueAsString(oid)
        assertEquals("\"$validOidString\"", result)
    }

    @Test
    fun `parsing incorrectly formatted string as OID returns a failure`() {
        assertTrue(Oid.parse(nonValidOidString).isLeft())
    }

    @Test
    fun `converting OID to string yields a correctly formatted OID string`() {
        assertEquals(validOidString, Oid.parse(validOidString).getOrThrow().toString())
    }

    @Test
    fun `implicit toString yields a correctly formatted OID string`() {
        val oid = Oid.parse(validOidString).getOrThrow()
        val string = "$oid"
        assertEquals(validOidString, string)
    }

    @Test
    fun `oppija-OID tunnistetaan OPPIJA-luokkaan kuuluvaksi`() {
        assertTrue(oppijaOidString.isOidOfClass(OidClass.OPPIJA))
        assertFalse(oppijaOidString.isOidOfClass(OidClass.ORG))
    }

    @Test
    fun `organisaatio-OID tunnistetaan ORG-luokkaan kuuluvaksi`() {
        assertTrue(orgOidString.isOidOfClass(OidClass.ORG))
        assertFalse(orgOidString.isOidOfClass(OidClass.OPPIJA))
    }

    @Test
    fun `virheellinen OID-merkkijono ei kuulu mihinkään luokkaan`() {
        assertFalse(nonValidOidString.isOidOfClass(OidClass.OPPIJA))
        assertFalse(nonValidOidString.isOidOfClass(OidClass.ORG))
        assertFalse(nonValidOidString.isOidOfClass(OidClass.USER))
    }

    @Test
    fun `oikean etuliitteen omaava mutta epäkelpo OID ei kuulu luokkaan`() {
        assertFalse("1.2.246.562.24.not-a-number".isOidOfClass(OidClass.OPPIJA))
    }

    @Test
    fun `USER-luokan OID ei mene OPPIJA-luokkaan pelkän etuliitteen takia`() {
        val userOidString = "1.2.246.562.240.12345678901"
        assertTrue(userOidString.isOidOfClass(OidClass.USER))
        assertFalse(userOidString.isOidOfClass(OidClass.OPPIJA))
    }
}
