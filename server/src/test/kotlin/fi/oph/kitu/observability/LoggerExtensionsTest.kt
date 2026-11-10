package fi.oph.kitu.observability

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

class LoggerExtensionsTest {
    @Test
    fun `kyselysta kirjataan vain parametrien nimet`() {
        assertEquals(
            "search=*&page=*&ticket=*&kieli=*&kieli=*",
            kyselynParametrienNimet("search=Matti+Meik%C3%A4l%C3%A4inen&page=2&ticket=ST-1-abc&kieli=FIN&kieli=SWE"),
        )
    }

    @Test
    fun `arvoton parametri ja tyhja arvo sailyvat nimina`() {
        assertEquals("lang=*&debug", kyselynParametrienNimet("lang=&debug"))
    }

    @Test
    fun `tyhja kysely ei tuota kenttaa`() {
        assertNull(kyselynParametrienNimet(null))
        assertNull(kyselynParametrienNimet(""))
        assertNull(kyselynParametrienNimet("&"))
    }
}
