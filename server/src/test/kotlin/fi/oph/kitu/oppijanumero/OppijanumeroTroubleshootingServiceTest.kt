package fi.oph.kitu.oppijanumero

import fi.oph.kitu.DBContainerConfiguration
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

@SpringBootTest
@Import(DBContainerConfiguration::class)
class OppijanumeroTroubleshootingServiceTest(
    @param:Autowired private val service: OppijanumeroTroubleshootingService,
) {
    @Test
    fun `test kutsumanimi combinations`() {
        val oppija =
            Oppija(
                "Minerva Alli Aniitta",
                "040265-9985",
                "Minerva",
                "Marttila",
            )

        val expectedOppija =
            Oppija(
                "Minerva Alli Aniitta",
                "040265-9985",
                "Aniitta",
                "Marttila",
            )
        val result = service.tryEachEtunimiAsKutsumanimi(oppija)
        assertEquals(expectedOppija, result?.oppija)
        assertNotNull(result?.oppijanumero, "oppijanumero palautetaan, jottei sita tarvitse hakea uudelleen")
    }
}
