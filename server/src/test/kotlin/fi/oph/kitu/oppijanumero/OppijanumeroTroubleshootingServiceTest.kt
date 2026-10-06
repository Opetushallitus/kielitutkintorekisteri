package fi.oph.kitu.oppijanumero

import fi.oph.kitu.DBContainerConfiguration
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.http.ResponseEntity
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

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
        assertEquals(expectedOppija, result)
    }

    @Test
    fun `kertoo oppijanumerorekisterin nimet kun henkilotunnus loytyy`() {
        val oppija =
            Oppija(
                "Magdaleena Testi",
                "010866-9260",
                "Magdaleena",
                "Salinen-Testi",
            )

        val result = service.troubleshootOppijanumero(oppija, ResponseEntity.notFound().build())

        assertEquals(
            "Oppijanumerorekisterissä on tällä henkilötunnuksella henkilö eri nimillä. " +
                "Nimet tulee kirjoittaa täsmälleen näin: " +
                "etunimet: \"Magdalena Testi\", kutsumanimi: \"Magdalena\", sukunimi: \"Sallinen-Testi\"",
            result,
        )
    }

    @Test
    fun `antaa yleisen ohjeen kun henkilotunnusta ei loydy`() {
        val oppija = Oppija("Tuntematon", "010101-123N", "Tuntematon", "Henkilö")

        val result = service.troubleshootOppijanumero(oppija, ResponseEntity.notFound().build())

        assertTrue(result.startsWith("Oppijanumerorekisteristä ei löytynyt oppijanumeroa"))
    }
}
