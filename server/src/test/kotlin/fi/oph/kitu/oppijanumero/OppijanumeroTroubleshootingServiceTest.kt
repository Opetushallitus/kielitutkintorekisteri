package fi.oph.kitu.oppijanumero

import fi.oph.kitu.DBContainerConfiguration
import org.junit.jupiter.api.assertAll
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.http.ResponseEntity
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
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
            OppijanumeroRatkaisuehdotus(
                HETULLA_LOYTYI,
                Oppija("Magdalena Testi", "010866-9260", "Magdalena", "Sallinen-Testi"),
            ),
            result,
        )
    }

    @Test
    fun `varoittaa kun henkilotunnuksella loytyy eri henkilo`() {
        val oppija = Oppija("Urho Kaleva", "010866-9260", "Urho", "Kekkonen")

        val result = service.troubleshootOppijanumero(oppija, ResponseEntity.notFound().build())

        assertEquals(
            OppijanumeroRatkaisuehdotus(
                HETULLA_LOYTYI_ERI_HENKILO,
                Oppija("Magdalena Testi", "010866-9260", "Magdalena", "Sallinen-Testi"),
            ),
            result,
        )
    }

    @Test
    fun `nimet vastaavat kun yksikin nimi on sama tai lahes sama`() {
        assertAll(
            { assertFalse(nimetVastaavat("Urho Kaleva", "Kekkonen", "Martti", "Ahtisaari")) },
            { assertTrue(nimetVastaavat("Mati", "Korhonen", "Matti", "Virtanen")) },
            { assertTrue(nimetVastaavat("Anna", "Virtanen", "Anna", "Korhonen")) },
            { assertTrue(nimetVastaavat("Ranja", "Öhman", "Liisa", "Öhman-Testi")) },
            { assertTrue(nimetVastaavat("ANNA", "Virtanen", "anna", "Korhonen")) },
            { assertTrue(nimetVastaavat("Magdaleena", "Salinen", "Magdalena", "Sallinen")) },
            { assertFalse(nimetVastaavat("Li", "Korhonen", "Le", "Virtanen")) },
            { assertFalse(nimetVastaavat("Urho", "Kekkonen", null, null)) },
        )
    }

    @Test
    fun `antaa yleisen ohjeen kun henkilotunnusta ei loydy`() {
        val oppija = Oppija("Tuntematon", "010101-123N", "Tuntematon", "Henkilö")

        val result = service.troubleshootOppijanumero(oppija, ResponseEntity.notFound().build())

        assertEquals(OppijanumeroRatkaisuehdotus(EI_LOYTYNYT), result)
    }

    @Test
    fun `laaja haku loytaa etunimet eri jarjestyksessa`() {
        val oppija = Oppija("Aniitta Minerva Alli", "040265-9985", "Minerva", "Marttila")

        val result = service.troubleshootOppijanumero(oppija, ResponseEntity.notFound().build())

        assertEquals(
            OppijanumeroRatkaisuehdotus(
                NIMIYHDISTELMA_LOYTYI,
                Oppija("Minerva Alli Aniitta", "040265-9985", "Aniitta", "Marttila"),
            ),
            result,
        )
    }

    @Test
    fun `laaja haku siirtaa nimen sukunimista etunimiin`() {
        val oppija = Oppija("Ranja", "010180-9026", "Ranja", "Testi Öhman-Testi")

        val result = service.troubleshootOppijanumero(oppija, ResponseEntity.notFound().build())

        assertEquals(
            OppijanumeroRatkaisuehdotus(
                NIMIYHDISTELMA_LOYTYI,
                Oppija("Ranja Testi", "010180-9026", "Ranja", "Öhman-Testi"),
            ),
            result,
        )
    }

    @Test
    fun `kaikki nimiyhdistelmat alkavat alkuperaisesta jarjestyksesta`() {
        val yhdistelmat =
            OppijanumeroTroubleshootingService.kaikkiNimiyhdistelmat(
                Oppija("Martti Vieno", "010101-123N", "Vieno", "Esimerkkinen"),
            )

        assertEquals(18, yhdistelmat.size)
        assertEquals(
            listOf(
                Oppija("Martti Vieno", "010101-123N", "Martti", "Esimerkkinen"),
                Oppija("Martti Vieno", "010101-123N", "Vieno", "Esimerkkinen"),
                Oppija("Vieno Martti", "010101-123N", "Vieno", "Esimerkkinen"),
                Oppija("Vieno Martti", "010101-123N", "Martti", "Esimerkkinen"),
            ),
            yhdistelmat.take(4),
        )
    }

    @Test
    fun `laajaa hakua ei tehda liian monelle nimelle`() {
        val oppija = Oppija("Yksi Kaksi Kolme Nelja Viisi", "010101-123N", "Yksi", "Kuusi Seitseman")

        assertTrue(OppijanumeroTroubleshootingService.kaikkiNimiyhdistelmat(oppija).isEmpty())
    }
}
