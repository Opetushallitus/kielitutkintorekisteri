package fi.oph.kitu.kotoutumiskoulutus.suoritukset

import fi.oph.kitu.DBContainerConfiguration
import fi.oph.kitu.kotoutumiskoulutus.suoritukset.Arvosana
import fi.oph.kitu.kotoutumiskoulutus.suoritukset.CustomKielitestiSuoritusRepository
import fi.oph.kitu.kotoutumiskoulutus.suoritukset.KielitestiSuoritus
import fi.oph.kitu.kotoutumiskoulutus.suoritukset.KielitestiSuoritusFilter
import fi.oph.kitu.kotoutumiskoulutus.suoritukset.KielitestiSuoritusRepository
import fi.oph.kitu.kotoutumiskoulutus.suoritukset.Testikieli
import fi.oph.kitu.oid.Oid
import fi.oph.kitu.util.equalsIgnoringAnnotated
import fi.oph.kitu.util.result.getOrThrow
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.assertAll
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.testcontainers.postgresql.PostgreSQLContainer
import java.time.Instant
import java.time.LocalDate
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

@SpringBootTest
@Import(DBContainerConfiguration::class)
class KielitestiSuoritusRepositoryTest(
    @param:Autowired private val postgres: PostgreSQLContainer,
    @param:Autowired private val kielitestiSuoritusRepository: KielitestiSuoritusRepository,
    @param:Autowired private val customKielitestiSuoritusRepository: CustomKielitestiSuoritusRepository,
) {
    private val suoritusRaija =
        KielitestiSuoritus(
            etunimet = "Raija Testi",
            sukunimi = "Öhman-Testi",
            kutsumanimi = "Ranja",
            oppijanumero = Oid.parse("1.2.246.562.24.20281155246").getOrThrow(),
            email = "ranja@example.com",
            suoritusaika = Instant.parse("2024-11-22T10:49:49Z"),
            oppilaitosOid = Oid.parse("1.2.246.562.10.14893989377").getOrThrow(),
            opettajanEmail = "ope@example.com",
            kurssiId = 32,
            kurssi = "Integraatio testaus",
            luetunYmmartaminen = Arvosana.A1,
            kuullunYmmartaminen = Arvosana.B1,
            puhe = Arvosana.B1,
            kirjoittaminen = Arvosana.A2,
            testikieli = Testikieli.FIN,
            tehtavapaketti = "fi_suomi",
        )

    private val suoritusJennika =
        KielitestiSuoritus(
            etunimet = "Kukka-Maaria Jennika Etel",
            sukunimi = "Haapakoski Henriksson",
            kutsumanimi = "Jennika",
            oppijanumero = Oid.parse("1.2.246.562.198.88975028874").getOrThrow(),
            email = "jennika@example.com",
            suoritusaika = Instant.parse("2025-05-28T10:44:19Z"),
            oppilaitosOid = Oid.parse("1.2.246.562.10.14893989377").getOrThrow(),
            opettajanEmail = "ope@example.com",
            kurssiId = 32,
            kurssi = "Integraatio testaus",
            luetunYmmartaminen = Arvosana.A1,
            kuullunYmmartaminen = Arvosana.B1,
            puhe = Arvosana.B1,
            kirjoittaminen = Arvosana.A2,
            testikieli = Testikieli.SWE,
            tehtavapaketti = null,
        )

    @BeforeEach
    fun nukeDB() {
        kielitestiSuoritusRepository.deleteAll()
        kielitestiSuoritusRepository.saveAll(listOf(suoritusRaija, suoritusJennika))
    }

    @Test
    fun `Kielitesti suoritusten haku etunimellä`() {
        val filter = KielitestiSuoritusFilter(search = "ranja")
        val result = customKielitestiSuoritusRepository.findSuoritukset(filter)
        assertEquals(1, result.size)
        assertTrue(suoritusRaija.equalsIgnoringAnnotated(result.first(), "KOTO"))
    }

    @Test
    fun `Kielitestisuoritusten haku etu- ja sukunimellä`() {
        val filter = KielitestiSuoritusFilter(search = "ranja öhman")
        val result = customKielitestiSuoritusRepository.findSuoritukset(filter)
        assertEquals(1, result.size)
        assertTrue(suoritusRaija.equalsIgnoringAnnotated(result.first(), "KOTO"))
    }

    @Test
    fun `kaikkien hakusanojen on osuttava samaan suoritukseen`() {
        val filter = KielitestiSuoritusFilter(search = "raija integraatio")
        val result = customKielitestiSuoritusRepository.findSuoritukset(filter)
        assertEquals(1, result.size)
        assertTrue(suoritusRaija.equalsIgnoringAnnotated(result.first(), "KOTO"))
    }

    @Test
    fun `Kielitestisuorituksen haku oppijanumerolla`() {
        val filter = KielitestiSuoritusFilter(search = "1.2.246.562.198.88975028874")
        val result = customKielitestiSuoritusRepository.findSuoritukset(filter)
        assertEquals(1, result.size)
        assertTrue(suoritusJennika.equalsIgnoringAnnotated(result.first(), "KOTO"))
    }

    @Test
    fun `Kielitestisuorituksen haku kurssin nimellä`() {
        val filter = KielitestiSuoritusFilter(search = "Integraatio testaus")
        val result = customKielitestiSuoritusRepository.findSuoritukset(filter)
        assertEquals(2, result.size)
    }

    @Test
    fun `Kielitestisuorituksen haku tyhjällä hakusanalla`() {
        val filter = KielitestiSuoritusFilter(search = "")
        val result = customKielitestiSuoritusRepository.findSuoritukset(filter)
        assertEquals(2, result.size)
    }

    @Test
    fun `duplikaattisuoritus tunnistetaan`() {
        val duplicateSuoritus = suoritusRaija.copy(lastModified = Instant.now())
        val result = customKielitestiSuoritusRepository.exists(duplicateSuoritus)
        assertTrue(result)
    }

    @Test
    fun `Päivitettyä suoritusta ei tulkita duplikaatiksi`() {
        val updatedSuoritus =
            suoritusRaija.copy(
                kuullunYmmartaminen = Arvosana.YLIB1,
                lastModified = Instant.now(),
            )
        val result = customKielitestiSuoritusRepository.exists(updatedSuoritus)
        assertFalse(result)
    }

    @Test
    fun `findSuoritukset palauttaa vain uusimmat versiot päivitetyistä suorituksista`() {
        val updatedSuoritusRaija =
            suoritusRaija.copy(
                kuullunYmmartaminen = Arvosana.YLIB1,
                lastModified = Instant.now(),
            )
        val updatedsuoritusJennika =
            suoritusJennika.copy(
                kuullunYmmartaminen = Arvosana.YLIB1,
                puhe = Arvosana.YLIB1,
                lastModified = Instant.now(),
            )
        kielitestiSuoritusRepository.saveAll(listOf(updatedSuoritusRaija, updatedsuoritusJennika))
        val suoritukset = customKielitestiSuoritusRepository.findSuoritukset()
        val resultRaija = suoritukset.find { it.oppijanumero == updatedSuoritusRaija.oppijanumero }
        val resultJennika = suoritukset.find { it.oppijanumero == updatedsuoritusJennika.oppijanumero }
        assertAll(
            fun () = assertEquals(2, suoritukset.size),
            fun () = assertEquals(Arvosana.YLIB1, resultRaija?.kuullunYmmartaminen),
            fun () = assertEquals(Arvosana.YLIB1, resultJennika?.kuullunYmmartaminen),
            fun () = assertEquals(Arvosana.YLIB1, resultJennika?.puhe),
        )
    }

    @Test
    fun `varhaisin suoritusaika ohittaa keskeneräiset suoritukset`() {
        kielitestiSuoritusRepository.save(
            suoritusJennika.copy(
                kurssiId = 33,
                suoritusaika = Instant.parse("2023-01-01T08:00:00Z"),
                completed = false,
            ),
        )

        assertEquals(
            Instant.parse("2024-11-22T10:49:49Z"),
            customKielitestiSuoritusRepository.findVarhaisinSuoritusaika(),
        )
    }

    @Test
    fun `varhaisin suoritusaika puuttuu kun valmiita suorituksia ei ole`() {
        kielitestiSuoritusRepository.deleteAll()
        kielitestiSuoritusRepository.save(suoritusRaija.copy(suoritusaika = null, completed = false))

        assertNull(customKielitestiSuoritusRepository.findVarhaisinSuoritusaika())
    }

    @Test
    fun `suoritusten rajaus testikielen perusteella`() {
        val filter = KielitestiSuoritusFilter(testikieli = listOf(Testikieli.SWE))
        val suoritukset = customKielitestiSuoritusRepository.findSuoritukset(filter)
        assertAll(
            fun () = assertEquals(1, suoritukset.size),
            fun () = assertTrue(suoritukset.first().equalsIgnoringAnnotated(suoritusJennika, "KOTO")),
        )
    }

    @Test
    fun `suoritusten rajaus suoritusaika alkaen perusteella`() {
        val filter = KielitestiSuoritusFilter(suoritusalku = LocalDate.of(2025, 5, 28))
        val suoritukset = customKielitestiSuoritusRepository.findSuoritukset(filter)
        assertAll(
            fun () = assertEquals(1, suoritukset.size),
            fun () = assertTrue(suoritukset.first().equalsIgnoringAnnotated(suoritusJennika, "KOTO")),
        )
    }

    @Test
    fun `suoritusten rajaus suoritusaika päättyen perusteella`() {
        val filter = KielitestiSuoritusFilter(suoritusloppu = LocalDate.of(2025, 5, 27))
        val suoritukset = customKielitestiSuoritusRepository.findSuoritukset(filter)
        assertAll(
            fun () = assertEquals(1, suoritukset.size),
            fun () = assertTrue(suoritukset.first().equalsIgnoringAnnotated(suoritusRaija, "KOTO")),
        )
    }
}
