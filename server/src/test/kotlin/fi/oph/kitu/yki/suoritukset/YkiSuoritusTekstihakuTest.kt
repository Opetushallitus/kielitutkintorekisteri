package fi.oph.kitu.yki.suoritukset

import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import fi.oph.kitu.DBContainerConfiguration
import fi.oph.kitu.auditlogs.AUDIT_LOGGER_NAME
import fi.oph.kitu.dev.mockdata.generateRandomYkiSuoritusEntity
import fi.oph.kitu.tyhjennaYkiSuoritukset
import fi.oph.kitu.yki.Tutkintokieli
import fi.oph.kitu.yki.YkiService
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.jdbc.core.JdbcTemplate
import java.time.Instant
import java.time.LocalDate
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import ch.qos.logback.classic.Logger as LogbackLogger

@SpringBootTest
@Import(DBContainerConfiguration::class)
class YkiSuoritusTekstihakuTest(
    @param:Autowired private val repository: YkiSuoritusRepository,
    @param:Autowired private val ykiService: YkiService,
    @param:Autowired private val jdbcTemplate: JdbcTemplate,
) {
    private val auditLogger = LoggerFactory.getLogger(AUDIT_LOGGER_NAME) as LogbackLogger
    private val auditLoki = ListAppender<ILoggingEvent>()

    @BeforeEach
    fun valmistele() {
        jdbcTemplate.tyhjennaYkiSuoritukset()
        auditLoki.start()
        auditLogger.addAppender(auditLoki)
    }

    @AfterEach
    fun siivoa() {
        auditLoki.stop()
        auditLogger.detachAppender(auditLoki)
        jdbcTemplate.tyhjennaYkiSuoritukset()
    }

    private fun suoritus(
        solkiId: Int,
        etunimet: String = "Testi",
        sukunimi: String = "Henkilö",
        email: String? = "muu@example.com",
        hetu: String? = null,
        jarjestajanNimi: String = "Muu järjestäjä",
        tutkintopaiva: LocalDate = LocalDate.of(2020, 1, 1),
        tutkintokieli: Tutkintokieli = Tutkintokieli.FIN,
        lastModified: Instant = Instant.parse("2024-01-01T10:00:00Z"),
    ) = generateRandomYkiSuoritusEntity().copy(
        solkiId = solkiId,
        etunimet = etunimet,
        sukunimi = sukunimi,
        email = email,
        hetu = hetu,
        jarjestajanNimi = jarjestajanNimi,
        tutkintopaiva = tutkintopaiva,
        tutkintokieli = tutkintokieli,
        lastModified = lastModified,
    )

    private fun tallenna(vararg suoritukset: YkiSuoritusEntity) {
        repository.saveAllNewEntities(suoritukset.toList())
    }

    private fun haku(
        hakusanat: String,
        versionHistory: Boolean = false,
        limit: Int = 100,
        offset: Int = 0,
    ) = ykiService.findSuorituksetSivu(
        filter = YkiSuoritusFilter.from(hakusanat),
        versionHistory = versionHistory,
        limit = limit,
        offset = offset,
    )

    private fun YkiSuoritusSivu.solkiIdt() = suoritukset.map { it.solkiId }

    @Test
    fun `hakusana löytyy jokaisesta haettavasta sarakkeesta`() {
        val kohde =
            suoritus(
                solkiId = 1,
                etunimet = "Ranja Maria",
                sukunimi = "Öhman",
                email = "ranja@posti.example",
                hetu = "010190-123A",
                jarjestajanNimi = "Kotkan aikuisopisto",
            )
        tallenna(kohde, suoritus(solkiId = 2))
        val oidinOsa = kohde.suorittajanOID.toString().removePrefix("1.2.")

        listOf("Maria", "Öhman", "posti.example", "123A", "aikuisopisto", oidinOsa).forEach { hakusana ->
            assertEquals(listOf(1), haku(hakusana).solkiIdt(), "Hakusana: $hakusana")
        }
    }

    @Test
    fun `kirjainkoko ei vaikuta hakuun myöskään ääkkösissä`() {
        tallenna(
            suoritus(solkiId = 1, sukunimi = "Öhman", hetu = "010190-123A"),
            suoritus(solkiId = 2, sukunimi = "ÄIJÄLÄ"),
            suoritus(solkiId = 3, sukunimi = "Åkerman"),
            suoritus(solkiId = 4),
        )

        listOf("öhman", "ÖHMAN", "öHmAn", "123a").forEach { hakusana ->
            assertEquals(listOf(1), haku(hakusana).solkiIdt(), "Hakusana: $hakusana")
        }
        assertEquals(listOf(2), haku("äijälä").solkiIdt())
        assertEquals(listOf(3), haku("ÅKERMAN").solkiIdt())
    }

    @Test
    fun `hakusanat voivat osua eri sarakkeisiin mutta yksi sana ei ylitä sarakkeen rajaa`() {
        tallenna(suoritus(solkiId = 1, etunimet = "Ranja", sukunimi = "Öhman"), suoritus(solkiId = 2))

        assertEquals(listOf(1), haku("Ranja Öhman").solkiIdt())
        assertEquals(listOf(1), haku("Öhman Ranja").solkiIdt())
        assertEquals(emptyList(), haku("Ranja Virtanen").solkiIdt())
        assertEquals(emptyList(), haku("RanjaÖhman").solkiIdt())
        assertEquals(emptyList(), haku("ÖhmanRanja").solkiIdt())
    }

    @Test
    fun `puuttuvat tiedot eivät estä osumaa`() {
        tallenna(
            generateRandomYkiSuoritusEntity().copy(
                solkiId = 1,
                suorittajanOID = null,
                etunimet = "Ranja",
                sukunimi = "Öhman",
                email = null,
                hetu = null,
                tutkintopaiva = LocalDate.of(2020, 1, 1),
            ),
        )

        assertEquals(listOf(1), haku("ranja").solkiIdt())
    }

    @Test
    fun `tekstihaku palauttaa sivun ja koko haun kokonaismäärän`() {
        tallenna(
            suoritus(solkiId = 1, etunimet = "Ranja", tutkintopaiva = LocalDate.of(2018, 1, 1)),
            suoritus(solkiId = 2, etunimet = "Ranja", tutkintopaiva = LocalDate.of(2020, 1, 1)),
            suoritus(solkiId = 3, etunimet = "Ranja", tutkintopaiva = LocalDate.of(2019, 1, 1)),
            suoritus(solkiId = 4),
        )

        val ensimmainen = haku("ranja", limit = 2, offset = 0)
        assertEquals(listOf(2, 3), ensimmainen.solkiIdt())
        assertEquals(3L, ensimmainen.kokonaismaara)

        val toinen = haku("ranja", limit = 2, offset = 2)
        assertEquals(listOf(1), toinen.solkiIdt())
        assertEquals(3L, toinen.kokonaismaara)
    }

    @Test
    fun `sivu tulosten jälkeen kertoo silti kokonaismäärän`() {
        tallenna(suoritus(solkiId = 1, etunimet = "Ranja"), suoritus(solkiId = 2, etunimet = "Ranja"))

        val sivu = haku("ranja", limit = 10, offset = 100)

        assertEquals(emptyList(), sivu.solkiIdt())
        assertEquals(2L, sivu.kokonaismaara)
    }

    @Test
    fun `osumaton haku palauttaa nollan`() {
        tallenna(suoritus(solkiId = 1))

        assertEquals(0L, haku("zzzqqq").kokonaismaara)
        assertEquals(0L, haku("zzzqqq", limit = 10, offset = 100).kokonaismaara)
    }

    @Test
    fun `haku kohdistuu viimeisimpään versioon ja versiohistoria näyttää kaikki`() {
        tallenna(
            suoritus(solkiId = 1, etunimet = "Vanhanimi", lastModified = Instant.parse("2024-01-01T10:00:00Z")),
            suoritus(solkiId = 1, etunimet = "Uusinimi", lastModified = Instant.parse("2024-02-01T10:00:00Z")),
        )

        assertEquals(0L, haku("vanhanimi").kokonaismaara)
        assertEquals(1L, haku("uusinimi").kokonaismaara)
        assertEquals(1L, haku("vanhanimi", versionHistory = true).kokonaismaara)
        assertEquals(listOf(1), haku("vanhanimi", versionHistory = true).solkiIdt())
        assertEquals(1L, haku("henkilö").kokonaismaara)
        assertEquals(2L, haku("henkilö", versionHistory = true).kokonaismaara)
    }

    @Test
    fun `tekstihaku yhdistyy muihin rajauksiin ja toimii täydessä haussa ja tilastoissa`() {
        tallenna(
            suoritus(solkiId = 1, etunimet = "Ranja", tutkintokieli = Tutkintokieli.FIN),
            suoritus(solkiId = 2, etunimet = "Ranja", tutkintokieli = Tutkintokieli.SWE),
            suoritus(solkiId = 3, tutkintokieli = Tutkintokieli.SWE),
        )
        val rajattu = YkiSuoritusFilter.from("ranja").copy(tutkintokieli = listOf(Tutkintokieli.SWE))

        val sivu = ykiService.findSuorituksetSivu(filter = rajattu, limit = 100, offset = 0)
        assertEquals(listOf(2), sivu.solkiIdt())
        assertEquals(1L, sivu.kokonaismaara)
        assertEquals(listOf(2), repository.find(rajattu).map { it.solkiId })
        assertEquals(1L, repository.countSuoritukset(rajattu))
        assertEquals(1, ykiService.countSuorituksetRyhmittain(rajattu).size)
        assertTrue(ykiService.countSuorituksetRyhmittain(YkiSuoritusFilter.from("zzzqqq")).isEmpty())
        assertEquals(setOf(1, 2), repository.find(YkiSuoritusFilter.from("ranja")).map { it.solkiId }.toSet())
    }

    @Test
    fun `yksi kysely palauttaa saman kuin erilliset listaus ja laskenta`() {
        tallenna(
            suoritus(solkiId = 1, etunimet = "Ranja", tutkintopaiva = LocalDate.of(2018, 1, 1)),
            suoritus(solkiId = 2, sukunimi = "Ranjanen", tutkintopaiva = LocalDate.of(2020, 1, 1)),
            suoritus(solkiId = 3, email = "ranja@example.com", tutkintopaiva = LocalDate.of(2019, 1, 1)),
            suoritus(solkiId = 4),
        )
        val suodatin = YkiSuoritusFilter.from("ranja")

        val sivu = haku("ranja", limit = 2, offset = 0)

        assertEquals(
            repository.findForListView(suodatin, limit = 2, offset = 0).map { it.id },
            sivu.suoritukset.map { it.id },
        )
        assertEquals(repository.countSuoritukset(suodatin), sivu.kokonaismaara)
    }

    @Test
    fun `vain vapaa teksti valitsee yhden kyselyn polun`() {
        assertTrue(YkiSuoritusFilter.from("ranja").hasTextSearch())
        assertTrue(YkiSuoritusFilter.from("ranja 314159").hasTextSearch())
        assertFalse(YkiSuoritusFilter().hasTextSearch())
        assertFalse(YkiSuoritusFilter.from("").hasTextSearch())
        assertFalse(YkiSuoritusFilter.from("314159").hasTextSearch())
        assertFalse(YkiSuoritusFilter.from("1.2.246.562.24.10000000001").hasTextSearch())
    }

    @Test
    fun `ilman tekstihakua sivu ja kokonaismäärä tulevat erillisistä kyselyistä`() {
        tallenna(
            suoritus(solkiId = 1, tutkintopaiva = LocalDate.of(2018, 1, 1)),
            suoritus(solkiId = 2, tutkintopaiva = LocalDate.of(2020, 1, 1)),
            suoritus(solkiId = 3, tutkintopaiva = LocalDate.of(2019, 1, 1)),
        )

        val sivu = ykiService.findSuorituksetSivu(limit = 2, offset = 0)

        assertEquals(listOf(2, 3), sivu.solkiIdt())
        assertEquals(3L, sivu.kokonaismaara)
    }

    private fun odotaAuditoidut(odotetut: Set<String>): Set<String> {
        val loppu = System.currentTimeMillis() + 5_000
        var nahdyt = emptySet<String>()
        while (System.currentTimeMillis() < loppu) {
            nahdyt =
                synchronized(auditLoki) { auditLoki.list.toList() }
                    .filter { it.message == "Yki suoritus viewed" }
                    .flatMap { tapahtuma -> tapahtuma.keyValuePairs.orEmpty() }
                    .filter { it.key == "suoritus.id" }
                    .map { it.value.toString() }
                    .toSet()
            if (nahdyt.containsAll(odotetut)) return nahdyt
            Thread.sleep(20)
        }
        return nahdyt
    }

    @Test
    fun `näytetyt suoritukset kirjataan auditlokiin molemmilla poluilla`() {
        tallenna(suoritus(solkiId = 1, etunimet = "Ranja"), suoritus(solkiId = 2))

        val tekstihaku = haku("ranja").suoritukset.map { it.id.toString() }.toSet()
        assertEquals(1, tekstihaku.size)
        assertTrue(odotaAuditoidut(tekstihaku).containsAll(tekstihaku), "Tekstihaku: $tekstihaku")

        val kaikki =
            ykiService
                .findSuorituksetSivu(limit = 100, offset = 0)
                .suoritukset
                .map { it.id.toString() }
                .toSet()
        assertEquals(2, kaikki.size)
        assertTrue(odotaAuditoidut(kaikki).containsAll(kaikki), "Oletusnäkymä: $kaikki")
    }
}
