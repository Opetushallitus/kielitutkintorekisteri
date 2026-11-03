package fi.oph.kitu.yki

import fi.oph.kitu.DBContainerConfiguration
import fi.oph.kitu.TestTimeService
import fi.oph.kitu.dev.mockdata.generateRandomYkiSuoritusEntity
import fi.oph.kitu.tyhjennaYkiSuoritukset
import fi.oph.kitu.yki.suoritukset.YkiSuoritusEntity
import fi.oph.kitu.yki.suoritukset.YkiSuoritusRepository
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.jdbc.core.JdbcTemplate
import java.time.Instant
import java.time.LocalDate
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull

@SpringBootTest
@Import(DBContainerConfiguration::class)
class YkiEiSuoritusMerkintaTest(
    @param:Autowired private val merkinta: YkiEiSuoritusMerkinta,
    @param:Autowired private val ykiService: YkiService,
    @param:Autowired private val suoritusRepository: YkiSuoritusRepository,
    @param:Autowired private val timeService: TestTimeService,
    @param:Autowired private val jdbcTemplate: JdbcTemplate,
) {
    private val keskipaiva = Instant.parse("2026-06-15T09:00:00Z")
    private val tanaan = LocalDate.of(2026, 6, 15)
    private val aiemminTallennettu = Instant.parse("2025-01-01T10:00:00Z")

    @BeforeEach
    @AfterEach
    fun tyhjenna() {
        jdbcTemplate.tyhjennaYkiSuoritukset()
        timeService.resetClock()
    }

    private fun suoritus(
        solkiId: Int,
        tutkintopaiva: LocalDate,
        arviointitila: Arviointitila = Arviointitila.ILMOITTAUTUNUT,
        lastModified: Instant = aiemminTallennettu,
    ): YkiSuoritusEntity =
        generateRandomYkiSuoritusEntity().copy(
            solkiId = solkiId,
            lahdejarjestelmanTunnus = "yki.$solkiId",
            tutkintopaiva = tutkintopaiva,
            arviointitila = arviointitila,
            lastModified = lastModified,
            receivedAt = lastModified,
        )

    private fun tallenna(vararg suoritukset: YkiSuoritusEntity) {
        suoritukset.forEach { suoritusRepository.save(it, false) }
    }

    private fun tila(solkiId: Int) = suoritusRepository.findLatestBySolkiIds(listOf(solkiId)).single().arviointitila

    private fun versiomaara(solkiId: Int): Int =
        jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM yki_suoritus WHERE solki_id = ?",
            Int::class.javaObjectType,
            solkiId,
        )!!

    private fun ajaKeskipaivalla(): Int =
        timeService.runWithFixedClock(keskipaiva) { merkinta.merkitseTutkinnostaPoisjaaneet() }

    @Test
    fun `merkitsee vähintään 14 päivää sitten järjestetyt mutta ei tuoreempia`() {
        tallenna(
            suoritus(1, tanaan.minusDays(30)),
            suoritus(2, tanaan.minusDays(14)),
            suoritus(3, tanaan.minusDays(13)),
            suoritus(4, tanaan),
            suoritus(5, tanaan.plusDays(10)),
        )

        assertEquals(2, ajaKeskipaivalla())

        assertEquals(Arviointitila.EI_SUORITUSTA, tila(1))
        assertEquals(Arviointitila.EI_SUORITUSTA, tila(2))
        assertEquals(Arviointitila.ILMOITTAUTUNUT, tila(3))
        assertEquals(Arviointitila.ILMOITTAUTUNUT, tila(4))
        assertEquals(Arviointitila.ILMOITTAUTUNUT, tila(5))
    }

    @Test
    fun `merkitsee vain tilassa ILMOITTAUTUNUT olevat`() {
        val vanha = tanaan.minusDays(100)
        val tilat = Arviointitila.entries.withIndex().associate { (indeksi, tila) -> 100 + indeksi to tila }
        tilat.forEach { (id, tila) -> tallenna(suoritus(id, vanha, tila)) }

        assertEquals(1, ajaKeskipaivalla())

        tilat.forEach { (id, tila) ->
            if (tila == Arviointitila.ILMOITTAUTUNUT) {
                assertEquals(Arviointitila.EI_SUORITUSTA, tila(id), "alkuperäinen tila $tila")
                assertEquals(2, versiomaara(id), "alkuperäinen tila $tila")
            } else {
                assertEquals(tila, tila(id), "alkuperäinen tila $tila")
                assertEquals(1, versiomaara(id), "alkuperäinen tila $tila")
            }
        }
    }

    @Test
    fun `merkintä kirjoittaa uuden version, jossa jokaisen osakokeen arvosana on 12`() {
        tallenna(
            suoritus(1, tanaan.minusDays(30))
                .copy(
                    arviointipaiva = null,
                    tekstinYmmartaminen = null,
                    kirjoittaminen = null,
                    rakenteetJaSanasto = null,
                    puheenYmmartaminen = null,
                    puhuminen = null,
                    yleisarvosana = null,
                    tarkistusarvioinninSaapumisPvm = null,
                    tarkistusarvioinninAsiatunnus = null,
                    tarkistusarvioidutOsakokeet = null,
                    arvosanaMuuttui = null,
                    perustelu = null,
                    tarkistusarvioinninKasittelyPvm = null,
                ).also { it.ilmoitetutOsakokeet = setOf(TutkinnonOsa.PU, TutkinnonOsa.KI) },
        )

        ajaKeskipaivalla()

        val merkitty = suoritusRepository.findLatestBySolkiIds(listOf(1)).single()
        assertEquals(12, merkitty.puhuminen)
        assertEquals(12, merkitty.kirjoittaminen)
        assertNull(merkitty.tekstinYmmartaminen, "osakoetta ei ollut ilmoittautumisessa")
        assertEquals(keskipaiva, merkitty.lastModified)
        assertEquals(2, versiomaara(1))
    }

    @Test
    fun `viimeisin versio ratkaisee - ilmoittautumisen jälkeen arvioitua ei merkitä`() {
        val vanha = tanaan.minusDays(30)
        tallenna(
            suoritus(1, vanha),
            suoritus(1, vanha, Arviointitila.ARVIOITU, lastModified = aiemminTallennettu.plusSeconds(60)),
        )

        assertEquals(0, ajaKeskipaivalla())

        assertEquals(Arviointitila.ARVIOITU, tila(1))
        assertEquals(2, versiomaara(1))
    }

    @Test
    fun `viimeisin versio ratkaisee - arvioinnin jälkeen ilmoittautunut merkitään`() {
        val vanha = tanaan.minusDays(30)
        tallenna(
            suoritus(1, vanha, Arviointitila.ARVIOITU),
            suoritus(1, vanha, lastModified = aiemminTallennettu.plusSeconds(60)),
        )

        assertEquals(1, ajaKeskipaivalla())

        assertEquals(Arviointitila.EI_SUORITUSTA, tila(1))
    }

    @Test
    fun `seuraava ajo ei kirjoita mitään`() {
        tallenna(suoritus(1, tanaan.minusDays(30)))
        ajaKeskipaivalla()

        assertEquals(0, ajaKeskipaivalla())

        assertEquals(2, versiomaara(1))
    }

    @Test
    fun `tämä päivä lasketaan Suomen aikavyöhykkeellä`() {
        val iltaUtc = Instant.parse("2026-06-15T21:30:00Z")
        tallenna(
            suoritus(1, LocalDate.of(2026, 6, 2)),
            suoritus(2, LocalDate.of(2026, 6, 3)),
        )

        val merkitty = timeService.runWithFixedClock(iltaUtc) { merkinta.merkitseTutkinnostaPoisjaaneet() }

        assertEquals(1, merkitty, "Helsingissä on jo 16.6., joten raja on 2.6.")
        assertEquals(Arviointitila.EI_SUORITUSTA, tila(1))
        assertEquals(Arviointitila.ILMOITTAUTUNUT, tila(2))
    }

    @Test
    fun `päivien määrä on asetettavissa`() {
        val kolmenPaivanRaja = YkiEiSuoritusMerkinta(ykiService, suoritusRepository, timeService, 3)
        tallenna(
            suoritus(1, tanaan.minusDays(3)),
            suoritus(2, tanaan.minusDays(2)),
        )

        val merkitty = timeService.runWithFixedClock(keskipaiva) { kolmenPaivanRaja.merkitseTutkinnostaPoisjaaneet() }

        assertEquals(1, merkitty)
        assertEquals(Arviointitila.EI_SUORITUSTA, tila(1))
        assertEquals(Arviointitila.ILMOITTAUTUNUT, tila(2))
    }

    @Test
    fun `epäonnistuva suoritus ei pysäytä muita mutta ajo kaatuu lopuksi`() {
        val vanha = tanaan.minusDays(30)
        val liianPitkaTunnus = "yki.${"1".repeat(1000)}"
        tallenna(
            suoritus(1, vanha).copy(lahdejarjestelmanTunnus = liianPitkaTunnus),
            suoritus(2, vanha),
            suoritus(3, vanha),
        )

        val virhe = assertFailsWith<IllegalStateException> { ajaKeskipaivalla() }

        assertContains(virhe.message.orEmpty(), "1 YKI-suorituksen merkintä epäonnistui")
        assertEquals(Arviointitila.ILMOITTAUTUNUT, tila(1))
        assertEquals(1, versiomaara(1), "auditoinnin kaatuminen peruu version")
        assertEquals(Arviointitila.EI_SUORITUSTA, tila(2))
        assertEquals(Arviointitila.EI_SUORITUSTA, tila(3))
    }
}
