package fi.oph.kitu.yki

import fi.oph.kitu.DBContainerConfiguration
import fi.oph.kitu.auditlogs.AuditLogger
import fi.oph.kitu.dev.mockdata.generateRandomYkiSuoritusEntity
import fi.oph.kitu.oppijanumero.OppijanumeroService
import fi.oph.kitu.util.TimeService
import fi.oph.kitu.yki.suoritukset.YkiSuoritusRepository
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.jdbc.core.JdbcTemplate
import java.time.LocalDate
import kotlin.test.assertEquals
import kotlin.test.assertNull

@SpringBootTest
@Import(DBContainerConfiguration::class)
class YkiServiceVarhaisinTutkintopaivaTest(
    @param:Autowired private val repository: YkiSuoritusRepository,
    @param:Autowired private val auditLogger: AuditLogger,
    @param:Autowired private val oppijanumeroService: OppijanumeroService,
    @param:Autowired private val timeService: TimeService,
    @param:Autowired private val jdbc: JdbcTemplate,
) {
    @BeforeEach
    @AfterEach
    fun tyhjenna() {
        jdbc.execute("TRUNCATE TABLE yki_suoritus_lisatieto")
        jdbc.execute("TRUNCATE TABLE yki_suoritus CASCADE")
    }

    private fun palvelu(ttl: String) = YkiService(repository, auditLogger, oppijanumeroService, timeService, ttl)

    private fun lisaaSuoritus(tutkintopaiva: LocalDate) {
        repository.saveAllNewEntities(listOf(generateRandomYkiSuoritusEntity().copy(tutkintopaiva = tutkintopaiva)))
    }

    @Test
    fun `varhaisin tutkintopäivä tulee välimuistista voimassaoloaikana`() {
        lisaaSuoritus(LocalDate.of(2020, 1, 1))
        val palvelu = palvelu("1h")
        assertEquals(LocalDate.of(2020, 1, 1), palvelu.findVarhaisinTutkintopaiva())

        lisaaSuoritus(LocalDate.of(2015, 1, 1))

        assertEquals(LocalDate.of(2020, 1, 1), palvelu.findVarhaisinTutkintopaiva())
        assertEquals(LocalDate.of(2015, 1, 1), palvelu("1h").findVarhaisinTutkintopaiva())
    }

    @Test
    fun `nolla-TTL lukee varhaisimman tutkintopäivän aina kannasta`() {
        lisaaSuoritus(LocalDate.of(2020, 1, 1))
        val palvelu = palvelu("0s")
        assertEquals(LocalDate.of(2020, 1, 1), palvelu.findVarhaisinTutkintopaiva())

        lisaaSuoritus(LocalDate.of(2015, 1, 1))

        assertEquals(LocalDate.of(2015, 1, 1), palvelu.findVarhaisinTutkintopaiva())
    }

    @Test
    fun `tyhjän rekisterin tulosta ei jätetä välimuistiin`() {
        val palvelu = palvelu("1h")
        assertNull(palvelu.findVarhaisinTutkintopaiva())

        lisaaSuoritus(LocalDate.of(2020, 1, 1))

        assertEquals(LocalDate.of(2020, 1, 1), palvelu.findVarhaisinTutkintopaiva())
    }
}
