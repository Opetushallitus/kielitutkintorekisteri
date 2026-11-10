package fi.oph.kitu.util.scheduling

import com.github.kagkarlsson.scheduler.task.Task
import fi.oph.kitu.DBContainerConfiguration
import io.opentelemetry.api.trace.Tracer
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Import
import org.springframework.jdbc.core.JdbcTemplate
import kotlin.test.assertTrue
import kotlin.test.fail

private const val TEHTAVA = "suorituslokitesti"

@SpringBootTest(properties = ["db-scheduler.polling-interval=1s"])
@Import(DBContainerConfiguration::class, SuorituslokiTest.Tehtava::class)
class SuorituslokiTest(
    @param:Autowired private val jdbc: JdbcTemplate,
) {
    @TestConfiguration
    class Tehtava {
        @Bean
        fun suorituslokitestiTehtava(tracer: Tracer): Task<Int> =
            tracer.recurringStatefulTask(TEHTAVA, "FIXED_DELAY|1s", 0) { it + 1 }
    }

    @AfterEach
    fun siivoa() {
        jdbc.update("DELETE FROM scheduled_execution_logs WHERE task_name = ?", TEHTAVA)
    }

    @Test
    fun `ajo kirjoittaa rivin suoritushistoriaan tehtavan datan kanssa`() {
        val rivi = odotaRivia()

        assertTrue(rivi.getValue("succeeded") as Boolean)
        assertTrue(rivi["task_data"] != null, "task_data puuttuu: sarjallistaja ei ratkennut")
    }

    private fun odotaRivia(): Map<String, Any?> {
        repeat(60) {
            jdbc
                .queryForList(
                    "SELECT succeeded, task_data FROM scheduled_execution_logs WHERE task_name = ? LIMIT 1",
                    TEHTAVA,
                ).firstOrNull()
                ?.let { return it }
            Thread.sleep(500)
        }
        fail("Tehtävän $TEHTAVA ajosta ei syntynyt riviä scheduled_execution_logs-tauluun 30 sekunnissa")
    }
}
