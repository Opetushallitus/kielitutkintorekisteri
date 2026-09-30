package fi.oph.kitu.vkt

import com.github.kagkarlsson.scheduler.task.Task
import fi.oph.kitu.util.scheduling.recurringTask
import io.opentelemetry.api.trace.Tracer
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class VktScheduledTasks(
    private val tracer: Tracer,
) {
    @Value($$"${kitu.vkt.scheduling.cleanup.schedule}")
    lateinit var vktCleanupSchedule: String

    @Bean
    fun cleanup(vktService: VktSuoritusService): Task<Void> =
        tracer.recurringTask("Poista merkityt VKT-suoritukset", vktCleanupSchedule) {
            vktService.cleanup()
        }
}
