package fi.oph.kitu.security.cas

import com.github.kagkarlsson.scheduler.task.Task
import fi.oph.kitu.util.scheduling.recurringTask
import io.opentelemetry.api.trace.Tracer
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class CasScheduledTasks(
    private val tracer: Tracer,
) {
    @Value($$"${kitu.cas.scheduling.sessionMappingCleanup.schedule}")
    lateinit var sessionMappingCleanupSchedule: String

    @Bean
    fun cleanupCasClientSessions(sessionMappingStorage: JdbcSessionMappingStorage): Task<Void> =
        tracer.recurringTask("Siivoa vanhentuneet CAS-session-kuvaukset", sessionMappingCleanupSchedule) {
            sessionMappingStorage.clean()
        }
}
