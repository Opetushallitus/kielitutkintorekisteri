package fi.oph.kitu.ilmoittautumisjarjestelma

import com.github.kagkarlsson.scheduler.task.Task
import fi.oph.kitu.util.scheduling.recurringTask
import io.opentelemetry.api.trace.Tracer
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class IlmoittautumisjarjestelmaScheduledTasks(
    private val tracer: Tracer,
) {
    @Value($$"${kitu.yki.scheduling.retrySendingFailedArviointitilat.schedule}")
    lateinit var retrySendingFailedArviointitilatSchedule: String

    @Bean
    fun retrySendingFailedArviointitilat(ilmoittautumisjarjestelma: IlmoittautumisjarjestelmaService): Task<Void> =
        tracer.recurringTask(
            "Lähetä YKI-arviointitilat KIOS-palveluun",
            retrySendingFailedArviointitilatSchedule,
        ) {
            ilmoittautumisjarjestelma.sendAllUpdatedArvioinninTilat()
        }
}
