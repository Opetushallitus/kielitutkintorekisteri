package fi.oph.kitu.yki

import com.github.kagkarlsson.scheduler.task.Task
import fi.oph.kitu.util.scheduling.recurringTask
import io.opentelemetry.api.trace.Tracer
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class YkiScheduledTasks(
    private val tracer: Tracer,
) {
    @Value($$"${kitu.yki.scheduling.merkitseEiSuoritetuiksi.schedule}")
    lateinit var merkitseEiSuoritetuiksiSchedule: String

    @Bean
    fun merkitseEiSuoritetuiksi(merkinta: YkiEiSuoritusMerkinta): Task<Void> =
        tracer.recurringTask(
            "Merkitse tutkinnosta poisjääneet YKI-ilmoittautuneet",
            merkitseEiSuoritetuiksiSchedule,
        ) {
            merkinta.merkitseTutkinnostaPoisjaaneet()
        }
}
