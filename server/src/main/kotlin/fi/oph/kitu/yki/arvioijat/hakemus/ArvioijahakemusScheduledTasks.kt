package fi.oph.kitu.yki.arvioijat.hakemus

import com.github.kagkarlsson.scheduler.task.Task
import fi.oph.kitu.config.ConditionalOnNonEmptyProperty
import fi.oph.kitu.util.scheduling.recurringTask
import io.opentelemetry.api.trace.Tracer
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
@ConditionalOnNonEmptyProperty("kitu.ataru.service.url")
class ArvioijahakemusScheduledTasks(
    private val tracer: Tracer,
) {
    @Value($$"${kitu.ataru.arvioijahakemus.schedule}")
    lateinit var schedule: String

    @Bean
    fun tuoArvioijahakemukset(service: ArvioijahakemusTuontiService): Task<Void> =
        tracer.recurringTask("Tuo YKI-arvioijahakemukset atarusta", schedule) {
            service.tuo()
        }
}
