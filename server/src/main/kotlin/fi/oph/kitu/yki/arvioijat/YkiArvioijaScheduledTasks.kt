package fi.oph.kitu.yki.arvioijat

import com.github.kagkarlsson.scheduler.task.Task
import fi.oph.kitu.util.scheduling.recurringTask
import io.opentelemetry.api.trace.Tracer
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class YkiArvioijaScheduledTasks(
    private val tracer: Tracer,
) {
    @Value($$"${kitu.yki.scheduling.poistaVanhentuneetArvioijat.schedule}")
    lateinit var sailytysaikaSchedule: String

    @Value($$"${kitu.yki.scheduling.paivitaArvioijaProjektiot.schedule}")
    lateinit var projektioSchedule: String

    @Value($$"${kitu.yki.scheduling.synkronoiArvioijaKaudet.schedule}")
    lateinit var kausisynkronointiSchedule: String

    @Bean
    fun poistaVanhentuneetArvioijat(arvioijaService: YkiArvioijaService): Task<Void> =
        tracer.recurringTask(
            "Poista sailytysajan ylittaneet YKI-arvioijamerkinnat",
            sailytysaikaSchedule,
        ) {
            arvioijaService.poistaSailytysajanYlittaneet()
        }

    @Bean
    fun paivitaArvioijaProjektiot(kausiService: YkiArvioijaKausiService): Task<Void> =
        tracer.recurringTask(
            "Paivita YKI-arvioijien arviointioikeusprojektio",
            projektioSchedule,
        ) {
            kausiService.paivitaProjektiot()
        }

    @Bean
    fun synkronoiArvioijaKaudet(kausiService: YkiArvioijaKausiService): Task<Void> =
        tracer.recurringTask(
            "Synkronoi YKI-arvioijien kaudet arviointioikeuksista",
            kausisynkronointiSchedule,
        ) {
            kausiService.synkronoiKaudetArviointioikeuksista()
        }
}
