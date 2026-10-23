package fi.oph.kitu.kotoutumiskoulutus.suoritukset

import fi.oph.kitu.auditlogs.AuditLogOperation
import fi.oph.kitu.auditlogs.AuditLogger
import fi.oph.kitu.observability.setAttribute
import fi.oph.kitu.observability.use
import fi.oph.kitu.organisaatiot.OrganisaatioService
import fi.oph.kitu.util.TimeService
import io.opentelemetry.api.trace.Tracer
import io.opentelemetry.instrumentation.annotations.WithSpan
import org.springframework.stereotype.Service
import java.time.LocalDate

@Service
class KielitestiSuoritusService(
    private val customKielitestiSuoritusRepository: CustomKielitestiSuoritusRepository,
    private val auditLogger: AuditLogger,
    private val organisaatioService: OrganisaatioService,
    private val tracer: Tracer,
) {
    @WithSpan
    fun getSuoritukset(
        filter: KielitestiSuoritusFilter = KielitestiSuoritusFilter(),
        order: KielitestiSuoritusOrder = KielitestiSuoritusOrder(),
    ): List<KielitestiSuoritus> =
        customKielitestiSuoritusRepository
            .findSuoritukset(filter, order)
            .toList()
            .also {
                auditLogger.logAllInternalOnly("Kielitesti suoritus viewed", it) { suoritus ->
                    arrayOf(
                        "suoritus.id" to suoritus.id,
                        "suoritus.oppijanumero" to suoritus.oppijanumero,
                    )
                }
            }

    @WithSpan
    fun countSuoritukset(filter: KielitestiSuoritusFilter = KielitestiSuoritusFilter()): Int =
        customKielitestiSuoritusRepository
            .countSuoritukset(filter)

    @WithSpan
    fun findVarhaisinSuorituspaiva(): LocalDate? =
        customKielitestiSuoritusRepository
            .findVarhaisinSuoritusaika()
            ?.atZone(TimeService.zoneId)
            ?.toLocalDate()

    @WithSpan
    fun getSuoritusById(id: Int): KielitestiSuoritus? =
        customKielitestiSuoritusRepository.findById(id).also { suoritus ->
            suoritus?.oppijanumero?.let { oid ->
                auditLogger.log(AuditLogOperation.KielitestiSuoritusViewed, oid)
            }
        }

    fun getSuorituksetForCsv(filter: KielitestiSuoritusFilter = KielitestiSuoritusFilter()): List<KielitestiSuoritus> =
        tracer
            .spanBuilder("KoealustaService.getSuorituksetForCsv")
            .startSpan()
            .use { span ->
                val suoritukset = getSuoritukset(filter, KielitestiSuoritusOrder(pageSize = Int.MAX_VALUE))
                span.setAttribute("dataCount", suoritukset.count())

                val organisaatiot = organisaatioService.getOrganisaatiot()

                return suoritukset.map { it.copy(oppilaitos = organisaatiot.nimet[it.oppilaitosOid]?.fi) }
            }
}
