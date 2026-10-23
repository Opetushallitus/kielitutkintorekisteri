package fi.oph.kitu.kotoutumiskoulutus.suoritukset.error

import fi.oph.kitu.auditlogs.AuditLogger
import fi.oph.kitu.jdbc.SortDirection
import fi.oph.kitu.jdbc.findAllSorted
import fi.oph.kitu.organisaatiot.OrganisaatioService
import io.opentelemetry.instrumentation.annotations.WithSpan
import org.springframework.stereotype.Service

@Service
class KielitestiErrorService(
    private val kielitestiSuoritusErrorRepository: KielitestiSuoritusErrorRepository,
    private val organisaatioService: OrganisaatioService,
    private val auditLogger: AuditLogger,
) {
    @WithSpan
    fun getErrors(
        sortColumn: KielitestiSuoritusErrorColumn,
        sortDirection: SortDirection,
    ): List<KielitestiSuoritusError> {
        val errors = kielitestiSuoritusErrorRepository.findAllSorted(sortColumn.entityName, sortDirection)
        auditLogger.logAllInternalOnly("Kielitesti suoritus error viewed", errors) { error ->
            arrayOf("suoritus.error.id" to error.id)
        }
        val organisaatiot = organisaatioService.getOrganisaatiot()
        return errors.map { it.copy(organisaatio = organisaatiot.nimet[it.schoolOid]) }
    }

    fun countErrors(): Long = kielitestiSuoritusErrorRepository.count()
}
