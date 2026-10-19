package fi.oph.kitu.kotoutumiskoulutus.suoritukset.error

import fi.oph.kitu.auditlogs.AuditLogger
import fi.oph.kitu.jdbc.SortDirection
import fi.oph.kitu.jdbc.findAllSorted
import io.opentelemetry.instrumentation.annotations.WithSpan
import org.springframework.stereotype.Service

@Service
class KielitestiErrorService(
    private val kielitestiSuoritusErrorRepository: KielitestiSuoritusErrorRepository,
    private val auditLogger: AuditLogger,
) {
    @WithSpan
    fun getErrors(
        sortColumn: KielitestiSuoritusErrorColumn,
        sortDirection: SortDirection,
    ) = kielitestiSuoritusErrorRepository
        .findAllSorted(sortColumn.entityName, sortDirection)
        .also {
            auditLogger.logAllInternalOnly("Kielitesti suoritus error viewed", it) { error ->
                arrayOf("suoritus.error.id" to error.id)
            }
        }

    fun countErrors(): Long = kielitestiSuoritusErrorRepository.count()
}
