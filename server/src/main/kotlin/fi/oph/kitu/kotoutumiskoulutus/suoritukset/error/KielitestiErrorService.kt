package fi.oph.kitu.kotoutumiskoulutus.suoritukset.error

import fi.oph.kitu.auditlogs.AuditLogger
import fi.oph.kitu.csvparsing.CsvGenerator
import fi.oph.kitu.jdbc.SortDirection
import fi.oph.kitu.jdbc.findAllSorted
import fi.oph.kitu.observability.setAttribute
import fi.oph.kitu.observability.use
import io.opentelemetry.api.trace.Tracer
import io.opentelemetry.instrumentation.annotations.WithSpan
import org.springframework.stereotype.Service
import java.io.ByteArrayOutputStream

@Service
class KielitestiErrorService(
    private val kielitestiSuoritusErrorRepository: KielitestiSuoritusErrorRepository,
    private val auditLogger: AuditLogger,
    private val tracer: Tracer,
    private val csvGenerator: CsvGenerator,
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

    fun generateErrorsCsvStream(
        orderBy: KielitestiSuoritusErrorColumn = KielitestiSuoritusErrorColumn.VirheenLuontiaika,
        orderByDirection: SortDirection = SortDirection.DESC,
    ): ByteArrayOutputStream =
        tracer
            .spanBuilder("KoealustaService.generateErrorsCsvStream")
            .startSpan()
            .use { span ->
                val errors = getErrors(orderBy, orderByDirection)
                span.setAttribute("dataCount", errors.count())
                val outputStream = ByteArrayOutputStream()
                csvGenerator
                    .withUseHeader(true)
                    .withColumnSeparator(';')
                    .streamDataAsCsv(outputStream, errors)

                return@use outputStream
            }
}
