package fi.oph.kitu.auditlogs

import fi.oph.kitu.observability.add
import fi.oph.kitu.oid.Oid
import fi.oph.kitu.security.cas.CasUserDetails
import fi.oph.kitu.util.TimeService
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.beans.factory.annotation.Value
import org.springframework.core.task.AsyncTaskExecutor
import org.springframework.security.core.context.SecurityContextHolder
import org.springframework.stereotype.Service
import tools.jackson.databind.ObjectMapper
import java.util.concurrent.atomic.AtomicInteger

const val AUDIT_LOGGER_NAME = "auditLogger"

@Service
class AuditLogger(
    @param:Qualifier("applicationTaskExecutor")
    private val taskExecutor: AsyncTaskExecutor,
    val objectMapper: ObjectMapper,
    private val timeService: TimeService,
) {
    private val slf4jLogger = LoggerFactory.getLogger(AUDIT_LOGGER_NAME)

    private val logSeq = AtomicInteger(0)
    val bootTime = timeService.now()

    @Value($$"${kitu.appUrl}")
    lateinit var appUrl: String

    @Value($$"${kitu.env.name}")
    lateinit var environment: String

    /**
     * Note: the logged events with this method will be passed to an audit log integration on a lambda.
     */
    fun log(
        operation: AuditLogOperation,
        oppijaHenkiloOid: Oid,
    ) {
        AuditContext.get().onRight { context ->
            slf4jLogger.info(
                objectMapper.writeValueAsString(
                    AuditLogEntry(
                        version = 1,
                        logSeq = logSeq.getAndIncrement(),
                        bootTime = bootTime,
                        type = "log",
                        environment = environment,
                        hostname = appUrl,
                        timestamp = timeService.now(),
                        serviceName = "kitu",
                        applicationType = "backend",
                        user = AuditLogEntry.User(context.userOid),
                        target = AuditLogEntry.Target(oppijaHenkiloOid),
                        organizationOid = context.opetushallitusOrganisaatioOid,
                        operation = operation,
                    ),
                ),
            )
        }
    }

    fun <E> logAllInternalOnly(
        message: String,
        entities: Iterable<E>,
        properties: (E) -> Array<Pair<String, Any?>>,
    ) {
        val userId =
            (SecurityContextHolder.getContext().authentication?.principal as? CasUserDetails)?.oid
        taskExecutor.execute {
            for (entity in entities) {
                val props = properties(entity) + ("principal.oid" to userId)
                slf4jLogger
                    .atInfo()
                    .add(*props)
                    .log(message)
            }
        }
    }
}
