package fi.oph.kitu.auditlogs

import fi.oph.kitu.observability.add
import fi.oph.kitu.observability.forEventType
import fi.oph.kitu.security.cas.CasUserDetails
import org.slf4j.LoggerFactory
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.security.authentication.event.AbstractAuthenticationEvent
import org.springframework.security.authentication.event.AbstractAuthenticationFailureEvent
import org.springframework.security.authentication.event.AuthenticationSuccessEvent
import org.springframework.security.authentication.event.LogoutSuccessEvent
import org.springframework.security.core.Authentication
import org.springframework.security.oauth2.jwt.Jwt
import org.springframework.web.context.request.RequestContextHolder
import org.springframework.web.context.request.ServletRequestAttributes

/**
 * Audit-logs Spring Security authentication events (login success/failure, logout)
 * onto the [AUDIT_LOGGER_NAME] channel. Failed-login telemetry enables brute-force
 * detection and account-takeover triage that the per-operation [AuditLogger] does
 * not cover.
 */
@Configuration
class AuthEventAuditListener {
    private val logger = LoggerFactory.getLogger(AUDIT_LOGGER_NAME)

    @Bean
    fun authenticationSuccessAuditListener() =
        forEventType<AuthenticationSuccessEvent> { event ->
            logger
                .atInfo()
                .add(*commonFields("auth.success", event))
                .log("Spring Security auth event")
        }

    @Bean
    fun authenticationFailureAuditListener() =
        forEventType<AbstractAuthenticationFailureEvent> { event ->
            logger
                .atInfo()
                .add(*commonFields("auth.failure", event))
                .add(
                    "auth.attempted_name" to event.authentication.name,
                    "auth.failure_reason" to event.exception.javaClass.simpleName,
                ).log("Spring Security auth event")
        }

    @Bean
    fun logoutSuccessAuditListener() =
        forEventType<LogoutSuccessEvent> { event ->
            logger
                .atInfo()
                .add(*commonFields("auth.logout", event))
                .log("Spring Security auth event")
        }

    private fun commonFields(
        event: String,
        evt: AbstractAuthenticationEvent,
    ): Array<Pair<String, Any?>> =
        arrayOf(
            "event" to event,
            "auth.principal_oid" to principalOid(evt.authentication),
            "auth.remote_addr" to remoteAddr(),
            "auth.timestamp" to evt.timestamp,
        )

    private fun principalOid(authentication: Authentication?): String? =
        when (val principal = authentication?.principal) {
            is CasUserDetails -> principal.oid.toString()
            is Jwt -> principal.subject
            else -> null
        }

    private fun remoteAddr(): String? =
        (RequestContextHolder.getRequestAttributes() as? ServletRequestAttributes)?.request?.remoteAddr
}
