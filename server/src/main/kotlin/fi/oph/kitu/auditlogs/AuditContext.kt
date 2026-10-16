package fi.oph.kitu.auditlogs

import arrow.core.Either
import arrow.core.left
import arrow.core.right
import fi.oph.kitu.oid.Oid
import fi.oph.kitu.security.cas.CasUserDetails
import fi.oph.kitu.util.result.getOrThrow
import org.springframework.core.io.ClassPathResource
import org.springframework.security.core.context.SecurityContextHolder
import org.springframework.web.context.request.RequestContextHolder
import org.springframework.web.context.request.ServletRequestAttributes
import java.net.InetAddress
import java.util.Properties

data class AuditContext(
    val userOid: Oid,
    val userAgent: String,
    val ip: InetAddress,
    val session: String,
    val opetushallitusOrganisaatioOid: Oid,
) {
    companion object {
        /**
         * Gets the value for `oph.oid` from `application.properties` without using Spring at all.
         */
        private fun getOpetushallitusOrganizationOid(): Oid {
            val props = Properties()

            val resource = ClassPathResource("application.properties")
            resource.inputStream.use { stream ->
                props.load(stream)
            }

            val oid = props.getProperty("oph.oid")
            val parsed = Oid.parse(oid).getOrThrow()
            return parsed
        }

        fun get(): Either<IllegalStateException, AuditContext> {
            val userDetails: CasUserDetails =
                SecurityContextHolder.getContext().authentication?.principal as? CasUserDetails?
                    ?: return IllegalStateException("User details not available via SecurityContextHolder").left()
            val servletRequestAttributes =
                RequestContextHolder.getRequestAttributes() as? ServletRequestAttributes?
                    ?: return IllegalStateException("HTTP request not available via RequestContextHolder").left()
            val request = servletRequestAttributes.request

            val userOid = userDetails.oid

            val opetushallitusOrganisaatioOid = getOpetushallitusOrganizationOid()

            val userAgent = request.getHeader("user-agent").orEmpty()
            val ip = InetAddress.getByName(request.remoteAddr)
            val session = request.session.id

            return AuditContext(userOid, userAgent, ip, session, opetushallitusOrganisaatioOid).right()
        }
    }
}
