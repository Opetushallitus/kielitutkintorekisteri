package fi.oph.kitu

import fi.oph.kitu.oid.Oid
import fi.oph.kitu.security.Authority
import fi.oph.kitu.security.cas.CasUserDetails
import fi.oph.kitu.util.result.getOrThrow
import org.springframework.mock.web.MockHttpSession
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken
import org.springframework.security.core.authority.SimpleGrantedAuthority
import org.springframework.security.core.context.SecurityContextHolder
import org.springframework.security.web.context.HttpSessionSecurityContextRepository

fun virkailijaSession(
    asiointikieli: String? = null,
    authorities: List<Authority> = listOf(Authority.VIRKAILIJA),
): MockHttpSession {
    val principal =
        CasUserDetails(
            name = "test-virkailija",
            oid = Oid.parse("1.2.246.562.24.20281155246").getOrThrow(),
            strongAuth = false,
            kayttajaTyyppi = "VIRKAILIJA",
            asiointikieli = asiointikieli,
            authorities = authorities.map { SimpleGrantedAuthority(it.role()) },
        )
    val securityContext =
        SecurityContextHolder.createEmptyContext().apply {
            authentication = UsernamePasswordAuthenticationToken(principal, null, principal.authorities)
        }
    return MockHttpSession().also {
        it.setAttribute(HttpSessionSecurityContextRepository.SPRING_SECURITY_CONTEXT_KEY, securityContext)
    }
}
