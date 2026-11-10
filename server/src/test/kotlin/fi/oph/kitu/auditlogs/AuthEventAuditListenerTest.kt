package fi.oph.kitu.auditlogs

import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import fi.oph.kitu.oid.Oid
import fi.oph.kitu.security.cas.CasUserDetails
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import org.springframework.context.ApplicationEvent
import org.springframework.security.authentication.BadCredentialsException
import org.springframework.security.authentication.TestingAuthenticationToken
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken
import org.springframework.security.authentication.event.AuthenticationFailureBadCredentialsEvent
import org.springframework.security.authentication.event.AuthenticationSuccessEvent
import org.springframework.security.authentication.event.LogoutSuccessEvent
import org.springframework.security.oauth2.jwt.Jwt
import org.springframework.security.oauth2.server.resource.authentication.BearerTokenAuthenticationToken
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken
import java.time.Instant
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue
import ch.qos.logback.classic.Logger as LogbackLogger

class AuthEventAuditListenerTest {
    private val listener = AuthEventAuditListener()
    private val auditLogbackLogger = LoggerFactory.getLogger(AUDIT_LOGGER_NAME) as LogbackLogger
    private val listAppender = ListAppender<ILoggingEvent>()

    @BeforeEach
    fun setup() {
        listAppender.start()
        auditLogbackLogger.addAppender(listAppender)
    }

    @AfterEach
    fun cleanup() {
        listAppender.stop()
        auditLogbackLogger.detachAppender(listAppender)
    }

    @Test
    fun `kirjautumisen onnistuminen audit-lokitetaan principal-OIDin kanssa`() {
        val oid = Oid.parse("1.2.246.562.24.19563255030").getOrNull()!!
        val auth =
            UsernamePasswordAuthenticationToken(
                CasUserDetails(
                    name = "test",
                    oid = oid,
                    strongAuth = true,
                    kayttajaTyyppi = "VIRKAILIJA",
                    authorities = emptyList(),
                ),
                null,
                emptyList(),
            )

        fire(listener.authenticationSuccessAuditListener(), AuthenticationSuccessEvent(auth))

        val event = listAppender.list.single()
        val kv = event.keyValuePairs.associate { it.key to it.value }
        assertEquals("auth.success", kv["event"])
        assertEquals(oid.toString(), kv["auth.principal_oid"])
        assertNotNull(kv["auth.timestamp"])
    }

    @Test
    fun `kirjautumisen epäonnistuminen audit-lokitetaan syyn ja yritetyn nimen kanssa`() {
        val auth = TestingAuthenticationToken("evil-user", "wrong-password")

        fire(
            listener.authenticationFailureAuditListener(),
            AuthenticationFailureBadCredentialsEvent(auth, BadCredentialsException("nope")),
        )

        val event = listAppender.list.single()
        val kv = event.keyValuePairs.associate { it.key to it.value }
        assertEquals("auth.failure", kv["event"])
        assertEquals("evil-user", kv["auth.attempted_name"])
        assertEquals("BadCredentialsException", kv["auth.failure_reason"])
        assertNull(kv["auth.principal_oid"], "Failed auth has no resolved CasUserDetails principal")
    }

    @Test
    fun `hylatty bearer-token ei paady audit-lokiin yritettyna nimena`() {
        val auth = BearerTokenAuthenticationToken("eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJ4In0.allekirjoitus")

        fire(
            listener.authenticationFailureAuditListener(),
            AuthenticationFailureBadCredentialsEvent(auth, BadCredentialsException("expired")),
        )

        val kv =
            listAppender.list
                .single()
                .keyValuePairs
                .associate { it.key to it.value }
        assertEquals("auth.failure", kv["event"])
        assertNull(kv["auth.attempted_name"])
        assertTrue(kv.values.none { it?.toString().orEmpty().contains("eyJ") })
    }

    @Test
    fun `OAuth2-kirjautumisen onnistuessa principal_oid resolvoidaan JWT-subjectista`() {
        val jwt =
            Jwt
                .withTokenValue("token")
                .header("alg", "RS256")
                .subject("1.2.246.562.24.24817310943")
                .claim("scope", "kitu")
                .issuedAt(Instant.now())
                .expiresAt(Instant.now().plusSeconds(60))
                .build()
        val auth = JwtAuthenticationToken(jwt)

        fire(listener.authenticationSuccessAuditListener(), AuthenticationSuccessEvent(auth))

        val event = listAppender.list.single()
        val kv = event.keyValuePairs.associate { it.key to it.value }
        assertEquals("auth.success", kv["event"])
        assertEquals("1.2.246.562.24.24817310943", kv["auth.principal_oid"])
    }

    @Test
    fun `uloskirjautuminen audit-lokitetaan`() {
        val oid = Oid.parse("1.2.246.562.24.19563255030").getOrNull()!!
        val auth =
            UsernamePasswordAuthenticationToken(
                CasUserDetails(
                    name = "test",
                    oid = oid,
                    strongAuth = true,
                    kayttajaTyyppi = "VIRKAILIJA",
                    authorities = emptyList(),
                ),
                null,
                emptyList(),
            )

        fire(listener.logoutSuccessAuditListener(), LogoutSuccessEvent(auth))

        val event = listAppender.list.single()
        val kv = event.keyValuePairs.associate { it.key to it.value }
        assertEquals("auth.logout", kv["event"])
        assertEquals(oid.toString(), kv["auth.principal_oid"])
    }

    private fun fire(
        applicationListener: org.springframework.context.event.GenericApplicationListener,
        event: ApplicationEvent,
    ) {
        applicationListener.onApplicationEvent(event)
    }
}
