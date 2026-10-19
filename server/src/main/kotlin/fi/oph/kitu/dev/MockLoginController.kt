package fi.oph.kitu.dev

import com.nimbusds.jose.jwk.source.ImmutableSecret
import fi.oph.kitu.dev.MockLoginController.Companion.E2E_TEST_SECRET_KEY
import fi.oph.kitu.oid.Oid
import fi.oph.kitu.security.Authority
import fi.oph.kitu.security.cas.CasUserDetails
import fi.oph.kitu.util.result.getOrThrow
import jakarta.annotation.PostConstruct
import jakarta.servlet.http.HttpServletRequest
import jakarta.servlet.http.HttpServletResponse
import kotlinx.html.a
import kotlinx.html.body
import kotlinx.html.h1
import kotlinx.html.head
import kotlinx.html.html
import kotlinx.html.lang
import kotlinx.html.li
import kotlinx.html.link
import kotlinx.html.main
import kotlinx.html.meta
import kotlinx.html.p
import kotlinx.html.span
import kotlinx.html.stream.createHTML
import kotlinx.html.strong
import kotlinx.html.title
import kotlinx.html.ul
import kotlinx.html.unsafe
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Profile
import org.springframework.core.env.Environment
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken
import org.springframework.security.core.authority.SimpleGrantedAuthority
import org.springframework.security.core.context.SecurityContextHolder
import org.springframework.security.oauth2.jose.jws.MacAlgorithm
import org.springframework.security.oauth2.jwt.JwsHeader
import org.springframework.security.oauth2.jwt.JwtClaimsSet
import org.springframework.security.oauth2.jwt.JwtEncoderParameters
import org.springframework.security.oauth2.jwt.NimbusJwtEncoder
import org.springframework.security.web.AuthenticationEntryPoint
import org.springframework.security.web.authentication.LoginUrlAuthenticationEntryPoint
import org.springframework.security.web.context.HttpSessionSecurityContextRepository
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import org.springframework.web.context.WebApplicationContext
import java.net.URI
import java.util.Date
import javax.crypto.spec.SecretKeySpec

@RestController
@RequestMapping("/dev")
@Profile("local", "e2e", "local-opintopolku")
class MockLoginController(
    private val environment: Environment,
    private val applicationContext: WebApplicationContext,
) {
    private val securityContextRepository = HttpSessionSecurityContextRepository()

    @Value($$"${kitu.appUrl}")
    private lateinit var rootUrl: String

    @PostConstruct
    fun init() = environment.exitIfProdLike(applicationContext, this)

    @GetMapping("/login", produces = [MediaType.TEXT_HTML_VALUE])
    fun mockLoginPage(): ResponseEntity<String> = ResponseEntity.ok(renderMockLoginPage(rootUrl))

    @GetMapping("/mocklogin")
    fun mocklogin(
        request: HttpServletRequest,
        response: HttpServletResponse,
    ): ResponseEntity<Unit> = mockLoginForUser(MockUser.DEFAULT, request, response)

    @GetMapping("/mocklogin/{user}")
    fun mocklogin2(
        request: HttpServletRequest,
        response: HttpServletResponse,
        @PathVariable user: MockUser,
    ): ResponseEntity<Unit> = mockLoginForUser(user, request, response)

    private fun mockLoginForUser(
        user: MockUser,
        request: HttpServletRequest,
        response: HttpServletResponse,
    ): ResponseEntity<Unit> {
        val userDetails = user.login.toCasUserDetails()
        val authentication =
            UsernamePasswordAuthenticationToken(
                userDetails,
                null,
                userDetails.authorities,
            )
        SecurityContextHolder.getContext().authentication = authentication
        securityContextRepository.saveContext(SecurityContextHolder.getContext(), request, response)

        return if (user.login.authorities.contains(Authority.VIRKAILIJA)) {
            ResponseEntity.status(HttpStatus.FOUND).location(URI.create(rootUrl)).build()
        } else {
            ResponseEntity.ok().build()
        }
    }

    @PostMapping(
        "/oauth/token",
        consumes = ["application/x-www-form-urlencoded"],
        produces = ["application/json"],
    )
    fun mockOAuth2Token(
        @RequestParam("grant_type") grantType: String,
        @RequestParam("client_id") mockUser: MockUser,
        @RequestParam("client_secret") clientSecret: String,
    ) = mockUser.login.toOAuthTokenResponse()

    companion object {
        // Salainen avain on 256 A-kirjainta
        val E2E_TEST_SECRET_KEY = SecretKeySpec(ByteArray(256) { 65 }, "HmacSHA256")
    }
}

data class MockLogin(
    val name: String,
    val authorities: List<Authority>,
) {
    fun toCasUserDetails() =
        CasUserDetails(
            name = name,
            // Can be any valid OID. Currently oppijanumero for Ranja Testi Öhman-Testi
            oid = Oid.parse("1.2.246.562.24.20281155246").getOrThrow(),
            strongAuth = false,
            kayttajaTyyppi = "VIRKAILIJA",
            authorities = authorities.map { SimpleGrantedAuthority(it.role()) },
        )

    fun toOAuthTokenResponse(): OAuthTokenResponse {
        val header = JwsHeader.with(MacAlgorithm.HS256).build()
        val claims =
            JwtClaimsSet
                .builder()
                .subject(name)
                .audience(listOf("test-audience"))
                .expiresAt(Date(System.currentTimeMillis() + 60_000).toInstant())
                .claim("scope", authorities.joinToString(" "))
                .build()
        val jwt =
            NimbusJwtEncoder(ImmutableSecret(E2E_TEST_SECRET_KEY.encoded)).encode(
                JwtEncoderParameters.from(header, claims),
            )
        return OAuthTokenResponse(jwt.tokenValue)
    }
}

enum class MockUser(
    val login: MockLogin,
) {
    DEFAULT(
        MockLogin(
            name = "kitu_mocklogin",
            authorities =
                listOf(
                    Authority.VIRKAILIJA,
                    Authority.YKI_TALLENNUS,
                    Authority.YKI_ARVIOIJAREKISTERI,
                    Authority.VKT_TALLENNUS,
                ),
        ),
    ),
    ROOT(
        MockLogin(
            name = "kitu_mocklogin",
            authorities =
                listOf(
                    Authority.VIRKAILIJA,
                    Authority.YKI_TALLENNUS,
                    Authority.YKI_ARVIOIJAREKISTERI,
                    Authority.VKT_TALLENNUS,
                    Authority.TODISTUS_YHTEYSTIEDOT_LUKEMINEN,
                ),
        ),
    ),
    VIRKAILIJA(
        MockLogin(
            name = "kitu_mocklogin_virkailija",
            authorities =
                listOf(
                    Authority.VIRKAILIJA,
                ),
        ),
    ),
    KIOS(
        MockLogin(
            name = "kitu_mocklogin_kios",
            authorities =
                listOf(
                    Authority.VKT_TALLENNUS,
                ),
        ),
    ),
    SOLKI(
        MockLogin(
            name = "kitu_mocklogin_solki",
            authorities =
                listOf(
                    Authority.YKI_TALLENNUS,
                ),
        ),
    ),
    KOSKI(
        MockLogin(
            name = "kitu_mocklogin_koski",
            authorities =
                listOf(
                    Authority.TODISTUS_YHTEYSTIEDOT_LUKEMINEN,
                ),
        ),
    ),
    NO_ROLES(
        MockLogin(
            name = "kitu_mocklogin_no_roles",
            authorities =
                listOf(),
        ),
    ),
}

data class OAuthTokenResponse(
    val access_token: String,
    val token_type: String = "bearer",
    val expires_in: Long = 3600,
)

private fun renderMockLoginPage(rootUrl: String): String =
    createHTML().html {
        lang = "fi"
        head {
            title { +"Mock-kirjautuminen" }
            meta(name = "color-scheme", content = "light")
            link(href = "$rootUrl/pico.min.css", rel = "stylesheet")
            link(href = "$rootUrl/style.css", rel = "stylesheet")
            unsafe { +"<style>.mocklogin-roles{color:var(--muted-color);font-size:0.875rem;}</style>" }
        }
        body {
            main("container") {
                h1 { +"Mock-kirjautuminen" }
                p { +"Valitse käyttäjätili offline-kehitystä varten." }
                ul {
                    MockUser.entries.forEach { user ->
                        li {
                            a(href = "$rootUrl/dev/mocklogin/${user.name}") {
                                strong { +user.name }
                            }
                            val roles =
                                user.login.authorities
                                    .joinToString(", ") { it.name }
                                    .ifEmpty { "ei rooleja" }
                            span(classes = "mocklogin-roles") { +" — $roles" }
                        }
                    }
                }
            }
        }
    }

/**
 * Offline-tilassa virkailijaa ei voi ohjata Untuvan CAS-loginiin, joten korvataan
 * CAS-entrypoint paikallisella ohjauksella mock-kirjautumissivulle.
 */
@Configuration
@Profile("local-opintopolku")
class MockLoginEntryPointConfig {
    @Bean
    fun authenticationEntryPoint(): AuthenticationEntryPoint = LoginUrlAuthenticationEntryPoint("/dev/login")
}
