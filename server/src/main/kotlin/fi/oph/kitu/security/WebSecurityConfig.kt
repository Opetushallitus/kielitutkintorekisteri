package fi.oph.kitu.security

import fi.oph.kitu.config.AppProfile
import fi.oph.kitu.config.hasNoneOfProfiles
import fi.oph.kitu.config.hasOneOfProfiles
import fi.oph.kitu.config.isE2ETest
import fi.oph.kitu.dev.MockLoginController.Companion.E2E_TEST_SECRET_KEY
import fi.oph.kitu.dev.MockUser
import fi.oph.kitu.security.cas.CasConfig
import fi.oph.kitu.security.oauth2.JwtAuthenticationTokenConverter
import fi.oph.kitu.yki.arvioijat.ArvioijarekisteriAsetukset
import jakarta.servlet.http.HttpServletRequest
import org.apereo.cas.client.session.SessionMappingStorage
import org.apereo.cas.client.session.SingleSignOutFilter
import org.springframework.boot.autoconfigure.condition.ConditionalOnBooleanProperty
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.boot.tomcat.TomcatConnectorCustomizer
import org.springframework.boot.tomcat.servlet.TomcatServletWebServerFactory
import org.springframework.boot.web.server.WebServerFactoryCustomizer
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Profile
import org.springframework.core.annotation.Order
import org.springframework.core.env.Environment
import org.springframework.http.HttpMethod.GET
import org.springframework.http.HttpMethod.POST
import org.springframework.http.HttpMethod.PUT
import org.springframework.security.authorization.AuthorizationDecision
import org.springframework.security.authorization.AuthorizationManager
import org.springframework.security.cas.web.CasAuthenticationFilter
import org.springframework.security.config.annotation.web.AuthorizeHttpRequestsDsl
import org.springframework.security.config.annotation.web.builders.HttpSecurity
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity
import org.springframework.security.config.annotation.web.invoke
import org.springframework.security.config.http.SessionCreationPolicy
import org.springframework.security.oauth2.jwt.JwtDecoder
import org.springframework.security.oauth2.jwt.NimbusJwtDecoder
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken
import org.springframework.security.web.AuthenticationEntryPoint
import org.springframework.security.web.SecurityFilterChain
import org.springframework.security.web.access.intercept.RequestAuthorizationContext
import org.springframework.security.web.csrf.CookieCsrfTokenRepository
import org.springframework.security.web.header.writers.ReferrerPolicyHeaderWriter

fun developmentProfileActive(environment: Environment): Boolean {
    val enableDevApiOn = listOf(AppProfile.Local, AppProfile.Test, AppProfile.E2ETest, AppProfile.LocalOpintopolku)
    val disableDevApiOn = listOf(AppProfile.QA, AppProfile.Prod)

    return environment.hasOneOfProfiles(enableDevApiOn) && environment.hasNoneOfProfiles(disableDevApiOn)
}

fun AuthorizeHttpRequestsDsl.configureCommonAuthorizations(environment: Environment) {
    authorize(PUT, "/api/vkt/kios", hasAnyAuthority(*Authority.VKT_TALLENNUS.authStrings()))
    authorize(POST, "/yki/api/suoritus", hasAnyAuthority(*Authority.YKI_TALLENNUS.authStrings()))
    authorize(POST, "/yki/api/arvioija", hasAnyAuthority(*Authority.YKI_TALLENNUS.authStrings()))
    authorize(POST, "/yki/api/oppijanumero-haku", hasAnyAuthority(*Authority.YKI_TALLENNUS.authStrings()))
    authorize(POST, "/yki/api/oppijanumero-haku-hetulista", sallitutHetulistahaunKutsujat(environment))
    authorize(GET, "/yhteystiedot/api/**", hasAnyAuthority(*Authority.TODISTUS_YHTEYSTIEDOT_LUKEMINEN.authStrings()))

    authorize("/actuator/health", permitAll)
    authorize("/api-docs", permitAll)
    authorize("/swagger-ui/**", permitAll)
    authorize("/v3/api-docs/**", permitAll)
    authorize("/schema-examples/**", permitAll)
    authorize("/uml/**", permitAll)
}

/**
 * Hetulistahaku palauttaa oppijanumeron pelkällä henkilötunnuksella, ilman nimivertailua.
 * YKI_TALLENNUS on myönnetty muillekin kuin migraatiolle, joten reitti rajataan
 * nimettyihin kutsujiin: tyhjä lista estää kutsut kokonaan.
 */
private fun sallitutHetulistahaunKutsujat(
    environment: Environment,
): AuthorizationManager<RequestAuthorizationContext> {
    val sallitut =
        environment
            .getProperty("kitu.yki.hetulistahaku.sallitutKutsujat")
            .orEmpty()
            .split(",")
            .map { it.trim() }
            .filter { it.isNotEmpty() }
            .toSet()

    return AuthorizationManager { authentication, _ ->
        val subject = (authentication.get() as? JwtAuthenticationToken)?.token?.subject
        AuthorizationDecision(subject != null && subject in sallitut)
    }
}

@Configuration
@EnableWebSecurity
class WebSecurityConfig {
    @Bean
    fun singleSignOutFilter(sessionMappingStorage: SessionMappingStorage): SingleSignOutFilter {
        SingleSignOutFilter.setSessionMappingStorage(sessionMappingStorage)
        return SingleSignOutFilter().apply {
            setIgnoreInitConfiguration(true)
        }
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty("spring.security.oauth2.resourceserver.jwt.issuer-uri")
    fun oauth2SecurityFilterChain(
        http: HttpSecurity,
        environment: Environment,
    ): SecurityFilterChain {
        http {
            authorizeHttpRequests {
                configureCommonAuthorizations(environment)
                authorize(anyRequest, denyAll)
            }
            csrf {
                disable()
            }
            headers {
                httpStrictTransportSecurity {
                    maxAgeInSeconds = 31_536_000L
                    includeSubDomains = true
                    preload = false
                }
                referrerPolicy {
                    policy = ReferrerPolicyHeaderWriter.ReferrerPolicy.STRICT_ORIGIN_WHEN_CROSS_ORIGIN
                }
            }
            securityMatcher({ request ->
                isOauth2Request(request)
            })
            sessionManagement {
                sessionCreationPolicy = SessionCreationPolicy.STATELESS
            }
            oauth2ResourceServer {
                jwt {
                    jwtAuthenticationConverter = JwtAuthenticationTokenConverter()
                }
            }
        }
        return http.build()
    }

    @Bean
    @Order(2)
    fun casSecurityFilterChain(
        http: HttpSecurity,
        casAuthenticationFilter: CasAuthenticationFilter,
        singleSignOutFilter: SingleSignOutFilter,
        authenticationEntryPoint: AuthenticationEntryPoint,
        casConfig: CasConfig,
        environment: Environment,
        arvioijarekisteri: ArvioijarekisteriAsetukset,
    ): SecurityFilterChain {
        http {
            csrf {
                csrfTokenRepository = CookieCsrfTokenRepository()
                // API-endpointit on tarkoitettu OAuth2-bearer-pohjaisille ulkoisille kutsujille
                // (oauth2SecurityFilterChain käsittelee ne ja CSRF on siellä jo pois). Mitkään
                // appin omat lomakkeet/JS eivät kutsu näitä, joten CSRF-suojaus ei tuo lisäarvoa
                // mutta rikkoo Swagger UI:n "Try it out":n session-pohjaisena kutsuna.
                ignoringRequestMatchers(
                    "/api/**",
                    "/yki/api/**",
                    "/koto-kielitesti/api/**",
                    "/yhteystiedot/api/**",
                    "/db-scheduler-api/**",
                    "/dev/**",
                )
                if (environment.isE2ETest()) {
                    disable()
                }
            }
            headers {
                httpStrictTransportSecurity {
                    maxAgeInSeconds = 31_536_000L
                    includeSubDomains = true
                    preload = false
                }
                referrerPolicy {
                    policy = ReferrerPolicyHeaderWriter.ReferrerPolicy.STRICT_ORIGIN_WHEN_CROSS_ORIGIN
                }
            }
            logout {
                logoutSuccessUrl = casConfig.getCasLogoutUrl()
            }
            authorizeHttpRequests {
                configureCommonAuthorizations(environment)

                val arvioijarekisterinMuokkaus: AuthorizationManager<in RequestAuthorizationContext> =
                    if (arvioijarekisteri.muokkausKaytossa) {
                        hasAuthority(Authority.YKI_ARVIOIJAREKISTERI.role())
                    } else {
                        denyAll
                    }
                authorize(GET, "/yki/arvioijat/uusi", arvioijarekisterinMuokkaus)
                authorize(GET, "/yki/arvioijat/*/muokkaa", arvioijarekisterinMuokkaus)
                // Yksitasoinen * ei kata kausilomakkeiden polkuja, ja GET /yki/arvioijat/{id} on
                // jaatava lukuoikeudella auki, joten kaudet luetellaan erikseen.
                authorize(GET, "/yki/arvioijat/*/kaudet/uusi", arvioijarekisterinMuokkaus)
                authorize(GET, "/yki/arvioijat/*/kaudet/*/muokkaa", arvioijarekisterinMuokkaus)
                authorize(POST, "/yki/arvioijat/**", arvioijarekisterinMuokkaus)

                if (developmentProfileActive(environment)) {
                    println("Developer profile active, allowing access to /dev/**")
                    authorize("/dev/**", permitAll)
                }

                authorize(anyRequest, hasAuthority(Authority.VIRKAILIJA.role()))
            }
            exceptionHandling {
                this.authenticationEntryPoint = authenticationEntryPoint
            }
            addFilterBefore<CasAuthenticationFilter>(singleSignOutFilter)
        }

        http.addFilter(casAuthenticationFilter)

        return http.build()
    }

    private fun isOauth2Request(request: HttpServletRequest): Boolean =
        request.getHeader("Authorization")?.startsWith("Bearer ") ?: false

    /**
     * Konfiguraatio kun palvelua ajetaan HTTPS proxyn läpi. Käytännössä tämä
     * muuttaa [jakarta.servlet.ServletRequest.getScheme] palauttamaan
     * `https` jolloin palvelun kautta luodut urlit muodostuvat oikein.
     *
     *
     * Aktivointi: `kayttooikeus.uses-ssl-proxy` arvoon `true`.
     *
     * @return EmbeddedServletContainerCustomizer jonka Spring automaattisesti
     * tunnistaa ja lisää servlet containerin konfigurointiin
     *
     * Katso myös: [Oppijanumerorekisterin toteutus](https://github.com/Opetushallitus/oppijanumerorekisteri/blob/e2de50dfc039280ff3f657456451d67e6af40bd3/henkilo-ui/src/main/java/fi/vm/sade/henkiloui/configurations/ServletContainerConfiguration.java)
     */
    @Bean
    @ConditionalOnBooleanProperty("kitu.uses-ssl-proxy")
    fun sslProxyCustomizer(): WebServerFactoryCustomizer<TomcatServletWebServerFactory> =
        WebServerFactoryCustomizer { container ->
            container.addConnectorCustomizers(
                { connector ->
                    connector.scheme = "https"
                    connector.secure = true
                },
            )
        }
}

@Configuration
@ConditionalOnMissingBean(JwtDecoder::class)
@Profile("test", "e2e", "local-opintopolku")
class TestJwtConfig {
    @Bean
    fun jwtDecoder(): NimbusJwtDecoder {
        val decoder = NimbusJwtDecoder.withSecretKey(E2E_TEST_SECRET_KEY).build()

        // Throws and halts application startup if the token is invalid.
        decoder.decode(
            MockUser.ROOT.login
                .toOAuthTokenResponse()
                .access_token,
        )

        return decoder
    }
}
