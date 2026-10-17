package fi.oph.kitu.openapi

import io.swagger.v3.oas.annotations.OpenAPIDefinition
import io.swagger.v3.oas.annotations.info.Info
import io.swagger.v3.oas.models.OpenAPI
import io.swagger.v3.oas.models.security.OAuthFlow
import io.swagger.v3.oas.models.security.OAuthFlows
import io.swagger.v3.oas.models.security.Scopes
import io.swagger.v3.oas.models.security.SecurityRequirement
import io.swagger.v3.oas.models.security.SecurityScheme
import org.springdoc.core.customizers.GlobalOpenApiCustomizer
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.annotation.Configuration
import org.springframework.web.util.UriComponentsBuilder

const val OAUTH_SECURITY_SCHEME = "oauth_client_credentials"

/** Konfiguroidaan OpenAPI-dokumentaation / Swagger-sivun käyttämä autentikaatio ohjelmallisesti, koska autentikaatio riippuu ajonaikaisesta auktorisaatiopalvelimen osoitteesta, joten emme voi laittaa sitä annotaatioon.  */
@OpenAPIDefinition(
    info =
        Info(
            title = "Kielitutkintorekisteri",
            version = "v1",
        ),
)
@Configuration
@ConditionalOnProperty("spring.security.oauth2.resourceserver.jwt.issuer-uri")
class OpenApiSecurityConfig : GlobalOpenApiCustomizer {
    @Value($$"${spring.security.oauth2.resourceserver.jwt.issuer-uri}")
    lateinit var oauthIssuerUri: String

    private fun oauthUriBuilder() = UriComponentsBuilder.fromUriString(oauthIssuerUri)

    override fun customise(openApi: OpenAPI) {
        openApi.components.addSecuritySchemes(
            OAUTH_SECURITY_SCHEME,
            SecurityScheme()
                .type(SecurityScheme.Type.OAUTH2)
                .flows(
                    OAuthFlows()
                        .clientCredentials(
                            // Spring Security ei tarjoa token-URLia, joten se muodostetaan käsin.
                            OAuthFlow()
                                .tokenUrl(
                                    // Javan URI-luokan uri.resolve(path) ylikirjoittaa koko polun, joten käytetään Springin UriComponentsBuilder-helpperiä polkusegmenttien lisäämiseksi loppuun.
                                    oauthUriBuilder()
                                        .pathSegment("oauth2", "token")
                                        .build()
                                        .toUriString(),
                                ).scopes(
                                    Scopes().addString(
                                        "APP_KIELITUTKINTOREKISTERI_YKI_TALLENNUS",
                                        "Tallenna YKI-tutkintoja ja -arvioijia",
                                    ),
                                ),
                        ),
                ),
        )

        // Oletus: OAuth2-autentikaatiota tukevat endpointit on merkitty "oauth2"-tagilla. Näin meidän ei tarvitse kovakoodata listaa kyseisistä endpointeista tänne piiloon. Toisaalta nyt pitää muistaa lisätä kyseinen tag jokaiselle OAuth2-endpointille.
        // Huom: Tämä ainoastaan lisää Swaggeriin tuen OAuth2-autentikaatiolle. Springin varsinainen autentikaatio-konfiguraatio on konfiguroitu metodissa [fi.oph.kitu.security.WebSecurityConfig.oauth2SecurityFilterChain].
        openApi.paths.values
            .flatMap { endpoint ->
                listOf(
                    endpoint?.post,
                    endpoint?.put,
                    endpoint?.get,
                    endpoint?.delete,
                )
            }.filterNotNull()
            .filter { operation -> operation.tags?.contains("oauth2") ?: false }
            .forEach { operation ->
                operation.addSecurityItem(
                    SecurityRequirement()
                        .addList(OAUTH_SECURITY_SCHEME, listOf("APP_KIELITUTKINTOREKISTERI_YKI_TALLENNUS")),
                )
            }
    }
}
