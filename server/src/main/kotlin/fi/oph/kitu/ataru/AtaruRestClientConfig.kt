package fi.oph.kitu.ataru

import fi.oph.kitu.config.ConditionalOnNonEmptyProperty
import fi.oph.kitu.restclient.withLenientStringConverter
import fi.oph.kitu.security.cas.client.CasSessionClient
import fi.oph.kitu.security.cas.client.CasSessionInterceptor
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Profile
import org.springframework.http.client.JdkClientHttpRequestFactory
import org.springframework.web.client.RestClient
import java.net.http.HttpClient

@Configuration
@ConditionalOnNonEmptyProperty("kitu.ataru.service.url")
@Profile("!e2e & !local-opintopolku")
class AtaruRestClientConfig(
    @param:Value($$"${kitu.ataru.service.url}")
    private val serviceUrl: String,
    @param:Value($$"${kitu.opintopolkuHostname}")
    private val opintopolkuHostname: String,
    @param:Value($$"${kitu.palvelukayttaja.username}")
    private val username: String,
    @param:Value($$"${kitu.palvelukayttaja.password}")
    private val password: String,
    @param:Value($$"${kitu.palvelukayttaja.callerid}")
    private val callerId: String,
) {
    @Bean
    fun ataruClient(): AtaruClient {
        val cas =
            CasSessionClient(
                restClient =
                    RestClient
                        .builder()
                        .requestFactory(ilmanUudelleenohjauksia())
                        .defaultHeaders { it["Caller-Id"] = callerId }
                        .build(),
                casUrl = "https://$opintopolkuHostname/cas",
                service = "${serviceUrl.trimEnd('/')}/auth/cas",
                username = username,
                password = password,
                sessionCookieName = "ring-session",
            )
        val restClient =
            RestClient
                .builder()
                .requestFactory(ilmanUudelleenohjauksia())
                .withLenientStringConverter()
                .defaultHeaders { it["Caller-Id"] = callerId }
                .requestInterceptor(CasSessionInterceptor(cas))
                .build()
        return AtaruClientImpl(restClient, serviceUrl.trimEnd('/'))
    }

    private fun ilmanUudelleenohjauksia() =
        JdkClientHttpRequestFactory(HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NEVER).build())
}
