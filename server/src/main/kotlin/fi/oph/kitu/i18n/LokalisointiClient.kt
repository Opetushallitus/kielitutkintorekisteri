package fi.oph.kitu.i18n

import fi.oph.kitu.config.ConditionalOnNonEmptyProperty
import fi.oph.kitu.observability.use
import fi.oph.kitu.util.retry.RetryOutboundIntegration
import io.opentelemetry.api.trace.Tracer
import io.opentelemetry.instrumentation.annotations.WithSpan
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.beans.factory.annotation.Value
import org.springframework.http.MediaType
import org.springframework.stereotype.Service
import org.springframework.web.client.HttpClientErrorException
import org.springframework.web.client.RestClient
import org.springframework.web.client.toEntity

@Service
@ConditionalOnNonEmptyProperty("kitu.lokalisointi.namespace")
class LokalisointiClient(
    @param:Qualifier("lokalisointiRestClient")
    private val restClient: RestClient,
    private val tracer: Tracer,
    @param:Value($$"${kitu.lokalisointi.namespace}")
    private val namespace: String,
) {
    @WithSpan
    @RetryOutboundIntegration
    fun fetchAll(): Map<String, LocalizedString> =
        Language.entries.associateWith { fetchLocale(it) }.yhdistaKaannokset()

    private fun fetchLocale(language: Language): Map<String, String> =
        tracer
            .spanBuilder("fetchLocale")
            .startSpan()
            .use { span ->
                val locale = language.name.lowercase()
                span.setAttribute("locale", locale)
                try {
                    restClient
                        .get()
                        .uri(
                            "/lokalisointi/tolgee/{namespace}/{locale}.json",
                            mapOf(
                                "namespace" to namespace,
                                "locale" to locale,
                            ),
                        ).accept(MediaType.APPLICATION_JSON)
                        .retrieve()
                        .toEntity<Map<String, String>>()
                        .body
                        ?: emptyMap()
                } catch (e: HttpClientErrorException.NotFound) {
                    span.setAttribute("localeNotPublished", "$locale (${e.statusCode.value()})")
                    emptyMap()
                }
            }
}
