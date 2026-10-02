package fi.oph.kitu.i18n.tolgee

import fi.oph.kitu.config.ConditionalOnNonEmptyProperty
import fi.oph.kitu.observability.use
import fi.oph.kitu.util.retry.RetryOutboundIntegration
import io.opentelemetry.api.trace.Tracer
import io.opentelemetry.instrumentation.annotations.WithSpan
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.beans.factory.annotation.Value
import org.springframework.http.HttpMethod
import org.springframework.http.MediaType
import org.springframework.stereotype.Service
import org.springframework.web.client.HttpClientErrorException
import org.springframework.web.client.RestClient

private const val SIVUKOKO = 200
private const val PALASEN_KOKO = 100

/**
 * Kaikki päätepisteet ovat projekti-id:ttömiä variantteja: projekti tulee Project API Keystä.
 */
@Service
@ConditionalOnNonEmptyProperty("kitu.tolgee.apiKey")
class TolgeeClientImpl(
    @param:Qualifier("tolgeeRestClient")
    private val restClient: RestClient,
    private val tracer: Tracer,
    @param:Value($$"${kitu.lokalisointi.namespace:}")
    private val namespace: String,
) : TolgeeClient {
    private val logger = LoggerFactory.getLogger(javaClass)

    @WithSpan
    @RetryOutboundIntegration
    override fun fetchKeys(): Map<String, Long> {
        val keys = mutableMapOf<String, Long>()
        var page = 0
        var totalPages: Int
        do {
            val response = fetchPage(page)
            response.embedded
                ?.keys
                .orEmpty()
                .filter { it.keyNamespace.orEmpty() == namespace }
                .forEach { keys[it.keyName] = it.keyId }
            totalPages = response.page?.totalPages ?: 0
            page++
        } while (page < totalPages)
        return keys
    }

    @WithSpan
    @RetryOutboundIntegration
    override fun createKeys(entries: Map<String, String>) {
        entries.entries.chunked(PALASEN_KOKO).forEach { palanen ->
            val request =
                TolgeeImportRequest(
                    keys =
                        palanen.map { (key, fi) ->
                            TolgeeImportRequest.Key(
                                name = key,
                                namespace = namespace,
                                translations =
                                    if (fi.isEmpty()) {
                                        emptyMap()
                                    } else {
                                        mapOf("fi" to TolgeeImportRequest.Translation(text = fi))
                                    },
                            )
                        },
                )
            restClient
                .post()
                .uri("/v2/projects/keys/import-resolvable")
                .contentType(MediaType.APPLICATION_JSON)
                .body(request)
                .retrieve()
                .toBodilessEntity()
        }
    }

    @WithSpan
    @RetryOutboundIntegration
    override fun deleteKeys(ids: List<Long>) {
        ids.chunked(PALASEN_KOKO).forEach { palanen ->
            try {
                restClient
                    .method(HttpMethod.DELETE)
                    .uri("/v2/projects/keys")
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(TolgeeDeleteRequest(ids = palanen))
                    .retrieve()
                    .toBodilessEntity()
            } catch (e: HttpClientErrorException) {
                logger.warn("Tolgee-avainten poisto ei onnistunut avaimille {}: {}", palanen, e.message)
            }
        }
    }

    private fun fetchPage(page: Int): TolgeeTranslationsResponse =
        tracer
            .spanBuilder("fetchKeysPage")
            .startSpan()
            .use { span ->
                span.setAttribute("page", page.toLong())
                restClient
                    .get()
                    .uri { uriBuilder ->
                        uriBuilder
                            .path("/v2/projects/translations")
                            .queryParam("filterNamespace", namespace)
                            .queryParam("languages", "fi")
                            .queryParam("size", SIVUKOKO)
                            .queryParam("page", page)
                            .build()
                    }.accept(MediaType.APPLICATION_JSON)
                    .retrieve()
                    .body(TolgeeTranslationsResponse::class.java)
                    ?: TolgeeTranslationsResponse()
            }
}
