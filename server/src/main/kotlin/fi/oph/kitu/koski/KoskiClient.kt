package fi.oph.kitu.koski

import fi.oph.kitu.util.retry.RetryOutboundIntegration
import io.opentelemetry.instrumentation.annotations.WithSpan
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.stereotype.Service
import org.springframework.web.client.RestClient
import org.springframework.web.client.toEntity

@Service
class KoskiClient(
    @param:Qualifier("koskiRestClient")
    private val koskiRestClient: RestClient,
) {
    @WithSpan
    @RetryOutboundIntegration
    fun put(koskiRequest: KoskiRequest): ResponseEntity<KoskiResponse> =
        koskiRestClient
            .put()
            .uri("oppija")
            .contentType(MediaType.APPLICATION_JSON)
            .accept(MediaType.APPLICATION_JSON)
            .body(koskiRequest)
            .retrieve()
            .toEntity<KoskiResponse>()
}
