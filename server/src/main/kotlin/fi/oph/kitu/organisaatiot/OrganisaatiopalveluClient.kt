package fi.oph.kitu.organisaatiot

import arrow.core.Either
import fi.oph.kitu.restclient.decodeFailureFor
import fi.oph.kitu.restclient.fetchOphService
import io.opentelemetry.instrumentation.annotations.WithSpan
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.beans.factory.annotation.Value
import org.springframework.http.HttpMethod
import org.springframework.stereotype.Service
import org.springframework.web.client.RestClient
import org.springframework.web.util.UriComponentsBuilder

@Service
class OrganisaatiopalveluClient(
    @param:Qualifier("oauth2RestClient")
    val restClient: RestClient,
    @param:Value($$"${kitu.organisaatiopalvelu.service.url}")
    val serviceUrl: String,
) {
    @WithSpan
    fun <T> get(
        endpoint: String,
        query: Map<String, Any> = emptyMap(),
        responseType: Class<T>,
    ): Either<OrganisaatiopalveluException, T> {
        val uriBuilder = UriComponentsBuilder.fromUriString("$serviceUrl/$endpoint")
        query.forEach { (key, value) -> uriBuilder.queryParam(key, value) }

        return restClient.fetchOphService(
            httpMethod = HttpMethod.GET,
            uri = uriBuilder.build().toUri(),
            body = null,
            emptyRequest = Unit,
            responseType = responseType,
            nullResponse = { OrganisaatiopalveluException.NullResponse() },
            notFound = { _, _ -> OrganisaatiopalveluException.NotFoundException() },
            badRequest = { _, res -> OrganisaatiopalveluException.BadRequest(res) },
            unexpected = { _, res -> OrganisaatiopalveluException.UnexpectedError(res) },
            decodeFailure = { _, res, cause ->
                decodeFailureFor(
                    serviceErrorType = OrganisaatiopalveluError::class.java,
                    response = res,
                    cause = cause,
                    badResponse = { orgError ->
                        OrganisaatiopalveluException.BadResponse(
                            response = res,
                            organisaatiopalveluError = orgError,
                            cause = cause,
                        )
                    },
                    malformed = {
                        OrganisaatiopalveluException.MalformedResponse(
                            response = res,
                            cause = cause,
                        )
                    },
                )
            },
        )
    }
}
