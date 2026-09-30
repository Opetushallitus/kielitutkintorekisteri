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
    ) = fetch(HttpMethod.GET, endpoint, query, responseType = responseType)

    @WithSpan
    fun <T> fetch(
        httpMethod: HttpMethod,
        endpoint: String,
        query: Map<String, Any>,
        body: OrganisaatiopalveluRequest? = null,
        responseType: Class<T>,
    ): Either<OrganisaatiopalveluException, T> {
        val uriBuilder = UriComponentsBuilder.fromUriString("$serviceUrl/$endpoint")
        query.forEach { (key, value) -> uriBuilder.queryParam(key, value) }

        return restClient.fetchOphService(
            httpMethod = httpMethod,
            uri = uriBuilder.build().toUri(),
            body = body,
            emptyRequest = EmptyRequest(),
            responseType = responseType,
            nullResponse = { OrganisaatiopalveluException.NullResponse(it) },
            notFound = { req, _ -> OrganisaatiopalveluException.NotFoundException(req) },
            badRequest = { req, res -> OrganisaatiopalveluException.BadRequest(req, res) },
            unexpected = { req, res -> OrganisaatiopalveluException.UnexpectedError(req, res) },
            decodeFailure = { req, res, cause ->
                decodeFailureFor(
                    serviceErrorType = OrganisaatiopalveluError::class.java,
                    response = res,
                    cause = cause,
                    badResponse = { orgError ->
                        OrganisaatiopalveluException.BadResponse(
                            request = req,
                            response = res,
                            organisaatiopalveluError = orgError,
                            cause = cause,
                        )
                    },
                    malformed = {
                        OrganisaatiopalveluException.MalformedResponse(
                            request = req,
                            response = res,
                            cause = cause,
                        )
                    },
                )
            },
        )
    }
}
