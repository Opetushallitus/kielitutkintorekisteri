package fi.oph.kitu.oppijanumero

import arrow.core.Either
import fi.oph.kitu.restclient.decodeFailureFor
import fi.oph.kitu.restclient.fetchOphService
import io.opentelemetry.instrumentation.annotations.WithSpan
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.beans.factory.annotation.Value
import org.springframework.http.HttpMethod
import org.springframework.stereotype.Service
import org.springframework.web.client.RestClient
import java.net.URI

@Service
class OppijanumerorekisteriClient(
    @param:Qualifier("oauth2RestClient")
    val restClient: RestClient,
    @param:Value($$"${kitu.oppijanumero.service.url}")
    val serviceUrl: String,
) {
    @WithSpan
    fun <T> onrGet(
        endpoint: String,
        responseType: Class<T>,
    ) = fetch(HttpMethod.GET, endpoint, responseType = responseType)

    @WithSpan
    fun <T, R : OppijanumerorekisteriRequest> onrPost(
        endpoint: String,
        body: R,
        clazz: Class<T>,
    ) = fetch(HttpMethod.POST, endpoint, body, clazz)

    fun <T> fetch(
        httpMethod: HttpMethod,
        endpoint: String,
        body: OppijanumerorekisteriRequest? = null,
        responseType: Class<T>,
    ): Either<OppijanumeroException, T> =
        restClient.fetchOphService(
            httpMethod = httpMethod,
            uri = URI.create("$serviceUrl/$endpoint"),
            body = body,
            emptyRequest = EmptyRequest(),
            responseType = responseType,
            nullResponse = { OppijanumeroException.NullResponse(it) },
            notFound = { req, res -> OppijanumeroException.OppijaNotFoundException(req, res) },
            badRequest = { req, res -> OppijanumeroException.BadRequest(req, res) },
            unexpected = { req, res -> OppijanumeroException.UnexpectedError(req, res) },
            decodeFailure = { req, res, cause ->
                decodeFailureFor(
                    serviceErrorType = OppijanumeroServiceError::class.java,
                    response = res,
                    cause = cause,
                    badResponse = { onrError ->
                        OppijanumeroException.BadResponse(
                            request = req,
                            response = res,
                            oppijanumeroServiceError = onrError,
                            cause = cause,
                        )
                    },
                    malformed = {
                        OppijanumeroException.MalformedResponse(
                            request = req,
                            response = res,
                            cause = cause,
                        )
                    },
                )
            },
        )
}
