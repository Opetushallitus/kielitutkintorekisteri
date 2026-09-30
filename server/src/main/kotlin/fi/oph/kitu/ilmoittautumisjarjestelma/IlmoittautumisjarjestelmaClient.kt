package fi.oph.kitu.ilmoittautumisjarjestelma

import arrow.core.Either
import fi.oph.kitu.restclient.fetchOphService
import io.opentelemetry.instrumentation.annotations.WithSpan
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.http.HttpMethod
import org.springframework.stereotype.Service
import org.springframework.web.client.RestClient
import java.net.URI

interface IlmoittautumisjarjestelmaClient {
    fun <T> post(
        endpoint: String,
        body: IlmoittautumisjarjestelmaRequest,
        responseType: Class<T>,
    ): Either<IlmoittautumisjarjestelmaException, T>
}

@Service
@ConditionalOnProperty("kitu.ilmoittautumispalvelu.service.url")
class IlmoittautumisjarjestelmaClientImpl(
    @param:Qualifier("oauth2RestClient")
    val restClient: RestClient,
    @param:Value($$"${kitu.ilmoittautumispalvelu.service.url}")
    val serviceUrl: String,
) : IlmoittautumisjarjestelmaClient {
    @WithSpan
    override fun <T> post(
        endpoint: String,
        body: IlmoittautumisjarjestelmaRequest,
        responseType: Class<T>,
    ): Either<IlmoittautumisjarjestelmaException, T> =
        restClient.fetchOphService(
            httpMethod = HttpMethod.POST,
            uri = URI.create("$serviceUrl/$endpoint"),
            body = body,
            emptyRequest = body,
            responseType = responseType,
            nullResponse = { IlmoittautumisjarjestelmaException.NullResponse(it) },
            // Tama palvelu ei erottele 404:aa muista 4xx-virheista, toisin kuin ONR ja
            // organisaatiopalvelu — sailytetaan entinen kaytos.
            notFound = { req, res -> IlmoittautumisjarjestelmaException.BadRequest(req, res) },
            badRequest = { req, res -> IlmoittautumisjarjestelmaException.BadRequest(req, res) },
            unexpected = { req, res -> IlmoittautumisjarjestelmaException.UnexpectedError(req, res) },
            decodeFailure = { req, res, _ ->
                IlmoittautumisjarjestelmaException.MalformedResponse(req, res)
            },
        )
}
