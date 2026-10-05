package fi.oph.kitu.ataru

import arrow.core.Either
import arrow.core.flatMap
import arrow.core.raise.either
import fi.oph.kitu.restclient.fetchOphService
import fi.oph.kitu.security.cas.client.CasKirjautuminenEpaonnistui
import fi.oph.kitu.util.defaultObjectMapper
import io.opentelemetry.instrumentation.annotations.WithSpan
import org.springframework.http.HttpMethod
import org.springframework.web.client.RestClient
import java.net.URI

interface AtaruClient {
    /** Hakuun kuulumattoman lomakkeen hakemusten avaimet; [ehdot] suodatetaan jo atarussa. */
    fun haeHakemusavaimet(
        formKey: String,
        ehdot: List<OptionAnswer>,
    ): Either<AtaruException, List<HakemusOtsake>>

    fun haeHakemukset(avaimet: List<String>): Either<AtaruException, List<SiirtoHakemus>>
}

open class AtaruClientImpl(
    private val restClient: RestClient,
    private val serviceUrl: String,
) : AtaruClient {
    @WithSpan
    override fun haeHakemusavaimet(
        formKey: String,
        ehdot: List<OptionAnswer>,
    ): Either<AtaruException, List<HakemusOtsake>> =
        either {
            val otsakkeet = mutableListOf<HakemusOtsake>()
            var sort = ApplicationListSort()
            do {
                val edellinen = sort.offset
                val sivu =
                    post(
                        "api/applications/list",
                        ApplicationListRequest(formKey, ehdot, sort),
                        ApplicationListResponse::class.java,
                    ).bind()
                otsakkeet += sivu.applications
                sort = sivu.sort ?: ApplicationListSort()
            } while (sivu.applications.isNotEmpty() && sort.offset != null && sort.offset != edellinen)
            otsakkeet.distinctBy { it.key }
        }

    @WithSpan
    override fun haeHakemukset(avaimet: List<String>): Either<AtaruException, List<SiirtoHakemus>> =
        either {
            avaimet.chunked(SIIRRON_ERAKOKO).flatMap { era ->
                post(
                    "api/external/siirto?salliYksiloimattomat=true",
                    era,
                    Array<SiirtoHakemus>::class.java,
                ).bind()
                    .toList()
            }
        }

    private fun <T> post(
        endpoint: String,
        body: Any,
        responseType: Class<T>,
    ): Either<AtaruException, T> =
        Either
            .catch {
                restClient.fetchOphService(
                    httpMethod = HttpMethod.POST,
                    uri = URI.create("$serviceUrl/$endpoint"),
                    body = defaultObjectMapper.writeValueAsString(body),
                    emptyRequest = "",
                    responseType = responseType,
                    nullResponse = { AtaruException.NullResponse(endpoint) },
                    notFound = { _, res -> AtaruException.BadRequest(endpoint, res) },
                    badRequest = { _, res -> AtaruException.BadRequest(endpoint, res) },
                    unexpected = { _, res -> AtaruException.UnexpectedError(endpoint, res) },
                    decodeFailure = { _, res, e -> AtaruException.MalformedResponse(endpoint, res, e) },
                )
            }.mapLeft { e ->
                if (e is CasKirjautuminenEpaonnistui) AtaruException.Unauthorized(endpoint, e) else throw e
            }.flatMap { it }

    companion object {
        const val SIIRRON_ERAKOKO = 200
    }
}
