package fi.oph.kitu.yki.arvioijat.solki

import arrow.core.Either
import arrow.core.left
import arrow.core.right
import fi.oph.kitu.restclient.retrieveEntitySafely
import io.opentelemetry.instrumentation.annotations.WithSpan
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.stereotype.Service
import org.springframework.web.client.ResourceAccessException
import org.springframework.web.client.RestClient

interface SolkiArvioijaClient {
    /** Palauttaa aina tuloksen: yhteysvirhekin on Left, ei poikkeus. */
    fun laheta(request: SolkiArvioijaRequest): Either<SolkiArvioijaException, Unit>
}

/**
 * Client on olemassa myos integraatiokytkimen ollessa pois: virkailijan kasin kaynnistama lahetys
 * toimii silloinkin, ja saannon "saako lahettaa" tuntee [SolkiArvioijaService], ei client. Osoite
 * `kitu.yki.baseUrl` on asetettu joka ymparistossa (myos local ja e2e dev-stubiin), joten client on
 * aina rakennettavissa.
 */
@Service
class SolkiArvioijaClientImpl(
    @param:Qualifier("solkiRestClient")
    val restClient: RestClient,
) : SolkiArvioijaClient {
    @WithSpan
    override fun laheta(request: SolkiArvioijaRequest): Either<SolkiArvioijaException, Unit> {
        val response =
            try {
                kutsu(request)
            } catch (e: ResourceAccessException) {
                // retrieveEntitySafely heittaa yhteysvirheen lapi. Ilman tata tallennuksen
                // synkroninen lahetysyritys kaataisi virkailijan pyynnon jo tallennetulle riville.
                return SolkiArvioijaException.ConnectionFailure(request.arvioijaOid, e).left()
            }

        return when {
            response == null -> {
                SolkiArvioijaException.NullResponse(request.arvioijaOid).left()
            }

            response.statusCode.is2xxSuccessful -> {
                Unit.right()
            }

            response.statusCode.value() == UNAUTHORIZED || response.statusCode.value() == FORBIDDEN -> {
                SolkiArvioijaException.Unauthorized(request.arvioijaOid, response).left()
            }

            response.statusCode.value() == CONFLICT -> {
                SolkiArvioijaException.Conflict(request.arvioijaOid, response).left()
            }

            response.statusCode.is4xxClientError -> {
                SolkiArvioijaException.BadRequest(request.arvioijaOid, response).left()
            }

            else -> {
                SolkiArvioijaException.UnexpectedError(request.arvioijaOid, response).left()
            }
        }
    }

    private fun kutsu(request: SolkiArvioijaRequest): ResponseEntity<String>? =
        restClient
            .post()
            .uri("arvioija")
            .contentType(MediaType.APPLICATION_JSON)
            .header("Idempotency-Key", "${request.arvioijaOid}:${request.versio}")
            .body(request)
            .retrieveEntitySafely(String::class.java)

    companion object {
        private const val CONFLICT = 409
        private const val UNAUTHORIZED = 401
        private const val FORBIDDEN = 403
    }
}
