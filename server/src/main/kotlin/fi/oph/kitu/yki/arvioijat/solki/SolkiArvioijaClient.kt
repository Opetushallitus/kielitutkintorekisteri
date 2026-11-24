package fi.oph.kitu.yki.arvioijat.solki

import arrow.core.Either
import arrow.core.left
import arrow.core.right
import fi.oph.kitu.restclient.retrieveEntitySafely
import fi.oph.kitu.util.defaultObjectMapper
import io.opentelemetry.instrumentation.annotations.WithSpan
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.stereotype.Service
import org.springframework.web.client.ResourceAccessException
import org.springframework.web.client.RestClient

interface SolkiArvioijaClient {
    /**
     * Palauttaa aina tuloksen: yhteysvirhekin on Left, ei poikkeus. Onnistuneen lahetyksen arvo on
     * Solkin arvioijalle muodostama tunnus, tai null jos vastaus ei sisaltanyt sita.
     */
    fun laheta(request: SolkiArvioijaRequest): Either<SolkiArvioijaException, String?>

    /** Ilmoittaa Solkille, etta arvioija on poistettu Kielitutkintorekisterista. */
    fun lahetaPoisto(request: SolkiArvioijaPoistoRequest): Either<SolkiArvioijaException, Unit>
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
    override fun laheta(request: SolkiArvioijaRequest): Either<SolkiArvioijaException, String?> =
        laheta(request.arvioijaOid, "${request.arvioijaOid}:${request.versio}", request).map(::tunnus)

    @WithSpan
    override fun lahetaPoisto(request: SolkiArvioijaPoistoRequest): Either<SolkiArvioijaException, Unit> =
        laheta(request.arvioijaOid, "${request.arvioijaOid}:${request.versio}", request).map { }

    private fun laheta(
        arvioijaOid: String,
        idempotenssiavain: String,
        runko: Any,
    ): Either<SolkiArvioijaException, ResponseEntity<String>> {
        val response =
            try {
                kutsu(idempotenssiavain, runko)
            } catch (e: ResourceAccessException) {
                // retrieveEntitySafely heittaa yhteysvirheen lapi. Ilman tata tallennuksen
                // synkroninen lahetysyritys kaataisi virkailijan pyynnon jo tallennetulle riville.
                return SolkiArvioijaException.ConnectionFailure(arvioijaOid, e).left()
            }

        return when {
            response == null -> {
                SolkiArvioijaException.NullResponse(arvioijaOid).left()
            }

            response.statusCode.is2xxSuccessful -> {
                response.right()
            }

            response.statusCode.value() == UNAUTHORIZED || response.statusCode.value() == FORBIDDEN -> {
                SolkiArvioijaException.Unauthorized(arvioijaOid, response).left()
            }

            response.statusCode.value() == CONFLICT -> {
                SolkiArvioijaException.Conflict(arvioijaOid, response).left()
            }

            response.statusCode.is4xxClientError -> {
                SolkiArvioijaException.BadRequest(arvioijaOid, response).left()
            }

            else -> {
                SolkiArvioijaException.UnexpectedError(arvioijaOid, response).left()
            }
        }
    }

    /**
     * Vastausrunko on `{"tunnus":"A00001"}`. Puuttuva tai jasentymaton runko ei ole lahetysvirhe:
     * Solki on ottanut rivin vastaan, ja tunnus saadaan viimeistaan seuraavassa lahetyksessa.
     */
    private fun tunnus(response: ResponseEntity<String>): String? =
        response.body
            ?.let { runCatching { defaultObjectMapper.readTree(it) }.getOrNull() }
            ?.get("tunnus")
            ?.asString()
            ?.takeIf { it.isNotBlank() }

    private fun kutsu(
        idempotenssiavain: String,
        runko: Any,
    ): ResponseEntity<String>? =
        restClient
            .post()
            .uri("arvioija")
            .contentType(MediaType.APPLICATION_JSON)
            .header("Idempotency-Key", idempotenssiavain)
            .body(runko)
            .retrieveEntitySafely(String::class.java)

    companion object {
        private const val CONFLICT = 409
        private const val UNAUTHORIZED = 401
        private const val FORBIDDEN = 403
    }
}
