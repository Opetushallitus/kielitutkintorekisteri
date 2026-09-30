package fi.oph.kitu.restclient

import arrow.core.Either
import arrow.core.left
import fi.oph.kitu.util.defaultObjectMapper
import org.springframework.http.HttpMethod
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.web.client.RestClient
import java.net.URI

/**
 * OPH-palvelukutsun yhteinen runko. Oppijanumerorekisteri, organisaatiopalvelu ja
 * ilmoittautumisjärjestelmä toistivat saman statusportaikon ja vastauksen purun
 * lähes rivilleen samanlaisena — kopioinnin jäljistä kertoo se, että
 * OrganisaatiopalveluClientin KDoc puhui yhä OppijanumeroServiceErrorista.
 *
 * Poikkeushierarkiat jäävät domainille: ne ovat sealed-luokkia, joiden alityyppejä
 * (esim. OppijanumeroException.OppijaNotFoundException) erikoiskäsitellään kutsujissa.
 * Tässä jaetaan vain kuljetus, ei virhetyyppejä.
 */
fun <T, REQ : Any, ERR : Any> RestClient.fetchOphService(
    httpMethod: HttpMethod,
    uri: URI,
    body: REQ?,
    emptyRequest: REQ,
    responseType: Class<T>,
    nullResponse: (REQ) -> ERR,
    notFound: (REQ, ResponseEntity<String>) -> ERR,
    badRequest: (REQ, ResponseEntity<String>) -> ERR,
    unexpected: (REQ, ResponseEntity<String>) -> ERR,
    decodeFailure: (REQ, ResponseEntity<String>, Throwable) -> ERR,
): Either<ERR, T> {
    val request = body ?: emptyRequest
    val rawResponse =
        method(httpMethod)
            .uri(uri)
            .contentType(MediaType.APPLICATION_JSON)
            .nullableBody(body)
            .retrieveEntitySafely(String::class.java)

    return when {
        rawResponse == null -> {
            nullResponse(request).left()
        }

        rawResponse.statusCode == HttpStatus.NOT_FOUND -> {
            notFound(request, rawResponse).left()
        }

        rawResponse.statusCode.is4xxClientError -> {
            badRequest(request, rawResponse).left()
        }

        !rawResponse.statusCode.is2xxSuccessful -> {
            unexpected(request, rawResponse).left()
        }

        else -> {
            Either
                .catch { defaultObjectMapper.readValue(rawResponse.body, responseType) }
                .mapLeft { decodeError -> decodeFailure(request, rawResponse, decodeError) }
        }
    }
}

/**
 * Purkuvirheen kääntäminen: jos runko jäsentyy palvelun omaksi virheobjektiksi, siitä
 * saadaan tarkempi virhe, muuten kyseessä on jäsentymätön vastaus.
 */
fun <SVC : Any, ERR : Any> decodeFailureFor(
    serviceErrorType: Class<SVC>,
    response: ResponseEntity<String>,
    cause: Throwable,
    badResponse: (SVC) -> ERR,
    malformed: () -> ERR,
): ERR =
    Either
        .catch { defaultObjectMapper.readValue(response.body, serviceErrorType) }
        .fold(ifRight = { badResponse(it) }, ifLeft = { malformed() })
