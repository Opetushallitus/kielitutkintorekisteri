package fi.oph.kitu.yki.arvioijat.solki

import org.springframework.http.ResponseEntity

sealed class SolkiArvioijaException(
    val oppijanumero: String,
    val response: ResponseEntity<String>?,
    message: String,
    cause: Throwable? = null,
) : Throwable(message, cause) {
    class BadRequest(
        oppijanumero: String,
        response: ResponseEntity<String>,
    ) : SolkiArvioijaException(oppijanumero, response, "Bad request")

    /**
     * Solki ei tallentanut mitaan: lahetetty sahkopostiosoite kuuluu jo toiselle Solkin
     * kayttajatunnukselle. Uusinta ei auta ennen kuin osoite korjataan kitussa.
     */
    class Conflict(
        oppijanumero: String,
        response: ResponseEntity<String>,
    ) : SolkiArvioijaException(oppijanumero, response, "Conflict: email address already in use in Solki")

    class Unauthorized(
        oppijanumero: String,
        response: ResponseEntity<String>,
    ) : SolkiArvioijaException(oppijanumero, response, "Unauthorized")

    class UnexpectedError(
        oppijanumero: String,
        response: ResponseEntity<String>,
    ) : SolkiArvioijaException(oppijanumero, response, "Unexpected error")

    class NullResponse(
        oppijanumero: String,
    ) : SolkiArvioijaException(oppijanumero, null, "Empty or unserializable response")

    /**
     * Yhteysvirhe. [fi.oph.kitu.restclient.retrieveEntitySafely] heittaa
     * `ResourceAccessException`in lapi, joten se on napattava tassa: muuten tallennuksen
     * synkroninen lahetysyritys kaataisi virkailijan pyynnon jo tallennetulle riville.
     */
    class ConnectionFailure(
        oppijanumero: String,
        cause: Throwable,
    ) : SolkiArvioijaException(oppijanumero, null, "Connection failure", cause)

    /**
     * Poikkeama KIOS-mallista: pyyntorunkoa **ei** serialisoida. Osoite, sahkoposti ja
     * syntymaaika ovat henkilotietoa, joka paatyisi lokeihin ja virhesarakkeeseen, josta se nakyy
     * myos virhenakymassa.
     *
     * Vastausrunko otetaan mukaan, koska ilman sita virheesta ei paattele mitaan, mutta se
     * katkaistaan: vastaus voi kaiuttaa lahetetyt arvot takaisin, ja teksti tallentuu
     * rajoittamattomaan solki_lahetysvirhe-sarakkeeseen josta se renderoidaan tietosivulle.
     */
    fun debugString(): String =
        listOfNotNull(
            message,
            "oppijanumero: $oppijanumero",
            response?.statusCode?.let { "response status: $it" },
            response?.body?.let { "response body: ${katkaise(it)}" },
            cause?.let { "cause: $it" },
        ).joinToString("; ")

    private fun katkaise(body: String): String =
        if (body.length <= VASTAUKSEN_ENIMMAISPITUUS) {
            body
        } else {
            body.take(VASTAUKSEN_ENIMMAISPITUUS) + "… (katkaistu, ${body.length} merkkia)"
        }

    companion object {
        private const val VASTAUKSEN_ENIMMAISPITUUS = 500
    }
}
