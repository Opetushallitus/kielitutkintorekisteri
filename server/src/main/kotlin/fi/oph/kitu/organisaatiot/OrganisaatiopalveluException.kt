package fi.oph.kitu.organisaatiot

import com.fasterxml.jackson.annotation.JsonProperty
import org.springframework.http.ResponseEntity
import java.util.Date

sealed class OrganisaatiopalveluException(
    message: String,
    val organisaatiopalveluError: OrganisaatiopalveluError? = null,
    cause: Throwable? = null,
) : Throwable(message, cause) {
    class UnexpectedError(
        val response: ResponseEntity<String>,
        message: String = "Unexpected error in organisaatio-service",
    ) : OrganisaatiopalveluException(message)

    class MalformedResponse(
        val response: ResponseEntity<String>,
        message: String = "Malformed response from organisaatio-service",
        cause: Throwable,
    ) : OrganisaatiopalveluException(message, cause = cause)

    class BadResponse(
        val response: ResponseEntity<String>,
        message: String = "Bad response from organisaatio-service",
        organisaatiopalveluError: OrganisaatiopalveluError,
        cause: Throwable,
    ) : OrganisaatiopalveluException(message, organisaatiopalveluError, cause)

    class BadRequest(
        val response: ResponseEntity<String>,
        message: String = "Bad request to organisaatio-service",
        organisaatiopalveluError: OrganisaatiopalveluError? = null,
        cause: Throwable? = null,
    ) : OrganisaatiopalveluException(message, organisaatiopalveluError, cause)

    class NotFoundException(
        message: String = "Organisaatio not found from organisaatio-service",
        organisaatiopalveluError: OrganisaatiopalveluError? = null,
        cause: Throwable? = null,
    ) : OrganisaatiopalveluException(message, organisaatiopalveluError, cause)

    class NullResponse(
        message: String = "Empty or unserializable response from organisaatio-service",
    ) : OrganisaatiopalveluException(message)
}

data class OrganisaatiopalveluError(
    @param:JsonProperty("timestamp")
    val timestamp: Date,
    @param:JsonProperty("status")
    val status: Int,
    @param:JsonProperty("error")
    val error: String,
    @param:JsonProperty("path")
    val path: String,
)
