package fi.oph.kitu.ataru

import org.springframework.http.ResponseEntity

sealed class AtaruException(
    val endpoint: String,
    val response: ResponseEntity<String>?,
    message: String,
    cause: Throwable? = null,
) : Throwable(message, cause) {
    class BadRequest(
        endpoint: String,
        response: ResponseEntity<String>,
    ) : AtaruException(endpoint, response, "Bad request")

    class UnexpectedError(
        endpoint: String,
        response: ResponseEntity<String>,
    ) : AtaruException(endpoint, response, "Unexpected error")

    class MalformedResponse(
        endpoint: String,
        response: ResponseEntity<String>,
        cause: Throwable,
    ) : AtaruException(endpoint, response, "Malformed response", cause)

    class NullResponse(
        endpoint: String,
    ) : AtaruException(endpoint, null, "Empty response")

    class Unauthorized(
        endpoint: String,
        cause: Throwable,
    ) : AtaruException(endpoint, null, "CAS login failed", cause)

    fun debugString(): String =
        listOfNotNull(
            message,
            "endpoint: $endpoint",
            response?.statusCode?.let { "response status: $it" },
            response?.body?.let { "response body: $it" },
            cause?.let { "cause: $it" },
        ).joinToString("; ")
}
