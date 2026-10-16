package fi.oph.kitu.restclient

import io.opentelemetry.api.trace.Span
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.http.converter.StringHttpMessageConverter
import org.springframework.http.converter.json.JacksonJsonHttpMessageConverter
import org.springframework.web.client.ResourceAccessException
import org.springframework.web.client.RestClient
import tools.jackson.core.StreamReadConstraints
import tools.jackson.core.json.JsonFactory
import tools.jackson.databind.json.JsonMapper
import java.util.Base64

fun RestClient.Builder.withBasicAuth(
    username: String,
    password: String,
): RestClient.Builder {
    val token = Base64.getEncoder().encodeToString("$username:$password".toByteArray())
    return this.defaultHeader("Authorization", "Basic $token")
}

fun <T> RestClient.RequestBodySpec.nullableBody(body: T?): RestClient.RequestBodySpec =
    if (body == null) {
        this
    } else {
        this.body(body)
    }

/**
 * Unlike [RestClient.RequestBodySpec.retrieve], does not throw on a non-2xx status code.
 * Connection errors ([org.springframework.web.client.ResourceAccessException]) still propagate.
 *
 * Returns nulls for various reasons, for example:
 *  - `T` is null
 *  - response body is null (eg. HTTP 204 No Content)
 *  - Serialization issue converting into `T`.
 */
fun <T : Any> RestClient.RequestBodySpec.retrieveEntitySafely(type: Class<T>): ResponseEntity<
    T,
>? =
    try {
        this.exchange { request, response ->
            ResponseEntity
                .status(response.statusCode)
                .headers(response.headers)
                .body(response.bodyTo(type))
        }
    } catch (e: ResourceAccessException) {
        // ResourceAccessException.toString() omits the wrapped IOException, so traces/logs can't
        // distinguish a read timeout from a connection reset. Surface the cause as span attributes.
        val cause = e.cause
        Span.current().apply {
            setAttribute("exception.cause.type", cause?.javaClass?.name.orEmpty())
            setAttribute("exception.cause.message", cause?.message.orEmpty())
        }
        throw e
    }

// Spring 7's default StringHttpMessageConverter only advertises text/*, so reading an
// application/json response into String (e.g. retrieveEntitySafely(String::class.java))
// falls through to Jackson and fails. Prepend a lenient converter that also claims
// application/json, before the Jackson converter takes over.
fun RestClient.Builder.withLenientStringConverter(): RestClient.Builder {
    val lenient =
        StringHttpMessageConverter(Charsets.UTF_8).apply {
            supportedMediaTypes = listOf(MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON)
        }
    return this.configureMessageConverters { cs ->
        cs.registerDefaults().configureMessageConvertersList { list -> list.add(0, lenient) }
    }
}

// Jackson 3's own default is 20 MB. Callers that have a documented reason
// (e.g. Koealusta tehtäväpankki — see TehtavapankkiXmlParser) override
// with an explicit larger value; the helper's own default stays modest.
fun RestClient.Builder.withJacksonStreamMaxStringLength(maxStringLength: Int = 10_000_000): RestClient.Builder =
    this
        .clone()
        .configureMessageConverters { cs ->
            cs.registerDefaults().withJsonConverter(
                JacksonJsonHttpMessageConverter(createObjectMapperWithLargerBuffer(maxStringLength)),
            )
        }

private fun createObjectMapperWithLargerBuffer(maxStringLen: Int): JsonMapper =
    JsonMapper
        .builder(
            JsonFactory
                .builder()
                .streamReadConstraints(
                    StreamReadConstraints
                        .builder()
                        .maxStringLength(maxStringLen)
                        .build(),
                ).build(),
        ).build()

// The OPH lokalisointi proxy serves the published translation JSON with
// Content-Type: application/octet-stream (nosniff), which the default Jackson
// converter refuses to read. Claim octet-stream too so the body still parses as JSON.
fun RestClient.Builder.withOctetStreamJsonConverter(): RestClient.Builder =
    this.clone().configureMessageConverters { cs ->
        cs.registerDefaults().withJsonConverter(
            JacksonJsonHttpMessageConverter().apply {
                supportedMediaTypes = supportedMediaTypes + MediaType.APPLICATION_OCTET_STREAM
            },
        )
    }
