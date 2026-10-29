package fi.oph.kitu.webmvc

import org.springframework.core.ResolvableType
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpInputMessage
import org.springframework.http.converter.json.JacksonJsonHttpMessageConverter
import tools.jackson.core.JsonParser
import tools.jackson.databind.DeserializationContext
import tools.jackson.databind.deser.jdk.StringDeserializer
import tools.jackson.databind.json.JsonMapper
import tools.jackson.databind.module.SimpleModule
import java.io.InputStream

class StrictUtf8JsonHttpMessageConverter(
    jsonMapper: JsonMapper,
) : JacksonJsonHttpMessageConverter(
        jsonMapper
            .rebuild()
            .addModule(SimpleModule().addDeserializer(String::class.java, NulRejectingStringDeserializer()))
            .build(),
    ) {
    override fun read(
        type: ResolvableType,
        inputMessage: HttpInputMessage,
        hints: Map<String, Any>?,
    ): Any = super.read(type, validating(inputMessage), hints)

    override fun readInternal(
        clazz: Class<*>,
        inputMessage: HttpInputMessage,
    ): Any = super.readInternal(clazz, validating(inputMessage))

    private fun validating(inputMessage: HttpInputMessage): HttpInputMessage {
        val charset = inputMessage.headers.contentType?.charset
        if (charset != null && charset != Charsets.UTF_8 && charset != Charsets.US_ASCII) return inputMessage
        return Utf8ValidatingInputMessage(inputMessage)
    }

    private class Utf8ValidatingInputMessage(
        private val delegate: HttpInputMessage,
    ) : HttpInputMessage {
        private val body by lazy { Utf8ValidatingInputStream(delegate.body) }

        override fun getBody(): InputStream = body

        override fun getHeaders(): HttpHeaders = delegate.headers
    }
}

class NulRejectingStringDeserializer : StringDeserializer() {
    override fun deserialize(
        p: JsonParser,
        ctxt: DeserializationContext,
    ): String? =
        super.deserialize(p, ctxt)?.also {
            if ('\u0000' in it) ctxt.reportInputMismatch<Unit>(this, "Merkkijono ei saa sisältää NUL-merkkiä (\\u0000)")
        }
}
