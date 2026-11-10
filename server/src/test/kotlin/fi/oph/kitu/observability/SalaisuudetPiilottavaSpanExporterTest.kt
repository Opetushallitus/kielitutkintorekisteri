package fi.oph.kitu.observability

import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.trace.StatusCode
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter
import io.opentelemetry.sdk.trace.SdkTracerProvider
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor
import org.junit.jupiter.api.Test
import org.springframework.web.client.ResourceAccessException
import java.io.IOException
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertSame

class SalaisuudetPiilottavaSpanExporterTest {
    private val muisti = InMemorySpanExporter.create()
    private val tracer =
        SdkTracerProvider
            .builder()
            .addSpanProcessor(SimpleSpanProcessor.create(SalaisuudetPiilottavaSpanExporter(muisti)))
            .build()
            .get("test")

    @Test
    fun `tokenit piilotetaan attribuuteista, tilasta ja poikkeustapahtumista`() {
        val osoite = "https://koealusta.test/webservice/rest/server.php?wstoken=salainen123&wsfunction=f"
        tracer
            .spanBuilder("GET")
            .setAttribute("url.full", osoite)
            .setAttribute("http.url", osoite)
            .setAttribute("http.response.status_code", 500L)
            .startSpan()
            .apply {
                recordException(
                    ResourceAccessException(
                        "I/O error on GET request for \"$osoite\": timeout",
                        IOException("timeout"),
                    ),
                )
                setStatus(StatusCode.ERROR, "Lataus ($osoite?token=salainen123) epäonnistui")
            }.end()

        val span = muisti.finishedSpanItems.single()
        val kaikki =
            span.attributes.asMap().values +
                span.events.flatMap { it.attributes.asMap().values } +
                span.status.description

        assertFalse(kaikki.any { it.toString().contains("salainen123") }, kaikki.toString())
        assertEquals(
            "https://koealusta.test/webservice/rest/server.php?wstoken=***&wsfunction=f",
            span.attributes.get(AttributeKey.stringKey("url.full")),
        )
        assertEquals(500L, span.attributes.get(AttributeKey.longKey("http.response.status_code")))
        assertEquals(StatusCode.ERROR, span.status.statusCode)
    }

    @Test
    fun `span ilman tokenia valitetaan sellaisenaan`() {
        val suoraan = InMemorySpanExporter.create()
        SdkTracerProvider
            .builder()
            .addSpanProcessor(SimpleSpanProcessor.create(suoraan))
            .build()
            .get("test")
            .spanBuilder("GET")
            .setAttribute("url.full", "https://example.test/a?b=c")
            .startSpan()
            .end()

        val span = suoraan.finishedSpanItems.single()
        assertSame(span, piilotaTokenitSpanista(span))
    }
}
