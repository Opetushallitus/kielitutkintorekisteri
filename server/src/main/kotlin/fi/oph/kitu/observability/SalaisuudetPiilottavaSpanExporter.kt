package fi.oph.kitu.observability

import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.common.AttributeType
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.sdk.autoconfigure.spi.AutoConfigurationCustomizerProvider
import io.opentelemetry.sdk.common.CompletableResultCode
import io.opentelemetry.sdk.trace.data.DelegatingSpanData
import io.opentelemetry.sdk.trace.data.EventData
import io.opentelemetry.sdk.trace.data.SpanData
import io.opentelemetry.sdk.trace.data.StatusData
import io.opentelemetry.sdk.trace.export.SpanExporter
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

private val TOKEN_PARAMETRI = Regex("""(token=)[^&\s"'<>]+""", RegexOption.IGNORE_CASE)

fun piilotaTokenit(arvo: String): String = TOKEN_PARAMETRI.replace(arvo, "$1***")

class SalaisuudetPiilottavaSpanExporter(
    private val delegate: SpanExporter,
) : SpanExporter {
    override fun export(spans: Collection<SpanData>): CompletableResultCode =
        delegate.export(spans.map(::piilotaTokenitSpanista))

    override fun flush(): CompletableResultCode = delegate.flush()

    override fun shutdown(): CompletableResultCode = delegate.shutdown()
}

fun piilotaTokenitSpanista(span: SpanData): SpanData {
    val attributes = span.attributes.piilotettu()
    val events = span.events.map { it.piilotettu() }
    val status = span.status.piilotettu()
    val muuttui =
        attributes !== span.attributes ||
            status !== span.status ||
            events.indices.any { events[it] !== span.events[it] }
    if (!muuttui) return span
    return object : DelegatingSpanData(span) {
        override fun getAttributes(): Attributes = attributes

        override fun getEvents(): List<EventData> = events

        override fun getStatus(): StatusData = status
    }
}

private fun EventData.piilotettu(): EventData {
    val piilotetut = attributes.piilotettu()
    return if (piilotetut === attributes) this else EventData.create(epochNanos, name, piilotetut, totalAttributeCount)
}

private fun StatusData.piilotettu(): StatusData {
    val piilotettu = piilotaTokenit(description)
    return if (piilotettu == description) this else StatusData.create(statusCode, piilotettu)
}

@Suppress("UNCHECKED_CAST")
private fun Attributes.piilotettu(): Attributes {
    var muuttui = false
    val builder = Attributes.builder()
    forEach { key, value ->
        if (key.type == AttributeType.STRING && value is String) {
            val piilotettu = piilotaTokenit(value)
            if (piilotettu != value) muuttui = true
            builder.put(key as AttributeKey<String>, piilotettu)
        } else {
            builder.put(key as AttributeKey<Any>, value)
        }
    }
    return if (muuttui) builder.build() else this
}

@Configuration
class SalaisuuksienPiilotusConfig {
    @Bean
    fun salaisuudetPiilottavaSpanExporter(): AutoConfigurationCustomizerProvider =
        AutoConfigurationCustomizerProvider { customizer ->
            customizer.addSpanExporterCustomizer { exporter, _ -> SalaisuudetPiilottavaSpanExporter(exporter) }
        }
}
