package fi.oph.kitu.observability

import io.opentelemetry.api.GlobalOpenTelemetry
import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter
import io.opentelemetry.sdk.trace.SdkTracerProvider
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean

@TestConfiguration
class OpenTelemetryTestConfig {
    @Bean
    fun inMemorySpanExporter(): InMemorySpanExporter = InMemorySpanExporter.create()

    @Bean
    fun openTelemetry(inMemorySpanExporter: InMemorySpanExporter): OpenTelemetry {
        val spanProcessor = SimpleSpanProcessor.create(inMemorySpanExporter)
        val tracerProvider =
            SdkTracerProvider
                .builder()
                .addSpanProcessor(spanProcessor)
                .build()
        val openTelemetry =
            OpenTelemetrySdk
                .builder()
                .setTracerProvider(tracerProvider)
                .build()

        GlobalOpenTelemetry.resetForTest()
        GlobalOpenTelemetry.set(openTelemetry)

        return openTelemetry
    }
}
