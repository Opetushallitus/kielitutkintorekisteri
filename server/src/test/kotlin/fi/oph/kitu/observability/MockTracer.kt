package fi.oph.kitu.observability

import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.api.trace.Span
import io.opentelemetry.api.trace.SpanBuilder
import io.opentelemetry.api.trace.SpanContext
import io.opentelemetry.api.trace.SpanKind
import io.opentelemetry.api.trace.StatusCode
import io.opentelemetry.api.trace.TraceFlags
import io.opentelemetry.api.trace.TraceState
import io.opentelemetry.api.trace.Tracer
import io.opentelemetry.context.Context
import java.util.concurrent.TimeUnit

/*
 * Don't use this on integration tests, use the actual tracer with inmemory exporter
 */
class MockTracer : Tracer {
    override fun spanBuilder(spanName: String): SpanBuilder = MockSpanbuilder()
}

class MockSpanContext : SpanContext {
    override fun getTraceId(): String? = null

    override fun getSpanId(): String? = null

    override fun getTraceFlags(): TraceFlags? = null

    override fun getTraceState(): TraceState? = null

    override fun isRemote(): Boolean = false
}

class MockSpan : Span {
    override fun <T : Any?> setAttribute(
        p0: AttributeKey<T?>,
        p1: T?,
    ): Span = MockSpan()

    override fun addEvent(
        name: String,
        attributes: Attributes,
    ): Span = MockSpan()

    override fun addEvent(
        name: String,
        attributes: Attributes,
        timestamp: Long,
        unit: TimeUnit,
    ): Span = MockSpan()

    override fun setStatus(
        statusCode: StatusCode,
        description: String,
    ): Span = MockSpan()

    override fun recordException(
        exception: Throwable,
        additionalAttributes: Attributes,
    ): Span = MockSpan()

    override fun updateName(name: String): Span = MockSpan()

    override fun end() {}

    override fun end(
        timestamp: Long,
        unit: TimeUnit,
    ) {}

    override fun getSpanContext(): SpanContext = MockSpanContext()

    override fun isRecording(): Boolean = false
}

class MockSpanbuilder : SpanBuilder {
    override fun setParent(context: Context): SpanBuilder = this

    override fun setNoParent(): SpanBuilder = this

    override fun addLink(spanContext: SpanContext): SpanBuilder = this

    override fun addLink(
        spanContext: SpanContext,
        attributes: Attributes,
    ): SpanBuilder = this

    override fun setAttribute(
        key: String,
        value: String?,
    ): SpanBuilder = this

    override fun setAttribute(
        key: String,
        value: Long,
    ): SpanBuilder = this

    override fun setAttribute(
        key: String,
        value: Double,
    ): SpanBuilder = this

    override fun setAttribute(
        key: String,
        value: Boolean,
    ): SpanBuilder = this

    override fun <T : Any> setAttribute(
        key: AttributeKey<T>,
        value: T?,
    ): SpanBuilder = this

    override fun setSpanKind(spanKind: SpanKind): SpanBuilder = this

    override fun setStartTimestamp(
        startTimestamp: Long,
        unit: TimeUnit,
    ): SpanBuilder = this

    override fun startSpan(): Span = MockSpan()
}
