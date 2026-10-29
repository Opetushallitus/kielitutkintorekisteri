package fi.oph.kitu.observability

import org.springframework.boot.json.JsonWriter
import org.springframework.boot.logging.structured.StructuredLoggingJsonMembersCustomizer

const val LOG_MESSAGE_MAX_BYTES = 4096
const val LOG_ERROR_MESSAGE_MAX_BYTES = 1024
const val LOG_STACK_TRACE_MAX_BYTES = 6144
const val LOG_VALUE_MAX_BYTES = 512

class LogLineSizeLimits : StructuredLoggingJsonMembersCustomizer<Any> {
    override fun customize(members: JsonWriter.Members<Any>) {
        members.applyingValueProcessor(
            JsonWriter
                .ValueProcessor<String> { path, value ->
                    value?.truncateToJsonUtf8Bytes(maxBytes(path.toUnescapedString()))
                }.whenInstanceOf(String::class.java),
        )
    }

    private fun maxBytes(path: String): Int =
        when (path) {
            "message" -> LOG_MESSAGE_MAX_BYTES
            "error.message" -> LOG_ERROR_MESSAGE_MAX_BYTES
            "error.stack_trace" -> LOG_STACK_TRACE_MAX_BYTES
            else -> LOG_VALUE_MAX_BYTES
        }
}

fun String.jsonUtf8Length(): Int = sumOf { it.jsonUtf8Length() }

fun String.truncateToJsonUtf8Bytes(maxBytes: Int): String {
    val length = jsonUtf8Length()
    if (length <= maxBytes) return this
    val marker = "...[truncated from $length bytes]"
    val budget = maxBytes - marker.jsonUtf8Length()
    var used = 0
    var end = 0
    while (end < this.length) {
        val step = if (this[end].isHighSurrogate() && end + 1 < this.length && this[end + 1].isLowSurrogate()) 2 else 1
        val cost = (end until end + step).sumOf { this[it].jsonUtf8Length() }
        if (used + cost > budget) break
        used += cost
        end += step
    }
    return substring(0, end) + marker
}

private const val TWO_CHARACTER_ESCAPES = "\"\\\b\u000C\n\r\t"

private fun Char.jsonUtf8Length(): Int =
    when {
        this in TWO_CHARACTER_ESCAPES -> 2
        Character.isISOControl(this) -> 6
        code < 0x80 -> 1
        code < 0x800 -> 2
        isSurrogate() -> 2
        else -> 3
    }
