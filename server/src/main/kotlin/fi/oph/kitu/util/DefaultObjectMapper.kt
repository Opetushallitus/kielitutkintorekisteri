package fi.oph.kitu.util

import tools.jackson.core.JacksonException
import tools.jackson.databind.DeserializationFeature
import tools.jackson.databind.JsonNode
import tools.jackson.databind.SerializationFeature
import tools.jackson.databind.ext.javatime.ser.LocalDateSerializer
import tools.jackson.databind.ext.javatime.ser.LocalDateTimeSerializer
import tools.jackson.databind.ext.javatime.ser.ZonedDateTimeSerializer
import tools.jackson.databind.json.JsonMapper
import tools.jackson.databind.module.SimpleModule
import tools.jackson.databind.node.StringNode
import tools.jackson.module.kotlin.jacksonMapperBuilder
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

fun isoJavaTimeModule(): SimpleModule =
    SimpleModule()
        .addSerializer(LocalDate::class.java, LocalDateSerializer(DateTimeFormatter.ISO_LOCAL_DATE))
        .addSerializer(
            LocalDateTime::class.java,
            LocalDateTimeSerializer(DateTimeFormatter.ISO_LOCAL_DATE_TIME),
        ).addSerializer(
            ZonedDateTime::class.java,
            ZonedDateTimeSerializer(DateTimeFormatter.ISO_ZONED_DATE_TIME),
        )

/**
 * Yleiskäyttöinen json-mapper, jolle on konffattu:
 *
 *      - Tuki ISO-aikaleimoille
 *      - Tuki Kotlin-tietotyypeille
 *      - Nätti JSON-tulostus
 *      - Ei välitä yllättävistä propertyista
 */
val defaultObjectMapper: JsonMapper by lazy {
    jacksonMapperBuilder()
        .addModule(isoJavaTimeModule())
        .enable(SerializationFeature.INDENT_OUTPUT)
        .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
        .build()
}

fun String.toJsonNode(): JsonNode =
    try {
        defaultObjectMapper.readTree(this)
    } catch (_: JacksonException) {
        StringNode(this)
    }
