package fi.oph.kitu.jdbc

import java.sql.ResultSet
import java.time.Instant
import java.time.LocalDate
import java.time.OffsetDateTime
import kotlin.collections.map

fun <T> ResultSet.getTypedArray(
    columnLabel: String,
    transform: (String) -> T,
): Iterable<T> = ((getArray(columnLabel).array) as Array<*>).map { transform(it as String) }

fun <T> ResultSet.getTypedArrayOrNull(
    columnLabel: String,
    transform: (String) -> T,
): Iterable<T>? =
    getArray(columnLabel)?.let {
        (it.array as Array<*>).map { x -> transform.invoke(x as String) }
    }

inline fun <reified E : Enum<E>> ResultSet.getEnum(columnLabel: String): E = enumValueOf<E>(getString(columnLabel))

inline fun <reified E : Enum<E>> ResultSet.getEnumOrNull(columnLabel: String): E? =
    getString(columnLabel)?.let { enumValueOf<E>(it) }

fun ResultSet.getOffsetDateTime(columnLabel: String): OffsetDateTime =
    getObject(columnLabel, OffsetDateTime::class.java)

fun ResultSet.getOffsetDateTimeOrNull(columnLabel: String): OffsetDateTime? =
    getObject(columnLabel, OffsetDateTime::class.java)

fun ResultSet.getLocalDate(columnLabel: String): LocalDate = getDate(columnLabel).toLocalDate()

fun ResultSet.getLocalDateOrNull(columnLabel: String): LocalDate? = getDate(columnLabel)?.toLocalDate()

fun ResultSet.getInstant(columnLabel: String): Instant = getTimestamp(columnLabel).toInstant()

fun ResultSet.getInstantOrNull(columnLabel: String): Instant? = getTimestamp(columnLabel)?.toInstant()
