package fi.oph.kitu.util.validation

import arrow.core.Either
import arrow.core.NonEmptyList
import arrow.core.nonEmptyListOf
import arrow.core.raise.Raise
import arrow.core.raise.either
import arrow.core.raise.withError

typealias ValidationResult<T> = Either<NonEmptyList<Validation.ValidationError>, T>

typealias ValidationRaise = Raise<NonEmptyList<Validation.ValidationError>>
typealias EnrichmentRaise = Raise<Validation.ValidationError.EnrichmentError>

fun <T> Validation<T>.validate(value: T): ValidationResult<T> = either { validateAndEnrich(value) }

/**
 * Heittaa ValidationExceptionin, jonka GlobalControllerExceptionHandler kaantaa 400:ksi.
 * Nimi erottaa taman util.result.getOrThrow'sta, joka heittaa Leftin sellaisenaan tai
 * IllegalStateExceptionin eli paatyy 500:aan — vaara import vaihtaisi statuskoodin hiljaa.
 */
fun <T> ValidationResult<T>.orThrowBadRequest(): T =
    fold(
        ifLeft = { errors: NonEmptyList<Validation.ValidationError> ->
            throw Validation.ValidationException(errors)
        },
        ifRight = { it },
    )

interface Validation<T> {
    fun ValidationRaise.validateAndEnrich(value: T): T {
        validateBeforeEnrichment(value)
        val enriched =
            withError({ e: ValidationError.EnrichmentError -> nonEmptyListOf<ValidationError>(e) }) {
                enrich(value)
            }
        validateAfterEnrichment(enriched)
        return enriched
    }

    fun ValidationRaise.validateBeforeEnrichment(value: T) {}

    fun EnrichmentRaise.enrich(value: T): T = value

    fun ValidationRaise.validateAfterEnrichment(value: T) {}

    data class ValidationException(
        val errors: NonEmptyList<ValidationError>,
    ) : Exception(errors.joinToString("; "))

    sealed interface ValidationError {
        val path: List<String>
        val message: String

        data class FieldError(
            override val path: List<String>,
            override val message: String,
        ) : ValidationError {
            override fun toString(): String = "${path.joinToString(".")}: $message"
        }

        data class EnrichmentError(
            override val path: List<String>,
            override val message: String,
        ) : ValidationError {
            override fun toString(): String = "${path.joinToString(".")}: $message"
        }

        companion object {
            operator fun invoke(
                path: List<String>,
                message: String,
            ): ValidationError = FieldError(path, message)
        }
    }
}
