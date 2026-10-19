package fi.oph.kitu.util.validation

import arrow.core.left
import arrow.core.nonEmptyListOf
import arrow.core.raise.either
import arrow.core.right
import fi.oph.kitu.util.validation.Validation.ValidationError
import kotlin.test.Test
import kotlin.test.assertEquals

class ValidationEnrichmentTest {
    @Test
    fun `enrichin epäonnistuminen kääntyy EnrichmentUnavailable-virheeksi`() {
        val validation =
            object : Validation<String> {
                override fun EnrichmentRaise.enrich(value: String): String =
                    raise(ValidationError.EnrichmentError(listOf("kenttä"), "Palvelu ei vastaa"))
            }

        val result = either { with(validation) { validateAndEnrich("x") } }

        assertEquals(
            nonEmptyListOf<ValidationError>(
                ValidationError.EnrichmentError(listOf("kenttä"), "Palvelu ei vastaa"),
            ).left(),
            result,
        )
    }

    @Test
    fun `onnistunut enrich palauttaa rikastetun arvon`() {
        val validation =
            object : Validation<String> {
                override fun EnrichmentRaise.enrich(value: String): String = value.uppercase()
            }

        val result = either { with(validation) { validateAndEnrich("x") } }

        assertEquals("X".right(), result)
    }
}
