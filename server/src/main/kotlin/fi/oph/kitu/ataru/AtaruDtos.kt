package fi.oph.kitu.ataru

import arrow.core.Either
import arrow.core.left
import arrow.core.right
import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.annotation.JsonProperty
import fi.oph.kitu.util.defaultObjectMapper
import tools.jackson.core.JacksonException
import tools.jackson.databind.JsonNode
import tools.jackson.databind.exc.InvalidNullException
import kotlin.jvm.optionals.getOrNull

/** `option-answers`: tarkka vastaavuus valintakentan arvoon; tyhja [options] osuu tyhjiin vastauksiin. */
data class OptionAnswer(
    val key: String,
    val options: List<String>,
)

@JsonInclude(JsonInclude.Include.NON_NULL)
data class ApplicationListRequest(
    @get:JsonProperty("form-key")
    val formKey: String,
    @get:JsonProperty("option-answers")
    val optionAnswers: List<OptionAnswer>,
    val sort: ApplicationListSort,
    @get:JsonProperty("attachment-review-states")
    val attachmentReviewStates: Map<String, Any> = emptyMap(),
    @get:JsonProperty("states-and-filters")
    val statesAndFilters: StatesAndFilters,
)

/**
 * Skeema sallii `states-and-filters`-kentan puuttumisen, mutta palvelin ei: ilman `filters`-ehtoja
 * listaus kaatuu 500:aan (`IllegalArgumentException`), ja puuttuva tilalista pudottaa hiljaa kaikki hakemukset.
 * Oletusarvot paastavat kaikki hakemukset lapi; rajaus korvaa [PERUSSUODATTIMET]-avaimia tai lyhentaa tilalistoja.
 */
data class StatesAndFilters(
    val filters: Map<String, Map<String, Boolean>> = PERUSSUODATTIMET,
    @get:JsonProperty("processing-states-to-include")
    val processingStatesToInclude: List<String> =
        listOf(
            "unprocessed",
            "processing",
            "invited-to-interview",
            "invited-to-exam",
            "evaluating",
            "valintaesitys",
            "processed",
            "information-request",
            "processing-fee-overdue",
            "processing-fee-paid",
            "decision-fee-outstanding",
            "decision-fee-overdue",
            "decision-fee-paid",
            "invoiced",
        ),
    @get:JsonProperty("attachment-states-to-include")
    val attachmentStatesToInclude: List<String> =
        listOf("not-checked", "checked", "incomplete-attachment", "attachment-missing", "overdue", "no-requirements"),
) {
    companion object {
        val PERUSSUODATTIMET: Map<String, Map<String, Boolean>> =
            mapOf(
                "only-ssn" to mapOf("with-ssn" to true, "without-ssn" to true),
                "only-identified" to mapOf("identified" to true, "unidentified" to true),
                "active-status" to mapOf("active" to true, "passive" to true),
                "eligibility-set-automatically" to mapOf("yes" to true, "no" to true),
            )
    }
}

@JsonInclude(JsonInclude.Include.NON_NULL)
data class ApplicationListSort(
    @param:JsonProperty("order-by")
    @get:JsonProperty("order-by")
    val orderBy: String = "created-time",
    val order: String = "asc",
    val offset: JsonNode? = null,
)

data class ApplicationListResponse(
    val sort: ApplicationListSort? = null,
    val applications: List<HakemusOtsake> = emptyList(),
)

data class HakemusOtsake(
    val key: String,
    val state: String? = null,
)

data class SiirtoHakemus(
    val hakemusOid: String,
    val personOid: String,
    val keyValues: Map<String, JsonNode> = emptyMap(),
) {
    companion object {
        fun jasenna(alkio: JsonNode): Either<SiirtoJasennysvirhe, SiirtoHakemus> {
            fun virhe(polku: String) =
                SiirtoJasennysvirhe(
                    hakemusOid = alkio.path("hakemusOid").tunniste(),
                    personOid = alkio.path("personOid").tunniste(),
                    polku = polku,
                ).left()

            if (!alkio.isObject) return virhe("")
            val hakemus =
                try {
                    defaultObjectMapper.treeToValue(alkio, SiirtoHakemus::class.java)
                } catch (e: JacksonException) {
                    return virhe(e.polku())
                }
            return when {
                hakemus.hakemusOid.isBlank() -> virhe("hakemusOid")
                hakemus.personOid.isBlank() -> virhe("personOid")
                else -> hakemus.right()
            }
        }

        private fun JsonNode.tunniste(): String? = stringValueOpt().getOrNull()?.takeIf { it.isNotBlank() }

        private fun JacksonException.polku(): String =
            path
                .mapNotNull { it.propertyName ?: it.index.takeIf { i -> i >= 0 }?.let { i -> "[$i]" } }
                .joinToString(".")
                .ifEmpty { (this as? InvalidNullException)?.propertyName?.simpleName.orEmpty() }
    }
}

/** Ei Jacksonin virheviestia: se voi toistaa syotteen arvon, esimerkiksi hetun. */
data class SiirtoJasennysvirhe(
    val hakemusOid: String?,
    val personOid: String?,
    val polku: String,
)
