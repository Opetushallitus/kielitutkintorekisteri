package fi.oph.kitu.ataru

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.annotation.JsonProperty
import tools.jackson.databind.JsonNode

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
)
