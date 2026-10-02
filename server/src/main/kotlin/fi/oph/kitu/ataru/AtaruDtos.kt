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
)

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
    val personOid: String?,
    val state: String? = null,
    val person: SiirtoHenkilo? = null,
    val keyValues: Map<String, JsonNode> = emptyMap(),
)

data class SiirtoHenkilo(
    val oidHenkilo: String? = null,
    val etunimet: String? = null,
    val sukunimi: String? = null,
)
