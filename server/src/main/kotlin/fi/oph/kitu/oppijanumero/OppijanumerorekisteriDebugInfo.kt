package fi.oph.kitu.oppijanumero

import fi.oph.kitu.util.defaultObjectMapper
import org.springframework.http.ResponseEntity
import tools.jackson.module.kotlin.readValue

data class OppijanumerorekisteriDebugInfo(
    val request: OppijanumerorekisteriRequest,
    val detectedTypicalErrors: List<String>,
    val error: OppijanumeroServiceError?,
    val rawResponse: String?,
) {
    val source = "oppijanumerorekisteri"

    override fun toString(): String = defaultObjectMapper.writeValueAsString(this)

    fun message(): String? =
        when (detectedTypicalErrors.size) {
            0 -> null
            1 -> detectedTypicalErrors[0]
            2 -> "${detectedTypicalErrors[0]} ja 1 muu virhe"
            else -> "${detectedTypicalErrors[0]} ja ${detectedTypicalErrors.size - 1} muuta virhettä"
        }

    companion object {
        fun from(
            request: OppijanumerorekisteriRequest,
            response: ResponseEntity<String>?,
        ): OppijanumerorekisteriDebugInfo {
            val error =
                try {
                    response?.body?.let { defaultObjectMapper.readValue<OppijanumeroServiceError>(it) }
                } catch (_: Exception) {
                    null
                }

            val validationErrors: List<String> =
                error?.message?.let { msg ->
                    mapOf(
                        "Nick name must be one of the first names" to "Kutsumanimen on oltava yksi etunimistä",
                        "Invalid pattern. Must contain an alphabetic character" to
                            "Kutsumanimessä ei saa olla erikoismerkkejä, mukaanlukien välilyönti",
                    ).mapNotNull { if (msg.contains(it.key)) it.value else null }
                } ?: emptyList()

            val statusCodeMessages: List<String> =
                listOfNotNull(
                    when (response?.statusCode?.value()) {
                        401 -> "Kielitutkintorekisterin järjestelmätunnuksen käyttöoikeudet eivät ole riittävät"
                        404 -> "Henkilöä ei löydy Oppijanumerorekisteristä"
                        409 -> "Kirjoitusvirhe henkilötunnuksessa tai nimessä"
                        else -> null
                    },
                )

            return OppijanumerorekisteriDebugInfo(
                request = request,
                detectedTypicalErrors = validationErrors + statusCodeMessages,
                error = error,
                rawResponse = if (error == null) response?.body else null,
            )
        }
    }
}
