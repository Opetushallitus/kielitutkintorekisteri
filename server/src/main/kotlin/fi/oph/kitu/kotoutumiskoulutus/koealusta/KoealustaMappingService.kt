package fi.oph.kitu.kotoutumiskoulutus.koealusta

import fi.oph.kitu.kotoutumiskoulutus.suoritukset.KielitestiSuoritus
import fi.oph.kitu.kotoutumiskoulutus.suoritukset.error.KielitestiSuoritusError
import fi.oph.kitu.oid.Oid
import fi.oph.kitu.oppijanumero.OppijanumeroException
import fi.oph.kitu.oppijanumero.OppijanumeroService
import fi.oph.kitu.oppijanumero.OppijanumeroTroubleshootingService
import fi.oph.kitu.oppijanumero.OppijanumerorekisteriDebugInfo
import fi.oph.kitu.oppijanumero.YleistunnisteHaeRequest
import fi.oph.kitu.oppijanumero.troubleshootOppijanumero
import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.trace.Span
import io.opentelemetry.instrumentation.annotations.WithSpan
import org.springframework.stereotype.Service
import java.time.Instant

@Service
class KoealustaMappingService(
    private val parser: KoealustaResponseParser,
    private val validator: KoealustaSuoritusValidator,
    private val oppijanumeroService: OppijanumeroService,
    private val oppijanumeroTroubleshootingService: OppijanumeroTroubleshootingService,
) {
    @WithSpan
    fun responseStringToKeskenerainenSuoritus(body: String) =
        convertToKielitestiKeskenerainen(parser.parseKeskenerainen(body))

    fun convertToKielitestiKeskenerainen(
        suorituksetResponse: KoealustaKeskeneraisetResponse,
    ): Pair<List<KielitestiSuoritus>, ValidationFailure?> {
        val validationErrors = mutableListOf<KoealustaMappingError>()

        val discardedZeroCourseUserIds =
            suorituksetResponse.users
                .filter { it.courses.isEmpty() }
                .map { it.userid.toLong() }
        Span
            .current()
            .setAttribute(
                AttributeKey.longArrayKey("discardedZeroCourseUserIds"),
                discardedZeroCourseUserIds,
            )

        val keskeneraiset =
            suorituksetResponse.users.flatMap { user ->
                if (!user.isOnrValidateable() || user.mustBeSkipped()) return@flatMap emptyList()

                user.courses.mapNotNull { course ->
                    validator
                        .courseToEntity(user, course)
                        .onLeft(validationErrors::add)
                        .getOrNull()
                }
            }

        val validationFailure =
            if (validationErrors.isNotEmpty()) {
                ValidationFailure(
                    message =
                        "Parsing KielitestiSuoritus failed: There were ${validationErrors.size} validation errors.",
                    oppijanumeroExceptions = emptyList(),
                    validationErrors = validationErrors,
                )
            } else {
                null
            }

        return Pair(keskeneraiset, validationFailure)
    }

    @WithSpan
    fun responseStringToKielitestiSuoritus(body: String) = convertToKielitestiSuoritus(parser.parseSuoritus(body))

    fun convertToKielitestiSuoritus(
        suorituksetResponse: KoealustaSuorituksetResponse,
    ): Pair<List<KielitestiSuoritus>, ValidationFailure?> {
        val oppijanumeroExceptions = mutableListOf<KoealustaMappingError>()
        val validationErrors = mutableListOf<KoealustaMappingError>()

        val suoritukset =
            suorituksetResponse.users.flatMap { user ->
                if (user.mustBeSkipped()) return@flatMap emptyList()

                val oppija =
                    validator
                        .toOppija(user)
                        .onLeft(validationErrors::add)
                        .getOrNull()

                val oppijanumero =
                    oppija
                        ?.let(oppijanumeroService::getMasterOid)
                        ?.mapLeft {
                            val response = if (it is OppijanumeroException.HasResponse) it.response else null
                            val debugInfo = OppijanumerorekisteriDebugInfo.from(it.request, response)
                            val onrInfo = oppijanumeroTroubleshootingService.troubleshootOppijanumero(oppija, response)

                            KoealustaMappingError.OppijanumeroFailure(
                                it,
                                "Oppijanumeron haku epäonnistui: ${debugInfo.message() ?: it.oppijanumeroServiceError?.error ?: it.message ?: "ei tarkempia tietoja"}",
                                Oid.parse(user.completions.first().schoolOID).getOrNull(),
                                moodleId = user.userid.toString(),
                                user.completions.first().teacheremail,
                                debugInfo.toString(),
                                onrInfo,
                            )
                        }?.onLeft { oppijanumeroExceptions.add(it) }
                        ?.getOrNull()

                user.completions.mapNotNull { completion ->
                    validator
                        .completionToEntity(user, oppijanumero, completion)
                        ?.onLeft { validationErrors.add(it) }
                        ?.getOrNull()
                }
            }

        val validationFailure =
            if (oppijanumeroExceptions.isNotEmpty() || validationErrors.isNotEmpty()) {
                ValidationFailure(
                    message =
                        "Parsing KielitestiSuoritus failed: There were ${validationErrors.size} validation errors and ${oppijanumeroExceptions.size} oppijanumero failures.",
                    oppijanumeroExceptions = oppijanumeroExceptions,
                    validationErrors = validationErrors,
                )
            } else {
                null
            }

        return Pair(suoritukset, validationFailure)
    }

    fun convertErrors(errors: Iterable<KoealustaMappingError>): Iterable<KielitestiSuoritusError> =
        errors.flatMap(::convertError)

    fun convertError(error: KoealustaMappingError): List<KielitestiSuoritusError> {
        val now = Instant.now()
        return when (error) {
            is KoealustaMappingError.ValidationFailure -> {
                error.validationErrors.map { validationError ->
                    val (field, value) = parseValidationError(validationError)
                    KielitestiSuoritusError(
                        id = null,
                        suorittajanOid = error.oppijanumero.toString(),
                        hetu = error.koealustaUser.ssn,
                        nimi = "${error.koealustaUser.lastname} ${error.koealustaUser.firstnames}",
                        etunimet = error.koealustaUser.firstnames,
                        sukunimi = error.koealustaUser.lastname,
                        kutsumanimi = error.koealustaUser.preferredname,
                        schoolOid = error.schoolOid,
                        teacherEmail = error.teacherEmail,
                        virheenLuontiaika = now,
                        viesti = validationError.message,
                        virheellinenKentta = field,
                        virheellinenArvo = value,
                        lisatietoja = null,
                        onrLisatietoja = null,
                    )
                }
            }

            is KoealustaMappingError.OppijanumeroFailure -> {
                listOf(
                    KielitestiSuoritusError(
                        id = null,
                        suorittajanOid = null,
                        hetu = (error.oppijanumeroException.request as YleistunnisteHaeRequest).hetu,
                        nimi =
                            "${error.oppijanumeroException.request.sukunimi} ${error.oppijanumeroException.request.etunimet}",
                        etunimet = error.oppijanumeroException.request.etunimet,
                        sukunimi = error.oppijanumeroException.request.sukunimi,
                        kutsumanimi = error.oppijanumeroException.request.kutsumanimi,
                        schoolOid = error.schoolOid,
                        teacherEmail = error.teacherEmail,
                        virheenLuontiaika = now,
                        viesti = error.message,
                        virheellinenKentta = null,
                        virheellinenArvo = null,
                        lisatietoja = error.debugInfo,
                        onrLisatietoja = error.onrInfo,
                    ),
                )
            }
        }
    }

    private fun parseValidationError(validationError: KoealustaMappingError.Validation): FieldInfo =
        when (validationError) {
            is KoealustaMappingError.Validation.MalformedField -> {
                FieldInfo(
                    validationError.field,
                    validationError.value,
                )
            }

            is KoealustaMappingError.Validation.MissingField -> {
                FieldInfo(validationError.field, null)
            }

            is KoealustaMappingError.Validation.MissingGrade -> {
                FieldInfo(validationError.resultName, null)
            }
        }

    data class FieldInfo(
        val fieldName: String,
        val fieldValue: String?,
    )

    class ValidationFailure(
        message: String,
        val oppijanumeroExceptions: List<KoealustaMappingError>,
        val validationErrors: List<KoealustaMappingError>,
    ) : Exception(message) {
        fun isEmpty(): Boolean = oppijanumeroExceptions.isEmpty() && validationErrors.isEmpty()

        fun isNotEmpty(): Boolean = !isEmpty()
    }
}
