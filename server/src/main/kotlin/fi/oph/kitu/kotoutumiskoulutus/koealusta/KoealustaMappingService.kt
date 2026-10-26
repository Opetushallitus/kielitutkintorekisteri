package fi.oph.kitu.kotoutumiskoulutus.koealusta

import arrow.core.Either
import arrow.core.left
import fi.oph.kitu.kotoutumiskoulutus.suoritukset.KielitestiSuoritus
import fi.oph.kitu.kotoutumiskoulutus.suoritukset.error.KielitestiSuoritusError
import fi.oph.kitu.oid.Oid
import fi.oph.kitu.oppijanumero.HENKILOTIETOVIRHEEN_TILAT
import fi.oph.kitu.oppijanumero.Oppija
import fi.oph.kitu.oppijanumero.OppijanumeroException
import fi.oph.kitu.oppijanumero.OppijanumeroService
import fi.oph.kitu.oppijanumero.OppijanumeroTroubleshootingService
import fi.oph.kitu.oppijanumero.OppijanumerorekisteriDebugInfo
import fi.oph.kitu.oppijanumero.troubleshootOppijanumero
import fi.oph.kitu.util.TimeService
import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.trace.Span
import io.opentelemetry.instrumentation.annotations.WithSpan
import org.springframework.stereotype.Service

@Service
class KoealustaMappingService(
    private val parser: KoealustaResponseParser,
    private val validator: KoealustaSuoritusValidator,
    private val oppijanumeroService: OppijanumeroService,
    private val oppijanumeroTroubleshootingService: OppijanumeroTroubleshootingService,
    private val timeService: TimeService,
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
    fun responseStringToKielitestiSuoritus(
        body: String,
        aiemmatVirheet: Collection<KielitestiSuoritusError> = emptyList(),
    ) = convertToKielitestiSuoritus(parser.parseSuoritus(body), aiemmatVirheet)

    fun convertToKielitestiSuoritus(
        suorituksetResponse: KoealustaSuorituksetResponse,
        aiemmatVirheet: Collection<KielitestiSuoritusError> = emptyList(),
    ): Pair<List<KielitestiSuoritus>, ValidationFailure?> {
        val oppijanumeroExceptions = mutableListOf<KoealustaMappingError>()
        val validationErrors = mutableListOf<KoealustaMappingError>()
        val tarkistetutOnrVirheet = aiemmatVirheet.tarkistetutOnrVirheet()

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
                        ?.let { haeOppijanumero(user, it, tarkistetutOnrVirheet[it]) }
                        ?.onLeft { oppijanumeroExceptions.add(it) }
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

    private fun haeOppijanumero(
        user: KoealustaSuorituksetResponse.User,
        oppija: Oppija,
        aiempiVirhe: KielitestiSuoritusError?,
    ): Either<KoealustaMappingError, Oid> {
        if (aiempiVirhe != null) {
            return KoealustaMappingError
                .AiemminTarkistettuOppijanumeroFailure(aiempiVirhe, user.firstCourse())
                .left()
        }

        return oppijanumeroService.getMasterOid(oppija).mapLeft {
            val response = if (it is OppijanumeroException.HasResponse) it.response else null
            val debugInfo = OppijanumerorekisteriDebugInfo.from(it.request, response)
            val ratkaisuehdotus = oppijanumeroTroubleshootingService.troubleshootOppijanumero(oppija, response)

            KoealustaMappingError.OppijanumeroFailure(
                "Oppijanumeron haku epäonnistui: ${debugInfo.message() ?: it.oppijanumeroServiceError?.error ?: it.message ?: "ei tarkempia tietoja"}",
                user.firstCourse(),
                debugInfo.toString(),
                oppija,
                ratkaisuehdotus,
                response?.statusCode?.value(),
            )
        }
    }

    private fun Collection<KielitestiSuoritusError>.tarkistetutOnrVirheet(): Map<Oppija, KielitestiSuoritusError> =
        filter { it.onrStatus in HENKILOTIETOVIRHEEN_TILAT }
            .mapNotNull { virhe ->
                val hetu = virhe.hetu ?: return@mapNotNull null
                val kutsumanimi = virhe.kutsumanimi ?: return@mapNotNull null
                Oppija(virhe.etunimet, hetu, kutsumanimi, virhe.sukunimi) to virhe
            }.toMap()

    fun convertErrors(errors: Iterable<KoealustaMappingError>): Iterable<KielitestiSuoritusError> =
        errors.flatMap(::convertError)

    fun convertError(error: KoealustaMappingError): List<KielitestiSuoritusError> {
        val now = timeService.now()
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
                        kurssiId = error.course?.courseid,
                        kurssi = error.course?.coursename,
                        suoritusaika = error.course?.suoritusaika(),
                    )
                }
            }

            is KoealustaMappingError.OppijanumeroFailure -> {
                listOf(
                    KielitestiSuoritusError(
                        id = null,
                        suorittajanOid = null,
                        hetu = error.oppija.hetu,
                        nimi = "${error.oppija.sukunimi} ${error.oppija.etunimet}",
                        etunimet = error.oppija.etunimet,
                        sukunimi = error.oppija.sukunimi,
                        kutsumanimi = error.oppija.kutsumanimi,
                        schoolOid = error.schoolOid,
                        teacherEmail = error.teacherEmail,
                        virheenLuontiaika = now,
                        viesti = error.message,
                        virheellinenKentta = null,
                        virheellinenArvo = null,
                        lisatietoja = error.debugInfo,
                        onrLisatietoja = error.ratkaisuehdotus.viesti,
                        onrEtunimet = error.ratkaisuehdotus.korjatutNimet?.etunimet,
                        onrKutsumanimi = error.ratkaisuehdotus.korjatutNimet?.kutsumanimi,
                        onrSukunimi = error.ratkaisuehdotus.korjatutNimet?.sukunimi,
                        onrStatus = error.onrStatus,
                        kurssiId = error.course?.courseid,
                        kurssi = error.course?.coursename,
                        suoritusaika = error.course?.suoritusaika(),
                    ),
                )
            }

            is KoealustaMappingError.AiemminTarkistettuOppijanumeroFailure -> {
                listOf(
                    error.virhe.copy(
                        id = null,
                        schoolOid = error.schoolOid,
                        teacherEmail = error.teacherEmail,
                        kurssiId = error.course?.courseid,
                        kurssi = error.course?.coursename,
                        suoritusaika = error.course?.suoritusaika(),
                        virheenLuontiaika = now,
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
