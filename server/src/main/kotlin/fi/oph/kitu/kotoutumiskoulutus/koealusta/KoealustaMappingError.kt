package fi.oph.kitu.kotoutumiskoulutus.koealusta

import fi.oph.kitu.kotoutumiskoulutus.suoritukset.error.KielitestiSuoritusError
import fi.oph.kitu.oid.Oid
import fi.oph.kitu.oppijanumero.Oppija
import fi.oph.kitu.oppijanumero.OppijanumeroRatkaisuehdotus

sealed class KoealustaMappingError(
    message: String,
    val course: KoealustaCourse?,
) : Exception(message) {
    val schoolOid: Oid? get() = Oid.parse(course?.schoolOID).getOrNull()
    val teacherEmail: String? get() = course?.teacheremail

    class OppijanumeroFailure(
        override val message: String,
        course: KoealustaCourse?,
        val debugInfo: String?,
        val oppija: Oppija,
        val ratkaisuehdotus: OppijanumeroRatkaisuehdotus,
        val onrStatus: Int?,
    ) : KoealustaMappingError(message, course)

    class AiemminTarkistettuOppijanumeroFailure(
        val virhe: KielitestiSuoritusError,
        course: KoealustaCourse?,
    ) : KoealustaMappingError(virhe.viesti, course)

    abstract class ValidationFailure(
        message: String,
        course: KoealustaCourse?,
        val koealustaUser: KoealustaOppija,
        val validationErrors: List<Validation>,
        val oppijanumero: Oid? = null,
    ) : KoealustaMappingError(message, course)

    class OppijaValidationFailure(
        message: String,
        course: KoealustaCourse?,
        koealustaUser: KoealustaOppija,
        validationErrors: List<Validation>,
    ) : ValidationFailure(message, course, koealustaUser, validationErrors)

    class SuoritusValidationFailure(
        message: String,
        course: KoealustaCourse?,
        koealustaUser: KoealustaOppija,
        validationErrors: List<Validation>,
        oppijanumero: Oid?,
    ) : ValidationFailure(message, course, koealustaUser, validationErrors, oppijanumero)

    sealed class Validation(
        val userId: Int,
        val message: String,
    ) {
        class MissingGrade(
            userId: Int,
            courseName: String,
            val resultName: String,
        ) : Validation(
                userId,
                """Puuttuva arvosana "$resultName" kurssilla "$courseName" käyttäjälle "$userId"""",
            )

        class MissingField(
            val field: String,
            userId: Int,
        ) : Validation(userId, """Puuttuva kenttä "$field" käyttäjälle "$userId"""")

        class MalformedField(
            userId: Int,
            val field: String,
            val value: String,
        ) : Validation(userId, """Virheellinen arvo "$value" kentässä "$field" käyttäjälle "$userId"""")
    }
}
