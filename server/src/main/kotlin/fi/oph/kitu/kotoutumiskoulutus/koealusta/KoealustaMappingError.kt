package fi.oph.kitu.kotoutumiskoulutus.koealusta

import fi.oph.kitu.kotoutumiskoulutus.koealusta.KoealustaSuorituksetResponse.User
import fi.oph.kitu.oid.Oid
import fi.oph.kitu.oppijanumero.Oppija
import fi.oph.kitu.oppijanumero.OppijanumeroException
import fi.oph.kitu.oppijanumero.OppijanumeroRatkaisuehdotus

sealed class KoealustaMappingError(
    message: String,
    val schoolOid: Oid?,
    val teacherEmail: String?,
) : Exception(message) {
    class OppijanumeroFailure(
        val oppijanumeroException: OppijanumeroException,
        override val message: String,
        schoolOid: Oid?,
        moodleId: String?,
        teacherEmail: String?,
        val debugInfo: String?,
        val oppija: Oppija,
        val ratkaisuehdotus: OppijanumeroRatkaisuehdotus,
    ) : KoealustaMappingError(message, schoolOid, teacherEmail)

    abstract class ValidationFailure(
        message: String,
        schoolOid: Oid?,
        teacherEmail: String?,
        val koealustaUser: KoealustaOppija,
        val validationErrors: List<Validation>,
        val oppijanumero: Oid? = null,
    ) : KoealustaMappingError(message, schoolOid, teacherEmail)

    class OppijaValidationFailure(
        message: String,
        schoolOid: Oid?,
        teacherEmail: String?,
        koealustaUser: KoealustaOppija,
        validationErrors: List<Validation>,
    ) : ValidationFailure(message, schoolOid, teacherEmail, koealustaUser, validationErrors)

    class SuoritusValidationFailure(
        message: String,
        schoolOid: Oid?,
        teacherEmail: String?,
        koealustaUser: KoealustaOppija,
        validationErrors: List<Validation>,
        oppijanumero: Oid?,
    ) : ValidationFailure(message, schoolOid, teacherEmail, koealustaUser, validationErrors, oppijanumero)

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
