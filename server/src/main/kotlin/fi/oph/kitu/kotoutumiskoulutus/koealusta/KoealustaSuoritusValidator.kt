package fi.oph.kitu.kotoutumiskoulutus.koealusta

import arrow.core.Either
import arrow.core.left
import arrow.core.right
import fi.oph.kitu.kotoutumiskoulutus.koealusta.KoealustaSuorituksetResponse.User
import fi.oph.kitu.kotoutumiskoulutus.koealusta.KoealustaSuorituksetResponse.User.Completion
import fi.oph.kitu.kotoutumiskoulutus.suoritukset.Arvosana
import fi.oph.kitu.kotoutumiskoulutus.suoritukset.KielitestiSuoritus
import fi.oph.kitu.kotoutumiskoulutus.suoritukset.Testikieli
import fi.oph.kitu.oid.Oid
import fi.oph.kitu.oppijanumero.Oppija
import fi.oph.kitu.util.result.getOrThrow
import org.springframework.stereotype.Service
import java.time.Instant

@Service
class KoealustaSuoritusValidator {
    fun toOppija(koealustaUser: KoealustaOppija): Either<KoealustaMappingError.OppijaValidationFailure, Oppija> {
        val errors = mutableListOf<KoealustaMappingError.Validation>()
        if (koealustaUser.ssn.isNullOrEmpty()) {
            errors.add(KoealustaMappingError.Validation.MissingField("SSN", koealustaUser.userid))
        }
        if (koealustaUser.preferredname.isNullOrEmpty()) {
            errors.add(KoealustaMappingError.Validation.MissingField("preferredname", koealustaUser.userid))
        }

        if (errors.isNotEmpty()) {
            return KoealustaMappingError
                .OppijaValidationFailure(
                    "Validation failure on converting user \"${koealustaUser.userid}\" to oppija",
                    schoolOid = Oid.parse(koealustaUser.schoolOID()).getOrNull(),
                    teacherEmail = koealustaUser.teacherEmail(),
                    koealustaUser,
                    errors,
                ).left()
        }

        return Oppija(
            etunimet = koealustaUser.firstnames.trim(),
            hetu = koealustaUser.ssn!!.trim(),
            kutsumanimi = koealustaUser.preferredname!!.trim(),
            sukunimi = koealustaUser.lastname.trim(),
        ).right()
    }

    fun completionToEntity(
        user: User,
        oppijanumero: Oid?,
        completion: Completion,
    ): Either<KoealustaMappingError.SuoritusValidationFailure, KielitestiSuoritus>? {
        val errors = mutableListOf<KoealustaMappingError.Validation>()
        val luetunYmmartaminen =
            validate("luetun ymmärtäminen", user.userid, completion)
                .onLeft { errors.add(it) }
                .getOrNull()
        val kuullunYmmartaminen =
            validate("kuullun ymmärtäminen", user.userid, completion)
                .onLeft { errors.add(it) }
                .getOrNull()
        val puhe =
            validate("puhuminen", user.userid, completion)
                .onLeft { errors.add(it) }
                .getOrNull()
        val kirjoittaminen =
            validate("kirjoittaminen", user.userid, completion)
                .onLeft { errors.add(it) }
                .getOrNull()

        val schoolOid =
            validate("schoolOID", user.userid, completion.schoolOID.orEmpty())
                .onLeft { errors.add(it) }
                .getOrNull()

        val testikieli =
            validate(user, completion.lang)
                .onLeft { errors.add(it) }
                .getOrNull()

        if (errors.isNotEmpty()) {
            return KoealustaMappingError
                .SuoritusValidationFailure(
                    message =
                        """
                        Validation failure on course completion on "${completion.coursename}" for user "${user.userid}"
                        """.trimIndent(),
                    schoolOid = Oid.parse(completion.schoolOID).getOrNull(),
                    teacherEmail = completion.teacheremail,
                    koealustaUser = user,
                    validationErrors = errors,
                    oppijanumero = oppijanumero,
                ).left()
        }

        if (user.preferredname == null || oppijanumero == null) return null

        return KielitestiSuoritus(
            etunimet = user.firstnames.trim(),
            sukunimi = user.lastname.trim(),
            kutsumanimi = user.preferredname.trim(),
            email = user.email,
            oppijanumero = oppijanumero,
            suoritusaika = Instant.ofEpochSecond(completion.timecompleted),
            oppilaitosOid = schoolOid!!,
            kurssiId = completion.courseid,
            kurssi = completion.coursename,
            luetunYmmartaminen = luetunYmmartaminen!!,
            kuullunYmmartaminen = kuullunYmmartaminen!!,
            puhe = puhe!!,
            kirjoittaminen = kirjoittaminen!!,
            testikieli = testikieli,
            opettajanEmail = completion.teacheremail,
            tehtavapaketti = completion.questionbank,
            completed = true,
        ).right()
    }

    fun courseToEntity(
        user: KoealustaOppija,
        course: KoealustaKeskeneraisetResponse.User.Course,
    ): Either<KoealustaMappingError.SuoritusValidationFailure, KielitestiSuoritus> {
        val errors = mutableListOf<KoealustaMappingError.Validation>()

        val schoolOid =
            validate("schoolOID", user.userid, course.schoolOID.orEmpty())
                .onLeft { errors.add(it) }
                .getOrNull()

        if (errors.isNotEmpty()) {
            return createValidationError(user, course, errors, null)
        }

        return KielitestiSuoritus(
            etunimet = user.firstnames.trim(),
            sukunimi = user.lastname.trim(),
            kutsumanimi = user.preferredname.orEmpty().trim(),
            email = user.email,
            oppijanumero = null,
            suoritusaika = null,
            oppilaitosOid = schoolOid!!,
            kurssiId = course.courseid,
            kurssi = course.coursename,
            luetunYmmartaminen = null,
            kuullunYmmartaminen = null,
            puhe = null,
            kirjoittaminen = null,
            testikieli = null,
            opettajanEmail = course.teacheremail,
            tehtavapaketti = null,
            completed = false,
        ).right()
    }

    private fun validate(
        resultName: String,
        userId: Int,
        completion: Completion,
    ): Either<KoealustaMappingError.Validation, Arvosana> {
        val result =
            completion
                .results
                .find { it.name == resultName }

        return if (result?.quizGrade.isNullOrEmpty()) {
            KoealustaMappingError.Validation
                .MissingGrade(
                    userId,
                    completion.coursename,
                    resultName,
                ).left()
        } else {
            try {
                Arvosana.fromString(result.quizGrade).right()
            } catch (_: IllegalArgumentException) {
                KoealustaMappingError.Validation
                    .MalformedField(
                        userId,
                        resultName,
                        result.quizGrade,
                    ).left()
            }
        }
    }

    private fun validate(
        user: User,
        lang: String?,
    ): Either<KoealustaMappingError.Validation, Testikieli?> =
        if (lang == null) {
            KoealustaMappingError.Validation
                .MissingField(
                    "lang",
                    user.userid,
                ).left()
        } else {
            try {
                Testikieli.fromString(lang).right()
            } catch (_: IllegalArgumentException) {
                KoealustaMappingError.Validation
                    .MalformedField(
                        userId = user.userid,
                        field = "lang",
                        value = lang,
                    ).left()
            }
        }

    private fun validate(
        fieldName: String,
        userId: Int,
        oid: String,
    ): Either<KoealustaMappingError.Validation, Oid> =
        Oid
            .parse(oid)
            .mapLeft { KoealustaMappingError.Validation.MalformedField(userId, fieldName, oid) }

    private fun createValidationError(
        user: KoealustaOppija,
        course: KoealustaCourse,
        errors: List<KoealustaMappingError.Validation>,
        oppijanumero: Oid?,
    ) = KoealustaMappingError
        .SuoritusValidationFailure(
            message =
                """
                Validation failure on course completion on "${course.coursename}" for user "${user.userid}"
                """.trimIndent(),
            schoolOid = Oid.parse(course.schoolOID).getOrNull(),
            teacherEmail = course.teacheremail,
            koealustaUser = user,
            validationErrors = errors.toList(),
            oppijanumero = oppijanumero,
        ).left()
}
