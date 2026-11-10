package fi.oph.kitu.kotoutumiskoulutus.koealusta

import com.fasterxml.jackson.annotation.JsonProperty
import fi.oph.kitu.oid.Oid
import fi.oph.kitu.util.result.getOrThrow
import java.time.Instant

interface KoealustaOppija {
    val userid: Int
    val firstnames: String
    val lastname: String
    val preferredname: String?
    val ssn: String?
    val email: String

    fun firstCourse(): KoealustaCourse?

    fun isOnrValidateable(): Boolean =
        listOf(firstnames, lastname, preferredname, ssn).all { it.orEmpty().isNotBlank() }
}

interface KoealustaCourse {
    val courseid: Int
    val coursename: String
    val schoolOID: String?
    val teacheremail: String?

    fun suoritusaika(): Instant? = null

    fun onOhitettavaOppilaitos(): Boolean =
        schoolOID.isNullOrBlank() || Oid.parse(schoolOID).getOrNull() in ignoredSchoolOids
}

data class KoealustaSuorituksetResponse(
    val users: List<User>,
) {
    data class User(
        override val userid: Int,
        override val firstnames: String,
        override val lastname: String,
        override val preferredname: String?,
        val oppijanumero: String?,
        @param:JsonProperty("SSN")
        override val ssn: String?,
        override val email: String,
        val completions: List<Completion>,
    ) : KoealustaOppija {
        override fun firstCourse(): KoealustaCourse? = completions.firstOrNull()

        fun ilmanOhitettaviaOppilaitoksia() = copy(completions = completions.filterNot { it.onOhitettavaOppilaitos() })

        data class Completion(
            override val courseid: Int,
            override val coursename: String,
            val lang: String?,
            override val schoolOID: String?,
            val results: List<Result>,
            val timecompleted: Long,
            override val teacheremail: String?,
            @param:JsonProperty("questionbank_release")
            val questionbank: String?,
        ) : KoealustaCourse {
            override fun suoritusaika(): Instant = Instant.ofEpochSecond(timecompleted)

            data class Result(
                val name: String,
                @param:JsonProperty("quiz_grade")
                val quizGrade: String?,
            )
        }
    }
}

data class KoealustaKeskeneraisetResponse(
    val users: List<User>,
) {
    data class User(
        override val userid: Int,
        override val firstnames: String,
        override val lastname: String,
        override val preferredname: String?,
        @param:JsonProperty("SSN")
        override val ssn: String?,
        override val email: String,
        val courses: List<Course>,
    ) : KoealustaOppija {
        override fun firstCourse(): KoealustaCourse? = courses.firstOrNull()

        fun ilmanOhitettaviaOppilaitoksia() = copy(courses = courses.filterNot { it.onOhitettavaOppilaitos() })

        data class Course(
            override val courseid: Int,
            override val coursename: String,
            override val schoolOID: String?,
            override val teacheremail: String?,
        ) : KoealustaCourse
    }
}

val ignoredSchoolOids =
    listOf(
        Oid.parse("1.2.246.562.10.1234567890").getOrThrow(),
    )
