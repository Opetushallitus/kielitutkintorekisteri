package fi.oph.kitu.ilmoittautumisjarjestelma

import arrow.core.NonEmptyList
import arrow.core.nonEmptyListOf
import arrow.core.toNonEmptyListOrNull
import fi.oph.kitu.oid.Oid
import fi.oph.kitu.yki.Arviointitila
import fi.oph.kitu.yki.TutkinnonOsa
import fi.oph.kitu.yki.Tutkintokieli
import fi.oph.kitu.yki.Tutkintotaso
import fi.oph.kitu.yki.suoritukset.YkiSuoritusEntity
import java.time.LocalDate

sealed interface IlmoittautumisjarjestelmaRequest

data class YkiArvioinninTilaRequest(
    val tilat: NonEmptyList<YkiArvioinninTila>,
) : IlmoittautumisjarjestelmaRequest {
    companion object {
        fun of(entity: YkiSuoritusEntity): YkiArvioinninTilaRequest? =
            YkiArvioinninTila.of(entity)?.let {
                YkiArvioinninTilaRequest(nonEmptyListOf(it))
            }

        fun of(entities: List<YkiSuoritusEntity>): YkiArvioinninTilaRequest? =
            entities
                .mapNotNull { YkiArvioinninTila.of(it) }
                .toNonEmptyListOrNull()
                ?.let { YkiArvioinninTilaRequest(it) }
    }
}

data class YkiArvioinninTila(
    val suoritus: YkiSuorituksenTunniste,
    val tila: Arviointitila,
) {
    companion object {
        fun of(entity: YkiSuoritusEntity) =
            if (entity.isVilppi()) {
                null
            } else {
                YkiSuorituksenTunniste.of(entity)?.let {
                    YkiArvioinninTila(suoritus = it, tila = entity.arviointitila)
                }
            }
    }
}

data class YkiSuorituksenTunniste(
    val oppijanumero: Oid,
    val tutkintopaiva: LocalDate,
    val tutkintokieli: Tutkintokieli,
    val tutkintotaso: Tutkintotaso,
    val osakokeet: List<TutkinnonOsa>,
) {
    override fun equals(other: Any?): Boolean =
        other is YkiSuorituksenTunniste &&
            other.oppijanumero == oppijanumero &&
            other.tutkintopaiva == tutkintopaiva &&
            other.tutkintokieli == tutkintokieli &&
            other.tutkintotaso == tutkintotaso &&
            other.osakokeet.containsAll(osakokeet) &&
            osakokeet.containsAll(other.osakokeet)

    override fun hashCode(): Int =
        listOf(oppijanumero, tutkintopaiva, tutkintokieli, tutkintotaso, osakokeet.sorted()).hashCode()

    companion object {
        fun of(entity: YkiSuoritusEntity) =
            entity.suorittajanOID?.let { oppijanumero ->
                YkiSuorituksenTunniste(
                    oppijanumero = oppijanumero,
                    tutkintopaiva = entity.tutkintopaiva,
                    tutkintokieli = entity.tutkintokieli,
                    tutkintotaso = entity.tutkintotaso,
                    osakokeet =
                        listOfNotNull(
                            entity.puhuminen?.let { TutkinnonOsa.PU },
                            entity.kirjoittaminen?.let { TutkinnonOsa.KI },
                            entity.tekstinYmmartaminen?.let { TutkinnonOsa.TY },
                            entity.puheenYmmartaminen?.let { TutkinnonOsa.PY },
                        ),
                )
            }
    }
}

data class IlmoittautumisjarjestelmaResponse(
    val hyvaksytyt: Int,
    val virheet: List<IlmoittautumisjarjestelmaResponseError>?,
) {
    companion object {
        fun empty() = IlmoittautumisjarjestelmaResponse(0, null)

        fun ok(hyvaksytyt: Int) = IlmoittautumisjarjestelmaResponse(hyvaksytyt, null)
    }
}

data class IlmoittautumisjarjestelmaResponseError(
    val suoritus: YkiSuorituksenTunniste,
    val tila: Arviointitila,
    val virhe: String,
)
