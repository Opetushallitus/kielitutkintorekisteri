package fi.oph.kitu.yki.suoritukset

import fi.oph.kitu.jdbc.getEnum
import fi.oph.kitu.yki.Arviointitila
import fi.oph.kitu.yki.Tutkintokieli
import fi.oph.kitu.yki.Tutkintotaso
import org.springframework.jdbc.core.RowMapper
import java.time.LocalDate

data class YkiSuoritusTilastoRivi(
    val tutkintopaiva: LocalDate,
    val tutkintokieli: Tutkintokieli,
    val tutkintotaso: Tutkintotaso,
    val arviointitila: Arviointitila,
    val lukumaara: Long,
) {
    companion object {
        val fromRow =
            RowMapper { rs, _ ->
                YkiSuoritusTilastoRivi(
                    tutkintopaiva = rs.getObject("tutkintopaiva", LocalDate::class.java),
                    tutkintokieli = rs.getEnum<Tutkintokieli>("tutkintokieli"),
                    tutkintotaso = rs.getEnum<Tutkintotaso>("tutkintotaso"),
                    arviointitila = rs.getEnum<Arviointitila>("arviointitila"),
                    lukumaara = rs.getLong("lukumaara"),
                )
            }
    }
}
