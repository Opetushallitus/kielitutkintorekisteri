package fi.oph.kitu.yki.suoritukset

import fi.oph.kitu.jdbc.getEnumOrNull
import fi.oph.kitu.yki.Arviointitila
import fi.oph.kitu.yki.Tutkintokieli
import fi.oph.kitu.yki.Tutkintotaso
import org.springframework.jdbc.core.RowMapper
import java.time.LocalDate

data class YkiSuoritusTilastoRivi(
    val tutkintovuosi: Int?,
    val tutkintopaiva: LocalDate?,
    val tutkintokieli: Tutkintokieli?,
    val tutkintotaso: Tutkintotaso?,
    val arviointitila: Arviointitila?,
    val lukumaara: Long,
) {
    companion object {
        val fromRow =
            RowMapper { rs, _ ->
                YkiSuoritusTilastoRivi(
                    tutkintovuosi = rs.getObject("tutkintovuosi", Integer::class.java)?.toInt(),
                    tutkintopaiva = rs.getObject("tutkintopaiva", LocalDate::class.java),
                    tutkintokieli = rs.getEnumOrNull<Tutkintokieli>("tutkintokieli"),
                    tutkintotaso = rs.getEnumOrNull<Tutkintotaso>("tutkintotaso"),
                    arviointitila = rs.getEnumOrNull<Arviointitila>("arviointitila"),
                    lukumaara = rs.getLong("lukumaara"),
                )
            }
    }
}
