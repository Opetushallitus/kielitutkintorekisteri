package fi.oph.kitu.vkt.uusintaoikeus

import fi.oph.kitu.jdbc.getEnum
import fi.oph.kitu.jdbc.getTypedArray
import fi.oph.kitu.koodisto.Koodisto
import org.springframework.jdbc.core.RowMapper
import java.time.LocalDate

data class VktUusintaoikeus(
    val suorittajanOid: String,
    val sukunimi: String,
    val etunimet: String,
    val tutkintokieli: Koodisto.Tutkintokieli,
    val taitotaso: Koodisto.VktTaitotaso,
    val osakoe: Koodisto.VktOsakoe,
    val tutkinnot: List<Koodisto.VktKielitaito>,
    val hylkaamispaatos: LocalDate,
    val oikeusPaattyy: LocalDate,
    val osakoeNimi: String = "",
    val tutkinnotNimet: String = "",
) {
    companion object {
        val fromRow =
            RowMapper { rs, _ ->
                VktUusintaoikeus(
                    suorittajanOid = rs.getString("suorittajan_oid"),
                    sukunimi = rs.getString("sukunimi"),
                    etunimet = rs.getString("etunimet"),
                    tutkintokieli = rs.getEnum<Koodisto.Tutkintokieli>("tutkintokieli"),
                    taitotaso = rs.getEnum<Koodisto.VktTaitotaso>("taitotaso"),
                    osakoe = rs.getEnum<Koodisto.VktOsakoe>("osakoe"),
                    tutkinnot = rs.getTypedArray("tutkinnot") { Koodisto.VktKielitaito.valueOf(it) }.toList(),
                    hylkaamispaatos = rs.getObject("hylkaamispaatos", LocalDate::class.java),
                    oikeusPaattyy = rs.getObject("oikeus_paattyy", LocalDate::class.java),
                )
            }
    }
}

data class VktUusintaoikeusraportti(
    val tarkastelupaiva: LocalDate,
    val rivit: List<VktUusintaoikeus>,
) {
    val yhteenveto: VktUusintaoikeusyhteenveto get() = VktUusintaoikeusyhteenveto.laske(rivit)
}

data class VktUusintaoikeusyhteenveto(
    val taitotasoittain: Map<Koodisto.VktTaitotaso, Maarat>,
    val yhteensa: Maarat,
) {
    data class Maarat(
        val henkiloita: Int,
        val osakokeita: Int,
    )

    companion object {
        fun laske(rivit: List<VktUusintaoikeus>): VktUusintaoikeusyhteenveto =
            VktUusintaoikeusyhteenveto(
                taitotasoittain =
                    Koodisto.VktTaitotaso.entries.associateWith { taso ->
                        maarat(rivit.filter { it.taitotaso == taso })
                    },
                yhteensa = maarat(rivit),
            )

        private fun maarat(rivit: List<VktUusintaoikeus>) =
            Maarat(
                henkiloita = rivit.distinctBy { it.suorittajanOid }.size,
                osakokeita = rivit.size,
            )
    }
}
