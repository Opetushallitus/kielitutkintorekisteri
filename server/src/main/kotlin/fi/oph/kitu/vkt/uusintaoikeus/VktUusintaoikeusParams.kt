package fi.oph.kitu.vkt.uusintaoikeus

import fi.oph.kitu.csv.buildCsvFilename
import fi.oph.kitu.html.table.ListViewParams
import fi.oph.kitu.i18n.UiText
import fi.oph.kitu.jdbc.SortDirection
import fi.oph.kitu.koodisto.Koodisto
import fi.oph.kitu.webmvc.toTrueOrNull
import org.springframework.format.annotation.DateTimeFormat
import java.time.LocalDate

data class VktUusintaoikeusParams(
    @param:DateTimeFormat(iso = DateTimeFormat.ISO.DATE)
    val tarkastelupaiva: LocalDate? = null,
    val tutkintokieli: Koodisto.Tutkintokieli? = null,
    val taitotaso: Koodisto.VktTaitotaso? = null,
    override val piilotaHenkilotiedot: Boolean = false,
    val sortColumn: VktUusintaoikeusColumn = VktUusintaoikeusColumn.Sukunimi,
    val sortDirection: SortDirection = SortDirection.ASC,
) : ListViewParams {
    fun toMap(): Map<String, String?> =
        mapOf(
            "tarkastelupaiva" to tarkastelupaiva?.toString(),
            "tutkintokieli" to tutkintokieli?.name,
            "taitotaso" to taitotaso?.name,
            "piilotaHenkilotiedot" to piilotaHenkilotiedot.toTrueOrNull(),
            "sortColumn" to sortColumn.urlParam,
            "sortDirection" to sortDirection.name,
        ).filterValues { it != null }

    fun filterDescriptions(): List<String> =
        listOfNotNull(
            tutkintokieli?.let { "${UiText.Vkt.Sarake.tutkintokieli}: ${it.nimi}" },
            taitotaso?.let { "${UiText.Vkt.Sarake.taitotaso}: ${it.nimi}" },
            if (piilotaHenkilotiedot) UiText.Filter.henkilotiedotPiilotettu.toString() else null,
        )

    fun rajaa(rivit: List<VktUusintaoikeus>): List<VktUusintaoikeus> =
        rivit.filter { rivi ->
            (tutkintokieli == null || rivi.tutkintokieli == tutkintokieli) &&
                (taitotaso == null || rivi.taitotaso == taitotaso)
        }

    fun csvFileName(tarkastelupaiva: LocalDate) =
        buildCsvFilename(
            "vkt_uusintaoikeudet",
            piilotaHenkilotiedot,
            tarkastelupaiva.toString(),
            tutkintokieli?.toString(),
            taitotaso?.koodiarvo,
        )
}
