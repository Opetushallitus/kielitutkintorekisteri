package fi.oph.kitu.vkt.uusintaoikeus

import fi.oph.kitu.csv.buildCsvFilename
import fi.oph.kitu.csv.toCsvFilenameSegment
import fi.oph.kitu.html.table.ListViewParams
import fi.oph.kitu.html.table.valintaDescription
import fi.oph.kitu.i18n.UiText
import fi.oph.kitu.jdbc.SortDirection
import fi.oph.kitu.koodisto.Koodisto
import fi.oph.kitu.webmvc.toTrueOrNull
import fi.oph.kitu.webmvc.toUrlParam
import org.springframework.format.annotation.DateTimeFormat
import java.time.LocalDate

data class VktUusintaoikeusParams(
    @param:DateTimeFormat(iso = DateTimeFormat.ISO.DATE)
    val tarkastelupaiva: LocalDate? = null,
    val tutkintokieli: List<Koodisto.Tutkintokieli> = emptyList(),
    val taitotaso: List<Koodisto.VktTaitotaso> = emptyList(),
    override val piilotaHenkilotiedot: Boolean = false,
    val sortColumn: VktUusintaoikeusColumn = VktUusintaoikeusColumn.Sukunimi,
    val sortDirection: SortDirection = SortDirection.ASC,
) : ListViewParams {
    fun toMap(): Map<String, String?> =
        mapOf(
            "tarkastelupaiva" to tarkastelupaiva?.toString(),
            "tutkintokieli" to tutkintokieli.toUrlParam(),
            "taitotaso" to taitotaso.toUrlParam(),
            "piilotaHenkilotiedot" to piilotaHenkilotiedot.toTrueOrNull(),
            "sortColumn" to sortColumn.urlParam,
            "sortDirection" to sortDirection.name,
        ).filterValues { it != null }

    fun filterDescriptions(): List<String> =
        listOfNotNull(
            valintaDescription(UiText.Vkt.Sarake.tutkintokieli, tutkintokieli) { it.nimi.toString() },
            valintaDescription(UiText.Vkt.Sarake.taitotaso, taitotaso) { it.nimi.toString() },
            if (piilotaHenkilotiedot) UiText.Filter.henkilotiedotPiilotettu.toString() else null,
        )

    fun rajaa(rivit: List<VktUusintaoikeus>): List<VktUusintaoikeus> =
        rivit.filter { rivi ->
            (tutkintokieli.isEmpty() || rivi.tutkintokieli in tutkintokieli) &&
                (taitotaso.isEmpty() || rivi.taitotaso in taitotaso)
        }

    fun csvFileName(tarkastelupaiva: LocalDate) =
        buildCsvFilename(
            "vkt_uusintaoikeudet",
            piilotaHenkilotiedot,
            tarkastelupaiva.toString(),
            tutkintokieli.toCsvFilenameSegment(),
            taitotaso.toCsvFilenameSegment { it.koodiarvo },
        )
}
