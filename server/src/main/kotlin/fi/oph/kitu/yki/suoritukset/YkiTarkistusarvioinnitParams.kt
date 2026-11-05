package fi.oph.kitu.yki.suoritukset

import fi.oph.kitu.csv.buildCsvFilename
import fi.oph.kitu.csv.toCsvFilenameSegment
import fi.oph.kitu.html.table.valintaDescription
import fi.oph.kitu.i18n.UiText
import fi.oph.kitu.i18n.aikarajausDescription
import fi.oph.kitu.jdbc.SortDirection
import fi.oph.kitu.jdbc.SqlFilter
import fi.oph.kitu.jdbc.SqlFilterBuilder
import fi.oph.kitu.jdbc.addAnyOf
import fi.oph.kitu.jdbc.addSearchTerms
import fi.oph.kitu.util.searchTerms
import fi.oph.kitu.webmvc.toUrlParam
import fi.oph.kitu.yki.TutkinnonOsa
import fi.oph.kitu.yki.Tutkintokieli
import fi.oph.kitu.yki.Tutkintotaso
import org.springframework.format.annotation.DateTimeFormat
import java.time.LocalDate

data class YkiTarkistusarvioinnitParams(
    var search: String = "",
    var tutkintokieli: List<Tutkintokieli> = emptyList(),
    var tutkintotaso: List<Tutkintotaso> = emptyList(),
    var osa: List<TutkinnonOsa> = emptyList(),
    @param:DateTimeFormat(iso = DateTimeFormat.ISO.DATE)
    var tutkintoalku: LocalDate? = null,
    @param:DateTimeFormat(iso = DateTimeFormat.ISO.DATE)
    var tutkintoloppu: LocalDate? = null,
    @param:DateTimeFormat(iso = DateTimeFormat.ISO.DATE)
    var saapumisalku: LocalDate? = null,
    @param:DateTimeFormat(iso = DateTimeFormat.ISO.DATE)
    var saapumisloppu: LocalDate? = null,
    @param:DateTimeFormat(iso = DateTimeFormat.ISO.DATE)
    var kasittelyalku: LocalDate? = null,
    @param:DateTimeFormat(iso = DateTimeFormat.ISO.DATE)
    var kasittelyloppu: LocalDate? = null,
    @param:DateTimeFormat(iso = DateTimeFormat.ISO.DATE)
    var hyvaksyttyalku: LocalDate? = null,
    @param:DateTimeFormat(iso = DateTimeFormat.ISO.DATE)
    var hyvaksyttyloppu: LocalDate? = null,
    var arvosanaMuuttui: Boolean? = null,
    var sortColumn: YkiTarkistusarviointiColumn = YkiTarkistusarviointiColumn.Kasittelypaiva,
    var sortDirection: SortDirection = SortDirection.DESC,
) : SqlFilter() {
    fun toMap(): Map<String, String?> =
        mapOf(
            "search" to search.ifEmpty { null },
            "tutkintokieli" to tutkintokieli.toUrlParam(),
            "tutkintotaso" to tutkintotaso.toUrlParam(),
            "osa" to osa.toUrlParam(),
            "tutkintoalku" to tutkintoalku?.toString(),
            "tutkintoloppu" to tutkintoloppu?.toString(),
            "saapumisalku" to saapumisalku?.toString(),
            "saapumisloppu" to saapumisloppu?.toString(),
            "kasittelyalku" to kasittelyalku?.toString(),
            "kasittelyloppu" to kasittelyloppu?.toString(),
            "hyvaksyttyalku" to hyvaksyttyalku?.toString(),
            "hyvaksyttyloppu" to hyvaksyttyloppu?.toString(),
            "arvosanaMuuttui" to arvosanaMuuttui?.toString(),
            "sortColumn" to sortColumn.urlParam,
            "sortDirection" to sortDirection.name,
        )

    fun csvFileName(prefix: String): String =
        buildCsvFilename(
            prefix,
            false,
            tutkintokieli.toCsvFilenameSegment(),
            tutkintotaso.toCsvFilenameSegment(),
            osa.toCsvFilenameSegment(),
            tutkintoalku?.toString(),
            tutkintoloppu?.toString(),
            saapumisalku?.let { "saapunut_$it" },
            saapumisloppu?.let { "saapunut_$it" },
            kasittelyalku?.let { "kasitelty_$it" },
            kasittelyloppu?.let { "kasitelty_$it" },
            hyvaksyttyalku?.let { "hyvaksytty_$it" },
            hyvaksyttyloppu?.let { "hyvaksytty_$it" },
            when (arvosanaMuuttui) {
                true -> "arvosana_muuttui"
                false -> "arvosana_ei_muuttunut"
                null -> null
            },
        )

    fun orderBySql(): String =
        listOfNotNull(
            sortColumn.entityName?.let { "$it $sortDirection NULLS LAST" },
            "tarkistusarvioinnin_kasittely_pvm DESC NULLS LAST",
            "tarkistusarvioinnin_saapumis_pvm DESC NULLS LAST",
            "solki_id",
        ).joinToString(", ")

    fun filterDescriptions(): List<String> =
        listOfNotNull(
            search.trim().takeIf { it.isNotEmpty() }?.let { "${UiText.Yki.hakusanaTarkistusarviointi}: $it" },
            aikarajausDescription(tutkintoalku, tutkintoloppu, prefix = UiText.Yki.Sarake.tutkintopaiva),
            aikarajausDescription(saapumisalku, saapumisloppu, prefix = UiText.Yki.saapunut),
            aikarajausDescription(kasittelyalku, kasittelyloppu, prefix = UiText.Yki.kasitelty),
            aikarajausDescription(hyvaksyttyalku, hyvaksyttyloppu, prefix = UiText.Yki.hyvaksytty),
            valintaDescription(UiText.Yki.Sarake.tutkintokieli, tutkintokieli),
            valintaDescription(UiText.Yki.Sarake.tutkintotaso, tutkintotaso),
            valintaDescription(UiText.Yki.Sarake.osat, osa),
            when (arvosanaMuuttui) {
                true -> UiText.Yki.arvosanaMuuttui.toString()
                false -> UiText.Yki.arvosanaEiMuuttunut.toString()
                null -> null
            },
        )

    override fun SqlFilterBuilder.build() {
        addSearchTerms(searchTerms(search), HAKUSARAKKEET, "hakusana")
        addAnyOf("tutkintokieli", "filter_kieli", tutkintokieli)
        addAnyOf("tutkintotaso", "filter_taso", tutkintotaso)
        if (osa.isNotEmpty()) {
            add(
                "tarkistusarvioidut_osakokeet && ARRAY[:filter_osa]::yki_osakoetyyppi[]",
                "filter_osa" to osa.map { it.name },
            )
        }
        addPaivavali("tutkintopaiva", tutkintoalku, tutkintoloppu)
        addPaivavali("tarkistusarvioinnin_saapumis_pvm", saapumisalku, saapumisloppu)
        addPaivavali("tarkistusarvioinnin_kasittely_pvm", kasittelyalku, kasittelyloppu)
        addPaivavali("tarkistusarviointi_hyvaksytty_pvm", hyvaksyttyalku, hyvaksyttyloppu)
        when (arvosanaMuuttui) {
            true -> {
                add("arvosana_muuttui IS NOT NULL")
            }

            false -> {
                add("arvosana_muuttui IS NULL")
            }

            null -> {}
        }
    }

    private fun SqlFilterBuilder.addPaivavali(
        sarake: String,
        alku: LocalDate?,
        loppu: LocalDate?,
    ) {
        add(alku?.let { "$sarake >= :${sarake}_alku" }, "${sarake}_alku" to alku)
        add(loppu?.let { "$sarake <= :${sarake}_loppu" }, "${sarake}_loppu" to loppu)
    }

    companion object {
        private val HAKUSARAKKEET =
            listOf("sukunimi", "etunimet", "suorittajan_oid", "tarkistusarvioinnin_asiatunnus")
    }
}
