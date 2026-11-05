package fi.oph.kitu.yki.suoritukset

import arrow.core.Either
import fi.oph.kitu.csv.buildCsvFilename
import fi.oph.kitu.csv.toCsvFilenameSegment
import fi.oph.kitu.html.Pagination
import fi.oph.kitu.html.table.httpParams
import fi.oph.kitu.html.table.valintaDescription
import fi.oph.kitu.i18n.UiText
import fi.oph.kitu.i18n.aikarajausDescription
import fi.oph.kitu.jdbc.SortDirection
import fi.oph.kitu.jdbc.SqlFilter
import fi.oph.kitu.jdbc.SqlFilterBuilder
import fi.oph.kitu.jdbc.addAnyOf
import fi.oph.kitu.jdbc.addSearchTerms
import fi.oph.kitu.oppijanumero.OppijanumeroException
import fi.oph.kitu.oppijanumero.OppijanumeroService
import fi.oph.kitu.util.SearchTerms
import fi.oph.kitu.webmvc.toUrlParam
import fi.oph.kitu.yki.TutkinnonOsa
import fi.oph.kitu.yki.Tutkintokieli
import fi.oph.kitu.yki.Tutkintotaso
import org.springframework.format.annotation.DateTimeFormat
import java.time.LocalDate

private const val OLETUSSIVUKOKO = 100

data class YkiTarkistusarvioinnitSivu(
    val suoritukset: List<YkiSuoritusEntity>,
    val kokonaismaara: Int,
    val sivunumero: Int,
)

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
    var page: Int = 1,
    var limit: Int = OLETUSSIVUKOKO,
) {
    fun toFilter(): YkiTarkistusarviointiFilter = YkiTarkistusarviointiFilter(this, SearchTerms.from(search))

    fun sivukoko(): Int = limit.coerceAtLeast(1)

    fun pagination(sivu: YkiTarkistusarvioinnitSivu): Pagination =
        Pagination.valueOf(
            currentPageNumber = sivu.sivunumero,
            numberOfRows = sivu.kokonaismaara,
            pageSize = sivukoko(),
            url = { sivunumero -> httpParams(toMap() + ("page" to sivunumero.toString())) },
        )

    fun toCsvMap(): Map<String, String?> = toMap() - "page" - "limit"

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
            "page" to page.toString(),
            "limit" to limit.takeIf { it != OLETUSSIVUKOKO }?.toString(),
        )

    fun csvFileName(
        prefix: String,
        henkilotiedot: Boolean = true,
    ): String =
        buildCsvFilename(
            prefix,
            !henkilotiedot,
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
}

data class YkiTarkistusarviointiFilter(
    val rajaus: YkiTarkistusarvioinnitParams,
    val hakutermit: SearchTerms,
) : SqlFilter() {
    fun extendHenkiloOids(onr: OppijanumeroService): Either<OppijanumeroException, YkiTarkistusarviointiFilter> =
        hakutermit.withLinkedHenkiloOids(onr).map { copy(hakutermit = it) }

    fun orderBySql(): String = rajaus.orderBySql()

    override fun SqlFilterBuilder.build() {
        addSearchTerms(hakutermit.termsExceptHenkiloOids(), HAKUSARAKKEET, "hakusana")
        hakutermit.henkiloOids()?.let { oids ->
            add("suorittajan_oid IN (:henkilo_oids)", "henkilo_oids" to oids)
        }
        addAnyOf("tutkintokieli", "filter_kieli", rajaus.tutkintokieli)
        addAnyOf("tutkintotaso", "filter_taso", rajaus.tutkintotaso)
        if (rajaus.osa.isNotEmpty()) {
            add(
                "tarkistusarvioidut_osakokeet && ARRAY[:filter_osa]::yki_osakoetyyppi[]",
                "filter_osa" to rajaus.osa.map { it.name },
            )
        }
        addPaivavali("tutkintopaiva", rajaus.tutkintoalku, rajaus.tutkintoloppu)
        addPaivavali("tarkistusarvioinnin_saapumis_pvm", rajaus.saapumisalku, rajaus.saapumisloppu)
        addPaivavali("tarkistusarvioinnin_kasittely_pvm", rajaus.kasittelyalku, rajaus.kasittelyloppu)
        addPaivavali("tarkistusarviointi_hyvaksytty_pvm", rajaus.hyvaksyttyalku, rajaus.hyvaksyttyloppu)
        when (rajaus.arvosanaMuuttui) {
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
