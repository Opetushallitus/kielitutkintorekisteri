package fi.oph.kitu.yki.suoritukset
import fi.oph.kitu.html.copyable
import fi.oph.kitu.html.infoModal
import fi.oph.kitu.html.orDash
import fi.oph.kitu.html.table.DisplayTableColumn
import fi.oph.kitu.html.table.DisplayTableEnum
import fi.oph.kitu.i18n.LocalizedString
import fi.oph.kitu.i18n.UiText
import fi.oph.kitu.i18n.finnishDate
import fi.oph.kitu.yki.TutkinnonOsa
import kotlinx.html.FlowContent
import kotlinx.html.div
import kotlinx.html.li
import kotlinx.html.p
import kotlinx.html.ul

enum class YkiTarkistusarviointiColumn(
    override val entityName: String?,
    override val uiHeaderValue: LocalizedString,
    override val urlParam: String,
    val getValue: (YkiSuoritusEntity) -> String,
    val renderHtml: (FlowContent.(YkiSuoritusEntity) -> Unit)? = null,
) : DisplayTableEnum {
    Sukunimi(
        entityName = "sukunimi",
        uiHeaderValue = UiText.Yki.Sarake.sukunimi,
        urlParam = "sukunimi",
        getValue = { it.sukunimi },
    ),
    Etunimet(
        entityName = "etunimet",
        uiHeaderValue = UiText.Yki.Sarake.etunimet,
        urlParam = "etunimet",
        getValue = { it.etunimet },
    ),
    Kieli(
        entityName = "tutkintokieli",
        uiHeaderValue = UiText.Yki.Sarake.kieli,
        urlParam = "kieli",
        getValue = { it.tutkintokieli.name },
    ),
    Tutkintotaso(
        entityName = "tutkintotaso",
        uiHeaderValue = UiText.Yki.Sarake.taso,
        urlParam = "tutkintotaso",
        getValue = { it.tutkintotaso.name },
    ),
    Tutkintopaiva(
        entityName = "tutkintopaiva",
        uiHeaderValue = UiText.Yki.Sarake.tutkintopaiva,
        urlParam = "tutkintopaiva",
        getValue = { it.tutkintopaiva.finnishDate() },
    ),
    Saapumispaiva(
        entityName = "tarkistusarvioinnin_saapumis_pvm",
        uiHeaderValue = UiText.Yki.Sarake.saapunut,
        urlParam = "saapunut",
        getValue = { it.tarkistusarvioinninSaapumisPvm?.finnishDate().orEmpty() },
    ),
    Kasittelypaiva(
        entityName = "tarkistusarvioinnin_kasittely_pvm",
        uiHeaderValue = UiText.Yki.Sarake.kasitelty,
        urlParam = "kasitelty",
        getValue = { it.tarkistusarvioinninKasittelyPvm?.finnishDate().orEmpty() },
    ),
    Hyvaksymispaiva(
        entityName = "tarkistusarviointi_hyvaksytty_pvm",
        uiHeaderValue = UiText.Yki.Sarake.hyvaksytty,
        urlParam = "hyvaksytty",
        getValue = { it.tarkistusarviointiHyvaksyttyPvm?.finnishDate().orEmpty() },
    ),
    Asiatunnus(
        entityName = "tarkistusarvioinnin_asiatunnus",
        uiHeaderValue = UiText.Yki.Sarake.asiatunnus,
        urlParam = "asiatunnus",
        getValue = { it.tarkistusarvioinninAsiatunnus.orEmpty() },
        renderHtml = { it.tarkistusarvioinninAsiatunnus?.let { asiatunnus -> copyable(asiatunnus) } },
    ),
    Osat(
        entityName = null,
        uiHeaderValue = UiText.Yki.Sarake.osat,
        urlParam = "osat",
        getValue = { osakoeLista(it.tarkistusarvioidutOsakokeet) },
        renderHtml = { osakoeRivit(it.tarkistusarvioidutOsakokeet.orEmpty()) },
    ),
    Muuttuneet(
        entityName = null,
        uiHeaderValue = UiText.Yki.Sarake.muuttuneet,
        urlParam = "muuttuneet",
        getValue = { it.muuttuneetTekstina() },
        renderHtml = { suoritus ->
            div(classes = "muuttuneet-osakokeet") {
                val muuttuneet = suoritus.muuttuneetOsakokeet()
                div {
                    if (muuttuneet.isNullOrEmpty()) {
                        +suoritus.muuttuneetTekstina()
                    } else {
                        osakoeRivit(muuttuneet)
                    }
                }
                suoritus.perustelu?.takeIf { it.isNotBlank() }?.let { perustelu ->
                    infoModal(
                        modalId = "perustelu-${suoritus.solkiId}",
                        title = UiText.Yki.perustelu.toString(),
                        label = UiText.Yki.naytaPerustelu.toString(),
                    ) {
                        p { +perustelu }
                    }
                }
            }
        },
    ),
    ;

    fun toDisplayColumn(): DisplayTableColumn<YkiSuoritusEntity> =
        DisplayTableColumn(
            label = uiHeaderValue.toString(),
            sortKey = entityName?.let { urlParam },
            testId = urlParam,
            getValue = getValue,
            renderHtml = renderHtml,
        )
}

private fun YkiSuoritusEntity.muuttuneetOsakokeet(): Set<TutkinnonOsa>? =
    tarkistusarvioinninKasittelyPvm?.let { arvosanaMuuttui.orEmpty() }

private fun YkiSuoritusEntity.muuttuneetTekstina(): String =
    muuttuneetOsakokeet()?.let { osakoeLista(it).ifEmpty { null }.orDash() }.orEmpty()

private fun FlowContent.osakoeRivit(osakokeet: Set<TutkinnonOsa>) {
    ul(classes = "flat") {
        osakokeet.sortedBy { it.ordinal }.forEach { li { +it.viewText } }
    }
}
