package fi.oph.kitu.yki.suoritukset
import fi.oph.kitu.html.copyable
import fi.oph.kitu.html.labelColon
import fi.oph.kitu.html.table.DisplayTableEnum
import fi.oph.kitu.i18n.LocalizedString
import fi.oph.kitu.i18n.UiText
import fi.oph.kitu.i18n.finnishDate
import fi.oph.kitu.i18n.unaryPlus
import kotlinx.html.FlowContent
import kotlinx.html.li
import kotlinx.html.p
import kotlinx.html.strong
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
        entityName = "kieli",
        uiHeaderValue = UiText.Yki.Sarake.kieli,
        urlParam = "kieli",
        getValue = { it.tutkintokieli.name },
    ),
    Tutkintotaso(
        entityName = "tutkintotaso",
        uiHeaderValue = UiText.Yki.Sarake.tutkintotaso,
        urlParam = "tutkintotaso",
        getValue = { it.tutkintotaso.name },
    ),
    Paivamaara(
        entityName = "tutkintoPvm",
        uiHeaderValue = UiText.Yki.Sarake.paivamaara,
        urlParam = "tutkintoPvm",
        getValue = { it.tutkintopaiva.finnishDate() },
        renderHtml = {
            ul(classes = "flat") {
                li {
                    labelColon(UiText.Yki.Sarake.tutkintopaiva)
                    +it.tutkintopaiva.finnishDate()
                }
                it.tarkistusarvioinninSaapumisPvm?.let { pvm ->
                    li {
                        labelColon(UiText.Yki.saapunut)
                        +pvm.finnishDate()
                    }
                }
                it.tarkistusarvioinninKasittelyPvm?.let { pvm ->
                    li {
                        labelColon(UiText.Yki.kasitelty)
                        +pvm.finnishDate()
                    }
                }
                it.tarkistusarviointiHyvaksyttyPvm?.let { pvm ->
                    li {
                        labelColon(UiText.Yki.hyvaksytty)
                        +pvm.finnishDate()
                    }
                }
            }
        },
    ),
    Asiatunnus(
        entityName = "asiatunnus",
        uiHeaderValue = UiText.Yki.Sarake.asiatunnus,
        urlParam = "asiatunnus",
        getValue = { it.tarkistusarvioinninAsiatunnus.orEmpty() },
        renderHtml = { it.tarkistusarvioinninAsiatunnus?.let { asiatunnus -> copyable(asiatunnus) } },
    ),
    Muutokset(
        entityName = "arviointi",
        uiHeaderValue = UiText.Yki.Sarake.tarkistusarviointi,
        urlParam = "arviointi",
        getValue = { "Not implemented" },
        renderHtml = {
            val arvosanaMuuttui = it.arvosanaMuuttui?.isNotEmpty() ?: false
            it.perustelu?.let { x ->
                p(classes = if (arvosanaMuuttui) null else "faded") {
                    +(
                        x.ifBlank {
                            "Arvosana ei muuttunut (tarkistusarvioitu: ${
                                it.tarkistusarvioidutOsakokeet
                                    .orEmpty()
                                    .joinToString(separator = ", ") { it.viewText }}"
                        }
                    )
                }
            }
            if (arvosanaMuuttui) {
                ul(classes = "flat") {
                    it.tarkistusarvioidutOsakokeet.orEmpty().forEach { osakoe ->
                        li {
                            if (it.arvosanaMuuttui.contains(osakoe) == true) {
                                strong { +"${osakoe.viewText}: " }
                                +"${UiText.Yki.arvosanaMuuttui}: ${it.arvosana(osakoe) ?: "-"}"
                            } else {
                                +"${osakoe.viewText}: "
                                +UiText.Yki.arvosanaEiMuuttunut
                            }
                        }
                    }
                }
            }
        },
    ),
}
