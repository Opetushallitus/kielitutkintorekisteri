package fi.oph.kitu.yki.suoritukset.error
import fi.oph.kitu.html.errorMessageDetails
import fi.oph.kitu.html.hideErrorLink
import fi.oph.kitu.html.koskiErrorsPage
import fi.oph.kitu.html.table.DisplayTableEnum
import fi.oph.kitu.i18n.LocalizedString
import fi.oph.kitu.i18n.UiText
import fi.oph.kitu.i18n.finnishDateTime
import fi.oph.kitu.i18n.unaryPlus
import fi.oph.kitu.koski.KoskiErrorEntity
import fi.oph.kitu.koski.YkiMappingId
import fi.oph.kitu.webmvc.Links
import fi.oph.kitu.yki.suoritukset.YkiSuoritusEntity
import kotlinx.html.a

object YkiKoskiErrors {
    fun render(
        errors: List<KoskiErrorEntity>,
        suoritukset: Iterable<YkiSuoritusEntity>,
        hiddenCount: Int?,
    ): String {
        val errorIdToSuoritusMap =
            errors
                .mapNotNull { error ->
                    YkiMappingId
                        .parse(error.id)
                        ?.suoritusId
                        ?.let { id -> suoritukset.find { it.solkiId == id } }
                        ?.let { error.id to it }
                }.toMap()

        return koskiErrorsPage(
            title = UiText.Nav.yki,
            subtitle = UiText.Yki.koskiTiedonsiirtovirheet,
            errors = errors,
            hiddenCount = hiddenCount,
            wideContent = true,
            columns =
                listOf(
                    Column.Oppijanumero.withHtml { error ->
                        val suoritus = errorIdToSuoritusMap[error.id]
                        val oppijanumero = suoritus?.suorittajanOID?.toString()
                        if (suoritus?.id != null && oppijanumero != null) {
                            a(href = Links.Yki.suoritus(suoritus.id)) { +oppijanumero }
                        } else {
                            +(oppijanumero ?: "???")
                        }
                    },
                    Column.SuorituksenTunniste.withHtml { error ->
                        val suoritus = errorIdToSuoritusMap[error.id]
                        val tunniste = suoritus?.solkiId?.toString() ?: "#${error.id}"
                        if (suoritus?.id != null) {
                            a(href = Links.Yki.suoritus(suoritus.id)) { +tunniste }
                        } else {
                            +tunniste
                        }
                    },
                    Column.Aikaleima.withHtml { finnishDateTime(it.timestamp) },
                    Column.Virhe.withHtml { errorMessageDetails(it) },
                    Column.Request.withHtml { error ->
                        a(href = Links.Yki.koskiRequestJson(error.id.toInt())) {
                            +UiText.Yki.naytaJson
                        }
                    },
                    Column.Hidden.withHtml { error -> hideErrorLink(error, ::hideErrorUrl) },
                ),
        )
    }

    enum class Column(
        override val entityName: String?,
        override val uiHeaderValue: LocalizedString,
        override val urlParam: String,
    ) : DisplayTableEnum {
        Oppijanumero("oppijanumero", UiText.Yki.Sarake.oppijanumero, "oppijanumero"),
        SuorituksenTunniste("tunniste", UiText.Yki.Sarake.suorituksenTunniste, "tunniste"),
        Virhe("error", UiText.Yki.Sarake.virhe, "error"),
        Aikaleima("timestamp", UiText.Yki.Sarake.aikaleima, "timestamp"),
        Request("request", UiText.Yki.Sarake.pyynto, "request"),
        Hidden("hidden", UiText.Yki.Sarake.piilotus, "hidden"),
    }

    fun hideErrorUrl(
        error: KoskiErrorEntity,
        hidden: Boolean,
    ): String? =
        YkiMappingId.parse(error.id)?.let { id ->
            id.suoritusId?.let { suoritusId ->
                Links.Yki.hideKoskiVirheet(suoritusId, hidden)
            }
        }
}
