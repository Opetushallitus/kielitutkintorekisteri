package fi.oph.kitu.vkt.html
import fi.oph.kitu.html.errorMessageDetails
import fi.oph.kitu.html.hideErrorLink
import fi.oph.kitu.html.koskiErrorsPage
import fi.oph.kitu.html.table.DisplayTableEnum
import fi.oph.kitu.i18n.LocalizedString
import fi.oph.kitu.i18n.Translations
import fi.oph.kitu.i18n.UiText
import fi.oph.kitu.i18n.finnishDateTime
import fi.oph.kitu.i18n.unaryPlus
import fi.oph.kitu.koski.KoskiErrorEntity
import fi.oph.kitu.koski.VktMappingId
import fi.oph.kitu.webmvc.Links
import kotlinx.html.a

object VktKoskiErrors {
    fun render(
        errors: List<KoskiErrorEntity>,
        hiddenCount: Int?,
        t: Translations,
    ): String =
        koskiErrorsPage(
            title = UiText.Nav.vkt,
            subtitle = UiText.Vkt.koskiTiedonsiirtovirheet,
            errors = errors,
            hiddenCount = hiddenCount,
            ohje = UiText.Ohje.Vkt.suoritukset,
            columns =
                listOf(
                    Column.Tutkintoryhma.withHtml { ryhma ->
                        VktMappingId.parse(ryhma.id)?.let {
                            a(
                                href =
                                    Links.Vkt.ilmoittautuneenArviointi(
                                        oppijanumero = it.ryhma.oppijanumero,
                                        kieli = it.ryhma.tutkintokieli,
                                        taso = it.ryhma.taitotaso,
                                    ),
                            ) {
                                +it.ryhma.oppijanumero
                                +" / "
                                +t.get(it.ryhma.tutkintokieli)
                                +" / "
                                +t.get(it.ryhma.taitotaso)
                            }
                        } ?: +ryhma.id
                    },
                    Column.Aikaleima.withHtml { finnishDateTime(it.timestamp) },
                    Column.Virhe.withHtml { errorMessageDetails(it) },
                    Column.Request.withHtml { error ->
                        VktMappingId.parse(error.id)?.let { id ->
                            a(
                                href =
                                    Links.Vkt.koskiRequestJson(
                                        id.ryhma.oppijanumero,
                                        id.ryhma.tutkintokieli,
                                        id.ryhma.taitotaso,
                                    ),
                            ) {
                                +UiText.Vkt.naytaJson
                            }
                        }
                    },
                    Column.Hidden.withHtml { error -> hideErrorLink(error, ::hideErrorUrl) },
                ),
        )

    fun hideErrorUrl(
        error: KoskiErrorEntity,
        hidden: Boolean,
    ): String? =
        VktMappingId.parse(error.id)?.let { id ->
            Links.Vkt.hideKoskiVirheet(
                oppijanumero = id.ryhma.oppijanumero,
                tutkintokieli = id.ryhma.tutkintokieli,
                taitotaso = id.ryhma.taitotaso,
                hidden = hidden,
            )
        }

    enum class Column(
        override val entityName: String?,
        override val uiHeaderValue: LocalizedString,
        override val urlParam: String,
    ) : DisplayTableEnum {
        Tutkintoryhma("id", UiText.Vkt.Sarake.tutkintoryhma, "id"),
        Virhe("error", UiText.Vkt.Sarake.virhe, "error"),
        Aikaleima("timestamp", UiText.Vkt.Sarake.aikaleima, "timestamp"),
        Request("request", UiText.Vkt.Sarake.pyynto, "request"),
        Hidden("hidden", UiText.Vkt.Sarake.piilotus, "hidden"),
    }
}

enum class KoskiTransferState {
    NOT_READY,
    PENDING,
    SUCCESS,
    INVALID,
}
