package fi.oph.kitu.html

import fi.oph.kitu.html.table.DisplayTableColumn
import fi.oph.kitu.html.table.displayTable
import fi.oph.kitu.i18n.LocalizedString
import fi.oph.kitu.i18n.UiText
import fi.oph.kitu.i18n.unaryPlus
import fi.oph.kitu.koski.KoskiErrorEntity
import kotlinx.html.FlowContent
import kotlinx.html.a
import kotlinx.html.article
import kotlinx.html.details
import kotlinx.html.h1
import kotlinx.html.h2
import kotlinx.html.summary

private const val ERROR_MESSAGE_SUMMARY_MAX_LENGTH = 60

fun FlowContent.hiddenErrorsBanner(hiddenCount: Int?) {
    hiddenCount?.let { count ->
        if (count > 0) {
            viewMessage(
                ViewMessageData.html(type = ViewMessageType.INFO) {
                    +"Yhteensä $count virhettä on piilotettu. "
                    a(href = "?hidden=true") { +"Näytä piilotetut virheet" }
                },
            )
        }
    } ?: article { a(href = "?hidden=false") { +"Palaa virhesivulle" } }
}

fun FlowContent.errorMessageDetails(error: KoskiErrorEntity) {
    val errorJson = error.errorJson()
    details {
        attributes["name"] = error.id
        summary {
            val msg = error.message.split(":").first()
            if (msg.length > ERROR_MESSAGE_SUMMARY_MAX_LENGTH) {
                +(msg.take(ERROR_MESSAGE_SUMMARY_MAX_LENGTH) + "...")
            } else {
                +msg
            }
        }
        if (errorJson != null) {
            json(errorJson)
        } else {
            +error.message
        }
    }
}

/**
 * KOSKI-virhesivujen yhteinen runko. Sarakkeet tulevat domainilta: VKT:llä ja YKI:llä on
 * eri tunnistesarakkeet ja omat UiText-nimiavaruutensa, eivätkä Kotlin-enumit voi periä
 * entryjä — joten jaettavaa on nimenomaan runko, ei saraketaulukko.
 */
fun koskiErrorsPage(
    title: LocalizedString,
    subtitle: LocalizedString,
    errors: List<KoskiErrorEntity>,
    hiddenCount: Int?,
    wideContent: Boolean = false,
    ohje: LocalizedString? = null,
    columns: List<DisplayTableColumn<KoskiErrorEntity>>,
): String =
    Page.renderHtml(wideContent = wideContent, ohje = ohje) {
        h1 { +title }
        h2 { +subtitle }

        hiddenErrorsBanner(hiddenCount)

        card(overflowAuto = true, compact = true) {
            displayTable(rows = errors, columns = columns)
        }
    }

/** Piilota/palauta-linkki on molemmilla sivuilla sama; vain URLin muodostus eroaa. */
fun FlowContent.hideErrorLink(
    error: KoskiErrorEntity,
    hideUrl: (KoskiErrorEntity, Boolean) -> String?,
) {
    hideUrl(error, !error.hidden)?.let { url ->
        a(href = url) {
            if (error.hidden) +UiText.Toiminto.palauta else +UiText.Toiminto.piilota
        }
    }
}
