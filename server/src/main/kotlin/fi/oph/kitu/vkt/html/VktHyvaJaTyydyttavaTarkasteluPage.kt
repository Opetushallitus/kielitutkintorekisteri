package fi.oph.kitu.vkt.html
import arrow.core.Either
import fi.oph.kitu.html.Page
import fi.oph.kitu.html.ViewMessageData
import fi.oph.kitu.html.card
import fi.oph.kitu.html.table.DisplayTableColumn
import fi.oph.kitu.html.table.displayTable
import fi.oph.kitu.html.viewMessage
import fi.oph.kitu.i18n.CurrentLanguage
import fi.oph.kitu.i18n.Translations
import fi.oph.kitu.i18n.UiText
import fi.oph.kitu.i18n.finnishDate
import fi.oph.kitu.i18n.unaryPlus
import fi.oph.kitu.oppijanumero.OppijanumeroException
import fi.oph.kitu.oppijanumero.OppijanumerorekisteriHenkilo
import fi.oph.kitu.tiedontuontischema.VktHenkilosuoritus
import fi.oph.kitu.vkt.VktOsakoe
import kotlinx.html.FlowContent
import kotlinx.html.h1
import kotlinx.html.h2
import kotlinx.html.h3

object VktHyvaJaTyydyttavaTarkasteluPage {
    fun render(
        data: VktHenkilosuoritus,
        henkilo: Either<OppijanumeroException, OppijanumerorekisteriHenkilo>,
        translations: Translations,
        messages: List<ViewMessageData>,
        koskiTransferState: Pair<KoskiTransferState, List<String>>,
    ): String =
        Page.renderHtml(ohje = UiText.Ohje.Vkt.hyvaJaTyydyttava) {
            h1 { +data.henkilo.kokoNimi() }
            h2 { +UiText.Nav.vkt }

            messages.forEach { viewMessage(it) }

            vktHenkilonTiedot(data, henkilo)
            vktSuorituksenTiedot(data, koskiTransferState, translations)

            h3 { +UiText.Vkt.tutkinnot }
            vktTutkinnot(data, translations)

            h3 { +UiText.Vkt.osakokeet }
            card(overflowAuto = true) {
                vktHyvaJaTyydyttavaOsakoeTable(data.suoritus.osat, translations)
            }
        }
}

fun FlowContent.vktHyvaJaTyydyttavaOsakoeTable(
    osat: List<VktOsakoe>,
    t: Translations,
) {
    displayTable(
        osat.sortedBy { it.tutkintopaiva }.reversed(),
        listOf(
            DisplayTableColumn(UiText.Vkt.osakoe.toString(), width = "20%") {
                +t.get(it.tyyppi)
            },
            DisplayTableColumn(
                UiText.Vkt.Sarake.tutkintopaiva
                    .toString(),
                width = "16%",
            ) {
                finnishDate(it.tutkintopaiva)
            },
            DisplayTableColumn(UiText.Vkt.arvosana.toString(), width = "16%") {
                it.arviointi?.arvosana?.let { arvosana -> +t.get(arvosana) }
            },
            DisplayTableColumn(UiText.Vkt.arviointipaiva.toString(), width = "16%") {
                it.arviointi?.paivamaara?.let { pvm -> finnishDate(pvm) }
            },
            DisplayTableColumn(
                UiText.Vkt.Sarake.vastaanottaja
                    .toString(),
                width = "16%",
            ) {
                +it.suorituksenVastaanottaja.toString()
            },
            DisplayTableColumn(
                UiText.Vkt.Sarake.suorituspaikkakunta
                    .toString(),
                width = "16%",
            ) {
                +t.getByKoodiviite("kunta", it.suorituspaikkakunta)
            },
        ),
        testId = "osakokeet",
    )
}
