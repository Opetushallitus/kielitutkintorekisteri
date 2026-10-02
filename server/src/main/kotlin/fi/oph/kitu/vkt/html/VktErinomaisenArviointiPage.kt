package fi.oph.kitu.vkt.html
import arrow.core.Either
import fi.oph.kitu.html.Navigation
import fi.oph.kitu.html.Navigation.setCurrentItem
import fi.oph.kitu.html.Page
import fi.oph.kitu.html.ViewMessageData
import fi.oph.kitu.html.card
import fi.oph.kitu.html.dateInput
import fi.oph.kitu.html.formPost
import fi.oph.kitu.html.hiddenValue
import fi.oph.kitu.html.itemSelect
import fi.oph.kitu.html.submitButton
import fi.oph.kitu.html.table.DisplayTableColumn
import fi.oph.kitu.html.table.displayTable
import fi.oph.kitu.html.viewMessage
import fi.oph.kitu.i18n.CurrentLanguage
import fi.oph.kitu.i18n.Translations
import fi.oph.kitu.i18n.UiText
import fi.oph.kitu.i18n.finnishDate
import fi.oph.kitu.i18n.finnishDateTime
import fi.oph.kitu.i18n.unaryPlus
import fi.oph.kitu.koodisto.Koodisto
import fi.oph.kitu.oppijanumero.OppijanumeroException
import fi.oph.kitu.oppijanumero.OppijanumerorekisteriHenkilo
import fi.oph.kitu.tiedontuontischema.VktHenkilosuoritus
import fi.oph.kitu.util.growToSize
import fi.oph.kitu.vkt.VktOsakoe
import kotlinx.html.FlowContent
import kotlinx.html.footer
import kotlinx.html.h1
import kotlinx.html.h2
import kotlinx.html.h3
import java.time.LocalDate

object VktErinomaisenArviointiPage {
    fun render(
        data: VktHenkilosuoritus,
        henkilo: Either<OppijanumeroException, OppijanumerorekisteriHenkilo>,
        translations: Translations,
        messages: List<ViewMessageData>,
        koskiTransferState: Pair<KoskiTransferState, List<String>>,
    ): String =
        Page.renderHtml(ohje = UiText.Ohje.Vkt.erinomainen) {
            h1 { +data.henkilo.kokoNimi() }
            h2 { +UiText.Nav.vkt }

            messages.forEach { viewMessage(it) }

            vktHenkilonTiedot(data, henkilo)
            vktSuorituksenTiedot(data, koskiTransferState, translations)

            h3 { +UiText.Vkt.tutkinnot }
            vktTutkinnot(data, translations)

            h3 { +UiText.Vkt.osakokeet }
            formPost(action = "") {
                card(overflowAuto = true, compact = true) {
                    vktErinomainenOsakoeTable(data.suoritus.osat, translations)
                    footer {
                        submitButton()
                    }
                }
            }
        }

    data class ArvosanaFormData(
        val id: List<Int>?,
        val arvosana: List<Koodisto.VktArvosana?>?,
        val arviointipaiva: List<LocalDate?>?,
    ) {
        fun toEntries(): List<ArvosanaFormEntry> {
            val ids = id.orEmpty()
            val arvosanat = arvosana.orEmpty()
            val arviointipaivat = arviointipaiva.orEmpty()

            return ids
                .zip(arvosanat.growToSize(ids.size, null))
                .zip(arviointipaivat.growToSize(ids.size, null))
                .map {
                    val arvosana = it.first.second
                    val eiSuoritusta = arvosana == Koodisto.VktArvosana.EiSuoritusta
                    ArvosanaFormEntry(
                        id = it.first.first,
                        arvosana = if (eiSuoritusta) null else arvosana,
                        arviointipaiva = if (eiSuoritusta) null else it.second,
                        merkittyPoistettavaksi = eiSuoritusta,
                    )
                }
        }

        data class ArvosanaFormEntry(
            val id: Int,
            val arvosana: Koodisto.VktArvosana?,
            val arviointipaiva: LocalDate?,
            val merkittyPoistettavaksi: Boolean = false,
        )
    }
}

fun FlowContent.vktErinomainenOsakoeTable(
    osat: List<VktOsakoe>,
    t: Translations,
) {
    displayTable(
        osat.sortedWith(compareBy(VktOsakoe::tutkintopaiva, VktOsakoe::tyyppi).reversed()),
        listOf(
            DisplayTableColumn(UiText.Vkt.osakoe.toString(), width = "20%", testId = "osakoe") {
                +t.get(it.tyyppi)
            },
            DisplayTableColumn(
                UiText.Vkt.Sarake.tutkintopaiva
                    .toString(),
                width = "20%",
                testId = "tutkintopaiva",
            ) {
                finnishDate(it.tutkintopaiva)
            },
            DisplayTableColumn(UiText.Vkt.arvosana.toString(), width = "20%") {
                hiddenValue("id", it.internalId?.toString().orEmpty())
                itemSelect(
                    inputName = "arvosana",
                    includeBlank = true,
                    items =
                        listOf(
                            Navigation.MenuItem(UiText.Vkt.erinomainen, Koodisto.VktArvosana.Erinomainen.name),
                            Navigation.MenuItem(UiText.Vkt.hylatty, Koodisto.VktArvosana.Hylätty.name),
                            Navigation.MenuItem(
                                "${UiText.Vkt.eiSuoritusta} (poistetaan${
                                    it.merkittyPoistettavaksi?.let { pvm ->
                                        " ${pvm.finnishDateTime()}"
                                    } ?: ""
                                })",
                                Koodisto.VktArvosana.EiSuoritusta.name,
                            ),
                        ).setCurrentItem(
                            if (it.merkittyPoistettavaksi != null) {
                                Koodisto.VktArvosana.EiSuoritusta.name
                            } else {
                                it.arviointi
                                    ?.arvosana
                                    ?.name
                            },
                        ),
                    testId = "arvosana",
                )
            },
            DisplayTableColumn(UiText.Vkt.arviointipaiva.toString(), width = "20%") {
                dateInput("arviointipaiva", it.arviointi?.paivamaara, testId = "arviointipaiva")
            },
            DisplayTableColumn(
                UiText.Vkt.Sarake.suorituspaikkakunta
                    .toString(),
                width = "20%",
            ) {
                +t.getByKoodiviite("kunta", it.suorituspaikkakunta)
            },
        ),
        testId = "osakokeet",
        rowTestId = { "${it.tyyppi.koodiarvo}-${it.tutkintopaiva}" },
    )
}
