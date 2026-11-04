package fi.oph.kitu.yki.suoritukset
import fi.oph.kitu.html.Page
import fi.oph.kitu.html.ViewMessageData
import fi.oph.kitu.html.card
import fi.oph.kitu.html.formPost
import fi.oph.kitu.html.hiddenValues
import fi.oph.kitu.html.horizontalGroup
import fi.oph.kitu.html.input
import fi.oph.kitu.html.listSearchForm
import fi.oph.kitu.html.listViewHeader
import fi.oph.kitu.html.table.CheckboxKey
import fi.oph.kitu.html.table.dateFilter
import fi.oph.kitu.html.table.displayTable
import fi.oph.kitu.html.table.enumFilter
import fi.oph.kitu.html.table.tableFilterDialog
import fi.oph.kitu.html.table.trueFalseOrAllFilter
import fi.oph.kitu.html.testId
import fi.oph.kitu.html.viewMessage
import fi.oph.kitu.i18n.UiText
import fi.oph.kitu.i18n.unaryPlus
import fi.oph.kitu.webmvc.Links
import fi.oph.kitu.yki.Tutkintokieli
import fi.oph.kitu.yki.Tutkintotaso
import kotlinx.html.FlowContent
import kotlinx.html.InputType
import kotlinx.html.a
import kotlinx.html.fieldSet
import kotlinx.html.h1
import kotlinx.html.h2
import kotlinx.html.label
import kotlinx.html.p
import java.time.LocalDate
import java.time.format.DateTimeFormatter.ISO_LOCAL_DATE

object YkiTarkistusarvioinnitPage {
    fun render(
        suoritukset: List<YkiSuoritusEntity>,
        params: YkiTarkistusarvioinnitParams,
        message: ViewMessageData?,
        oletuspaiva: LocalDate,
    ): String =
        Page.renderHtml(wideContent = true, ohje = UiText.Ohje.Yki.tarkistusarvioinnit) {
            h1 { +UiText.Yki.tarkistusarvioinnit }

            viewMessage(message)

            p {
                a(href = Links.Yki.hyvaksytytTarkistusArvioinnit()) {
                    testId("hyvaksytytLink")
                    +UiText.Yki.naytaHyvaksytyt
                }
            }

            ykiTarkistusarviointiTable(
                title = UiText.Yki.odottavatHyvaksyntaa.toString(),
                submitButtonText = UiText.Yki.merkitseHyvaksynta.toString(),
                suoritukset = suoritukset,
                params = params,
                oletuspaiva = oletuspaiva,
                testId = "odottaaHyvaksyntaa",
                naytaHyvaksymispaiva = false,
            )
        }

    fun renderHyvaksytyt(
        suoritukset: List<YkiSuoritusEntity>,
        params: YkiTarkistusarvioinnitParams,
        message: ViewMessageData?,
        oletuspaiva: LocalDate,
    ): String =
        Page.renderHtml(wideContent = true, ohje = UiText.Ohje.Yki.tarkistusarvioinnit) {
            h1 { +UiText.Yki.tarkistusarvioinnit }

            viewMessage(message)

            p {
                a(href = Links.Yki.tarkistusArvioinnit()) {
                    testId("takaisinLink")
                    +UiText.Yki.takaisinOdottaviin
                }
            }

            ykiTarkistusarviointiTable(
                title = UiText.Yki.hyvaksytytTarkistusarvioinnit.toString(),
                submitButtonText = UiText.Yki.korjaaHyvaksymispaiva.toString(),
                suoritukset = suoritukset,
                params = params,
                oletuspaiva = oletuspaiva,
                testId = "hyvaksytty",
                naytaHyvaksymispaiva = true,
            )
        }

    private fun FlowContent.ykiTarkistusarviointiTable(
        title: String,
        submitButtonText: String,
        suoritukset: List<YkiSuoritusEntity>,
        params: YkiTarkistusarvioinnitParams,
        oletuspaiva: LocalDate,
        testId: String,
        naytaHyvaksymispaiva: Boolean,
    ) {
        h2 { +title }

        listSearchForm(
            value = params.search,
            placeholder = UiText.Yki.hakusanaTarkistusarviointi,
            buttonText = UiText.Yki.suodata,
            params = params.toMap(),
        )

        listViewHeader(
            countLabel = UiText.Yki.tarkistusarviointejaYhteensa,
            numberOfItems = suoritukset.size,
            csvHref = null,
            filterDescriptions = params.filterDescriptions(),
        ) { tarkistusarviointiFilterButton(params, naytaHyvaksymispaiva) }

        if (suoritukset.isNotEmpty()) {
            formPost(action = "") {
                label {
                    attributes["for"] = "hyvaksyttyPvm"
                    +UiText.Yki.tutkintotoimikunnanKokous
                }
                horizontalGroup {
                    input(
                        id = "hyvaksyttyPvm",
                        type = InputType.date,
                        name = "hyvaksyttyPvm",
                        value = oletuspaiva.format(ISO_LOCAL_DATE),
                    ) {
                        testId(testId + "Date")
                    }
                    input(type = InputType.submit, value = submitButtonText) {
                        testId(testId + "Submit")
                    }
                }

                card(overflowAuto = true, compact = true) {
                    displayTable(
                        rows = suoritukset,
                        columns =
                            YkiTarkistusarviointiColumn.entries
                                .filter { naytaHyvaksymispaiva || it != YkiTarkistusarviointiColumn.Hyvaksymispaiva }
                                .map { it.toDisplayColumn() },
                        sortedBy = params.sortColumn,
                        sortDirection = params.sortDirection,
                        urlParams = params.toMap(),
                        selectableRowName = {
                            CheckboxKey(name = "suoritukset", value = it.solkiId.toString())
                        },
                        testId = testId + "Table",
                    )
                }
            }
        }
    }

    private fun FlowContent.tarkistusarviointiFilterButton(
        params: YkiTarkistusarvioinnitParams,
        naytaHyvaksymispaiva: Boolean,
    ) {
        tableFilterDialog("") {
            hiddenValues(params.toMap().filterKeys { it in setOf("search", "sortColumn", "sortDirection") })
            fieldSet(classes = "grid") {
                dateFilter("tutkintoalku", UiText.Yki.tutkintopaivaAlkaen.toString(), params.tutkintoalku)
                dateFilter("tutkintoloppu", UiText.Yki.tutkintopaivaPaattyen.toString(), params.tutkintoloppu)
            }
            fieldSet(classes = "grid") {
                dateFilter("saapumisalku", UiText.Yki.saapunutAlkaen.toString(), params.saapumisalku)
                dateFilter("saapumisloppu", UiText.Yki.saapunutPaattyen.toString(), params.saapumisloppu)
            }
            fieldSet(classes = "grid") {
                dateFilter("kasittelyalku", UiText.Yki.kasiteltyAlkaen.toString(), params.kasittelyalku)
                dateFilter("kasittelyloppu", UiText.Yki.kasiteltyPaattyen.toString(), params.kasittelyloppu)
            }
            if (naytaHyvaksymispaiva) {
                fieldSet(classes = "grid") {
                    dateFilter("hyvaksyttyalku", UiText.Yki.hyvaksyttyAlkaen.toString(), params.hyvaksyttyalku)
                    dateFilter("hyvaksyttyloppu", UiText.Yki.hyvaksyttyPaattyen.toString(), params.hyvaksyttyloppu)
                }
            }
            fieldSet {
                enumFilter<Tutkintokieli>(
                    "tutkintokieli",
                    UiText.Yki.Sarake.tutkintokieli
                        .toString(),
                    params.tutkintokieli,
                )
            }
            fieldSet {
                enumFilter<Tutkintotaso>(
                    "tutkintotaso",
                    UiText.Yki.Sarake.tutkintotaso
                        .toString(),
                    params.tutkintotaso,
                )
            }
            fieldSet {
                trueFalseOrAllFilter(
                    "arvosanaMuuttui",
                    UiText.Yki.arvosanaMuuttui.toString(),
                    params.arvosanaMuuttui,
                )
            }
        }
    }
}
