package fi.oph.kitu.yki.arvioijat

import fi.oph.kitu.html.Page
import fi.oph.kitu.html.Pagination
import fi.oph.kitu.html.buttonLink
import fi.oph.kitu.html.card
import fi.oph.kitu.html.hiddenValue
import fi.oph.kitu.html.hiddenValues
import fi.oph.kitu.html.input
import fi.oph.kitu.html.listViewHeader
import fi.oph.kitu.html.pagination
import fi.oph.kitu.html.table.ColumnTag
import fi.oph.kitu.html.table.DisplayTableColumn
import fi.oph.kitu.html.table.dateFilter
import fi.oph.kitu.html.table.displayTable
import fi.oph.kitu.html.table.enumFilter
import fi.oph.kitu.html.table.httpParams
import fi.oph.kitu.html.table.tableFilterDialog
import fi.oph.kitu.html.table.toggleFilter
import fi.oph.kitu.html.testId
import fi.oph.kitu.i18n.UiText
import fi.oph.kitu.i18n.unaryPlus
import fi.oph.kitu.security.Authority
import fi.oph.kitu.security.CurrentUser
import fi.oph.kitu.webmvc.Links
import kotlinx.html.ButtonType
import kotlinx.html.FlowContent
import kotlinx.html.FormMethod
import kotlinx.html.InputType
import kotlinx.html.button
import kotlinx.html.fieldSet
import kotlinx.html.form
import kotlinx.html.h1
import kotlinx.html.h2
import kotlinx.html.li
import kotlinx.html.section

object YkiArvioijaPage {
    fun render(
        arvioijat: List<YkiArvioijaListRow>,
        params: YkiArvioijaParams,
        pagination: Pagination,
        muokkausKaytossa: Boolean,
    ): String =
        Page.renderHtml(
            wideContent = true,
            ohje = UiText.Ohje.Yki.arvioijat,
        ) {
            h1 { +UiText.Nav.yki }
            h2 { +UiText.Nav.arvioijat }

            arvioijaSearch(params)

            listViewHeader(
                countLabel = UiText.Yki.arvioijiaYhteensa,
                numberOfItems = pagination.numberOfItems,
                csvHref = Links.Yki.arvioijatCsv() + httpParams(params.toMap()),
                filterDescriptions = params.filterDescriptions(),
                extraActions = {
                    if (CurrentUser.hasAuthority(Authority.YKI_ARVIOIJAREKISTERI)) {
                        li {
                            buttonLink(
                                href = Links.Yki.uusiArvioija(),
                                enabled = muokkausKaytossa,
                                testId = "lisaaArvioija",
                                disabledTooltip = UiText.Yki.Arvioija.kirjoitusEiKaytossa,
                            ) {
                                +UiText.Yki.lisaaArvioija
                            }
                        }
                        li {
                            buttonLink(
                                href = Links.Yki.arvioijahakemukset(),
                                testId = "arvioijahakemukset",
                            ) {
                                +UiText.Yki.Arvioijahakemus.linkki
                            }
                        }
                    }
                },
            ) { arvioijaFilterButton(params) }

            arvioijaTable(arvioijat, params, pagination)
        }
}

fun FlowContent.arvioijaTable(
    arvioijat: List<YkiArvioijaListRow>,
    params: YkiArvioijaParams,
    pagination: Pagination,
) {
    card(overflowAuto = true, compact = true) {
        val columns =
            DisplayTableColumn.of<YkiArvioijaColumn, YkiArvioijaListRow>(
                setOf(ColumnTag.LIST_VIEW),
                params.excludeTags(),
            )

        displayTable(
            arvioijat,
            columns,
            sortedBy = params.sortColumn,
            sortDirection = params.sortDirection,
            testId = "arvioijat",
            rowTestId = { "${it.arvioijaOid}-${it.kieli}" },
            urlParams = params.toMap(),
        )
    }

    pagination(pagination)
}

fun FlowContent.arvioijaFilterButton(params: YkiArvioijaParams) {
    tableFilterDialog("") {
        params.search.takeIf { it.isNotEmpty() }?.let { hiddenValue("search", it) }
        fieldSet {
            enumFilter(
                "tila",
                UiText.Yki.Sarake.tila
                    .toString(),
                params.tila,
            )
        }
        fieldSet {
            enumFilter(
                "kieli",
                UiText.Yki.Sarake.kieli
                    .toString(),
                params.kieli,
            )
        }
        fieldSet {
            enumFilter(
                "taso",
                UiText.Yki.Sarake.taso
                    .toString(),
                params.taso,
            )
        }
        fieldSet {
            dateFilter(
                "kausiPaattyyEnnen",
                UiText.Yki.Sarake.kaudenPaattymispaiva
                    .toString(),
                params.kausiPaattyyEnnen,
            )
        }
        fieldSet {
            toggleFilter(
                "vainSolkiVirheet",
                UiText.Yki.solkiLahetystenVirheet.toString(),
                params.vainSolkiVirheet,
            )
            toggleFilter(
                "piilotaHenkilotiedot",
                UiText.Filter.piilotaHenkilotiedot.toString(),
                params.piilotaHenkilotiedot,
            )
        }
    }
}

/**
 * Haku on GET-lomake toisin kuin suoritusnakymassa: suodattimet, jarjestys ja sivutus
 * kulkevat jo URL-parametreina, joten hakusanan on kuljettava samaa reittia. Muuten
 * lajittelulinkin klikkaus pyyhkisi haun.
 */
fun FlowContent.arvioijaSearch(params: YkiArvioijaParams) {
    section(classes = "grid center-vertically") {
        form(action = Links.Yki.arvioijat(), method = FormMethod.get) {
            // Sailyta suodattimet ja jarjestys, mutta palaa ensimmaiselle sivulle.
            hiddenValues(params.toMap() - "search" - "page")
            fieldSet {
                attributes["role"] = "search"
                input(
                    id = "search",
                    type = InputType.text,
                    name = "search",
                    value = params.search,
                    placeholder = UiText.Yki.hakusanaArvioija.toString(),
                ) {
                    testId("arvioijaSearch")
                    button(type = ButtonType.submit) { +UiText.Yki.suodata }
                }
            }
        }
    }
}
