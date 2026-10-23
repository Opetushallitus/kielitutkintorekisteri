package fi.oph.kitu.kotoutumiskoulutus.suoritukset.error
import fi.oph.kitu.html.Page
import fi.oph.kitu.html.table.ColumnTag
import fi.oph.kitu.html.table.RenderableDisplayTableEnum
import fi.oph.kitu.html.table.displayTableBody
import fi.oph.kitu.html.table.displayTableHeader
import fi.oph.kitu.i18n.UiText
import fi.oph.kitu.i18n.unaryPlus
import fi.oph.kitu.jdbc.SortDirection
import fi.oph.kitu.webmvc.Links
import kotlinx.html.a
import kotlinx.html.article
import kotlinx.html.h1
import kotlinx.html.h2
import kotlinx.html.header
import kotlinx.html.li
import kotlinx.html.nav
import kotlinx.html.table
import kotlinx.html.ul

object KielitestiSuoritusErrorPage {
    fun render(
        sortColumn: KielitestiSuoritusErrorColumn,
        sortDirection: SortDirection,
        errors: Iterable<KielitestiSuoritusError>,
    ): String =
        Page.renderHtml(
            wideContent = true,
            ohje = UiText.Ohje.Koto.virheet,
        ) {
            h1 { +UiText.Nav.kotoutumiskoulutuksenPaattotesti }
            h2 { +UiText.Koto.suoritustenTuonninVirheet }
            article(classes = "overflow-auto") {
                header {
                    nav {
                        ul {
                            li {
                                +UiText.Koto.virheitaYhteensa
                                +": ${errors.count()}"
                            }
                            li {
                                a(href = Links.Kielitesti.virheetCsv()) {
                                    attributes["download"] = ""
                                    +UiText.Koto.lataaCsv
                                }
                            }
                        }
                    }
                }
                table(classes = "compact striped") {
                    val columns =
                        RenderableDisplayTableEnum
                            .getByTags<KielitestiSuoritusErrorColumn, KielitestiSuoritusError>(
                                setOf(ColumnTag.LIST_VIEW),
                            ).map { it.withValue(it.getValue, it.renderHtml) }

                    displayTableHeader(
                        columns = columns,
                        sortedBy = sortColumn,
                        sortDirection = sortDirection,
                        preserveSortDirection = false,
                        selectableRows = false,
                        tableId = "kielitesti-suoritukset-virheet-table",
                    )

                    displayTableBody(
                        rows = errors.toList(),
                        columns = columns,
                        tbodyClasses = "virheet",
                        rowTestId = { "virhe-summary-row" },
                    )
                }
            }
        }
}
