package fi.oph.kitu.html

import fi.oph.kitu.i18n.LocalizedString
import fi.oph.kitu.i18n.unaryPlus
import kotlinx.html.FlowContent
import kotlinx.html.UL
import kotlinx.html.a
import kotlinx.html.article
import kotlinx.html.header
import kotlinx.html.li
import kotlinx.html.nav
import kotlinx.html.span
import kotlinx.html.ul

fun FlowContent.filterDescriptionList(descriptions: List<String>) {
    if (descriptions.isNotEmpty()) {
        ul {
            descriptions.forEach { li { +it } }
        }
    }
}

fun FlowContent.csvDownloadButton(href: String) {
    a(href = href) {
        attributes["download"] = ""
        +"Lataa tiedot CSV:nä"
    }
}

/**
 * Listanäkymien yhteinen toimintorivi: rivimäärä, CSV-lataus ja suodatinnappi.
 *
 * Rivimäärä kääritään aina testId("numberOfRows")-elementtiin, johon e2e-testit nojaavat.
 */
fun FlowContent.listViewActions(
    countLabel: LocalizedString,
    numberOfItems: Number,
    csvHref: String,
    extraActions: (UL.() -> Unit)? = null,
    filterButton: FlowContent.() -> Unit,
) {
    nav {
        ul {
            li {
                +countLabel
                +": "
                span {
                    testId("numberOfRows")
                    +numberOfItems.toString()
                }
            }
            li { csvDownloadButton(csvHref) }
            li { filterButton() }
            extraActions?.invoke(this)
        }
    }
}

/**
 * Sivuille, joilla toimintorivi on omassa artikkelissaan. Ne joilla sama artikkeli
 * kääriä myös taulukon kutsuvat listViewActionsia suoraan.
 */
fun FlowContent.listViewHeader(
    countLabel: LocalizedString,
    numberOfItems: Number,
    csvHref: String,
    filterDescriptions: List<String>,
    extraActions: (UL.() -> Unit)? = null,
    filterButton: FlowContent.() -> Unit,
) {
    article {
        header {
            listViewActions(countLabel, numberOfItems, csvHref, extraActions, filterButton)
        }
        filterDescriptionList(filterDescriptions)
    }
}
