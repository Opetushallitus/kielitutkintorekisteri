package fi.oph.kitu.html

import fi.oph.kitu.i18n.LocalizedString
import kotlinx.html.FlowContent
import kotlinx.html.stream.createHTML
import kotlinx.html.table
import kotlinx.html.tbody
import kotlinx.html.td
import kotlinx.html.th
import kotlinx.html.thead
import kotlinx.html.tr
import kotlinx.html.unsafe

fun FlowContent.infoTable(vararg rows: Pair<Any, FlowContent.() -> Unit>?) {
    table(classes = "info-table compact striped") {
        debugTrace()
        tbody {
            rows.filterNotNull().forEach { (name, render) ->
                tr {
                    th { +name.toString() }
                    td { render() }
                }
            }
        }
    }
}

fun FlowContent.comparisonTable(
    firstColumn: LocalizedString,
    secondColumn: LocalizedString,
    vararg rows: Comparison?,
) {
    table(classes = "info-table compact striped") {
        debugTrace()
        thead {
            tr {
                th {}
                th { +firstColumn.toString() }
                th { +secondColumn.toString() }
            }
        }
        tbody {
            rows.filterNotNull().forEach { cmp ->
                val firstTd = createHTML().td { cmp.renderFirst(this) }
                val secondTd = createHTML().td { cmp.renderSecond?.let { it() } }
                tr(classes = if (!cmp.ignoreDiff && firstTd != secondTd) "diff" else null) {
                    th { +cmp.name.toString() }
                    unsafe { raw(firstTd) }
                    unsafe { raw(secondTd) }
                }
            }
        }
    }
}

data class Comparison(
    val name: Any,
    val renderFirst: FlowContent.() -> Unit,
    val renderSecond: (FlowContent.() -> Unit)? = null,
    val ignoreDiff: Boolean = false,
) {
    companion object {
        fun of(
            name: Any,
            first: String?,
            second: String?,
            ignoreDiff: Boolean = false,
        ) = Comparison(
            name = name,
            renderFirst = { +first.orDash() },
            renderSecond = { +second.orDash() },
            ignoreDiff = ignoreDiff,
        )
    }
}

fun String?.orDash() = this ?: "–"
