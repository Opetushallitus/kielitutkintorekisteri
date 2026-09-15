package fi.oph.kitu.html

import fi.oph.kitu.i18n.LocalizedString
import fi.oph.kitu.i18n.unaryPlus
import kotlinx.html.A
import kotlinx.html.ARTICLE
import kotlinx.html.FlowContent
import kotlinx.html.a
import kotlinx.html.article
import kotlinx.html.div
import kotlinx.html.fieldSet
import kotlinx.html.option
import kotlinx.html.section
import kotlinx.html.select
import kotlinx.html.span

// https://picocss.com/docs/card
fun FlowContent.card(
    overflowAuto: Boolean = false,
    compact: Boolean = false,
    content: ARTICLE.() -> Unit,
) {
    article(classes = classes(overflowAuto to "overflow-auto", compact to "compact")) {
        content()
    }
}

fun FlowContent.cardContent(block: FlowContent.() -> Unit) {
    section(classes = "cardContent") {
        block()
    }
}

// https://picocss.com/docs/forms/select
fun FlowContent.itemSelect(
    inputName: String,
    items: List<Navigation.MenuItem>,
    includeBlank: Boolean = false,
    testId: String? = null,
) {
    select {
        name = inputName
        testId(testId)
        if (includeBlank) {
            option {}
        }
        items.forEach {
            option {
                value = it.ref.orEmpty()
                selected = it.current
                disabled = it.ref == null
                +it.title
            }
        }
    }
}

// https://picocss.com/docs/group
fun FlowContent.horizontalGroup(f: FlowContent.() -> Unit) {
    fieldSet {
        attributes["role"] = "group"
        f()
    }
}

/**
 * Selite disabloidulle kontrollille. Pico asettaa `:where(button, …)[disabled]`ille
 * `pointer-events: none`, joten napin omalle `data-tooltip`ille ei koskaan tule hoveria — selite
 * ripustetaan siksi kaareen. `buttonLink` ei tarvitse tata, koska se renderoi
 * `<a aria-disabled>`in eika `[disabled]`ia.
 */
fun FlowContent.selitettyEsto(
    estonSyy: LocalizedString?,
    block: FlowContent.() -> Unit,
) {
    if (estonSyy == null) {
        block()
        return
    }
    span(classes = "esto-selite") {
        data("tooltip", estonSyy.toString())
        block()
    }
}

/**
 * Vierekkaiset napit ja nappilinkit; fieldset-pohjainen [horizontalGroup] on lomakekenttia varten.
 *
 * https://picocss.com/docs/group
 */
fun FlowContent.buttonGroup(f: FlowContent.() -> Unit) {
    div {
        attributes["role"] = "group"
        f()
    }
}

// https://picocss.com/docs/button#usage-with-links
fun FlowContent.buttonLink(
    href: String,
    enabled: Boolean = true,
    testId: String? = null,
    disabledTooltip: LocalizedString? = null,
    content: A.() -> Unit,
) {
    a(href = href.takeIf { enabled }) {
        attributes["role"] = "button"
        testId(testId)
        if (!enabled) {
            attributes["aria-disabled"] = "true"
            disabledTooltip?.let { data("tooltip", it.toString()) }
        }
        content()
    }
}
