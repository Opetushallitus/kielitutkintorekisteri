package fi.oph.kitu.html

import fi.oph.kitu.i18n.UiText
import kotlinx.html.ButtonType
import kotlinx.html.FlowContent
import kotlinx.html.FlowOrInteractiveOrPhrasingContent
import kotlinx.html.FlowOrPhrasingContent
import kotlinx.html.SPAN
import kotlinx.html.button
import kotlinx.html.span
import kotlinx.html.unsafe

private const val SVG_ATTRIBUTES =
    """viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" """ +
        """stroke-linecap="round" stroke-linejoin="round" aria-hidden="true" focusable="false""""

private const val COPY_ICON =
    """<svg class="copy-icon" $SVG_ATTRIBUTES>""" +
        """<rect x="9" y="9" width="12" height="12" rx="2"/>""" +
        """<path d="M15 9V5a2 2 0 0 0-2-2H5a2 2 0 0 0-2 2v8a2 2 0 0 0 2 2h4"/>""" +
        """</svg>"""

private const val COPIED_ICON =
    """<svg class="copied-icon" $SVG_ATTRIBUTES><path d="M5 12l5 5L20 7"/></svg>"""

fun FlowOrInteractiveOrPhrasingContent.copyButton(value: String) {
    button(type = ButtonType.button, classes = "copy-button") {
        val label = UiText.Toiminto.kopioi.toString()
        attributes["aria-label"] = label
        data("tooltip", label)
        data("copy", value)
        data("copied", UiText.Toiminto.kopioitu.toString())
        unsafe { raw(COPY_ICON + COPIED_ICON) }
    }
}

fun FlowOrPhrasingContent.copyable(
    value: String,
    content: SPAN.() -> Unit = { +value },
) {
    span(classes = "copyable") {
        content()
        copyButton(value)
    }
}

fun FlowContent.copyableOrDash(value: String?) {
    if (value == null) +value.orDash() else copyable(value)
}

fun FlowContent.copyButtonScript() {
    javascript(
        """
        document.addEventListener("click", (event) => {
            const button = event.target.closest("button[data-copy]")
            if (!button) return
            navigator.clipboard.writeText(button.dataset.copy).then(() => {
                button.classList.add("copied")
                button.dataset.tooltip = button.dataset.copied
                setTimeout(() => {
                    button.classList.remove("copied")
                    button.dataset.tooltip = button.getAttribute("aria-label")
                }, 1500)
            })
        })
        """.trimIndent(),
    )
}
