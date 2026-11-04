package fi.oph.kitu.html

import fi.oph.kitu.i18n.UiText
import fi.oph.kitu.i18n.unaryPlus
import kotlinx.html.BUTTON
import kotlinx.html.ButtonType
import kotlinx.html.FlowContent
import kotlinx.html.article
import kotlinx.html.button
import kotlinx.html.dialog
import kotlinx.html.div
import kotlinx.html.footer
import kotlinx.html.header
import kotlinx.html.unsafe

private const val INFO_ICON =
    """<svg $SVG_ATTRIBUTES><circle cx="12" cy="12" r="10"/><path d="M12 16v-4"/><path d="M12 8h.01"/></svg>"""

enum class ModalCommand(
    val command: String,
) {
    OPEN("show-modal"),
    CLOSE("close"),
}

fun FlowContent.modal(
    id: String,
    title: String,
    block: FlowContent.() -> Unit,
) {
    dialog {
        attributes["id"] = id
        testId(id)
        article {
            header { +title }
            block()
        }
    }
}

fun FlowContent.modalCommandButton(
    modalId: String,
    command: ModalCommand,
    classes: String? = null,
    fn: BUTTON.() -> Unit,
) {
    button(type = ButtonType.button, classes = classes) {
        attributes["commandfor"] = modalId
        attributes["command"] = command.command
        fn()
    }
}

fun FlowContent.infoModal(
    modalId: String,
    title: String,
    label: String,
    content: FlowContent.() -> Unit,
) {
    div(classes = "info-modal") {
        modalCommandButton(modalId, ModalCommand.OPEN, classes = "info-button") {
            attributes["aria-label"] = label
            data("tooltip", label)
            data("placement", "left")
            unsafe { raw(INFO_ICON) }
        }
        modal(modalId, title) {
            content()
            footer {
                modalCommandButton(modalId, ModalCommand.CLOSE, classes = "secondary") {
                    +UiText.Toiminto.sulje
                }
            }
        }
    }
}
