package fi.oph.kitu.html

import kotlinx.html.BUTTON
import kotlinx.html.ButtonType
import kotlinx.html.FlowContent
import kotlinx.html.article
import kotlinx.html.button
import kotlinx.html.dialog
import kotlinx.html.header

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
