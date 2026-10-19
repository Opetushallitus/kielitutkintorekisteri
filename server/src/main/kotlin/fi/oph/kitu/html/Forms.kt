package fi.oph.kitu.html

import fi.oph.kitu.i18n.LocalizedString
import fi.oph.kitu.i18n.unaryPlus
import kotlinx.html.ButtonType
import kotlinx.html.FORM
import kotlinx.html.FlowContent
import kotlinx.html.FormMethod
import kotlinx.html.InputType
import kotlinx.html.button
import kotlinx.html.fieldSet
import kotlinx.html.form
import kotlinx.html.id
import kotlinx.html.input
import org.springframework.security.web.csrf.CsrfToken
import java.time.LocalDate

fun FlowContent.formPost(
    action: String,
    csrfToken: CsrfToken? = KituRequest.currentCsrfToken(),
    formClasses: String? = null,
    testId: String? = null,
    content: FORM.() -> Unit,
) {
    form(action = action, method = FormMethod.post, classes = formClasses) {
        testId?.let { data("testid", it) }
        debugTrace()
        csrfToken?.let { hiddenValue(csrfToken.parameterName, csrfToken.token) }
        content()
    }
}

fun FlowContent.dateInput(
    name: String,
    date: LocalDate?,
    testId: String? = null,
    invalid: Boolean = false,
) {
    input(type = InputType.date, name = name) {
        testId(testId)
        ariaInvalid(invalid)
        value = date?.toString().orEmpty()
    }
}

fun FlowContent.submitButton(text: String = "Tallenna") {
    val buttonId = "submit-${(0..9999999).random()}"

    input(type = InputType.submit) {
        attributes["id"] = buttonId
        value = text
        disabled = true
    }

    javascript(
        """
        const button = document.getElementById("$buttonId")
        button.form
            .querySelectorAll('input, select')
            .forEach(e => e.addEventListener(
                'change', 
                () => { button.disabled = false }
            ))
        """.trimIndent(),
    )
}

fun FlowContent.hiddenValue(
    name: String,
    value: String,
) {
    input(type = InputType.hidden) {
        this.name = name
        this.value = value
    }
}

fun FlowContent.hiddenValues(values: Map<String, String?>) {
    values.forEach { (key, value) ->
        value?.let { hiddenValue(key, value) }
    }
}

fun FlowContent.listSearchForm(
    value: String?,
    placeholder: LocalizedString,
    buttonText: LocalizedString,
    params: Map<String, String?>,
    action: String = "",
    inputTestId: String = "search",
) {
    form(action = action, method = FormMethod.get, classes = "grid center-vertically") {
        hiddenValues(params - "search" - "page")
        fieldSet {
            attributes["role"] = "search"
            input {
                testId(inputTestId)
                id = "search"
                type = InputType.search
                name = "search"
                this.value = value.orEmpty()
                this.placeholder = placeholder.toString()
            }
            button {
                testId("search-button")
                type = ButtonType.submit
                +buttonText
            }
        }
    }
}
