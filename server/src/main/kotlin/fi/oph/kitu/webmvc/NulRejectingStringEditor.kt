package fi.oph.kitu.webmvc

import org.springframework.web.bind.WebDataBinder
import org.springframework.web.bind.annotation.ControllerAdvice
import org.springframework.web.bind.annotation.InitBinder
import java.beans.PropertyEditorSupport

const val NUL_VIRHE = "Merkkijono ei saa sisältää NUL-merkkiä (\\u0000)"

@ControllerAdvice
class NulRejectingBindingAdvice {
    @InitBinder
    fun rejectNul(binder: WebDataBinder) {
        binder.registerCustomEditor(String::class.java, NulRejectingStringEditor())
    }
}

class NulRejectingStringEditor : PropertyEditorSupport() {
    override fun setAsText(text: String?) {
        require(text == null || '\u0000' !in text) { NUL_VIRHE }
        value = text
    }
}
