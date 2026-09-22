package fi.oph.kitu.i18n

import fi.oph.kitu.html.ViewMessage
import fi.oph.kitu.webmvc.Links
import org.springframework.beans.factory.ObjectProvider
import org.springframework.stereotype.Controller
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.servlet.view.RedirectView

@Controller
@RequestMapping("/kaannokset")
class LokalisointiViewController(
    private val lokalisointiLoader: ObjectProvider<LokalisointiLoader>,
) {
    @PostMapping("/tyhjenna-valimuisti")
    fun tyhjennaValimuisti(viewMessage: ViewMessage? = null): RedirectView {
        TolgeeMessages.clear()

        val loader = lokalisointiLoader.ifAvailable
        if (loader == null) {
            viewMessage?.showInfo(
                "Käännösvälimuisti tyhjennettiin. Lokalisointipalvelu ei ole käytössä tässä ympäristössä, " +
                    "joten käyttöliittymä näytetään suomeksi.",
            )
        } else {
            loader.refresh().fold(
                onSuccess = { avaimia ->
                    viewMessage?.showSuccess(
                        "Käännösvälimuisti tyhjennettiin ja lokalisointipalvelusta ladattiin " +
                            "$avaimia käännösavainta.",
                    )
                },
                onFailure = {
                    viewMessage?.showError(
                        "Käännösvälimuisti tyhjennettiin, mutta käännösten lataus lokalisointipalvelusta " +
                            "epäonnistui. Käyttöliittymä näytetään suomeksi kunnes lataus onnistuu.",
                    )
                },
            )
        }

        return RedirectView(Links.home())
    }
}
