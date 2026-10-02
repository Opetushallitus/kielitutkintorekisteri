@file:Suppress("ktlint:standard:no-wildcard-imports")

package fi.oph.kitu.html

import fi.oph.kitu.config.ApplicationProperties
import fi.oph.kitu.config.isDeployedToOpintopolku
import fi.oph.kitu.html.Navigation.flatten
import fi.oph.kitu.html.Navigation.mainNavigation
import fi.oph.kitu.i18n.CurrentLanguage
import fi.oph.kitu.i18n.LocalizedString
import fi.oph.kitu.i18n.UiText
import fi.oph.kitu.i18n.unaryPlus
import fi.oph.kitu.webmvc.Links
import kotlinx.html.*
import kotlinx.html.stream.createHTML

object Page {
    fun HEAD.loadRaamit() {
        if (ApplicationProperties.environment.isDeployedToOpintopolku()) {
            script(
                type = "text/javascript",
                src = "https://${ApplicationProperties.kitu.opintopolkuHostname}/virkailija-raamit/apply-raamit.js",
                crossorigin = null,
            ) {}
        }
    }

    fun renderHtml(
        wideContent: Boolean = false,
        ohje: LocalizedString? = null,
        renderBody: SECTION.() -> Unit,
    ): String =
        createHTML().html {
            lang = CurrentLanguage.get().name.lowercase()
            data("theme", "light")

            head {
                title { +UiText.appTitle }
                meta(name = "color-scheme", content = "light")
                link(href = "${ApplicationProperties.kitu.appUrl}/pico.min.css", rel = "stylesheet")
                link(href = "${ApplicationProperties.kitu.appUrl}/style.css", rel = "stylesheet")

                loadRaamit()
            }
            body {
                testId("page-body")
                debugTrace()

                main {
                    nav(classes = "container-fluid main") {
                        testId("page-main-nav-header")
                        ul(classes = "nav-title") {
                            li {
                                ul(classes = "breadcrumbs") {
                                    testId("breadcrumbs")
                                    li {
                                        a(href = Links.home()) { strong { +UiText.appTitle } }
                                    }
                                }
                            }
                        }
                        ul(classes = "nav-menu") {
                            testId("main-nav")
                            dropdown(
                                "☰",
                                mainNavigation.flatten(),
                            )
                        }
                        ul(classes = "nav-help") {
                            li {
                                a(href = ohjeUrl(ohje), target = ATarget.blank, classes = "outline secondary") {
                                    testId("ohje-linkki")
                                    role = "button"
                                    rel = "noopener noreferrer"
                                    +UiText.Ohje.painike
                                }
                            }
                        }
                    }

                    section(classes = if (wideContent) "container-fluid" else "container") {
                        testId("page-content")
                        renderBody()
                    }
                }
            }
        }
}
