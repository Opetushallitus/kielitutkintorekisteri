package fi.oph.kitu.html

import fi.oph.kitu.i18n.UiText
import fi.oph.kitu.i18n.unaryPlus
import kotlinx.html.FlowContent
import kotlinx.html.a
import kotlinx.html.h1
import kotlinx.html.p
import kotlinx.html.pre
import org.springframework.http.HttpStatus
import org.springframework.http.HttpStatusCode
import org.springframework.http.ResponseEntity

object ErrorPage {
    fun error(
        error: Throwable,
        traceId: String?,
        traceUrl: String?,
        isLocal: Boolean,
    ): ResponseEntity<String> =
        ResponseEntity(
            Page.renderHtml {
                h1 {
                    if (isLocal) {
                        +error.toString()
                    } else {
                        +UiText.Error.internalServerError
                    }
                }

                traceInfo(traceId, traceUrl)

                if (isLocal) {
                    pre { +error.stackTraceToString() }
                }
            },
            HttpStatus.INTERNAL_SERVER_ERROR,
        )

    fun notFound(
        traceId: String?,
        traceUrl: String?,
    ): ResponseEntity<String> =
        ResponseEntity(
            Page.renderHtml {
                h1 { +UiText.Error.sivuaEiLoydy }
                traceInfo(traceId, traceUrl)
            },
            HttpStatus.NOT_FOUND,
        )

    fun badRequest(
        traceId: String?,
        traceUrl: String?,
        status: HttpStatusCode = HttpStatus.BAD_REQUEST,
    ): ResponseEntity<String> =
        ResponseEntity(
            Page.renderHtml {
                h1 { +UiText.Error.virheellinenPyynto }
                p { +UiText.Error.virheellinenPyyntoOhje }
                traceInfo(traceId, traceUrl)
            },
            status,
        )

    fun accessDenied(
        traceId: String?,
        traceUrl: String?,
    ): ResponseEntity<String> =
        ResponseEntity(
            Page.renderHtml {
                h1 { +UiText.Error.eiKayttooikeuksia }
                traceInfo(traceId, traceUrl)
            },
            HttpStatus.FORBIDDEN,
        )

    fun FlowContent.traceInfo(
        traceId: String?,
        traceUrl: String?,
    ) {
        traceId?.let {
            p {
                +UiText.Error.jaljitystunniste
                +": "
                if (traceUrl != null) {
                    a(href = traceUrl, target = "_blank") { +it }
                } else {
                    +it
                }
            }
        }
    }
}
