package fi.oph.kitu.webmvc

import fi.oph.kitu.config.isLocal
import fi.oph.kitu.dev.MockResourceNotFoundError
import fi.oph.kitu.html.ErrorPage
import fi.oph.kitu.kotoutumiskoulutus.KielitestiSuoritusNotFoundError
import fi.oph.kitu.kotoutumiskoulutus.koealusta.tehtavapankki.TehtavapankkiNotFoundError
import fi.oph.kitu.oppijanumero.OppijanumeroException
import fi.oph.kitu.tiedontuontischema.TiedonsiirtoFailure
import fi.oph.kitu.util.validation.Validation
import fi.oph.kitu.vkt.VktSuoritusNotFoundError
import fi.oph.kitu.yki.YkiSuoritusNotFoundError
import fi.oph.kitu.yki.arvioijat.YkiArvioijaNotFoundError
import io.opentelemetry.api.trace.Span
import io.opentelemetry.api.trace.StatusCode
import org.springframework.beans.factory.annotation.Value
import org.springframework.core.env.Environment
import org.springframework.http.ResponseEntity
import org.springframework.http.converter.HttpMessageNotReadableException
import org.springframework.security.access.AccessDeniedException
import org.springframework.web.bind.annotation.ControllerAdvice
import org.springframework.web.bind.annotation.ExceptionHandler
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException

@ControllerAdvice
class GlobalControllerExceptionHandler(
    val environment: Environment,
) {
    @Value($$"${trace-ui:}")
    private lateinit var traceUiUrl: String

    @ExceptionHandler
    fun handleValidationException(e: Validation.ValidationException): ResponseEntity<TiedonsiirtoFailure> =
        TiedonsiirtoFailure
            .badRequest(e.errors.map { it.toString() })
            .toResponseEntity()
            .also {
                val span = Span.current()
                span.recordException(e)
                span.setStatus(StatusCode.ERROR)
            }

    @ExceptionHandler
    fun handleHttpMessageNotReadableException(e: HttpMessageNotReadableException): ResponseEntity<TiedonsiirtoFailure> =
        TiedonsiirtoFailure
            .badRequest(e.message.toString())
            .toResponseEntity()
            .also {
                val span = Span.current()
                span.recordException(e)
                span.setStatus(StatusCode.ERROR)
            }

    @ExceptionHandler(produces = ["text/html"])
    fun handleServerException(error: Throwable): ResponseEntity<String> {
        val traceId = Span.current().spanContext?.traceId
        val traceUrl =
            if (traceId !== null && traceUiUrl.isNotEmpty()) {
                traceUiUrl.replace("{traceId}", traceId)
            } else {
                null
            }

        return when (error) {
            is VktSuoritusNotFoundError,
            is YkiArvioijaNotFoundError,
            is YkiSuoritusNotFoundError,
            is KielitestiSuoritusNotFoundError,
            is TehtavapankkiNotFoundError,
            is MockResourceNotFoundError,
            is OppijanumeroException.OppijaNotFoundException,
            is org.springframework.web.servlet.resource.NoResourceFoundException,
            -> {
                ErrorPage.notFound(traceId, traceUrl)
            }

            is MethodArgumentTypeMismatchException,
            -> {
                ErrorPage.badRequest(traceId, traceUrl)
            }

            is AccessDeniedException,
            -> {
                ErrorPage.accessDenied(traceId, traceUrl)
            }

            else -> {
                ErrorPage.error(error, traceId, traceUrl, environment.isLocal())
            }
        }
    }
}
