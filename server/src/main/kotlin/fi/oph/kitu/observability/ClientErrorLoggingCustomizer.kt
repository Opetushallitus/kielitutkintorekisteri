package fi.oph.kitu.observability

import org.apache.catalina.connector.Request
import org.apache.catalina.connector.Response
import org.apache.catalina.valves.ErrorReportValve
import org.slf4j.LoggerFactory
import org.springframework.boot.tomcat.servlet.TomcatServletWebServerFactory
import org.springframework.boot.web.server.WebServerFactoryCustomizer
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

/**
 * Customizes Tomcat's ErrorReportValve so that 4xx client errors are logged
 * at INFO level instead of the default WARN/ERROR level.
 */
@Configuration
class ClientErrorLoggingCustomizer {
    @Bean
    fun errorReportValveCustomizer(): WebServerFactoryCustomizer<TomcatServletWebServerFactory> =
        WebServerFactoryCustomizer { factory ->
            factory.addContextCustomizers({ context ->
                val pipeline = context.parent.pipeline
                val existingValve =
                    pipeline.valves.filterIsInstance<ErrorReportValve>().firstOrNull()
                if (existingValve != null) {
                    pipeline.removeValve(existingValve)
                }
                pipeline.addValve(InfoLevelClientErrorReportValve())
            })
        }
}

/**
 * A custom ErrorReportValve that downgrades 4xx status code logging from
 * WARN/ERROR to INFO level. Server errors (5xx) are still logged at ERROR.
 */
class InfoLevelClientErrorReportValve : ErrorReportValve() {
    private val log = LoggerFactory.getLogger(javaClass)

    override fun report(
        request: Request,
        response: Response,
        throwable: Throwable?,
    ) {
        val statusCode = response.status
        if (statusCode in 400..499) {
            log.info(
                "HTTP {} {} responded with status {}",
                request.method,
                request.requestURI,
                statusCode,
            )
        }
        super.report(request, response, throwable)
    }
}
