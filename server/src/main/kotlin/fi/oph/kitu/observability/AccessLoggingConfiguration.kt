package fi.oph.kitu.observability

import jakarta.servlet.FilterChain
import jakarta.servlet.http.HttpServletRequest
import jakarta.servlet.http.HttpServletResponse
import org.slf4j.LoggerFactory
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.web.filter.OncePerRequestFilter

@Configuration
class AccessLoggingConfiguration {
    @Bean
    fun logFilter(): OncePerRequestFilter =
        object : OncePerRequestFilter() {
            private val logFilterLogger = LoggerFactory.getLogger(javaClass)

            override fun doFilterInternal(
                request: HttpServletRequest,
                response: HttpServletResponse,
                filterChain: FilterChain,
            ) {
                try {
                    filterChain.doFilter(request, response)
                } finally {
                    if (request.requestURI != "/actuator/health") {
                        logFilterLogger
                            .atInfo()
                            .addServletResponse(response)
                            .addServletRequest(request)
                            .log("HTTP ${request.method} ${request.requestURL}")
                    }
                }
            }
        }
}
