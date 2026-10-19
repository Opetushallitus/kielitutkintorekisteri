package fi.oph.kitu.dev

import fi.oph.kitu.config.isProduction
import fi.oph.kitu.config.isQA
import org.slf4j.LoggerFactory
import org.springframework.boot.SpringApplication
import org.springframework.context.ApplicationContext
import org.springframework.core.env.Environment
import kotlin.system.exitProcess

fun Environment.exitIfProdLike(
    applicationContext: ApplicationContext,
    owner: Any,
) {
    if (isProduction() || isQA()) {
        LoggerFactory
            .getLogger(owner.javaClass)
            .error("Fatal error: ${owner.javaClass.simpleName} loaded in a prod-like environment")
        exitProcess(SpringApplication.exit(applicationContext))
    }
}
