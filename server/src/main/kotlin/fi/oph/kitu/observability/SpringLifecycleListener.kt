package fi.oph.kitu.observability

import org.slf4j.LoggerFactory
import org.springframework.context.ApplicationEvent
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.event.ApplicationContextEvent
import org.springframework.context.event.GenericApplicationListener
import java.util.function.Consumer
import kotlin.jvm.javaClass

@Configuration
class SpringLifecycleListener {
    private val logger = LoggerFactory.getLogger(javaClass)

    @Bean
    fun applicationListener() =
        forEventType<ApplicationContextEvent> { event ->
            logger
                .atInfo()
                .add(
                    "event" to event.javaClass.simpleName,
                    "timestamp" to event.timestamp,
                ).log("spring lifecycle event")
        }
}

inline fun <reified E : ApplicationEvent> forEventType(consumer: Consumer<E>): GenericApplicationListener =
    GenericApplicationListener.forEventType(E::class.java, consumer)
