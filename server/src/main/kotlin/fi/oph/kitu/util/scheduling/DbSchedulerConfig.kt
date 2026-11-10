package fi.oph.kitu.util.scheduling

import com.github.kagkarlsson.scheduler.boot.config.DbSchedulerCustomizer
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class DbSchedulerConfig {
    @Suppress("DEPRECATION", "removal")
    @Bean
    fun dbSchedulerCustomizer(): DbSchedulerCustomizer = object : DbSchedulerCustomizer {}
}
