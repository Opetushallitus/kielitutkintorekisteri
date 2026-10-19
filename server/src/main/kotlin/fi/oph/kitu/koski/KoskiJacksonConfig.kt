package fi.oph.kitu.koski

import fi.oph.kitu.koodisto.KoskiKoodiviite
import fi.oph.kitu.util.isoJavaTimeModule
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Primary
import tools.jackson.databind.json.JsonMapper
import tools.jackson.module.kotlin.jacksonMapperBuilder

@Configuration
class KoskiJacksonConfig {
    // @Primary: when jackson-dataformat-xml is on the classpath, Spring Boot
    // auto-configures an `xmlMapper` bean that competes with this one for any
    // generic `ObjectMapper` injection point (e.g. AuditLogger).
    @Bean("koskiObjectMapper")
    @Primary
    fun koskiObjectMapper(): JsonMapper =
        jacksonMapperBuilder()
            .addModule(KoskiKoodiviite.Companion.KoskiKoodiviiteModule())
            .addModule(isoJavaTimeModule())
            .build()
}
