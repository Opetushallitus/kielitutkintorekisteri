package fi.oph.kitu.observability

import fi.oph.kitu.observability.add
import io.opentelemetry.contrib.aws.resource.EcsResourceProvider
import io.opentelemetry.instrumentation.resources.ContainerResourceProvider
import io.opentelemetry.instrumentation.resources.HostIdResourceProvider
import io.opentelemetry.instrumentation.resources.HostResourceProvider
import io.opentelemetry.instrumentation.resources.ManifestResourceProvider
import io.opentelemetry.instrumentation.resources.OsResourceProvider
import io.opentelemetry.instrumentation.resources.ProcessResourceProvider
import io.opentelemetry.instrumentation.resources.ProcessRuntimeResourceProvider
import io.opentelemetry.sdk.autoconfigure.spi.ResourceProvider
import io.opentelemetry.sdk.autoconfigure.spi.internal.ConditionalResourceProvider
import io.opentelemetry.sdk.autoconfigure.spi.internal.DefaultConfigProperties
import io.opentelemetry.sdk.resources.Resource
import org.slf4j.LoggerFactory
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class OpenTelemetryResourceConfiguration {
    @Bean
    fun openTelemetryResource(): Resource {
        val providers: List<ResourceProvider> =
            listOf(
                EcsResourceProvider(),
                ProcessRuntimeResourceProvider(),
                HostResourceProvider(),
                HostIdResourceProvider(),
                OsResourceProvider(),
                ProcessResourceProvider(),
                ContainerResourceProvider(),
                ManifestResourceProvider(),
            )
        val emptyConfig = DefaultConfigProperties.createFromMap(emptyMap())
        return providers.fold(Resource.getDefault()) { acc, provider ->
            val shouldApply =
                provider !is ConditionalResourceProvider || provider.shouldApply(emptyConfig, acc)
            if (shouldApply) acc.merge(provider.createResource(emptyConfig)) else acc
        }
    }
}

@Configuration
class OpenTelemetryListener(
    resource: Resource,
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    init {
        logger
            .atInfo()
            .add("resource" to resource.attributes)
            .log("Initialized OpenTelemetry resource")
    }
}
