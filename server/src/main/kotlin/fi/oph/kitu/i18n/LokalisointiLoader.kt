package fi.oph.kitu.i18n

import fi.oph.kitu.config.ConditionalOnNonEmptyProperty
import org.slf4j.LoggerFactory
import org.springframework.boot.ApplicationArguments
import org.springframework.boot.ApplicationRunner
import org.springframework.stereotype.Component

@Component
@ConditionalOnNonEmptyProperty("kitu.lokalisointi.namespace")
class LokalisointiLoader(
    private val lokalisointiClient: LokalisointiClient,
) : ApplicationRunner {
    private val logger = LoggerFactory.getLogger(javaClass)

    override fun run(args: ApplicationArguments) {
        refresh()
    }

    fun refresh(): Result<Int> =
        runCatching { lokalisointiClient.fetchAll() }
            .onSuccess { messages ->
                TolgeeMessages.set(messages)
                logger.info("Ladattiin {} käännösavainta lokalisointipalvelusta", messages.size)
            }.onFailure { e ->
                logger.warn("Käännösten lataus lokalisointipalvelusta epäonnistui, käytetään oletuskäännöksiä", e)
            }.map { it.size }
}
