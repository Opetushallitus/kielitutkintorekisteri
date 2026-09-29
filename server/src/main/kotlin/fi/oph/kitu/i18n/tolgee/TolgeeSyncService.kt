package fi.oph.kitu.i18n.tolgee

import fi.oph.kitu.config.ConditionalOnNonEmptyProperty
import fi.oph.kitu.i18n.TolgeeMessages
import fi.oph.kitu.i18n.UiTextRegistry
import io.opentelemetry.instrumentation.annotations.WithSpan
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service

@Service
@ConditionalOnNonEmptyProperty("kitu.tolgee.apiKey")
class TolgeeSyncService(
    private val tolgeeClient: TolgeeClient,
    @param:Value($$"${kitu.lokalisointi.namespace:}")
    private val namespace: String,
    @param:Value($$"${kitu.tolgee.sync.maxDeleteRatio}")
    private val maxDeleteRatio: Double,
    @param:Value($$"${kitu.tolgee.sync.minDeleteAllowance}")
    private val minDeleteAllowance: Int,
    @param:Value($$"${kitu.tolgee.sync.dryRun}")
    private val dryRun: Boolean,
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    @WithSpan
    fun sync(): TolgeeSyncResult = sync(UiTextRegistry.all().associateWith { TolgeeMessages.sourceText(it) ?: it })

    fun sync(koodinAvaimet: Map<String, String>): TolgeeSyncResult {
        if (namespace.isBlank()) {
            logger.error("Tolgee-synkronointi ohitettiin: kitu.lokalisointi.namespace on tyhjä")
            return TolgeeSyncResult.Virhe("Nimiavaruus kitu.lokalisointi.namespace on tyhjä")
        }

        if (koodinAvaimet.isEmpty()) {
            logger.error("Tolgee-synkronointi ohitettiin: koodin avainrekisteri on tyhjä")
            return TolgeeSyncResult.RekisteriTyhja
        }

        val tolgeenAvaimet = tolgeeClient.fetchKeys()
        val lisattavat = koodinAvaimet.filterKeys { it !in tolgeenAvaimet }
        val poistettavat = tolgeenAvaimet.filterKeys { it !in koodinAvaimet }
        val raja = maxOf(minDeleteAllowance, (tolgeenAvaimet.size * maxDeleteRatio).toInt())

        logger.info(
            "Tolgee-synkronointi ({}): koodissa {} avainta, Tolgeessa {}, lisättäviä {}, poistettavia {} (raja {})",
            if (dryRun) "kuivaharjoitus" else "ajo",
            koodinAvaimet.size,
            tolgeenAvaimet.size,
            lisattavat.size,
            poistettavat.size,
            raja,
        )

        if (lisattavat.isNotEmpty()) {
            logger.info("Tolgeehen lisättävät avaimet: {}", lisattavat.keys.sorted())
            if (!dryRun) tolgeeClient.createKeys(lisattavat)
        }

        if (poistettavat.size > raja) {
            logger.error(
                "Tolgee-synkronointi ohitti poistot: poistettavia {} > raja {}. Avaimet: {}",
                poistettavat.size,
                raja,
                poistettavat.keys.sorted(),
            )
            return TolgeeSyncResult.PoistorajaYlittyi(poistettavat.size, raja)
        }

        if (poistettavat.isNotEmpty()) {
            logger.info("Tolgeesta poistettavat avaimet: {}", poistettavat.keys.sorted())
            if (!dryRun) tolgeeClient.deleteKeys(poistettavat.values.toList())
        }

        return TolgeeSyncResult.Ok(lisatty = lisattavat.size, poistettu = poistettavat.size)
    }
}
