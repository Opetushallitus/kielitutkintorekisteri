package fi.oph.kitu.i18n

import java.util.concurrent.ConcurrentHashMap

/**
 * Koodissa käytössä olevat käännösavaimet. Lähdetekstit eivät ole täällä vaan
 * luokkapolun `lokalisointi/fi.json`issa; rekisterin tehtävä on kertoa
 * Tolgee-synkronoinnille mitkä avaimet ovat oikeasti käytössä, jottei käytössä
 * olevaa avainta poisteta.
 */
object UiTextRegistry {
    private val registry = ConcurrentHashMap.newKeySet<String>()

    fun record(key: String) {
        registry.add(key)
    }

    fun all(): Set<String> = registry.toSet()
}
