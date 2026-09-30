package fi.oph.kitu.util.cache

import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import kotlin.time.Duration
import kotlin.time.toJavaDuration

class InMemoryCache<I, O>(
    val ttl: Duration,
    val fn: (I) -> O?,
) {
    private val items = ConcurrentHashMap<I, CacheItem<O>>()

    fun get(key: I): O? {
        // Ei-positiivinen ttl tarkoittaa "ei valimuistia". Vanhenemislogiikka tuottaisi
        // kaytannossa saman tuloksen, mutta vasta kun kello on ehtinyt edeta expiresAtin
        // ohi — tama tekee sopimuksesta kellon tarkkuudesta riippumattoman eika jata
        // turhia riveja mappiin.
        if (!ttl.isPositive()) return fn(key)

        items[key]?.takeIf { !it.isExpired() }?.let { return it.value }
        return items
            .compute(key) { _, current ->
                current?.takeIf { !it.isExpired() }
                    ?: fn(key)?.let { value ->
                        CacheItem(value, Instant.now().plus(ttl.toJavaDuration()))
                    }
            }?.value
    }

    data class CacheItem<T>(
        val value: T,
        val expiresAt: Instant,
    ) {
        fun isExpired(): Boolean = expiresAt.isBefore(Instant.now())
    }
}
