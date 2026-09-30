package fi.oph.kitu.util.cache

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertEquals
import kotlin.test.assertNotSame
import kotlin.test.assertNull
import kotlin.test.assertSame
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

class InMemoryCacheTest {
    @Test
    fun `get palauttaa saman instanssin TTL-ikkunan sisalla`() {
        val cache = InMemoryCache<Unit, Any>(ttl = 1.minutes) { Any() }

        val first = cache.get(Unit)
        val second = cache.get(Unit)

        assertSame(first, second)
    }

    @Test
    fun `nolla-ttl palauttaa aina tuoreen arvon`() {
        val kutsuja = AtomicInteger()
        val cache =
            InMemoryCache<Unit, Any>(ttl = Duration.ZERO) {
                kutsuja.incrementAndGet()
                Any()
            }

        val first = cache.get(Unit)
        val second = cache.get(Unit)

        assertNotSame(first, second)
        assertEquals(2, kutsuja.get())
    }

    @Test
    fun `negatiivinen ttl palauttaa aina tuoreen arvon`() {
        val cache = InMemoryCache<Unit, Any>(ttl = (-1).seconds) { Any() }

        assertNotSame(cache.get(Unit), cache.get(Unit))
    }

    @Test
    fun `eri avaimet eivat tormaa keskenaan`() {
        val cache = InMemoryCache<Int, String>(ttl = 1.minutes) { "value-$it" }

        assertEquals("value-1", cache.get(1))
        assertEquals("value-2", cache.get(2))
    }

    @Test
    fun `null-arvo ei jaa cacheen vaan fn ajetaan uudelleen`() {
        val calls = AtomicInteger(0)
        val cache =
            InMemoryCache<Unit, String>(ttl = 1.minutes) {
                calls.incrementAndGet()
                null
            }

        assertNull(cache.get(Unit))
        assertNull(cache.get(Unit))
        assertEquals(2, calls.get(), "null-arvoa ei talleteta, joten fn pitäisi ajaa uudelleen")
    }

    @Test
    fun `vanhentunut arvo lasketaan uudelleen`() {
        val calls = AtomicInteger(0)
        val cache =
            InMemoryCache<Unit, Int>(ttl = 1.milliseconds) {
                calls.incrementAndGet()
            }

        cache.get(Unit)
        Thread.sleep(20)
        cache.get(Unit)

        assertEquals(2, calls.get())
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    fun `rinnakkaiset get-kutsut samaan avaimeen ajavat fn-funktion vain kerran`() {
        val calls = AtomicInteger(0)
        val startGate = CountDownLatch(1)
        val cache =
            InMemoryCache<Unit, String>(ttl = 1.minutes) {
                startGate.await(2, TimeUnit.SECONDS)
                calls.incrementAndGet()
                Thread.sleep(50)
                "value"
            }

        val executor = Executors.newFixedThreadPool(16)
        try {
            val futures = (1..16).map { CompletableFuture.supplyAsync({ cache.get(Unit) }, executor) }
            startGate.countDown()
            val results = futures.map { it.get(2, TimeUnit.SECONDS) }

            assertEquals(List(16) { "value" }, results)
            assertEquals(1, calls.get(), "fn:n pitäisi olla ajettu vain kerran 16 rinnakkaiselle pyynnölle")
        } finally {
            executor.shutdownNow()
        }
    }
}
