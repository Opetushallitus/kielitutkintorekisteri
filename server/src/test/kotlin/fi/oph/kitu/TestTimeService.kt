package fi.oph.kitu

import fi.oph.kitu.util.TimeService
import fi.oph.kitu.util.TimeService.Companion.clock
import fi.oph.kitu.util.TimeService.Companion.zoneId
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Service
import java.time.Clock
import java.time.Instant

@Service
@Profile("test")
class TestTimeService : TimeService {
    private var fixedClock: Clock? = null

    override fun now(): Instant = Instant.now(fixedClock ?: clock)

    fun fixClock(time: Instant) {
        fixedClock = Clock.fixed(time, zoneId)
    }

    fun resetClock() {
        fixedClock = null
    }

    fun <T> runWithFixedClock(
        time: Instant,
        f: () -> T,
    ): T {
        fixClock(time)
        try {
            return f()
        } finally {
            resetClock()
        }
    }
}
