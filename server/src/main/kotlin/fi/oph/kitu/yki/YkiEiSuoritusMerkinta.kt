package fi.oph.kitu.yki

import fi.oph.kitu.util.TimeService
import fi.oph.kitu.yki.suoritukset.YkiSuoritusRepository
import io.opentelemetry.api.trace.Span
import io.opentelemetry.instrumentation.annotations.WithSpan
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service

@Service
class YkiEiSuoritusMerkinta(
    private val ykiService: YkiService,
    private val suoritusRepository: YkiSuoritusRepository,
    private val timeService: TimeService,
    @param:Value($$"${kitu.yki.scheduling.merkitseEiSuoritetuiksi.paivatTutkinnosta}")
    private val paivatTutkinnosta: Long,
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    @WithSpan
    fun merkitseTutkinnostaPoisjaaneet(): Int {
        val viimeinenTutkintopaiva = timeService.today().minusDays(paivatTutkinnosta)
        val tulokset =
            suoritusRepository.findIlmoittautuneidenTunnukset(viimeinenTutkintopaiva).map { tunnus ->
                try {
                    ykiService.poistaSuoritus(tunnus).isRight()
                } catch (e: Exception) {
                    logger.error("YKI-suorituksen $tunnus merkintä ei-suoritetuksi epäonnistui", e)
                    null
                }
            }

        val merkitty = tulokset.count { it == true }
        val epaonnistui = tulokset.count { it == null }
        Span.current().apply {
            setAttribute("eiSuoritusta.merkitty", merkitty.toLong())
            setAttribute("eiSuoritusta.ohitettu", tulokset.count { it == false }.toLong())
            setAttribute("eiSuoritusta.epaonnistui", epaonnistui.toLong())
        }
        check(epaonnistui == 0) { "$epaonnistui YKI-suorituksen merkintä epäonnistui, ks. lokit" }
        return merkitty
    }
}
