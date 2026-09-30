package fi.oph.kitu.webmvc

import fi.oph.kitu.koski.KoskiErrorService
import fi.oph.kitu.kotoutumiskoulutus.koealusta.tehtavapankki.TehtavapankkiService
import fi.oph.kitu.kotoutumiskoulutus.suoritukset.CustomKielitestiSuoritusRepository
import fi.oph.kitu.kotoutumiskoulutus.suoritukset.error.KielitestiSuoritusErrorRepository
import fi.oph.kitu.util.cache.InMemoryCache
import fi.oph.kitu.util.scheduling.SchedulerStatsRepository
import fi.oph.kitu.vkt.CustomVktSuoritusRepository
import fi.oph.kitu.yki.arvioijat.YkiArvioijaRepository
import fi.oph.kitu.yki.historia.YkiHistoriaSiirtymatonParams
import fi.oph.kitu.yki.historia.YkiHistoriaSiirtymatonRepository
import fi.oph.kitu.yki.suoritukset.YkiSuoritusRepository
import fi.oph.kitu.yki.suoritukset.error.YkiSuoritusErrorService
import io.opentelemetry.instrumentation.annotations.WithSpan
import org.springframework.beans.factory.ObjectProvider
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service
import java.time.Instant
import kotlin.time.Duration

@Service
class DashboardService(
    private val ykiSuoritusRepository: YkiSuoritusRepository,
    private val ykiArvioijaRepository: YkiArvioijaRepository,
    private val ykiSuoritusErrorService: YkiSuoritusErrorService,
    private val ykiHistoriaSiirtymatonRepository: YkiHistoriaSiirtymatonRepository,
    private val customVktSuoritusRepository: CustomVktSuoritusRepository,
    private val customKielitestiSuoritusRepository: CustomKielitestiSuoritusRepository,
    private val kielitestiSuoritusErrorRepository: KielitestiSuoritusErrorRepository,
    private val tehtavapankkiServiceProvider: ObjectProvider<TehtavapankkiService>,
    private val schedulerStatsRepository: SchedulerStatsRepository,
    private val koskiErrorService: KoskiErrorService,
    /**
     * Konstruktorissa eika @Value-kenttana, koska valimuistit alustetaan property-initialisoijissa
     * — kenttainjektio tapahtuisi vasta niiden jalkeen. Nolla poistaa valimuistin kaytosta;
     * e2e-profiili tekee niin, koska sovelluksen valimuisti ei tyhjenny kun testi tyhjentaa kannan.
     */
    @param:Value($$"${kitu.dashboard.cache.ttl}")
    private val cacheTtlSetting: String,
) {
    private val cacheTtl = Duration.parse(cacheTtlSetting)

    private val ykiCache = InMemoryCache<Unit, YkiStats>(ttl = cacheTtl) { computeYki() }
    private val vktCache = InMemoryCache<Unit, VktStats>(ttl = cacheTtl) { computeVkt() }
    private val kotoCache = InMemoryCache<Unit, KotoStats>(ttl = cacheTtl) { computeKoto() }
    private val adminCache = InMemoryCache<Unit, AdminStats>(ttl = cacheTtl) { computeAdmin() }

    @WithSpan
    fun getYkiStats(): YkiStats = ykiCache.get(Unit) ?: error("ykiCache returned null; computeYki must yield non-null")

    @WithSpan
    fun getVktStats(): VktStats = vktCache.get(Unit) ?: error("vktCache returned null; computeVkt must yield non-null")

    @WithSpan
    fun getKotoStats(): KotoStats =
        kotoCache.get(Unit) ?: error("kotoCache returned null; computeKoto must yield non-null")

    @WithSpan
    fun getAdminStats(): AdminStats =
        adminCache.get(Unit) ?: error("adminCache returned null; computeAdmin must yield non-null")

    private fun computeYki(): YkiStats =
        YkiStats(
            suoritusCount = ykiSuoritusRepository.countSuoritukset(),
            arvioijaCount = ykiArvioijaRepository.count(),
            arvioijaSolkiErrorCount = ykiArvioijaRepository.countSolkiLahetysvirheet(),
            tarkistusarvioinnitOdottamassaCount =
                ykiSuoritusRepository.countTarkistusarvioinnitOdottamassaHyvaksyntaa(),
            latestReceivedAt = ykiSuoritusRepository.findLatestReceivedAt(),
            suoritusImportErrorCount = ykiSuoritusErrorService.countErrors(),
            koskiErrorCount = koskiErrorService.countByEntity("yki", hidden = false).toLong(),
            historiaSiirtymattomatCount =
                ykiHistoriaSiirtymatonRepository.laske(YkiHistoriaSiirtymatonParams()).toLong(),
        )

    private fun computeVkt(): VktStats {
        val counts = customVktSuoritusRepository.countSuorituksetByTaitotaso()
        return VktStats(
            suoritusCount = counts.total,
            ilmoittautuneetErinomaisenTaso = counts.erinomaisenTasonIlmoittautuneet,
            suorituksetErinomaisenTaso = counts.erinomaisenTasonSuoritukset,
            suorituksetHyvaJaTyydyttavaTaso = counts.hyvanJaTyydyttavanTasonSuoritukset,
            latestReceivedAt = customVktSuoritusRepository.findLatestCreatedAt()?.toInstant(),
            koskiErrorCount = koskiErrorService.countByEntity("vkt", hidden = false).toLong(),
        )
    }

    private fun computeKoto(): KotoStats =
        KotoStats(
            suoritusCount = customKielitestiSuoritusRepository.countSuoritukset().toLong(),
            tehtavapaketitCount = tehtavapankkiServiceProvider.getIfAvailable()?.countTehtavapaketit() ?: 0L,
            latestReceivedAt = customKielitestiSuoritusRepository.findLatestLastModified(),
            importErrorCount = kielitestiSuoritusErrorRepository.count(),
        )

    private fun computeAdmin(): AdminStats =
        AdminStats(
            runningCount = schedulerStatsRepository.countCurrentlyRunning(),
            failingCount = schedulerStatsRepository.countCurrentlyFailing(),
        )
}

data class YkiStats(
    val suoritusCount: Long,
    val arvioijaCount: Long,
    val arvioijaSolkiErrorCount: Long,
    val tarkistusarvioinnitOdottamassaCount: Long,
    val latestReceivedAt: Instant?,
    val suoritusImportErrorCount: Long,
    val koskiErrorCount: Long,
    val historiaSiirtymattomatCount: Long,
)

data class VktStats(
    val suoritusCount: Long,
    val ilmoittautuneetErinomaisenTaso: Long,
    val suorituksetErinomaisenTaso: Long,
    val suorituksetHyvaJaTyydyttavaTaso: Long,
    val latestReceivedAt: Instant?,
    val koskiErrorCount: Long,
)

data class KotoStats(
    val suoritusCount: Long,
    val tehtavapaketitCount: Long,
    val latestReceivedAt: Instant?,
    val importErrorCount: Long,
)

data class AdminStats(
    val runningCount: Long,
    val failingCount: Long,
)
