package fi.oph.kitu.webmvc

import fi.oph.kitu.DBContainerConfiguration
import fi.oph.kitu.dev.mockdata.generateRandomKielitestiSuoritus
import fi.oph.kitu.dev.mockdata.generateRandomYkiSuoritusEntity
import fi.oph.kitu.kotoutumiskoulutus.suoritukset.KielitestiSuoritusRepository
import fi.oph.kitu.yki.Arviointitila
import fi.oph.kitu.yki.suoritukset.YkiSuoritusRepository
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.test.util.AopTestUtils
import java.time.Instant
import kotlin.test.assertEquals
import kotlin.test.assertSame

@SpringBootTest
@Import(DBContainerConfiguration::class)
class DashboardServiceTest(
    @param:Autowired private val dashboardService: DashboardService,
    @param:Autowired private val jdbc: JdbcTemplate,
    @param:Autowired private val ykiSuoritusRepository: YkiSuoritusRepository,
    @param:Autowired private val kielitestiSuoritusRepository: KielitestiSuoritusRepository,
) {
    @BeforeEach
    fun cleanup() {
        jdbc.execute("TRUNCATE TABLE yki_suoritus_lisatieto")
        jdbc.execute("TRUNCATE TABLE yki_suoritus CASCADE")
        jdbc.execute("TRUNCATE TABLE yki_arvioija CASCADE")
        jdbc.execute("TRUNCATE TABLE koto_suoritus CASCADE")
        jdbc.execute("TRUNCATE TABLE vkt_suoritus CASCADE")
        jdbc.execute("TRUNCATE TABLE koski_error")
        jdbc.execute("TRUNCATE TABLE tehtavapaketti CASCADE")
        // Virhetaulut luetaan importErrorCounteihin, ja koto-testit siivoavat vain
        // @BeforeEachissa — ilman naita rivit jaavat elamaan yli ja "tyhja kanta" ei ole tyhja.
        jdbc.execute("TRUNCATE TABLE yki_suoritus_error CASCADE")
        jdbc.execute("TRUNCATE TABLE koto_suoritus_error CASCADE")
        clearAllCaches()
    }

    @Test
    fun `tyhjasta kannasta saadaan nollat ja null aikaleimat`() {
        val yki = dashboardService.getYkiStats()
        assertEquals(0L, yki.suoritusCount)
        assertEquals(0L, yki.arvioijaCount)
        assertEquals(0L, yki.tarkistusarvioinnitOdottamassaCount)
        assertEquals(null, yki.latestReceivedAt)
        assertEquals(0L, yki.suoritusImportErrorCount)
        assertEquals(0L, yki.koskiErrorCount)

        val vkt = dashboardService.getVktStats()
        assertEquals(0L, vkt.suoritusCount)
        assertEquals(0L, vkt.ilmoittautuneetErinomaisenTaso)
        assertEquals(0L, vkt.suorituksetErinomaisenTaso)
        assertEquals(0L, vkt.suorituksetHyvaJaTyydyttavaTaso)
        assertEquals(null, vkt.latestReceivedAt)
        assertEquals(0L, vkt.koskiErrorCount)

        val koto = dashboardService.getKotoStats()
        assertEquals(0L, koto.suoritusCount)
        assertEquals(0L, koto.tehtavapaketitCount)
        assertEquals(null, koto.latestReceivedAt)
        assertEquals(0L, koto.importErrorCount)
    }

    @Test
    fun `per-section accessor palauttaa saman instanssin TTL-ikkunan sisalla`() {
        assertSame(dashboardService.getYkiStats(), dashboardService.getYkiStats())
        assertSame(dashboardService.getVktStats(), dashboardService.getVktStats())
        assertSame(dashboardService.getKotoStats(), dashboardService.getKotoStats())
        assertSame(dashboardService.getAdminStats(), dashboardService.getAdminStats())
    }

    @Test
    fun `vain hidas sektio kuluttaa hidasta laskentaa ja muut sektiot palaavat erikseen`() {
        val ykiBefore = dashboardService.getYkiStats()
        clearVktCache()

        assertSame(ykiBefore, dashboardService.getYkiStats(), "YKI-cache ei tyhjentynyt VKT-tyhjennyksen mukana")
    }

    @Test
    fun `tarkistusarvioinnin hyvaksynta ei muuta YKI viimeisin saapunut -aikaleimaa`() {
        val solkiId = 654321
        val ulkoinenSaapumisaika = Instant.parse("2026-05-01T08:00:00Z")
        val virkailijanHyvaksyntaaika = Instant.parse("2026-06-01T12:00:00Z")

        val ulkoinenSuoritus =
            generateRandomYkiSuoritusEntity().copy(
                solkiId = solkiId,
                lastModified = ulkoinenSaapumisaika,
                receivedAt = ulkoinenSaapumisaika,
                arviointitila = Arviointitila.ARVIOITU,
            )
        ykiSuoritusRepository.save(ulkoinenSuoritus, false)
        clearAllCaches()
        assertEquals(ulkoinenSaapumisaika, dashboardService.getYkiStats().latestReceivedAt)

        // Virkailija hyväksyy tarkistusarvioinnin: data class .copy() säilyttää receivedAt:n alkuperäisessä arvossa.
        val hyvaksyntaVersio =
            ulkoinenSuoritus.copy(
                id = null,
                lastModified = virkailijanHyvaksyntaaika,
                arviointitila = Arviointitila.TARKISTUSARVIOINTI_HYVAKSYTTY,
            )
        ykiSuoritusRepository.save(hyvaksyntaVersio, true)
        clearAllCaches()

        assertEquals(
            ulkoinenSaapumisaika,
            dashboardService.getYkiStats().latestReceivedAt,
            "received_at ei saa edetä virkailijan tarkistusarvioinnin hyväksymisestä",
        )
    }

    @Test
    fun `koton viimeisin saapunut suoritus ei seuraa keskeneraisten uudelleentuontia`() {
        val valmiinSaapumisaika = Instant.parse("2026-05-01T08:00:00Z")
        val keskeneraisenTuontiaika = Instant.parse("2026-06-01T12:00:00Z")
        kielitestiSuoritusRepository.saveAll(
            listOf(
                generateRandomKielitestiSuoritus().copy(lastModified = valmiinSaapumisaika, completed = true),
                generateRandomKielitestiSuoritus().copy(lastModified = keskeneraisenTuontiaika, completed = false),
            ),
        )
        clearAllCaches()

        assertEquals(valmiinSaapumisaika, dashboardService.getKotoStats().latestReceivedAt)
    }

    @Test
    fun `cache tyhjennys saa seuraavan kutsun laskemaan tulokset uudelleen`() {
        val first = dashboardService.getYkiStats()
        clearAllCaches()
        val second = dashboardService.getYkiStats()

        assertEquals(first, second)
    }

    private fun clearAllCaches() {
        clearCache("ykiCache")
        clearCache("vktCache")
        clearCache("kotoCache")
        clearCache("adminCache")
    }

    private fun clearVktCache() = clearCache("vktCache")

    private fun clearCache(fieldName: String) {
        val target = AopTestUtils.getTargetObject<DashboardService>(dashboardService)
        val cacheField =
            DashboardService::class.java
                .getDeclaredField(fieldName)
                .apply { isAccessible = true }
        val cache = cacheField.get(target)
        val itemsField =
            cache.javaClass
                .getDeclaredField("items")
                .apply { isAccessible = true }
        @Suppress("UNCHECKED_CAST")
        (itemsField.get(cache) as MutableMap<Any?, Any?>).clear()
    }
}
