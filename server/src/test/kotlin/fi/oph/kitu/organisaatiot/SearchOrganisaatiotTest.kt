package fi.oph.kitu.organisaatiot

import fi.oph.kitu.DBContainerConfiguration
import fi.oph.kitu.observability.OpenTelemetryTestConfig
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import kotlin.test.assertTrue

@SpringBootTest
@Import(OpenTelemetryTestConfig::class, DBContainerConfiguration::class)
class SearchOrganisaatiotTest(
    @param:Autowired private val organisaatioService: OrganisaatioService,
) {
    @Test
    fun `search matches Finnish name`() {
        val result = organisaatioService.searchOrganisaatiot("Jyväskylän yliopisto")
        assertTrue(result.nimet.isNotEmpty())
        assertTrue(result.nimet.values.any { it.fi?.contains("Jyväskylän yliopisto") == true })
    }

    @Test
    fun `search is case-insensitive`() {
        val result = organisaatioService.searchOrganisaatiot("jyväskylän yliopisto")
        assertTrue(result.nimet.isNotEmpty())
        assertTrue(result.nimet.values.any { it.fi?.contains("Jyväskylän yliopisto") == true })
    }

    @Test
    fun `search matches Swedish name`() {
        val result = organisaatioService.searchOrganisaatiot("Jyväskylä universitet")
        assertTrue(result.nimet.isNotEmpty())
    }

    @Test
    fun `search matches English name`() {
        val result = organisaatioService.searchOrganisaatiot("Centre for Applied Language Studies")
        assertTrue(result.nimet.isNotEmpty())
    }

    @Test
    fun `search with partial match`() {
        val result = organisaatioService.searchOrganisaatiot("Ressun")
        assertTrue(result.nimet.isNotEmpty())
        assertTrue(result.nimet.values.any { it.fi?.contains("Ressun") == true })
    }

    @Test
    fun `search with no match returns empty`() {
        val result = organisaatioService.searchOrganisaatiot("tätä organisaatiota ei ole olemassa")
        assertTrue(result.nimet.isEmpty())
    }

    @Test
    fun `search returns only matching organisations`() {
        val allOrganisaatiot = organisaatioService.getOrganisaatiot()
        val result = organisaatioService.searchOrganisaatiot("Ressun")
        assertTrue(result.nimet.isNotEmpty())
        assertTrue(result.nimet.size < allOrganisaatiot.nimet.size)
    }
}
