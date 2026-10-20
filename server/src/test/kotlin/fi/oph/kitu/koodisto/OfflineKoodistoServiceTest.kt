package fi.oph.kitu.koodisto

import fi.oph.kitu.i18n.LocalizedString
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

class OfflineKoodistoServiceTest {
    private val service = OfflineKoodistoService()

    @Test
    fun `nakymien kayttamat koodistot loytyvat fixtuureista`() {
        listOf(
            "kieli",
            "kunta",
            "maatjavaltiot1",
            "maatjavaltiot2",
            "vkttutkintotaso",
            "vktosakoe",
            "vktarvosana",
            "vktkielitaito",
        ).forEach { uri ->
            assertTrue(service.getKoodiviitteet(uri).orEmpty().isNotEmpty(), uri)
        }
    }

    @Test
    fun `koodiarvo kaantyy kaikille kielille`() {
        val tyydyttava = service.getKoodiviitteet("vktarvosana")!!.single { it.koodiArvo == "tyydyttava" }

        assertEquals(
            LocalizedString(fi = "Tyydyttävä", sv = "Nöjaktig", en = "Satisfactory"),
            tyydyttava.metadata.toLocalizedString(),
        )
    }

    @Test
    fun `tuntematon koodisto palauttaa nullin`() {
        assertNull(service.getKoodiviitteet("eiolemassa"))
    }
}
