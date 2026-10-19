package fi.oph.kitu.i18n

import fi.oph.kitu.DBContainerConfiguration
import fi.oph.kitu.virkailijaSession
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.csrf
import org.springframework.security.test.web.servlet.setup.SecurityMockMvcConfigurers.springSecurity
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.redirectedUrl
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.status
import org.springframework.test.web.servlet.setup.DefaultMockMvcBuilder
import org.springframework.test.web.servlet.setup.MockMvcBuilders
import org.springframework.web.context.WebApplicationContext
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull

@SpringBootTest
@Import(DBContainerConfiguration::class)
class LokalisointiViewControllerTest(
    @param:Autowired private val context: WebApplicationContext,
) {
    private lateinit var mockMvc: MockMvc

    @BeforeEach
    fun setup() {
        mockMvc =
            MockMvcBuilders
                .webAppContextSetup(context)
                .apply<DefaultMockMvcBuilder>(springSecurity())
                .build()
    }

    @AfterEach
    fun clearTolgee() = TolgeeMessages.clear()

    @Test
    fun `reitti tyhjentaa kaannosvalimuistin ja ohjaa etusivulle`() {
        TolgeeMessages.set(mapOf("nav.yki" to LocalizedString(sv = "Allmän språkexamen")))

        mockMvc
            .perform(post("/kaannokset/tyhjenna-valimuisti").session(virkailijaSession()).with(csrf()))
            .andExpect(status().is3xxRedirection)
            .andExpect(redirectedUrl("http://localhost/"))

        assertNull(
            TolgeeMessages.get("nav.yki")?.sv,
            "Elävä ruotsinkielinen käännös katosi välimuistin mukana",
        )
        assertNotNull(
            TolgeeMessages.get("nav.yki")?.fi,
            "Repon lähdeteksti jää voimaan: tyhjennys koskee vain elävää kerrosta",
        )
    }

    @Test
    fun `tyhjennyksen jalkeen kaannokset eivat enaa nay ja etusivu kertoo tuloksen`() {
        TolgeeMessages.set(mapOf("nav.yki" to LocalizedString(sv = "Allmän språkexamen")))
        val session = virkailijaSession()

        mockMvc
            .perform(post("/kaannokset/tyhjenna-valimuisti").session(session).with(csrf()))
            .andExpect(status().is3xxRedirection)

        val etusivu =
            mockMvc
                .perform(get("/?lang=sv").session(session))
                .andExpect(status().isOk)
                .andReturn()
                .response.contentAsString

        assertContains(etusivu, "Käännösvälimuisti tyhjennettiin")
        assertFalse(
            etusivu.contains("Allmän språkexamen"),
            "Tyhjennetystä välimuistista ei enää saada ruotsinkielistä käännöstä",
        )
    }

    @Test
    fun `tyhjennys vaatii kirjautumisen`() {
        TolgeeMessages.set(mapOf("nav.yki" to LocalizedString(sv = "Allmän språkexamen")))

        mockMvc
            .perform(post("/kaannokset/tyhjenna-valimuisti").with(csrf()))
            .andExpect(status().is3xxRedirection)

        assertEquals("Allmän språkexamen", TolgeeMessages.get("nav.yki")?.sv)
    }
}
