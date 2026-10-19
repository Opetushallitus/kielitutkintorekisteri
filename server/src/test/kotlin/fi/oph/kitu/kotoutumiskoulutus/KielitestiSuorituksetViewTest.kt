package fi.oph.kitu.kotoutumiskoulutus

import fi.oph.kitu.DBContainerConfiguration
import fi.oph.kitu.kotoutumiskoulutus.suoritukset.error.KielitestiSuoritusError
import fi.oph.kitu.kotoutumiskoulutus.suoritukset.error.KielitestiSuoritusErrorRepository
import fi.oph.kitu.virkailijaSession
import org.junit.jupiter.api.BeforeEach
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.security.test.web.servlet.setup.SecurityMockMvcConfigurers.springSecurity
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.status
import org.springframework.test.web.servlet.setup.DefaultMockMvcBuilder
import org.springframework.test.web.servlet.setup.MockMvcBuilders
import org.springframework.web.context.WebApplicationContext
import java.time.Instant
import kotlin.test.Test
import kotlin.test.assertContains
import kotlin.test.assertFalse

@SpringBootTest
@Import(DBContainerConfiguration::class)
class KielitestiSuorituksetViewTest(
    @param:Autowired private val context: WebApplicationContext,
    @param:Autowired private val errorRepository: KielitestiSuoritusErrorRepository,
) {
    private lateinit var mockMvc: MockMvc

    @BeforeEach
    fun setup() {
        mockMvc =
            MockMvcBuilders
                .webAppContextSetup(context)
                .apply<DefaultMockMvcBuilder>(springSecurity())
                .build()
        errorRepository.deleteAll()
    }

    @Test
    fun `listasivu nayttaa tuontivirheiden maaran`() {
        errorRepository.saveAll(listOf(virhe("Ranja"), virhe("Eino")))

        assertContains(getHtml("/koto-kielitesti/suoritukset"), "Järjestelmässä on 2 virhettä.")
    }

    @Test
    fun `hakulomake ei sailyta sivunumeroa`() {
        val html = getHtml("/koto-kielitesti/suoritukset", "page" to "3", "testikieli" to "FIN")

        assertFalse(html.contains("""name="page""""), "haku sivulta 3 voisi osua tyhjälle sivulle")
        assertContains(html, """<input type="hidden" name="testikieli" value="FIN">""")
    }

    private fun getHtml(
        path: String,
        vararg params: Pair<String, String>,
    ): String =
        mockMvc
            .perform(
                get(path).session(virkailijaSession()).apply {
                    params.forEach { (name, value) -> param(name, value) }
                },
            ).andExpect(status().isOk)
            .andReturn()
            .response.contentAsString

    private fun virhe(etunimet: String) =
        KielitestiSuoritusError(
            id = null,
            suorittajanOid = null,
            hetu = null,
            nimi = "$etunimet Testi",
            etunimet = etunimet,
            sukunimi = "Testi",
            kutsumanimi = etunimet,
            schoolOid = null,
            teacherEmail = null,
            virheenLuontiaika = Instant.parse("2026-10-01T10:00:00Z"),
            viesti = "Puuttuva arvosana",
            virheellinenKentta = null,
            virheellinenArvo = null,
            lisatietoja = null,
        )
}
