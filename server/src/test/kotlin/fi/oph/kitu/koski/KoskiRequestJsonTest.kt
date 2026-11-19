package fi.oph.kitu.koski

import fi.oph.kitu.DBContainerConfiguration
import fi.oph.kitu.dev.mockdata.VktSuoritusMockGenerator
import fi.oph.kitu.dev.mockdata.generateRandomYkiSuoritusEntity
import fi.oph.kitu.tiedontuontischema.VktValidation
import fi.oph.kitu.tyhjennaYkiSuoritukset
import fi.oph.kitu.virkailijaSession
import fi.oph.kitu.vkt.VktSuoritusRepository
import fi.oph.kitu.yki.Arviointitila
import fi.oph.kitu.yki.suoritukset.YkiSuoritusRepository
import org.junit.jupiter.api.BeforeEach
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.security.test.web.servlet.setup.SecurityMockMvcConfigurers.springSecurity
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.content
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.status
import org.springframework.test.web.servlet.setup.DefaultMockMvcBuilder
import org.springframework.test.web.servlet.setup.MockMvcBuilders
import org.springframework.web.context.WebApplicationContext
import kotlin.test.Test

@SpringBootTest
@Import(DBContainerConfiguration::class)
class KoskiRequestJsonTest(
    @param:Autowired private val context: WebApplicationContext,
    @param:Autowired private val ykiSuoritukset: YkiSuoritusRepository,
    @param:Autowired private val vktSuoritukset: VktSuoritusRepository,
    @param:Autowired private val vktValidation: VktValidation,
    @param:Autowired private val koskiVktRequestMapper: KoskiVktRequestMapper,
    @param:Autowired private val jdbc: JdbcTemplate,
) {
    private lateinit var mockMvc: MockMvc

    @BeforeEach
    fun setup() {
        mockMvc =
            MockMvcBuilders
                .webAppContextSetup(context)
                .apply<DefaultMockMvcBuilder>(springSecurity())
                .build()
        jdbc.tyhjennaYkiSuoritukset()
        vktSuoritukset.deleteAll()
    }

    @Test
    fun `YKI-suorituksen KOSKI-pyynto palautetaan JSON-objektina`() {
        ykiSuoritukset.save(
            generateRandomYkiSuoritusEntity().copy(solkiId = 5151, arviointitila = Arviointitila.ARVIOITU),
            false,
        )

        mockMvc
            .perform(get("/yki/koski-request/5151").session(virkailijaSession()))
            .andExpect(status().isOk)
            .andExpect(content().contentTypeCompatibleWith("application/json"))
            .andExpect(jsonPath("$.henkilö.oid").isString)
    }

    @Test
    fun `YKI-suorituksen KOSKI-siirron eston syyt palautetaan JSON-objektina`() {
        ykiSuoritukset.save(generateRandomYkiSuoritusEntity().copy(solkiId = 5252, suorittajanOID = null), false)

        mockMvc
            .perform(get("/yki/koski-request/5252").session(virkailijaSession()))
            .andExpect(status().isOk)
            .andExpect(jsonPath("$.syyt").isArray)
    }

    @Test
    fun `VKT-suorituksen KOSKI-pyynto palautetaan JSON-objektina`() {
        val generaattori = VktSuoritusMockGenerator()
        val suoritus =
            vktSuoritukset.save(
                generateSequence { generaattori.generateRandomVktSuoritusEntity(vktValidation) }
                    .first { koskiVktRequestMapper.vktSuoritusToKoskiRequest(it.toHenkilosuoritus()).isRight() },
            )

        mockMvc
            .perform(
                get(
                    "/vkt/koski-request/${suoritus.suorittajanOid}/${suoritus.tutkintokieli.name}/${suoritus.taitotaso.name}",
                ).session(virkailijaSession()),
            ).andExpect(status().isOk)
            .andExpect(content().contentTypeCompatibleWith("application/json"))
            .andExpect(jsonPath("$.henkilö.oid").isString)
    }
}
