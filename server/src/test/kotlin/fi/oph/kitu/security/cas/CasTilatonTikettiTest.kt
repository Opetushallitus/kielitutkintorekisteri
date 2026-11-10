package fi.oph.kitu.security.cas

import fi.oph.kitu.DBContainerConfiguration
import fi.oph.kitu.security.Authority
import org.apereo.cas.client.authentication.AttributePrincipalImpl
import org.apereo.cas.client.validation.AssertionImpl
import org.apereo.cas.client.validation.TicketValidationException
import org.apereo.cas.client.validation.TicketValidator
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Import
import org.springframework.context.annotation.Primary
import org.springframework.security.test.web.servlet.setup.SecurityMockMvcConfigurers.springSecurity
import org.springframework.session.web.http.SessionRepositoryFilter
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.status
import org.springframework.test.web.servlet.setup.DefaultMockMvcBuilder
import org.springframework.test.web.servlet.setup.MockMvcBuilders
import org.springframework.web.context.WebApplicationContext
import java.util.concurrent.ConcurrentHashMap
import kotlin.test.assertEquals

@SpringBootTest
@Import(DBContainerConfiguration::class, CasTilatonTikettiTest.KertakayttoisetTiketit::class)
class CasTilatonTikettiTest(
    @param:Autowired private val context: WebApplicationContext,
    @param:Autowired private val sessionRepositoryFilter: SessionRepositoryFilter<*>,
    @param:Autowired private val validator: KertakayttoinenValidator,
) {
    class KertakayttoinenValidator : TicketValidator {
        val validoinnit: MutableMap<String, Int> = ConcurrentHashMap()

        override fun validate(
            ticket: String,
            service: String,
        ) = if (validoinnit.merge(ticket, 1, Int::plus) == 1) {
            AssertionImpl(
                AttributePrincipalImpl(
                    "kios-palvelukayttaja",
                    mapOf(
                        "oidHenkilo" to "1.2.246.562.24.20281155246",
                        "kayttajaTyyppi" to "PALVELU",
                        "roles" to listOf(Authority.VIRKAILIJA.role(), Authority.VKT_TALLENNUS.role()),
                    ),
                ),
            )
        } else {
            throw TicketValidationException("Tiketti $ticket on jo käytetty")
        }
    }

    @TestConfiguration(proxyBeanMethods = false)
    class KertakayttoisetTiketit {
        @Bean
        @Primary
        fun kertakayttoinenValidator() = KertakayttoinenValidator()
    }

    private lateinit var mockMvc: MockMvc

    @BeforeEach
    fun setup() {
        validator.validoinnit.clear()
        mockMvc =
            MockMvcBuilders
                .webAppContextSetup(context)
                .addFilters<DefaultMockMvcBuilder>(sessionRepositoryFilter)
                .apply<DefaultMockMvcBuilder>(springSecurity())
                .build()
    }

    @Test
    fun `tilattoman pyynnon tiketti validoidaan CASissa joka kerta eika sita voi kayttaa uudelleen`() {
        mockMvc
            .perform(get("/api/vkt/kios/j_spring_cas_security_check?ticket=ST-1"))
            .andExpect(status().isOk)

        mockMvc
            .perform(get("/vkt/?ticket=ST-1"))
            .andExpect(status().isUnauthorized)

        assertEquals(2, validator.validoinnit["ST-1"])
    }
}
