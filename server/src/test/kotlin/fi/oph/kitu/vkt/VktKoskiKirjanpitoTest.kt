package fi.oph.kitu.vkt

import fi.oph.kitu.DBContainerConfiguration
import fi.oph.kitu.TestTimeService
import fi.oph.kitu.dev.mockdata.VktSuoritusMockGenerator
import fi.oph.kitu.koodisto.Koodisto
import fi.oph.kitu.koski.KoskiClient
import fi.oph.kitu.koski.KoskiErrorService
import fi.oph.kitu.koski.KoskiService
import fi.oph.kitu.koski.KoskiVktRequestMapper
import fi.oph.kitu.koski.KoskiYkiRequestMapper
import fi.oph.kitu.tiedontuontischema.VktValidation
import fi.oph.kitu.util.result.getOrThrow
import fi.oph.kitu.yki.suoritukset.YkiSuoritusRepository
import org.junit.jupiter.api.BeforeEach
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.http.MediaType
import org.springframework.mock.http.client.MockClientHttpRequest
import org.springframework.test.web.client.ExpectedCount
import org.springframework.test.web.client.MockRestServiceServer
import org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo
import org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess
import org.springframework.web.client.RestClient
import tools.jackson.databind.json.JsonMapper
import java.time.LocalDate
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

private const val OO = "1.2.246.562.15.82831651349"

@SpringBootTest
@Import(DBContainerConfiguration::class)
class VktKoskiKirjanpitoTest(
    @param:Autowired private val suoritusRepository: VktSuoritusRepository,
    @param:Autowired private val customSuoritusRepository: CustomVktSuoritusRepository,
    @param:Autowired private val osakoeRepository: VktOsakoeRepository,
    @param:Autowired private val vktValidation: VktValidation,
    @param:Autowired private val vktSuoritusService: VktSuoritusService,
    @param:Autowired private val koskiVktRequestMapper: KoskiVktRequestMapper,
    @param:Autowired private val koskiYkiRequestMapper: KoskiYkiRequestMapper,
    @param:Autowired private val ykiSuoritusRepository: YkiSuoritusRepository,
    @param:Autowired private val koskiErrorService: KoskiErrorService,
    @param:Autowired private val timeService: TestTimeService,
    @param:Autowired private val restClientBuilder: RestClient.Builder,
) {
    private val generaattori = VktSuoritusMockGenerator()
    private val json = JsonMapper.builder().build()
    private val pyynnot = mutableListOf<String>()

    @BeforeEach
    fun tyhjenna() {
        suoritusRepository.deleteAll()
        pyynnot.clear()
    }

    private fun koski(vastaus: () -> Unit = {}): KoskiService {
        val server = MockRestServiceServer.bindTo(restClientBuilder).build()
        server
            .expect(ExpectedCount.manyTimes(), requestTo("oppija"))
            .andExpect { pyynnot += (it as MockClientHttpRequest).bodyAsString }
            .andRespond { req ->
                vastaus()
                withSuccess(
                    """
                    {"henkilö":{"oid":"1.2.246.562.24.00000000001"},
                     "opiskeluoikeudet":[{"oid":"$OO","versionumero":1,
                       "lähdejärjestelmänId":{"id":"1","lähdejärjestelmä":{"koodiarvo":"kielitutkintorekisteri",
                         "nimi":{"fi":"Kielitutkintorekisteri"},"koodistoUri":"lahdejarjestelma","koodistoVersio":1}}}]}
                    """.trimIndent(),
                    MediaType.APPLICATION_JSON,
                ).createResponse(req)
            }
        return KoskiService(
            KoskiClient(restClientBuilder.build()),
            koskiYkiRequestMapper,
            koskiVktRequestMapper,
            ykiSuoritusRepository,
            customSuoritusRepository,
            vktSuoritusService,
            koskiErrorService,
            timeService,
        )
    }

    private fun lahetettyOid(pyynto: String): String? =
        json
            .readTree(pyynto)
            .path("opiskeluoikeudet")
            .path(0)
            .path("oid")
            .takeIf { it.isString }
            ?.stringValue()

    private fun lahdejarjestelmanId(pyynto: String): String =
        json
            .readTree(pyynto)
            .path("opiskeluoikeudet")
            .path(0)
            .path("lähdejärjestelmänId")
            .path("id")
            .stringValue()

    private fun VktSuoritusEntity.ryhma() =
        CustomVktSuoritusRepository.Tutkintoryhma(suorittajanOid.toString(), tutkintokieli, taitotaso)

    private fun siirrettavaHyva(): VktSuoritusEntity =
        generateSequence { generaattori.generateRandomVktSuoritusEntity(vktValidation) }
            .first {
                it.taitotaso == Koodisto.VktTaitotaso.HyväJaTyydyttävä &&
                    it.osakokeet.all { o -> o.arvosana != null } &&
                    koskiVktRequestMapper.vktSuoritusToKoskiRequest(it.toHenkilosuoritus()).isRight()
            }

    private fun kiosinErinomainen(): VktSuoritusEntity =
        generateSequence { generaattori.generateRandomVktSuoritusEntity(vktValidation) }
            .first { it.taitotaso == Koodisto.VktTaitotaso.Erinomainen }
            .let { s ->
                s.copy(
                    osakokeet = s.osakokeet.map { it.copy(arvosana = null, arviointipaiva = null) }.toSet(),
                    tutkinnot = s.tutkinnot.map { it.copy(arvosana = null, arviointipaiva = null) }.toSet(),
                )
            }

    private fun arvioiKaikki(ryhma: CustomVktSuoritusRepository.Tutkintoryhma) {
        val suoritus = assertNotNull(vktSuoritusService.getOppijanSuoritukset(ryhma, false))
        suoritus.suoritus.osat.forEach {
            osakoeRepository.updateArvosana(
                it.internalId!!,
                Koodisto.VktArvosana.Erinomainen,
                LocalDate.of(2026, 10, 1),
            )
        }
        customSuoritusRepository.requestTransferToKoski(ryhma)
    }

    @Test
    fun `muuttunut KIOS-lahetys siirron jalkeen lahtee KOSKI-jarjestelmaan oidilla`() {
        val suoritus = siirrettavaHyva()
        customSuoritusRepository.save(suoritus)
        val koski = koski()
        koski.sendVktSuorituksetToKoski().getOrThrow()

        customSuoritusRepository.save(suoritus.copy(sukunimi = suoritus.sukunimi + "-Korjattu"))
        koski.sendVktSuorituksetToKoski().getOrThrow()

        assertEquals(2, pyynnot.size)
        assertEquals(null, lahetettyOid(pyynnot[0]))
        assertEquals(OO, lahetettyOid(pyynnot[1]))
    }

    @Test
    fun `muuttumaton KIOS-lahetys siirron jalkeen ei laheta uudelleen`() {
        val suoritus = siirrettavaHyva()
        customSuoritusRepository.save(suoritus)
        val koski = koski()
        koski.sendVktSuorituksetToKoski().getOrThrow()

        customSuoritusRepository.save(suoritus)
        koski.sendVktSuorituksetToKoski().getOrThrow()

        assertEquals(1, pyynnot.size)
    }

    @Test
    fun `erinomaisen muuttunut lahetys arvioinnin ja siirron jalkeen lahtee arvosanoin ja oidilla`() {
        val suoritus = kiosinErinomainen()
        customSuoritusRepository.save(suoritus)
        arvioiKaikki(suoritus.ryhma())
        val koski = koski()
        koski.sendVktSuorituksetToKoski().getOrThrow()
        assertEquals(1, pyynnot.size)

        customSuoritusRepository.save(suoritus.copy(etunimet = suoritus.etunimet + " Uusi"))
        koski.sendVktSuorituksetToKoski().getOrThrow()

        assertEquals(2, pyynnot.size)
        assertEquals(OO, lahetettyOid(pyynnot[1]))
    }

    @Test
    fun `ensimmaisen siirron aikana saapunut muutos lahtee seuraavassa ajossa oidilla`() {
        val suoritus = siirrettavaHyva()
        customSuoritusRepository.save(suoritus)
        var kerta = 0
        val koski =
            koski {
                kerta += 1
                if (kerta == 1) customSuoritusRepository.save(suoritus.copy(sukunimi = suoritus.sukunimi + "-Kesken"))
            }

        koski.sendVktSuorituksetToKoski().getOrThrow()
        koski.sendVktSuorituksetToKoski().getOrThrow()

        assertEquals(2, pyynnot.size)
        assertTrue(lahdejarjestelmanId(pyynnot[1]) != lahdejarjestelmanId(pyynnot[0]))
        assertEquals(OO, lahetettyOid(pyynnot[1]), "siirron aikana saapunut versio lähti ilman oidia")
    }

    @Test
    fun `ennen korjausta syntynyt oiditon versio lahtee uudelleenarvioinnin jalkeen oidilla`() {
        val suoritus = kiosinErinomainen()
        customSuoritusRepository.save(suoritus)
        arvioiKaikki(suoritus.ryhma())
        val koski = koski()
        koski.sendVktSuorituksetToKoski().getOrThrow()

        suoritusRepository.save(suoritus)
        arvioiKaikki(suoritus.ryhma())
        koski.sendVktSuorituksetToKoski().getOrThrow()

        assertEquals(2, pyynnot.size)
        assertEquals(OO, lahetettyOid(pyynnot[1]), "ennen korjausta syntynyt versio lähti ilman oidia")
    }
}
