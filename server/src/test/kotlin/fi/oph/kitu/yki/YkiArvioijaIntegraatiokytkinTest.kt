package fi.oph.kitu.yki

import fi.oph.kitu.DBContainerConfiguration
import fi.oph.kitu.yki.arvioijat.solki.SolkiArvioijaClient
import fi.oph.kitu.yki.arvioijat.solki.SolkiArvioijaService
import fi.oph.kitu.yki.arvioijat.solki.SolkiArvioijaServiceImpl
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import kotlin.test.Test
import kotlin.test.assertTrue

/**
 * Lahetyskoneiston on oltava pystyssa myos kytkimen ollessa pois, koska virkailijan kasin
 * kaynnistama lahetys kayttaa sita. Jos client jaisi luomatta, koko arvioijarekisteri ei
 * kaynnistyisi. Kytkimen vaikutus itsessaan on testattu YkiArvioijaSolkiTestissa.
 */
@SpringBootTest(properties = ["kitu.yki.arvioijarekisteri.integraatio.enabled=false"])
@Import(DBContainerConfiguration::class)
class YkiArvioijaIntegraatiokytkinPoisTest(
    @param:Autowired private val service: SolkiArvioijaService,
    @param:Autowired private val client: SolkiArvioijaClient,
) {
    @Test
    fun `kytkin pois jattaa lahettavan toteutuksen paikalleen`() {
        assertTrue(service is SolkiArvioijaServiceImpl, "sai ${service.javaClass.simpleName}")
    }

    @Test
    fun `kytkin pois ei poista clientia`() {
        assertTrue(client.javaClass.simpleName.startsWith("SolkiArvioijaClientImpl"))
    }
}

@SpringBootTest(properties = ["kitu.yki.arvioijarekisteri.integraatio.enabled=true"])
@Import(DBContainerConfiguration::class)
class YkiArvioijaIntegraatiokytkinPaallaTest(
    @param:Autowired private val service: SolkiArvioijaService,
) {
    @Test
    fun `kytkin paalla valitsee lahettavan toteutuksen`() {
        assertTrue(service is SolkiArvioijaServiceImpl, "sai ${service.javaClass.simpleName}")
    }
}
