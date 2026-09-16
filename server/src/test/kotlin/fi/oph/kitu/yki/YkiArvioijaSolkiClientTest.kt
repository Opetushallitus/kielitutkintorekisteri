package fi.oph.kitu.yki

import fi.oph.kitu.restclient.withLenientStringConverter
import fi.oph.kitu.yki.arvioijat.solki.SolkiArvioijaClientImpl
import fi.oph.kitu.yki.arvioijat.solki.SolkiArvioijaException
import fi.oph.kitu.yki.arvioijat.solki.SolkiArvioijaRequest
import org.springframework.http.HttpMethod
import org.springframework.http.HttpStatus
import org.springframework.mock.http.client.MockClientHttpRequest
import org.springframework.test.web.client.MockRestServiceServer
import org.springframework.test.web.client.match.MockRestRequestMatchers.header
import org.springframework.test.web.client.match.MockRestRequestMatchers.method
import org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo
import org.springframework.test.web.client.response.MockRestResponseCreators.withException
import org.springframework.test.web.client.response.MockRestResponseCreators.withStatus
import org.springframework.web.client.RestClient
import java.io.IOException
import java.time.LocalDate
import java.time.OffsetDateTime
import java.time.ZoneOffset
import kotlin.test.Test
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/** Vastauskohtainen kasittely: yhteysvirhe ei saa vuotaa poikkeuksena kutsujalle. */
class YkiArvioijaSolkiClientTest {
    private val versio = OffsetDateTime.of(2026, 6, 1, 9, 0, 0, 0, ZoneOffset.UTC)

    private fun clientJaServer(): Pair<SolkiArvioijaClientImpl, MockRestServiceServer> {
        val builder = RestClient.builder().baseUrl("https://solki.test/oph/").withLenientStringConverter()
        val server = MockRestServiceServer.bindTo(builder).build()
        return SolkiArvioijaClientImpl(builder.build()) to server
    }

    private fun request(
        syntymapaiva: LocalDate? = LocalDate.of(1980, 1, 1),
        sahkopostiosoite: String? = "testi@testi.fi",
    ) = SolkiArvioijaRequest(
        arvioijaOid = "1.2.246.562.24.20281155246",
        versio = versio,
        sukunimi = "Öhman-Testi",
        etunimet = "Ranja Testi",
        syntymapaiva = syntymapaiva,
        sahkopostiosoite = sahkopostiosoite,
        katuosoite = "Testikuja 5",
        postinumero = "40100",
        postitoimipaikka = "Testila",
        ensimmainenRekisterointipaiva = LocalDate.of(2019, 1, 15),
        arviointioikeudet = emptyList(),
    )

    /** Serialisoitu runko: se on ainoa paikka jossa kentan lankamuoto nakyy. */
    private fun runko(request: SolkiArvioijaRequest): String {
        val (client, server) = clientJaServer()
        var runko = ""
        server
            .expect(method(HttpMethod.POST))
            .andExpect { req -> runko = (req as MockClientHttpRequest).bodyAsString }
            .andRespond(withStatus(HttpStatus.NO_CONTENT))

        client.laheta(request)

        return runko
    }

    @Test
    fun `yhteysvirhe palautuu Leftina eika poikkeuksena`() {
        val (client, server) = clientJaServer()
        server
            .expect(requestTo("https://solki.test/oph/arvioija"))
            .andRespond(withException(IOException("connection refused")))

        val tulos = client.laheta(request())

        assertTrue(
            tulos.leftOrNull() is SolkiArvioijaException.ConnectionFailure,
            "yhteysvirheen on tultava Leftina, muuten tallennuspolku kaatuu: $tulos",
        )
    }

    @Test
    fun `konflikti tulkitaan onnistumiseksi`() {
        val (client, server) = clientJaServer()
        server.expect(method(HttpMethod.POST)).andRespond(withStatus(HttpStatus.CONFLICT))

        assertTrue(client.laheta(request()).isRight(), "Solkilla on uudempi versio, ei virhe")
    }

    @Test
    fun `palvelinvirhe on UnexpectedError ja idempotency-key on mukana`() {
        val (client, server) = clientJaServer()
        server
            .expect(header("Idempotency-Key", "1.2.246.562.24.20281155246:$versio"))
            .andRespond(withStatus(HttpStatus.INTERNAL_SERVER_ERROR).body("hajosi"))

        val tulos = client.laheta(request())

        assertTrue(tulos.leftOrNull() is SolkiArvioijaException.UnexpectedError)
    }

    @Test
    fun `pitka vastausrunko katkaistaan virheilmoituksesta`() {
        val (client, server) = clientJaServer()
        // Solki voi kaiuttaa lahetetyt arvot takaisin, ja teksti paatyy kayttoliittymaan asti.
        val pitkaVastaus = "katuosoite 'Testikuja 5' on virheellinen. ".repeat(50)
        server.expect(method(HttpMethod.POST)).andRespond(withStatus(HttpStatus.BAD_REQUEST).body(pitkaVastaus))

        val virhe = client.laheta(request()).leftOrNull()!!.debugString()

        assertTrue(virhe.length < pitkaVastaus.length, "vastausrunko on katkaistava: ${virhe.length}")
        assertContains(virhe, "katkaistu")
        assertContains(virhe, "response status: 400")
    }

    @Test
    fun `syntymapaiva serialisoituu paivamaarana ilman kellonaikaa`() {
        assertContains(runko(request(syntymapaiva = LocalDate.of(1980, 1, 1))), """"syntymapaiva":"1980-01-01"""")
    }

    @Test
    fun `tyhja syntymapaiva jatetaan pois rungosta`() {
        // Solki johti syntymaajan ennen henkilotunnuksesta, joten tyhja arvo on sille eri asia
        // kuin puuttuva kentta. Muut kentat serialisoituvat yha nullina, ks. alla.
        val runko = runko(request(syntymapaiva = null, sahkopostiosoite = null))

        assertFalse(runko.contains("syntymapaiva"), """tyhjan kentan on kadottava kokonaan: $runko""")
        assertContains(
            runko,
            """"sahkopostiosoite":null""",
            message = "muiden kenttien lankamuoto ei saa muuttua",
        )
    }

    @Test
    fun `onnistunut lahetys palauttaa Rightin`() {
        val (client, server) = clientJaServer()
        server.expect(method(HttpMethod.POST)).andRespond(withStatus(HttpStatus.NO_CONTENT))

        assertEquals(true, client.laheta(request()).isRight())
    }
}
