package fi.oph.kitu.yki.arvioijat.solki

import fi.oph.kitu.restclient.withLenientStringConverter
import fi.oph.kitu.util.defaultObjectMapper
import fi.oph.kitu.yki.Tutkintokieli
import fi.oph.kitu.yki.Tutkintotaso
import fi.oph.kitu.yki.arvioijat.arvioija
import fi.oph.kitu.yki.arvioijat.arvioijaOid
import fi.oph.kitu.yki.arvioijat.solki.SolkiArvioijaClientImpl
import fi.oph.kitu.yki.arvioijat.solki.SolkiArvioijaException
import fi.oph.kitu.yki.arvioijat.solki.SolkiArvioijaPoistoRequest
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
        tila = SolkiArvioijaRequest.Tila.ARVIOIJA,
        sukunimi = "Öhman-Testi",
        etunimet = "Ranja Testi",
        syntymapaiva = syntymapaiva,
        sahkopostiosoite = sahkopostiosoite,
        katuosoite = "Testikuja 5",
        postinumero = "40100",
        postitoimipaikka = "Testila",
        ensimmainenRekisterointipaiva = LocalDate.of(2019, 1, 15),
        arviointikausi = null,
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
    fun `poistoilmoituksessa on vain oppijanumero, poistohetki ja tila`() {
        val (client, server) = clientJaServer()
        var runko = ""
        server
            .expect(requestTo("https://solki.test/oph/arvioija"))
            .andExpect(method(HttpMethod.POST))
            .andExpect(header("Idempotency-Key", "1.2.246.562.24.20281155246:$versio"))
            .andExpect { req -> runko = (req as MockClientHttpRequest).bodyAsString }
            .andRespond(withStatus(HttpStatus.NO_CONTENT))

        val tulos = client.lahetaPoisto(SolkiArvioijaPoistoRequest("1.2.246.562.24.20281155246", versio))

        assertTrue(tulos.isRight(), "2xx on onnistunut ilmoitus: $tulos")
        val json = defaultObjectMapper.readTree(runko)
        assertEquals(listOf("arvioijaOid", "versio", "tila"), json.propertyNames().asSequence().toList())
        assertEquals("POISTETTU", json["tila"].asString())
        server.verify()
    }

    @Test
    fun `epaonnistunut poistoilmoitus on Left`() {
        val (client, server) = clientJaServer()
        server.expect(method(HttpMethod.POST)).andRespond(withStatus(HttpStatus.BAD_REQUEST))

        val tulos = client.lahetaPoisto(SolkiArvioijaPoistoRequest("1.2.246.562.24.20281155246", versio))

        assertTrue(tulos.leftOrNull() is SolkiArvioijaException.BadRequest, "$tulos")
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
    fun `konflikti on lahetysvirhe eika onnistuminen`() {
        val (client, server) = clientJaServer()
        server.expect(method(HttpMethod.POST)).andRespond(withStatus(HttpStatus.CONFLICT))

        assertTrue(
            client.laheta(request()).leftOrNull() is SolkiArvioijaException.Conflict,
            "409 = Solki ei tallentanut mitaan, joten rivia ei saa merkita lahetetyksi",
        )
    }

    @Test
    fun `uusi arvioija vastaa 201 ja tunnus luetaan vastauksesta`() {
        val (client, server) = clientJaServer()
        server
            .expect(method(HttpMethod.POST))
            .andRespond(withStatus(HttpStatus.CREATED).body("""{"tunnus":"A00001"}"""))

        assertEquals("A00001", client.laheta(request()).getOrNull())
    }

    @Test
    fun `jasentymaton vastausrunko ei kaada lahetysta`() {
        // Solki on ottanut rivin vastaan, joten tunnuksen puuttuminen ei ole lahetysvirhe.
        val (client, server) = clientJaServer()
        server.expect(method(HttpMethod.POST)).andRespond(withStatus(HttpStatus.OK).body("OK"))

        val tulos = client.laheta(request())

        assertTrue(tulos.isRight())
        assertEquals(null, tulos.getOrNull())
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
    fun `runko vastaa Solkin dokumentoimaa kenttajoukkoa`() {
        // Kenttien nimet ja taso ovat koko sopimus: vaara nimi ei nay mistaan muusta kuin
        // Solkin 4xx-vastauksesta, ja vaara taso menee lapi hiljaisesti.
        val kausi =
            SolkiArvioijaRequest.Arviointikausi(
                id = 412,
                alkupaiva = LocalDate.of(2025, 1, 1),
                paattymispaiva = LocalDate.of(2029, 12, 31),
                jatkorekisterointi = false,
                arviointioikeudet =
                    listOf(
                        SolkiArvioijaRequest.Arviointioikeus(
                            id = 981,
                            kieli = Tutkintokieli.FIN,
                            tasot = listOf(Tutkintotaso.PT, Tutkintotaso.KT),
                        ),
                        SolkiArvioijaRequest.Arviointioikeus(
                            id = null,
                            kieli = Tutkintokieli.SWE,
                            tasot = listOf(Tutkintotaso.YT),
                        ),
                    ),
            )
        val runko = defaultObjectMapper.readTree(runko(request().copy(arviointikausi = kausi)))

        assertEquals(
            listOf(
                "arvioijaOid",
                "versio",
                "tila",
                "sukunimi",
                "etunimet",
                "syntymapaiva",
                "sahkopostiosoite",
                "katuosoite",
                "postinumero",
                "postitoimipaikka",
                "ensimmainenRekisterointipaiva",
                "arviointikausi",
            ),
            runko.propertyNames().asSequence().toList(),
        )
        val lahetettyKausi = runko["arviointikausi"]
        assertEquals(
            listOf("id", "alkupaiva", "paattymispaiva", "jatkorekisterointi", "arviointioikeudet"),
            lahetettyKausi.propertyNames().asSequence().toList(),
        )
        assertEquals(412, lahetettyKausi["id"].asInt())
        assertEquals("2025-01-01", lahetettyKausi["alkupaiva"].asString())
        assertEquals("2029-12-31", lahetettyKausi["paattymispaiva"].asString())
        assertEquals(2, lahetettyKausi["arviointioikeudet"].size())
        assertEquals(
            listOf("id", "kieli", "tasot"),
            lahetettyKausi["arviointioikeudet"][0].propertyNames().asSequence().toList(),
        )
        assertEquals(981, lahetettyKausi["arviointioikeudet"][0]["id"].asInt())
        assertTrue(lahetettyKausi["arviointioikeudet"][1]["id"].isNull, "tyhja tunniste lahetetaan nullina")
        assertEquals("fin", lahetettyKausi["arviointioikeudet"][0]["kieli"].asString())
        assertEquals("YT", lahetettyKausi["arviointioikeudet"][1]["tasot"][0].asString())
    }

    @Test
    fun `onnistunut lahetys palauttaa Rightin`() {
        val (client, server) = clientJaServer()
        server.expect(method(HttpMethod.POST)).andRespond(withStatus(HttpStatus.NO_CONTENT))

        assertEquals(true, client.laheta(request()).isRight())
    }
}
