package fi.oph.kitu.oppijanumero

import fi.oph.kitu.assertLeftIsThrowable
import fi.oph.kitu.oid.Oid
import fi.oph.kitu.util.defaultObjectMapper
import fi.oph.kitu.util.result.getOrThrow
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.springframework.http.HttpMethod
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.test.web.client.MockRestServiceServer
import org.springframework.test.web.client.match.MockRestRequestMatchers.content
import org.springframework.test.web.client.match.MockRestRequestMatchers.method
import org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo
import org.springframework.test.web.client.response.MockRestResponseCreators.withStatus
import org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess
import org.springframework.web.client.RestClient
import kotlin.test.assertEquals
import kotlin.test.assertIs

class OppijanumeroServiceTests {
    @Test
    fun `hetulistahaku lahettaa hetut taulukkona ja ratkaisee oppijanumerot`() {
        val baseUrl = "http://localhost:8080/oppijanumerorekisteri-service"
        val restClientBuilder = RestClient.builder().baseUrl(baseUrl)
        val mockServer = MockRestServiceServer.bindTo(restClientBuilder).build()

        mockServer
            .expect(requestTo("$baseUrl/henkilo/henkiloPerustietosByHenkiloHetuList"))
            .andExpect(method(HttpMethod.POST))
            .andExpect(content().json("""["010180-9026", "010866-9260"]"""))
            .andRespond(
                withSuccess(
                    """[{"oidHenkilo": "1.2.246.562.24.10691606777", "hetu": "010180-9026"}]""",
                    MediaType.APPLICATION_JSON,
                ),
            )
        mockServer
            .expect(requestTo("$baseUrl/yleistunniste/hae/1.2.246.562.24.10691606777"))
            .andRespond(
                withSuccess(
                    """{"oid": "1.2.246.562.24.10691606777", "oppijanumero": "1.2.246.562.24.59267607404"}""",
                    MediaType.APPLICATION_JSON,
                ),
            )

        val service =
            OppijanumeroServiceImpl(
                OppijanumerorekisteriClient(restClientBuilder.build(), baseUrl),
            )

        val tulos = service.getOppijanumerotByHetut(listOf("010180-9026", "010866-9260")).getOrThrow()

        assertEquals(
            mapOf("010180-9026" to Oid.parse("1.2.246.562.24.59267607404").getOrThrow()),
            tulos,
            "vain löytyneet palautetaan, ja arvona on oppijanumero",
        )
        mockServer.verify()
    }

    @Test
    fun `oppijanumero service does not find user`() {
        // Facade
        val restClientBuilder = RestClient.builder().baseUrl("http://localhost:8080/oppijanumerorekisteri-service")
        val mockServer = MockRestServiceServer.bindTo(restClientBuilder).build()
        mockServer
            .expect(requestTo("http://localhost:8080/oppijanumerorekisteri-service/yleistunniste/hae"))
            .andRespond(
                withStatus(HttpStatus.NOT_FOUND)
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(
                        """
                        {
                            "timestamp": 1734962667439,
                            "status":404,
                            "error":"Not Found",
                            "path":"/oppijanumerorekisteri-service/yleistunniste/hae"
                        }
                        """.trimIndent(),
                    ),
            )
        val oppijanumeroRestClient = restClientBuilder.build()
        val oppijanumeroService =
            OppijanumeroServiceImpl(
                OppijanumerorekisteriClient(
                    oppijanumeroRestClient,
                    "http://localhost:8080/oppijanumerorekisteri-service",
                ),
            )

        assertThrows<OppijanumeroException.OppijaNotFoundException> {
            oppijanumeroService
                .getMasterOid(
                    Oppija(
                        "Magdalena Testi",
                        "010866-9260",
                        "Magdalena",
                        "Sallinen-Testi",
                    ),
                ).getOrThrow()
        }
    }

    @Test
    fun `getOppijanumero hylkaa yksiloimattoman henkilon`() {
        val service = yleistunnisteVastaa("""{"oid":"$YKSILOIMATON","oppijanumero":null}""")

        assertIs<OppijanumeroException.OppijaNotIdentifiedException>(
            service.getOppijanumero(Oid.parse(YKSILOIMATON).getOrThrow()).leftOrNull(),
        )
    }

    @Test
    fun `getOppijanumero palauttaa oppijanumeron yksiloidylle henkilolle`() {
        val service = yleistunnisteVastaa("""{"oid":"$YKSILOIMATON","oppijanumero":"$YKSILOITY"}""")

        assertEquals(
            Oid.parse(YKSILOITY).getOrThrow(),
            service.getOppijanumero(Oid.parse(YKSILOIMATON).getOrThrow()).getOrThrow(),
        )
    }

    /**
     * getMasterOid putoaa tarkoituksella takaisin henkilo-OIDiin — siita riippuu mm. virkailijan
     * asiointikielen haku ja yksiloimattomien oppijoiden suoritusnakymat. Vahti sille, ettei
     * getOppijanumeron tiukkuus vuoda sinne.
     */
    @Test
    fun `getMasterOid putoaa yha takaisin henkilo-oidiin`() {
        val service = yleistunnisteVastaa("""{"oid":"$YKSILOIMATON","oppijanumero":null}""")

        assertEquals(
            Oid.parse(YKSILOIMATON).getOrThrow(),
            service.getMasterOid(Oid.parse(YKSILOIMATON).getOrThrow()).getOrThrow(),
        )
    }

    private fun yleistunnisteVastaa(json: String): OppijanumeroServiceImpl {
        val restClientBuilder = RestClient.builder().baseUrl(BASE_URL)
        MockRestServiceServer
            .bindTo(restClientBuilder)
            .build()
            .expect(requestTo("$BASE_URL/yleistunniste/hae/$YKSILOIMATON"))
            .andRespond(
                withStatus(HttpStatus.OK).contentType(MediaType.APPLICATION_JSON).body(json),
            )

        return OppijanumeroServiceImpl(OppijanumerorekisteriClient(restClientBuilder.build(), BASE_URL))
    }

    @Test
    fun `oppijanumero service received bad request`() {
        // Facade
        val restClientBuilder = RestClient.builder().baseUrl("http://localhost:8080/oppijanumerorekisteri-service")
        val mockServer = MockRestServiceServer.bindTo(restClientBuilder).build()
        mockServer
            .expect(requestTo("http://localhost:8080/oppijanumerorekisteri-service/yleistunniste/hae"))
            .andRespond(
                withStatus(HttpStatus.CONFLICT)
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(
                        """
                        {
                            "timestamp": 1734962667439,
                            "status":409,
                            "error":"Conflict",
                            "path":"/oppijanumerorekisteri-service/yleistunniste/hae"
                        }
                        """.trimIndent(),
                    ),
            )
        val oppijanumeroRestClient = restClientBuilder.build()
        val oppijanumeroService =
            OppijanumeroServiceImpl(
                OppijanumerorekisteriClient(
                    oppijanumeroRestClient,
                    "http://localhost:8080/oppijanumerorekisteri-service",
                ),
            )
        val result =
            oppijanumeroService.getMasterOid(
                Oppija(
                    "Magdalena Testi",
                    "Sallinen-Testi",
                    "Magdalena",
                    "010866-9260",
                ),
            )

        assertLeftIsThrowable<OppijanumeroException.BadRequest>(
            result,
            "Bad request to oppijanumero-service",
        )
    }

    @Test
    fun `Ramonan parsinta onnistuu`() {
        val json = """{
              "oidHenkilo": "1.2.246.562.98.89505889280",
              "hetu": "271258-9988",
              "kaikkiHetut": [],
              "passivoitu": false,
              "etunimet": "Ramona Ulla",
              "kutsumanimi": "Ramona Ulla",
              "sukunimi": "Tuulispää",
              "aidinkieli": {
                "kieliKoodi": "VK",
                "kieliTyyppi": null
              },
              "asiointiKieli": {
                "kieliKoodi": "VK",
                "kieliTyyppi": null
              },
              "kansalaisuus": [],
              "kasittelijaOid": "testidatantuonti",
              "syntymaaika": "1958-12-27",
              "sukupuoli": "2",
              "kotikunta": null,
              "oppijanumero": "1.2.246.562.98.89505889280",
              "turvakielto": false,
              "eiSuomalaistaHetua": false,
              "yksiloity": false,
              "yksiloityVTJ": true,
              "yksilointiYritetty": true,
              "duplicate": false,
              "created": 1741614259903,
              "modified": 1741614259903,
              "vtjsynced": null,
              "yhteystiedotRyhma": [],
              "yksilointivirheet": [],
              "passinumerot": [],
              "kielisyys": []
            }"""

        val obj = defaultObjectMapper.readValue(json, OppijanumerorekisteriHenkilo::class.java)

        assertEquals("1.2.246.562.98.89505889280", obj.oppijanumero)
    }

    companion object {
        private const val BASE_URL = "http://localhost:8080/oppijanumerorekisteri-service"
        private const val YKSILOIMATON = "1.2.246.562.24.10691606777"
        private const val YKSILOITY = "1.2.246.562.24.20281155246"
    }
}
