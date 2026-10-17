package fi.oph.kitu.organisaatiot

import arrow.core.Either
import fi.oph.kitu.restclient.withLenientStringConverter
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.test.web.client.MockRestServiceServer
import org.springframework.test.web.client.match.MockRestRequestMatchers.method
import org.springframework.test.web.client.match.MockRestRequestMatchers.queryParam
import org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo
import org.springframework.test.web.client.response.MockRestResponseCreators.withStatus
import org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess
import org.springframework.web.client.RestClient
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import org.springframework.http.HttpMethod as SpringHttpMethod

class OrganisaatiopalveluClientTest {
    private val serviceUrl = "http://organisaatio.test"

    private lateinit var builder: RestClient.Builder
    private lateinit var mockServer: MockRestServiceServer
    private lateinit var client: OrganisaatiopalveluClient

    @BeforeTest
    fun setup() {
        builder = RestClient.builder().withLenientStringConverter()
        mockServer = MockRestServiceServer.bindTo(builder).build()
        client = OrganisaatiopalveluClient(builder.build(), serviceUrl)
    }

    @Test
    fun `2xx ja oikein muotoiltu JSON palauttaa Right-haarana parsittu vastaus`() {
        mockServer
            .expect(requestTo("$serviceUrl/api/hierarkia/hae?aktiiviset=true&suunnitellut=false&lakkautetut=false"))
            .andExpect(method(SpringHttpMethod.GET))
            .andExpect(queryParam("aktiiviset", "true"))
            .andRespond(withSuccess(HIERARKIA_PAYLOAD, MediaType.APPLICATION_JSON))

        val result =
            client.get(
                endpoint = "api/hierarkia/hae",
                query =
                    mapOf(
                        "aktiiviset" to true,
                        "suunnitellut" to false,
                        "lakkautetut" to false,
                    ),
                responseType = GetOrganisaatiohierarkiaResponse::class.java,
            )

        val value = assertIs<Either.Right<GetOrganisaatiohierarkiaResponse>>(result).value
        assertEquals(1, value.numHits)
        assertEquals(1, value.organisaatiot.size)
        assertEquals(
            "1.2.246.562.10.1",
            value.organisaatiot
                .first()
                .oid
                .toString(),
        )
        mockServer.verify()
    }

    @Test
    fun `404 palauttaa NotFoundException-haaran`() {
        mockServer
            .expect(requestTo("$serviceUrl/api/1.2.246.562.10.999"))
            .andRespond(withStatus(HttpStatus.NOT_FOUND))

        val result =
            client.get("api/1.2.246.562.10.999", responseType = GetOrganisaatiohierarkiaResponse::class.java)

        val left = assertIs<Either.Left<OrganisaatiopalveluException>>(result).value
        assertIs<OrganisaatiopalveluException.NotFoundException>(left)
        mockServer.verify()
    }

    @Test
    fun `muu 4xx-virhe palauttaa BadRequest-haaran`() {
        mockServer
            .expect(requestTo("$serviceUrl/api/x"))
            .andRespond(withStatus(HttpStatus.BAD_REQUEST).body("validation failed"))

        val result = client.get("api/x", responseType = GetOrganisaatiohierarkiaResponse::class.java)

        val left = assertIs<Either.Left<OrganisaatiopalveluException>>(result).value
        val badRequest = assertIs<OrganisaatiopalveluException.BadRequest>(left)
        assertEquals(HttpStatus.BAD_REQUEST.value(), badRequest.response.statusCode.value())
        mockServer.verify()
    }

    @Test
    fun `5xx-virhe palauttaa UnexpectedError-haaran`() {
        mockServer
            .expect(requestTo("$serviceUrl/api/x"))
            .andRespond(withStatus(HttpStatus.INTERNAL_SERVER_ERROR))

        val result = client.get("api/x", responseType = GetOrganisaatiohierarkiaResponse::class.java)

        val left = assertIs<Either.Left<OrganisaatiopalveluException>>(result).value
        assertIs<OrganisaatiopalveluException.UnexpectedError>(left)
        mockServer.verify()
    }

    @Test
    fun `2xx mutta vahingoittunut JSON palauttaa MalformedResponse-haaran`() {
        mockServer
            .expect(requestTo("$serviceUrl/api/hierarkia/hae"))
            .andRespond(withSuccess("not json at all", MediaType.APPLICATION_JSON))

        val result =
            client.get(
                endpoint = "api/hierarkia/hae",
                responseType = GetOrganisaatiohierarkiaResponse::class.java,
            )

        val left = assertIs<Either.Left<OrganisaatiopalveluException>>(result).value
        assertIs<OrganisaatiopalveluException.MalformedResponse>(left)
        mockServer.verify()
    }

    @Test
    fun `2xx ja OrganisaatiopalveluError-runko palauttaa BadResponse-haaran`() {
        mockServer
            .expect(requestTo("$serviceUrl/api/hierarkia/hae"))
            .andRespond(withSuccess(ORGANISAATIO_ERROR_PAYLOAD, MediaType.APPLICATION_JSON))

        val result =
            client.get(
                endpoint = "api/hierarkia/hae",
                responseType = GetOrganisaatiohierarkiaResponse::class.java,
            )

        val left = assertIs<Either.Left<OrganisaatiopalveluException>>(result).value
        val badResponse = assertIs<OrganisaatiopalveluException.BadResponse>(left)
        assertEquals(500, badResponse.organisaatiopalveluError?.status)
        assertEquals("Internal Server Error", badResponse.organisaatiopalveluError?.error)
        mockServer.verify()
    }

    @Test
    fun `query-parametrit liitetaan URIin`() {
        mockServer
            .expect(requestTo("$serviceUrl/api/hierarkia/hae?aktiiviset=true&lakkautetut=true"))
            .andRespond(withSuccess(HIERARKIA_PAYLOAD, MediaType.APPLICATION_JSON))

        val result =
            client.get(
                endpoint = "api/hierarkia/hae",
                query = mapOf("aktiiviset" to true, "lakkautetut" to true),
                responseType = GetOrganisaatiohierarkiaResponse::class.java,
            )

        assertIs<Either.Right<GetOrganisaatiohierarkiaResponse>>(result)
        mockServer.verify()
    }

    companion object {
        private val HIERARKIA_PAYLOAD =
            """
            {
              "numHits": 1,
              "organisaatiot": [
                {
                  "aliOrganisaatioMaara": 0,
                  "alkuPvm": 0,
                  "children": [],
                  "kieletUris": [],
                  "kotipaikkaUri": "kunta_091",
                  "lyhytNimi": {"fi": "Yliopisto"},
                  "match": false,
                  "nimi": {"fi": "Yliopisto"},
                  "oid": "1.2.246.562.10.1",
                  "organisaatiotyypit": [],
                  "parentOid": null,
                  "parentOidPath": null,
                  "status": "AKTIIVINEN",
                  "toimipistekoodi": null,
                  "tyypit": [],
                  "yTunnus": null
                }
              ]
            }
            """.trimIndent()

        private val ORGANISAATIO_ERROR_PAYLOAD =
            """
            {
              "timestamp": "2026-01-01T12:00:00.000+00:00",
              "status": 500,
              "error": "Internal Server Error",
              "path": "/organisaatio-service/api/hierarkia/hae"
            }
            """.trimIndent()
    }
}
