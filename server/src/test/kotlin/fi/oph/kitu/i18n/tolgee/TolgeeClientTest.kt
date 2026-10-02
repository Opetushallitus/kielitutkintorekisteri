package fi.oph.kitu.i18n.tolgee

import io.opentelemetry.api.OpenTelemetry
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.test.web.client.MockRestServiceServer
import org.springframework.test.web.client.match.MockRestRequestMatchers.content
import org.springframework.test.web.client.match.MockRestRequestMatchers.header
import org.springframework.test.web.client.match.MockRestRequestMatchers.jsonPath
import org.springframework.test.web.client.match.MockRestRequestMatchers.method
import org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo
import org.springframework.test.web.client.response.MockRestResponseCreators.withStatus
import org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess
import org.springframework.web.client.RestClient
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import org.springframework.http.HttpMethod as SpringHttpMethod

private const val BASE_URL = "https://app.tolgee.io"
private const val NAMESPACE = "kielitutkintorekisteri"
private const val API_KEY = "tgpak_testiavain"

class TolgeeClientTest {
    private lateinit var mockServer: MockRestServiceServer
    private lateinit var client: TolgeeClient

    @BeforeTest
    fun setup() {
        val builder = RestClient.builder()
        mockServer = MockRestServiceServer.bindTo(builder).build()
        val restClient = TolgeeRestClientConfig(builder, BASE_URL, API_KEY).restClient()
        client = TolgeeClientImpl(restClient, OpenTelemetry.noop().getTracer("test"), NAMESPACE)
    }

    private fun keysPageUrl(page: Int) =
        "$BASE_URL/v2/projects/translations?filterNamespace=$NAMESPACE&languages=fi&size=200&page=$page"

    @Test
    fun `fetchKeys sivuttaa, lahettaa API-avaimen ja palauttaa avaimen tunnisteet`() {
        mockServer
            .expect(requestTo(keysPageUrl(0)))
            .andExpect(method(SpringHttpMethod.GET))
            .andExpect(header("X-API-Key", API_KEY))
            .andRespond(
                withSuccess(
                    """
                    {
                      "_embedded": {"keys": [
                        {"keyId": 1, "keyName": "nav.yki", "keyNamespace": "$NAMESPACE"},
                        {"keyId": 2, "keyName": "nav.vkt", "keyNamespace": "$NAMESPACE"}
                      ]},
                      "page": {"totalPages": 2}
                    }
                    """.trimIndent(),
                    MediaType.APPLICATION_JSON,
                ),
            )
        mockServer
            .expect(requestTo(keysPageUrl(1)))
            .andExpect(header("X-API-Key", API_KEY))
            .andRespond(
                withSuccess(
                    """
                    {
                      "_embedded": {"keys": [{"keyId": 3, "keyName": "nav.koto", "keyNamespace": "$NAMESPACE"}]},
                      "page": {"totalPages": 2}
                    }
                    """.trimIndent(),
                    MediaType.APPLICATION_JSON,
                ),
            )

        val keys = client.fetchKeys()

        assertEquals(mapOf("nav.yki" to 1L, "nav.vkt" to 2L, "nav.koto" to 3L), keys)
        mockServer.verify()
    }

    @Test
    fun `fetchKeys sivuuttaa toisen nimiavaruuden avaimet`() {
        mockServer
            .expect(requestTo(keysPageUrl(0)))
            .andRespond(
                withSuccess(
                    """
                    {
                      "_embedded": {"keys": [
                        {"keyId": 1, "keyName": "nav.yki", "keyNamespace": "$NAMESPACE"},
                        {"keyId": 9, "keyName": "koski.oma", "keyNamespace": "koski"},
                        {"keyId": 8, "keyName": "juureton"}
                      ]},
                      "page": {"totalPages": 1}
                    }
                    """.trimIndent(),
                    MediaType.APPLICATION_JSON,
                ),
            )

        assertEquals(mapOf("nav.yki" to 1L), client.fetchKeys())
        mockServer.verify()
    }

    @Test
    fun `createKeys vie avaimet import-resolvableen NEW-resoluutiolla ja nimiavaruudella`() {
        mockServer
            .expect(requestTo("$BASE_URL/v2/projects/keys/import-resolvable"))
            .andExpect(method(SpringHttpMethod.POST))
            .andExpect(header("X-API-Key", API_KEY))
            .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
            .andExpect(jsonPath("$.keys.length()").value(1))
            .andExpect(jsonPath("$.keys[0].name").value("nav.yki"))
            .andExpect(jsonPath("$.keys[0].namespace").value(NAMESPACE))
            .andExpect(jsonPath("$.keys[0].translations.fi.text").value("Yleinen kielitutkinto"))
            .andExpect(jsonPath("$.keys[0].translations.fi.resolution").value("NEW"))
            .andRespond(withSuccess())

        client.createKeys(mapOf("nav.yki" to "Yleinen kielitutkinto"))

        mockServer.verify()
    }

    @Test
    fun `createKeys luo tyhjan lahdetekstin avaimen ilman suomenkielista kaannosta`() {
        mockServer
            .expect(requestTo("$BASE_URL/v2/projects/keys/import-resolvable"))
            .andExpect(jsonPath("$.keys[0].name").value("ohje.etusivu"))
            .andExpect(jsonPath("$.keys[0].translations").isEmpty())
            .andRespond(withSuccess())

        client.createKeys(mapOf("ohje.etusivu" to ""))

        mockServer.verify()
    }

    @Test
    fun `createKeys palastelee yli sadan avaimen erat`() {
        repeat(2) {
            mockServer
                .expect(requestTo("$BASE_URL/v2/projects/keys/import-resolvable"))
                .andRespond(withSuccess())
        }

        client.createKeys((1..150).associate { "avain.$it" to "teksti $it" })

        mockServer.verify()
    }

    @Test
    fun `deleteKeys lahettaa tunnisteet DELETE-rungossa`() {
        mockServer
            .expect(requestTo("$BASE_URL/v2/projects/keys"))
            .andExpect(method(SpringHttpMethod.DELETE))
            .andExpect(header("X-API-Key", API_KEY))
            .andExpect(jsonPath("$.ids.length()").value(3))
            .andExpect(jsonPath("$.ids[0]").value(7))
            .andRespond(withSuccess())

        client.deleteKeys(listOf(7L, 8L, 9L))

        mockServer.verify()
    }

    @Test
    fun `deleteKeys palastelee yli sadan tunnisteen erat`() {
        repeat(2) {
            mockServer
                .expect(requestTo("$BASE_URL/v2/projects/keys"))
                .andRespond(withSuccess())
        }

        client.deleteKeys((1L..150L).toList())

        mockServer.verify()
    }

    @Test
    fun `deleteKeys sietaa 4xx-vastauksen esimerkiksi jo poistetusta avaimesta`() {
        mockServer
            .expect(requestTo("$BASE_URL/v2/projects/keys"))
            .andRespond(withStatus(HttpStatus.NOT_FOUND))

        client.deleteKeys(listOf(7L))

        mockServer.verify()
    }
}
