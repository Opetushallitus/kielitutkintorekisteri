package fi.oph.kitu.ataru

import fi.oph.kitu.restclient.withLenientStringConverter
import org.junit.jupiter.api.BeforeEach
import org.springframework.http.HttpMethod
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.test.web.client.MockRestServiceServer
import org.springframework.test.web.client.match.MockRestRequestMatchers.content
import org.springframework.test.web.client.match.MockRestRequestMatchers.jsonPath
import org.springframework.test.web.client.match.MockRestRequestMatchers.method
import org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo
import org.springframework.test.web.client.response.MockRestResponseCreators.withStatus
import org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess
import org.springframework.web.client.RestClient
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs

class AtaruClientImplTest {
    private val serviceUrl = "https://virkailija.test/lomake-editori"
    private lateinit var server: MockRestServiceServer
    private lateinit var client: AtaruClientImpl

    @BeforeEach
    fun setup() {
        val builder = RestClient.builder().withLenientStringConverter()
        server = MockRestServiceServer.bindTo(builder).build()
        client = AtaruClientImpl(builder.build(), serviceUrl)
    }

    @Test
    fun `listaus kulkee sivukursorin lapi ja valittaa lomakkeen ja ehdot`() {
        server
            .expect(requestTo("$serviceUrl/api/applications/list"))
            .andExpect(method(HttpMethod.POST))
            .andExpect(jsonPath("$['form-key']").value("lomake"))
            .andExpect(jsonPath("$['option-answers'][0].key").value("kentta"))
            .andExpect(jsonPath("$.sort.offset").doesNotExist())
            .andRespond(
                withSuccess(
                    """{"sort":{"order-by":"created-time","order":"asc","offset":{"key":"a2"}},
                       "applications":[{"key":"a1"},{"key":"a2"}]}""",
                    MediaType.APPLICATION_JSON,
                ),
            )
        server
            .expect(requestTo("$serviceUrl/api/applications/list"))
            .andExpect(jsonPath("$.sort.offset.key").value("a2"))
            .andRespond(
                withSuccess(
                    """{"sort":{"order-by":"created-time","order":"asc"},"applications":[{"key":"a3"}]}""",
                    MediaType.APPLICATION_JSON,
                ),
            )

        val otsakkeet =
            client
                .haeHakemusavaimet("lomake", listOf(OptionAnswer("kentta", listOf("x"))))
                .getOrNull()

        assertEquals(listOf("a1", "a2", "a3"), otsakkeet?.map { it.key })
        server.verify()
    }

    @Test
    fun `siirto pilkkoo avaimet eriin ja jasentaa vastausten muodot`() {
        val avaimet = (1..AtaruClientImpl.SIIRRON_ERAKOKO + 1).map { "h$it" }
        server
            .expect(requestTo("$serviceUrl/api/external/siirto?salliYksiloimattomat=true"))
            .andExpect(jsonPath("$.length()").value(AtaruClientImpl.SIIRRON_ERAKOKO))
            .andRespond(
                withSuccess(
                    """[{"hakemusOid":"h1","personOid":"1.2.246.562.24.1","state":"active",
                        "keyValues":{"a":"yksi","b":["x","y"],"c":[["g1"],["g2"]]},"tuntematon":1}]""",
                    MediaType.APPLICATION_JSON,
                ),
            )
        server
            .expect(requestTo("$serviceUrl/api/external/siirto?salliYksiloimattomat=true"))
            .andExpect(content().json("""["h${AtaruClientImpl.SIIRRON_ERAKOKO + 1}"]"""))
            .andRespond(withSuccess("[]", MediaType.APPLICATION_JSON))

        val hakemukset = client.haeHakemukset(avaimet).getOrNull()!!

        assertEquals(1, hakemukset.size)
        val keyValues = hakemukset.single().keyValues
        assertEquals("yksi", keyValues["a"]?.asString())
        assertEquals(2, keyValues["b"]?.size())
        assertEquals(true, keyValues["c"]?.get(0)?.isArray)
        server.verify()
    }

    @Test
    fun `virhestatukset ja jasentymaton vastaus palautetaan vasempana`() {
        server
            .expect(requestTo("$serviceUrl/api/external/siirto?salliYksiloimattomat=true"))
            .andRespond(withStatus(HttpStatus.CONFLICT))
        assertIs<AtaruException.BadRequest>(client.haeHakemukset(listOf("h1")).leftOrNull())

        server.reset()
        server
            .expect(requestTo("$serviceUrl/api/external/siirto?salliYksiloimattomat=true"))
            .andRespond(withStatus(HttpStatus.BAD_GATEWAY))
        assertIs<AtaruException.UnexpectedError>(client.haeHakemukset(listOf("h1")).leftOrNull())

        server.reset()
        server
            .expect(requestTo("$serviceUrl/api/external/siirto?salliYksiloimattomat=true"))
            .andRespond(withSuccess("{ei jsonia", MediaType.APPLICATION_JSON))
        assertIs<AtaruException.MalformedResponse>(client.haeHakemukset(listOf("h1")).leftOrNull())
    }
}
