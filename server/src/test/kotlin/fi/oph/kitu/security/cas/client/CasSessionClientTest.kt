package fi.oph.kitu.security.cas.client

import org.junit.jupiter.api.BeforeEach
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpMethod
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.test.web.client.ExpectedCount.once
import org.springframework.test.web.client.MockRestServiceServer
import org.springframework.test.web.client.match.MockRestRequestMatchers.content
import org.springframework.test.web.client.match.MockRestRequestMatchers.header
import org.springframework.test.web.client.match.MockRestRequestMatchers.method
import org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo
import org.springframework.test.web.client.response.MockRestResponseCreators.withStatus
import org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess
import org.springframework.web.client.RestClient
import java.net.URI
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class CasSessionClientTest {
    private val casUrl = "https://virkailija.test/cas"
    private val service = "https://virkailija.test/lomake-editori/auth/cas"
    private val api = "https://virkailija.test/lomake-editori/api/x"

    private lateinit var cas: MockRestServiceServer
    private lateinit var palvelu: MockRestServiceServer
    private lateinit var client: CasSessionClient
    private lateinit var restClient: RestClient

    @BeforeEach
    fun setup() {
        val casBuilder = RestClient.builder()
        cas = MockRestServiceServer.bindTo(casBuilder).build()
        client = CasSessionClient(casBuilder.build(), casUrl, service, "kayttaja", "salasana", "ring-session")

        val apiBuilder = RestClient.builder().requestInterceptor(CasSessionInterceptor(client))
        palvelu = MockRestServiceServer.bindTo(apiBuilder).build()
        restClient = apiBuilder.build()
    }

    private fun odotaTgt(tgt: String = "$casUrl/v1/tickets/TGT-1") {
        cas
            .expect(once(), requestTo("$casUrl/v1/tickets"))
            .andExpect(method(HttpMethod.POST))
            .andExpect(content().formDataContains(mapOf("username" to "kayttaja", "password" to "salasana")))
            .andRespond(withStatus(HttpStatus.CREATED).location(URI(tgt)))
    }

    private fun odotaSt(
        tgt: String = "$casUrl/v1/tickets/TGT-1",
        st: String = "ST-1",
    ) {
        cas
            .expect(once(), requestTo(tgt))
            .andExpect(method(HttpMethod.POST))
            .andExpect(content().formDataContains(mapOf("service" to service)))
            .andRespond(withSuccess(st, MediaType.TEXT_PLAIN))
    }

    private fun odotaSessio(
        st: String = "ST-1",
        sessio: String = "s1",
    ) {
        cas
            .expect(once(), requestTo("$service?ticket=$st"))
            .andExpect(method(HttpMethod.GET))
            .andRespond(
                withStatus(HttpStatus.FOUND)
                    .header(HttpHeaders.SET_COOKIE, "ring-session=$sessio; Path=/lomake-editori; HttpOnly"),
            )
    }

    private fun kutsu(): String? =
        restClient
            .get()
            .uri(api)
            .retrieve()
            .body(String::class.java)

    @Test
    fun `kirjautuu TGT-ST-sessio-ketjulla ja kayttaa evastetta uudelleen`() {
        odotaTgt()
        odotaSt()
        odotaSessio()
        repeat(2) {
            palvelu
                .expect(requestTo(api))
                .andExpect(header(HttpHeaders.COOKIE, "ring-session=s1"))
                .andRespond(withSuccess("ok", MediaType.TEXT_PLAIN))
        }

        assertEquals("ok", kutsu())
        assertEquals("ok", kutsu())

        cas.verify()
        palvelu.verify()
    }

    @Test
    fun `vanhentunut sessio johtaa uuteen kirjautumiseen ja uusintayritykseen`() {
        odotaTgt()
        odotaSt()
        odotaSessio()
        odotaSt(st = "ST-2")
        odotaSessio(st = "ST-2", sessio = "s2")
        palvelu
            .expect(requestTo(api))
            .andExpect(header(HttpHeaders.COOKIE, "ring-session=s1"))
            .andRespond(withStatus(HttpStatus.UNAUTHORIZED))
        palvelu
            .expect(requestTo(api))
            .andExpect(header(HttpHeaders.COOKIE, "ring-session=s2"))
            .andRespond(withSuccess("ok", MediaType.TEXT_PLAIN))

        assertEquals("ok", kutsu())

        cas.verify()
        palvelu.verify()
    }

    @Test
    fun `vanhentunut TGT haetaan uudelleen`() {
        odotaTgt()
        odotaSt()
        odotaSessio()
        cas
            .expect(once(), requestTo("$casUrl/v1/tickets/TGT-1"))
            .andRespond(withStatus(HttpStatus.NOT_FOUND))
        odotaTgt(tgt = "$casUrl/v1/tickets/TGT-2")
        odotaSt(tgt = "$casUrl/v1/tickets/TGT-2", st = "ST-2")
        odotaSessio(st = "ST-2", sessio = "s2")

        assertEquals("ring-session=s1", client.sessionCookie())
        client.invalidate("ring-session=s1")
        assertEquals("ring-session=s2", client.sessionCookie())

        cas.verify()
    }

    @Test
    fun `hylatty kirjautuminen on virhe`() {
        cas
            .expect(once(), requestTo("$casUrl/v1/tickets"))
            .andRespond(withStatus(HttpStatus.UNAUTHORIZED))

        assertFailsWith<CasKirjautuminenEpaonnistui> { client.sessionCookie() }
    }
}
