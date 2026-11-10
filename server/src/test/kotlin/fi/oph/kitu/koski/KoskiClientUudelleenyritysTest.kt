package fi.oph.kitu.koski

import fi.oph.kitu.oid.Oid
import fi.oph.kitu.util.result.getOrThrow
import org.junit.jupiter.api.Test
import org.springframework.context.annotation.AnnotationConfigApplicationContext
import org.springframework.context.annotation.Configuration
import org.springframework.http.MediaType
import org.springframework.retry.annotation.EnableRetry
import org.springframework.test.web.client.ExpectedCount
import org.springframework.test.web.client.MockRestServiceServer
import org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo
import org.springframework.test.web.client.response.MockRestResponseCreators.withBadRequest
import org.springframework.test.web.client.response.MockRestResponseCreators.withServerError
import org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess
import org.springframework.web.client.HttpClientErrorException
import org.springframework.web.client.RestClient
import java.util.function.Supplier
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class KoskiClientUudelleenyritysTest {
    @Configuration
    @EnableRetry
    class Uudelleenyritys

    private val builder = RestClient.builder().baseUrl("http://koski.test/")
    private val palvelin = MockRestServiceServer.bindTo(builder).build()

    private fun koskiClient(): KoskiClient =
        AnnotationConfigApplicationContext()
            .apply {
                register(Uudelleenyritys::class.java)
                registerBean("koskiClient", KoskiClient::class.java, Supplier { KoskiClient(builder.build()) })
                refresh()
            }.getBean(KoskiClient::class.java)

    private val pyynto =
        KoskiRequest(
            henkilö = KoskiRequest.Henkilo(Oid.parse("1.2.246.562.24.20281155246").getOrThrow()),
            opiskeluoikeudet = emptyList(),
        )

    @Test
    fun `palvelinvirhe yritetaan uudelleen`() {
        val client = koskiClient()
        palvelin.expect(requestTo("http://koski.test/oppija")).andRespond(withServerError())
        palvelin
            .expect(requestTo("http://koski.test/oppija"))
            .andRespond(
                withSuccess(
                    """{"henkilö":{"oid":"1.2.246.562.24.20281155246"},"opiskeluoikeudet":[]}""",
                    MediaType.APPLICATION_JSON,
                ),
            )

        assertEquals(emptyList(), client.put(pyynto).body!!.opiskeluoikeudet)
        palvelin.verify()
    }

    @Test
    fun `validointivirhetta ei yriteta uudelleen`() {
        val client = koskiClient()
        palvelin.expect(ExpectedCount.once(), requestTo("http://koski.test/oppija")).andRespond(withBadRequest())

        assertFailsWith<HttpClientErrorException> { client.put(pyynto) }
        palvelin.verify()
    }
}
