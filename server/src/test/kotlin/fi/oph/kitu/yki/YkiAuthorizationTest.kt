package fi.oph.kitu.yki

import com.nimbusds.jose.jwk.source.ImmutableSecret
import fi.oph.kitu.DBContainerConfiguration
import fi.oph.kitu.dev.MockLoginController
import fi.oph.kitu.security.Authority
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.web.server.LocalServerPort
import org.springframework.context.annotation.Import
import org.springframework.http.HttpEntity
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpMethod
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.http.client.ClientHttpResponse
import org.springframework.security.oauth2.jose.jws.MacAlgorithm
import org.springframework.security.oauth2.jwt.JwsHeader
import org.springframework.security.oauth2.jwt.JwtClaimsSet
import org.springframework.security.oauth2.jwt.JwtEncoderParameters
import org.springframework.security.oauth2.jwt.NimbusJwtEncoder
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.TestPropertySource
import org.springframework.web.client.DefaultResponseErrorHandler
import org.springframework.web.client.RestTemplate
import java.util.Date
import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * Oauth2-ketju päättyy `anyRequest -> denyAll`, ja reittisäännöt ovat tarkkoja polkuja.
 * Api-reitti ilman omaa sääntöä vastaa siis tyhjällä 403:lla ennen kontrolleria — mikä
 * näyttää kutsujalle samalta kuin puuttuva oikeus kohdejärjestelmässä.
 *
 * Hetulistahaku on lisäksi rajattu nimettyihin kutsujiin, koska YKI_TALLENNUS on myönnetty
 * muillekin: sillä oikeudella ei pidä saada hetu → oppijanumero -hakua käyttöön.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Import(DBContainerConfiguration::class)
@ActiveProfiles("test")
@TestPropertySource(
    properties = [
        "spring.security.oauth2.resourceserver.jwt.issuer-uri=test-issuer",
        "server.servlet.context-path=/kielitutkinnot",
        "kitu.yki.hetulistahaku.sallitutKutsujat=migraatio-kayttaja",
    ],
)
class YkiAuthorizationTest {
    @LocalServerPort
    private var port: Int = 0

    private val restTemplate =
        RestTemplate().apply {
            errorHandler =
                object : DefaultResponseErrorHandler() {
                    override fun hasError(response: ClientHttpResponse): Boolean = false
                }
        }

    @Test
    fun `hetulistahaku on sallittu nimetylle kutsujalle`() {
        val response =
            postJson(
                "/yki/api/oppijanumero-haku-hetulista",
                """{"hetut": ["010180-9026"]}""",
                token(subject = "migraatio-kayttaja", authorities = arrayOf(Authority.YKI_TALLENNUS)),
            )
        assertEquals(HttpStatus.OK, response.statusCode)
    }

    @Test
    fun `hetulistahaku on kielletty muulta kutsujalta vaikka YKI_TALLENNUS on`() {
        val response =
            postJson(
                "/yki/api/oppijanumero-haku-hetulista",
                """{"hetut": ["010180-9026"]}""",
                token(subject = "solki", authorities = arrayOf(Authority.YKI_TALLENNUS)),
            )
        assertEquals(HttpStatus.FORBIDDEN, response.statusCode)
    }

    @Test
    fun `sallittu kutsuja ei tarvitse muita oikeuksia`() {
        val response =
            postJson(
                "/yki/api/oppijanumero-haku-hetulista",
                """{"hetut": ["010180-9026"]}""",
                token(subject = "migraatio-kayttaja", authorities = emptyArray()),
            )
        assertEquals(HttpStatus.OK, response.statusCode)
    }

    @Test
    fun `nimilla tehty oppijanumerohaku on sallittu YKI_TALLENNUS-oikeudella`() {
        val response =
            postJson(
                "/yki/api/oppijanumero-haku",
                """{"hetu": "010180-9026", "etunimet": "Ranja Testi", "sukunimi": "Öhman-Testi"}""",
                token(subject = "solki", authorities = arrayOf(Authority.YKI_TALLENNUS)),
            )
        assertEquals(HttpStatus.OK, response.statusCode)
    }

    private fun postJson(
        path: String,
        body: String,
        bearer: String,
    ): ResponseEntity<String> {
        val headers =
            HttpHeaders().apply {
                contentType = MediaType.APPLICATION_JSON
                accept = listOf(MediaType.APPLICATION_JSON)
                setBearerAuth(bearer)
            }
        return restTemplate.exchange(
            "http://localhost:$port/kielitutkinnot$path",
            HttpMethod.POST,
            HttpEntity(body, headers),
            String::class.java,
        )
    }

    private fun token(
        subject: String,
        authorities: Array<Authority>,
    ): String {
        val header = JwsHeader.with(MacAlgorithm.HS256).build()
        val claims =
            JwtClaimsSet
                .builder()
                .subject(subject)
                .audience(listOf("test-audience"))
                .expiresAt(Date(System.currentTimeMillis() + 60_000).toInstant())
                .claim("scope", authorities.joinToString(" ") { it.role() })
                .build()
        return NimbusJwtEncoder(ImmutableSecret(MockLoginController.E2E_TEST_SECRET_KEY.encoded))
            .encode(JwtEncoderParameters.from(header, claims))
            .tokenValue
    }
}
