package fi.oph.kitu.openapi

import fi.oph.kitu.DBContainerConfiguration
import fi.oph.kitu.util.defaultObjectMapper
import fi.oph.kitu.vkt.VktSuoritusRepository
import fi.oph.kitu.yki.arvioijat.YkiArvioijaRepository
import fi.oph.kitu.yki.suoritukset.YkiSuoritusRepository
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DynamicTest
import org.junit.jupiter.api.TestFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.http.HttpMethod
import org.springframework.http.MediaType
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.get
import org.springframework.test.web.servlet.request
import org.springframework.test.web.servlet.setup.MockMvcBuilders
import org.springframework.web.context.WebApplicationContext
import org.testcontainers.postgresql.PostgreSQLContainer
import tools.jackson.databind.JsonNode
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.test.fail

@SpringBootTest
@Import(DBContainerConfiguration::class)
class OpenApiExamplesTest(
    @param:Autowired private val context: WebApplicationContext,
    @param:Autowired private val postgres: PostgreSQLContainer,
    @param:Autowired private val vktSuoritusRepository: VktSuoritusRepository,
    @param:Autowired private val ykiSuoritusRepository: YkiSuoritusRepository,
    @param:Autowired private val ykiArvioijaRepository: YkiArvioijaRepository,
) {
    private lateinit var mockMvc: MockMvc

    @BeforeEach
    fun setup() {
        mockMvc = MockMvcBuilders.webAppContextSetup(context).build()
    }

    @AfterEach
    fun cleanup() {
        vktSuoritusRepository.deleteAll()
        ykiSuoritusRepository.deleteAll()
        ykiArvioijaRepository.deleteAll()
    }

    private data class Esimerkki(
        val method: HttpMethod,
        val path: String,
        val kohde: String,
        val nimi: String,
        val esimerkki: JsonNode,
    ) {
        val pyyntorunko = kohde == "requestBody"

        override fun toString() = "${method.name()} $path $kohde: $nimi"
    }

    @TestFactory
    fun `Swagger-esimerkit ovat kelvollista JSONia`() =
        esimerkit().map { esimerkki ->
            DynamicTest.dynamicTest(esimerkki.toString()) {
                lueEsimerkki(esimerkki)
            }
        }

    @TestFactory
    fun `Swagger-esimerkkipyynnot kelpaavat rajapinnalle`() =
        esimerkit().filter { it.pyyntorunko }.map { esimerkki ->
            DynamicTest.dynamicTest(esimerkki.toString()) {
                val response =
                    mockMvc
                        .request(esimerkki.method, esimerkki.path) {
                            contentType = MediaType.APPLICATION_JSON
                            accept = MediaType.APPLICATION_JSON
                            content = lueEsimerkki(esimerkki)
                        }.andReturn()
                        .response
                assertEquals(200, response.status, response.getContentAsString(Charsets.UTF_8))
            }
        }

    @Test
    fun `Swagger-dokumentaatiossa on esimerkkeja`() {
        val esimerkit = esimerkit()
        assertTrue(esimerkit.any { it.pyyntorunko }, "Ei yhtään pyyntörungon esimerkkiä: $esimerkit")
        assertTrue(esimerkit.any { !it.pyyntorunko }, "Ei yhtään vastauksen esimerkkiä: $esimerkit")
    }

    private fun lueEsimerkki(esimerkki: Esimerkki): String {
        val json =
            esimerkki.esimerkki["externalValue"]?.asString()?.let { haeUlkoinenEsimerkki(it) }
                ?: esimerkki.esimerkki["value"]?.let { if (it.isString) it.asString() else it.toString() }
                ?: fail("Esimerkiltä puuttuu sekä value että externalValue: ${esimerkki.esimerkki}")
        val node =
            runCatching { defaultObjectMapper.readTree(json) }
                .getOrElse { fail("Esimerkki ei ole kelvollista JSONia: ${it.message}\n$json") }
        assertTrue(node.isObject || node.isArray, "Esimerkki ei ole JSON-objekti tai -taulukko:\n$json")
        return json
    }

    private fun haeUlkoinenEsimerkki(externalValue: String): String {
        assertTrue(externalValue.startsWith(CONTEXT_PATH), "externalValue $externalValue ei ala $CONTEXT_PATH")
        val response =
            mockMvc
                .get(externalValue.removePrefix(CONTEXT_PATH))
                .andReturn()
                .response
        assertEquals(200, response.status, "externalValue $externalValue ei löydy")
        return response.getContentAsString(Charsets.UTF_8)
    }

    private fun esimerkit(): List<Esimerkki> {
        val apiDocs =
            defaultObjectMapper.readTree(
                mockMvc
                    .get("/v3/api-docs")
                    .andReturn()
                    .response
                    .getContentAsString(Charsets.UTF_8),
            )
        return apiDocs["paths"].properties().flatMap { (path, operaatiot) ->
            operaatiot.properties().flatMap { (method, operaatio) ->
                val sisallot =
                    listOfNotNull(operaatio["requestBody"]?.let { "requestBody" to it }) +
                        operaatio["responses"]?.properties().orEmpty().map { (koodi, vastaus) -> koodi to vastaus }
                sisallot.flatMap { (kohde, runko) ->
                    runko["content"]?.properties().orEmpty().flatMap { (_, sisalto) ->
                        sisalto["examples"]?.properties().orEmpty().map { (nimi, esimerkki) ->
                            Esimerkki(HttpMethod.valueOf(method.uppercase()), path, kohde, nimi, esimerkki)
                        } +
                            listOfNotNull(
                                sisalto["example"]?.let {
                                    Esimerkki(
                                        HttpMethod.valueOf(method.uppercase()),
                                        path,
                                        kohde,
                                        "example",
                                        defaultObjectMapper.createObjectNode().set("value", it),
                                    )
                                },
                            )
                    }
                }
            }
        }
    }

    companion object {
        private const val CONTEXT_PATH = "/kielitutkinnot"
    }
}
