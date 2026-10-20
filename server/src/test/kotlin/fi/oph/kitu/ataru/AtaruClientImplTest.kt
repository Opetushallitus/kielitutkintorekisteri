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
import kotlin.test.assertFalse
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
            .andExpect(jsonPath("$['states-and-filters']['processing-states-to-include'][0]").value("processed"))
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
                .haeHakemusavaimet(
                    "lomake",
                    listOf(OptionAnswer("kentta", listOf("x"))),
                    StatesAndFilters(processingStatesToInclude = listOf("processed")),
                ).getOrNull()

        assertEquals(listOf("a1", "a2", "a3"), otsakkeet?.map { it.key })
        server.verify()
    }

    @Test
    fun `oletussuodattimet paastavat kaikki hakemukset lapi`() {
        server
            .expect(requestTo("$serviceUrl/api/applications/list"))
            .andExpect(
                content().json(
                    """{"states-and-filters":{
                         "filters":{
                           "only-ssn":{"with-ssn":true,"without-ssn":true},
                           "only-identified":{"identified":true,"unidentified":true},
                           "active-status":{"active":true,"passive":true},
                           "eligibility-set-automatically":{"yes":true,"no":true}},
                         "processing-states-to-include":["unprocessed","processing","invited-to-interview",
                           "invited-to-exam","evaluating","valintaesitys","processed","information-request",
                           "processing-fee-overdue","processing-fee-paid","decision-fee-outstanding",
                           "decision-fee-overdue","decision-fee-paid","invoiced"],
                         "attachment-states-to-include":["not-checked","checked","incomplete-attachment",
                           "attachment-missing","overdue","no-requirements"]}}""",
                ),
            ).andRespond(
                withSuccess(
                    """{"sort":{"order-by":"created-time","order":"asc"},"applications":[]}""",
                    MediaType.APPLICATION_JSON,
                ),
            )

        client.haeHakemusavaimet("lomake", emptyList(), StatesAndFilters())

        server.verify()
    }

    @Test
    fun `siirto pilkkoo avaimet eriin ja jasentaa atarun litistetyt vastaukset`() {
        val avaimet = (1..AtaruClientImpl.SIIRRON_ERAKOKO + 1).map { "h$it" }
        server
            .expect(requestTo("$serviceUrl/api/external/siirto?salliYksiloimattomat=true"))
            .andExpect(jsonPath("$.length()").value(AtaruClientImpl.SIIRRON_ERAKOKO))
            .andRespond(
                withSuccess(
                    """[{"hakemusOid":"h1","personOid":"1.2.246.562.98.1",
                        "person":{"oidHenkilo":"1.2.246.562.98.1","etunimet":"Testi","sukunimi":"Hakija",
                          "kutsumanimi":"Testi","syntymaaika":"2016-05-02","turvakielto":false,"sukupuoli":"1",
                          "aidinkieli":{"kieliKoodi":"fi","kieliTyyppi":"suomi"},
                          "asiointiKieli":{"kieliKoodi":"fi","kieliTyyppi":"suomi"},"kansalaisuudet":["246"]},
                        "keyValues":{"a886d639-ef13-4442-9b0f-f0818c849450_0":"0",
                          "0b0f7c13-cab3-4169-8fff-076b73fb158e_0":"608f3e27-b57d-4211-bd19-a547b8e55e2b",
                          "0817208a-6b81-434c-9db6-8432d8a2d1c4":"1","email":"hakija@example.com"},
                        "attachments":{"0b0f7c13-cab3-4169-8fff-076b73fb158e_0":"608f3e27-b57d-4211-bd19-a547b8e55e2b"},
                        "hakukohdeReviews":[{"requirement":"processing-state","state":"processing","hakukohde":"form"}],
                        "hakukohdeAttachmentReviews":[],"applicationReviewNotes":[],"applicationPaymentStates":[],
                        "state":"active","hakuOid":null,"hakutoiveet":[],
                        "originallySubmitted":"2026-10-01T09:47:40.810+03:00",
                        "versionCreated":"2026-10-01T09:47:40.810+03:00",
                        "versionModified":"2026-10-01T14:08:54.164+03:00"}]""",
                    MediaType.APPLICATION_JSON,
                ),
            )
        server
            .expect(requestTo("$serviceUrl/api/external/siirto?salliYksiloimattomat=true"))
            .andExpect(content().json("""["h${AtaruClientImpl.SIIRRON_ERAKOKO + 1}"]"""))
            .andRespond(withSuccess("[]", MediaType.APPLICATION_JSON))

        val hakemukset = client.haeHakemukset(avaimet).getOrNull()!!

        val hakemus = hakemukset.single().getOrNull()!!
        assertEquals("1.2.246.562.98.1", hakemus.personOid)
        assertEquals("0", hakemus.keyValues["a886d639-ef13-4442-9b0f-f0818c849450_0"]?.asString())
        assertEquals("hakija@example.com", hakemus.keyValues["email"]?.asString())
        server.verify()
    }

    @Test
    fun `jasentymaton alkio ei kaada muita saman erän hakemuksia`() {
        server
            .expect(requestTo("$serviceUrl/api/external/siirto?salliYksiloimattomat=true"))
            .andRespond(
                withSuccess(
                    """[{"hakemusOid":"h1","personOid":"p1","keyValues":{"email":"hakija@example.com"}},
                        {"hakemusOid":"h2","personOid":"p2","keyValues":null},
                        {"hakemusOid":"h3","personOid":"p3","keyValues":[]},
                        {"personOid":"p4","keyValues":{}},
                        {"hakemusOid":"h5","keyValues":{}},
                        {"hakemusOid":"h6","personOid":" ","keyValues":{}},
                        42,
                        null]""",
                    MediaType.APPLICATION_JSON,
                ),
            )

        val tulokset = client.haeHakemukset(listOf("h1")).getOrNull()!!

        assertEquals("h1", tulokset[0].getOrNull()?.hakemusOid)
        assertEquals(
            listOf(
                SiirtoJasennysvirhe("h2", "p2", "keyValues"),
                SiirtoJasennysvirhe("h3", "p3", "keyValues"),
                SiirtoJasennysvirhe(null, "p4", "hakemusOid"),
                SiirtoJasennysvirhe("h5", null, "personOid"),
                SiirtoJasennysvirhe("h6", null, "personOid"),
                SiirtoJasennysvirhe(null, null, ""),
                SiirtoJasennysvirhe(null, null, ""),
            ),
            tulokset.drop(1).map { it.leftOrNull() },
        )
    }

    @Test
    fun `jasennysvirhe ei toista syotteen arvoa`() {
        server
            .expect(requestTo("$serviceUrl/api/external/siirto?salliYksiloimattomat=true"))
            .andRespond(
                withSuccess(
                    """[{"hakemusOid":"h1","personOid":"p1","keyValues":"010101-123N"}]""",
                    MediaType.APPLICATION_JSON,
                ),
            )

        val virhe =
            client
                .haeHakemukset(listOf("h1"))
                .getOrNull()!!
                .single()
                .leftOrNull()!!

        assertEquals(SiirtoJasennysvirhe("h1", "p1", "keyValues"), virhe)
        assertFalse(virhe.toString().contains("010101-123N"))
    }

    @Test
    fun `siirron henkilotietoja ei jasenneta`() {
        server
            .expect(requestTo("$serviceUrl/api/external/siirto?salliYksiloimattomat=true"))
            .andRespond(
                withSuccess(
                    """[{"hakemusOid":"h1","personOid":"p1","person":"ei objekti","keyValues":{}}]""",
                    MediaType.APPLICATION_JSON,
                ),
            )

        assertEquals(
            SiirtoHakemus("h1", "p1"),
            client
                .haeHakemukset(listOf("h1"))
                .getOrNull()!!
                .single()
                .getOrNull(),
        )
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

        server.reset()
        server
            .expect(requestTo("$serviceUrl/api/external/siirto?salliYksiloimattomat=true"))
            .andRespond(withSuccess("""{"hakemusOid":"h1"}""", MediaType.APPLICATION_JSON))
        assertIs<AtaruException.MalformedResponse>(client.haeHakemukset(listOf("h1")).leftOrNull())
    }
}
