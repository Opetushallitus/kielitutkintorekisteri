package fi.oph.kitu.yhteystiedot

import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import fi.oph.kitu.DBContainerConfiguration
import fi.oph.kitu.auditlogs.AUDIT_LOGGER_NAME
import fi.oph.kitu.dev.mockdata.generateRandomYkiSuoritusEntity
import fi.oph.kitu.oid.Oid
import fi.oph.kitu.util.result.getOrThrow
import fi.oph.kitu.yki.suoritukset.YkiSuoritusRepository
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.http.MediaType
import org.springframework.security.test.web.reactive.server.SecurityMockServerConfigurers.springSecurity
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.get
import org.springframework.test.web.servlet.setup.MockMvcBuilders
import org.springframework.web.context.WebApplicationContext
import org.testcontainers.postgresql.PostgreSQLContainer
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import ch.qos.logback.classic.Logger as LogbackLogger

@SpringBootTest
@Import(DBContainerConfiguration::class)
class YhteystiedotApiControllerTest {
    @Autowired
    private lateinit var context: WebApplicationContext

    @Autowired private var postgres: PostgreSQLContainer? = null
    private var mockMvc: MockMvc? = null

    @Autowired private lateinit var ykiSuoritukset: YkiSuoritusRepository

    private val auditLogbackLogger = LoggerFactory.getLogger(AUDIT_LOGGER_NAME) as LogbackLogger
    private val listAppender = ListAppender<ILoggingEvent>()

    @BeforeEach
    fun setup() {
        mockMvc =
            MockMvcBuilders
                .webAppContextSetup(context)
                .apply { springSecurity() }
                .build()
        listAppender.start()
        auditLogbackLogger.addAppender(listAppender)
    }

    @AfterEach
    fun cleanup() {
        listAppender.stop()
        auditLogbackLogger.detachAppender(listAppender)
    }

    private fun lookupAuditEntry() = listAppender.list.single { it.message == "Yhteystiedot lookup" }

    private fun auditFields() = lookupAuditEntry().keyValuePairs.associate { it.key to it.value }

    @Test
    fun `Hakeminen olemassaolevalla oidilla palauttaa yhteystiedot`() {
        val opiskeluoikeusOid = Oid.parse("1.2.246.562.24.20281155246").getOrThrow()
        val suoritus =
            generateRandomYkiSuoritusEntity().copy(
                koskiOpiskeluoikeus = opiskeluoikeusOid,
                koskiSiirtoKasitelty = true,
            )
        ykiSuoritukset.save(suoritus, updateOnConflict = true)

        mockMvc!!
            .get("/yhteystiedot/api/opiskeluoikeus/$opiskeluoikeusOid") {
                accept = MediaType.APPLICATION_JSON
            }.andExpect {
                status { isOk() }
                content {
                    json(
                        """
                        {
                            "sukunimi": "${suoritus.sukunimi}",
                            "etunimet": "${suoritus.etunimet}",
                            "katuosoite": "${suoritus.katuosoite}",
                            "postinumero": "${suoritus.postinumero}",
                            "postitoimipaikka": "${suoritus.postitoimipaikka}",
                            "maa": {
                                "koodiarvo": "${suoritus.maa}",
                                "koodistoUri": "maatjavaltiot1"
                            },
                            "email": "${suoritus.email}",
                            "todistuskieli": {
                                "koodiarvo": "${suoritus.todistuskieli!!.kieliKoodistoarvo}",
                                "koodistoUri": "kieli"
                            }
                        }
                        """.trimIndent(),
                    )
                }
            }

        val audit = auditFields()
        assertEquals("opiskeluoikeus_oid", audit["lookup.field"])
        assertEquals(opiskeluoikeusOid.toString(), audit["lookup.value"])
        assertEquals(true, audit["lookup.found"])
    }

    @Test
    fun `Hakeminen muulla oidilla palauttaa yhteystiedot`() {
        val opiskeluoikeusOid = "1.2.246.562.24.00000000000"

        mockMvc!!
            .get("/yhteystiedot/api/opiskeluoikeus/$opiskeluoikeusOid") {
                accept = MediaType.APPLICATION_JSON
            }.andExpect {
                status { isNotFound() }
                content {
                    json(
                        """
                        {
                            "request": "$opiskeluoikeusOid",
                            "error": "NOT_FOUND"
                        }
                        """.trimIndent(),
                    )
                }
            }

        val audit = auditFields()
        assertEquals("opiskeluoikeus_oid", audit["lookup.field"])
        assertEquals(opiskeluoikeusOid, audit["lookup.value"])
        assertEquals(false, audit["lookup.found"])
        assertTrue(
            audit.containsKey("auth.principal_oid"),
            "principal_oid field must be present even when JWT is absent",
        )
    }

    @Test
    fun `Hakeminen olemassaolevalla lahdejarjestelman tunnuksella palauttaa yhteystiedot`() {
        val tunnus = "yki.123456"
        val suoritus =
            generateRandomYkiSuoritusEntity().copy(
                lahdejarjestelmanTunnus = tunnus,
                koskiSiirtoKasitelty = true,
            )
        ykiSuoritukset.save(suoritus, updateOnConflict = true)

        mockMvc!!
            .get("/yhteystiedot/api/opiskeluoikeus/lahdejarjestelman/$tunnus") {
                accept = MediaType.APPLICATION_JSON
            }.andExpect {
                status { isOk() }
                content {
                    json(
                        """
                        {
                            "sukunimi": "${suoritus.sukunimi}",
                            "etunimet": "${suoritus.etunimet}",
                            "katuosoite": "${suoritus.katuosoite}",
                            "postinumero": "${suoritus.postinumero}",
                            "postitoimipaikka": "${suoritus.postitoimipaikka}",
                            "maa": {
                                "koodiarvo": "${suoritus.maa}",
                                "koodistoUri": "maatjavaltiot1"
                            },
                            "email": "${suoritus.email}",
                            "todistuskieli": {
                                "koodiarvo": "${suoritus.todistuskieli!!.kieliKoodistoarvo}",
                                "koodistoUri": "kieli"
                            }
                        }
                        """.trimIndent(),
                    )
                }
            }

        val audit = auditFields()
        assertEquals("lahdejarjestelman_tunnus", audit["lookup.field"])
        assertEquals(tunnus, audit["lookup.value"])
        assertEquals(true, audit["lookup.found"])
    }

    @Test
    fun `Hakeminen tuntemattomalla lahdejarjestelman tunnuksella palauttaa 404`() {
        val tunnus = "yki.tuntematon-tunnus"

        mockMvc!!
            .get("/yhteystiedot/api/opiskeluoikeus/lahdejarjestelman/$tunnus") {
                accept = MediaType.APPLICATION_JSON
            }.andExpect {
                status { isNotFound() }
                content {
                    json(
                        """
                        {
                            "request": "$tunnus",
                            "error": "NOT_FOUND"
                        }
                        """.trimIndent(),
                    )
                }
            }

        val audit = auditFields()
        assertEquals("lahdejarjestelman_tunnus", audit["lookup.field"])
        assertEquals(tunnus, audit["lookup.value"])
        assertEquals(false, audit["lookup.found"])
    }
}
