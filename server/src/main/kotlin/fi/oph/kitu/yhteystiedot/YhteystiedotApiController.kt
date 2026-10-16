package fi.oph.kitu.yhteystiedot

import fi.oph.kitu.auditlogs.AUDIT_LOGGER_NAME
import fi.oph.kitu.auditlogs.add
import fi.oph.kitu.oid.Oid
import io.swagger.v3.oas.annotations.Operation
import io.swagger.v3.oas.annotations.tags.Tag
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.http.converter.json.JacksonJsonHttpMessageConverter
import org.springframework.security.core.context.SecurityContextHolder
import org.springframework.security.oauth2.jwt.Jwt
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import tools.jackson.databind.json.JsonMapper

/**
 * Keskitetty B2B-lookup todistuksen yhteystiedoille. KOSKI:lle myönnetty
 * `KIELITUTKINTOREKISTERI_TODISTUS_YHTEYSTIEDOT_LUKEMINEN` -käyttöoikeus
 * oikeuttaa hakemaan minkä tahansa opiskeluoikeuden yhteystiedot — tämä on
 * tarkoitettu toimintatapa, koska KOSKI tarvitsee tiedot kielitutkintojen
 * todistusten postitusta varten. Älä myönnä tätä oikeutta
 * organisaatiokohtaisille virkailijoille ilman erillistä tietuetason
 * pääsynvalvontaa.
 *
 * Jokainen kutsu kirjautuu audit-lokiin (kutsujan JWT-subject + haettu
 * tunniste + löytyikö) jälkikäteistä jäljitettävyyttä varten.
 */
@RestController
@RequestMapping("/yhteystiedot/api")
@Tag(name = "Todistuksen yhteystiedot")
class YhteystiedotApiController(
    val yhteystiedotService: YhteystiedotService,
) {
    private val auditLogger = LoggerFactory.getLogger(AUDIT_LOGGER_NAME)

    private fun auditLookup(
        field: String,
        value: String,
        found: Boolean,
    ) {
        val principalOid =
            (SecurityContextHolder.getContext().authentication?.principal as? Jwt)?.subject
        auditLogger
            .atInfo()
            .add(
                "event" to "yhteystiedot.lookup",
                "auth.principal_oid" to principalOid,
                "lookup.field" to field,
                "lookup.value" to value,
                "lookup.found" to found,
            ).log("Yhteystiedot lookup")
    }

    @GetMapping("/opiskeluoikeus/{oid}", produces = ["application/yhteystiedot+json", "application/json"])
    @Operation(
        summary = "Palauttaa todistuksen postitusosoitteen sekä toivotun kielen todistukselle",
        description = """
           Palauttaa tiedot opiskeluoikeus-OIDin perusteella. 
           Palauttaa 404, jos yhteystietoja ei löydy.
           Rajapinta vaatii käyttöoikeuden KIELITUTKINTOREKISTERI_TODISTUS_YHTEYSTIEDOT_LUKEMINEN.
        """,
        responses = [
            io.swagger.v3.oas.annotations.responses.ApiResponse(
                responseCode = "200",
                description = "Yhteystiedot löytyivät",
                content = [
                    io.swagger.v3.oas.annotations.media.Content(
                        mediaType = "application/yhteystiedot+json",
                        schema =
                            io.swagger.v3.oas.annotations.media
                                .Schema(implementation = Yhteystiedot::class),
                        examples = [
                            io.swagger.v3.oas.annotations.media.ExampleObject(
                                value = """
                        {
                            "sukunimi": "Meikäläinen",
                            "etunimet": "Matti Johannes",
                            "katuosoite": "Esimerkkikatu 123",
                            "postinumero": "00100",
                            "postitoimipaikka": "Helsinki",
                            "maa": {
                                "koodiarvo": "FIN",
                                "koodistoUri": "maatjavaltiot1"
                            },
                            "email": "matti.meikalainen@example.com",
                            "todistuskieli": {
                                "koodiarvo": "FI",
                                "koodistoUri": "kieli"
                            }
                        }
                    """,
                            ),
                        ],
                    ),
                ],
            ),
            io.swagger.v3.oas.annotations.responses.ApiResponse(
                responseCode = "404",
                description = "Yhteystietoja ei löytynyt annetulla opiskeluoikeus-OIDilla",
                content = [
                    io.swagger.v3.oas.annotations.media.Content(
                        mediaType = "application/yhteystiedot+json",
                        schema =
                            io.swagger.v3.oas.annotations.media
                                .Schema(implementation = YhteystietoNotFound::class),
                        examples = [
                            io.swagger.v3.oas.annotations.media.ExampleObject(
                                value = """
                        {
                            "request": "1.2.246.562.15.00000000001",
                            "error": "NOT_FOUND"
                        }
                    """,
                            ),
                        ],
                    ),
                ],
            ),
        ],
    )
    fun getYhteystiedotByOid(
        @PathVariable oid: Oid,
    ): ResponseEntity<*> {
        val result = yhteystiedotService.getYhteystiedotByOpiskeluoikeusOid(oid)
        auditLookup("opiskeluoikeus_oid", oid.toString(), result != null)
        return result
            ?.let { ResponseEntity(it, HttpStatus.OK) }
            ?: ResponseEntity(YhteystietoNotFound(oid.toString()), HttpStatus.NOT_FOUND)
    }

    @GetMapping(
        "/opiskeluoikeus/lahdejarjestelman/{tunnus}",
        produces = ["application/yhteystiedot+json", "application/json"],
    )
    @Operation(
        summary = "Palauttaa todistuksen postitusosoitteen sekä toivotun kielen todistukselle",
        description = """
           Palauttaa tiedot opiskeluoikeus-OIDin perusteella. 
           Palauttaa 404, jos yhteystietoja ei löydy.
           Rajapinta vaatii käyttöoikeuden KIELITUTKINTOREKISTERI_TODISTUS_YHTEYSTIEDOT_LUKEMINEN.
        """,
        responses = [
            io.swagger.v3.oas.annotations.responses.ApiResponse(
                responseCode = "200",
                description = "Yhteystiedot löytyivät",
                content = [
                    io.swagger.v3.oas.annotations.media.Content(
                        mediaType = "application/yhteystiedot+json",
                        schema =
                            io.swagger.v3.oas.annotations.media
                                .Schema(implementation = Yhteystiedot::class),
                        examples = [
                            io.swagger.v3.oas.annotations.media.ExampleObject(
                                value = """
                        {
                            "sukunimi": "Meikäläinen",
                            "etunimet": "Matti Johannes",
                            "katuosoite": "Esimerkkikatu 123",
                            "postinumero": "00100",
                            "postitoimipaikka": "Helsinki",
                            "maa": {
                                "koodiarvo": "FIN",
                                "koodistoUri": "maatjavaltiot1"
                            },
                            "email": "matti.meikalainen@example.com",
                            "todistuskieli": {
                                "koodiarvo": "FI",
                                "koodistoUri": "kieli"
                            }
                        }
                    """,
                            ),
                        ],
                    ),
                ],
            ),
            io.swagger.v3.oas.annotations.responses.ApiResponse(
                responseCode = "404",
                description = "Yhteystietoja ei löytynyt annetulla opiskeluoikeus-OIDilla",
                content = [
                    io.swagger.v3.oas.annotations.media.Content(
                        mediaType = "application/yhteystiedot+json",
                        schema =
                            io.swagger.v3.oas.annotations.media
                                .Schema(implementation = YhteystietoNotFound::class),
                        examples = [
                            io.swagger.v3.oas.annotations.media.ExampleObject(
                                value = """
                        {
                            "request": "1.2.246.562.15.00000000001",
                            "error": "NOT_FOUND"
                        }
                    """,
                            ),
                        ],
                    ),
                ],
            ),
        ],
    )
    fun getYhteystiedotByLahdenjarjestelmanTunnus(
        @PathVariable tunnus: String,
    ): ResponseEntity<*> {
        val result = yhteystiedotService.getYhteystiedotByLahdejarjestelmanTunnus(tunnus)
        auditLookup("lahdejarjestelman_tunnus", tunnus, result != null)
        return result
            ?.let { ResponseEntity(it, HttpStatus.OK) }
            ?: ResponseEntity(YhteystietoNotFound(tunnus), HttpStatus.NOT_FOUND)
    }
}

data class YhteystietoNotFound(
    val request: String,
) {
    val error = "NOT_FOUND"
}

@Configuration
class YhteystiedotApiJacksonConfig {
    @Bean
    fun yhteystiedotJacksonConverter(
        @Qualifier("koskiObjectMapper") koskiObjectMapper: JsonMapper,
    ): JacksonJsonHttpMessageConverter {
        val converter = JacksonJsonHttpMessageConverter(koskiObjectMapper)
        converter.supportedMediaTypes =
            listOf(
                MediaType.APPLICATION_JSON,
                MediaType("application", "yhteystiedot+json"),
            )
        return converter
    }
}
