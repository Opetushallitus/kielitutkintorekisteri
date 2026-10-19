package fi.oph.kitu.dev

import jakarta.annotation.PostConstruct
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.context.annotation.Profile
import org.springframework.core.env.Environment
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import org.springframework.web.context.WebApplicationContext

@RestController
@RequestMapping("/dev")
@Profile("local", "test", "e2e")
class YkiController(
    private val environment: Environment,
    private val applicationContext: WebApplicationContext,
) {
    private val logger: Logger = LoggerFactory.getLogger(javaClass)

    @PostConstruct
    fun init() = environment.exitIfProdLike(applicationContext, this)

    /**
     * Solki-stubi arvioijan lahetykselle. Virhepolkuja ei ohjata taalta: client maarittaa URLin,
     * joten testi ei voi valittaa stubille haluttua statusta. Ne katetaan yksikkotesteissa.
     */
    @PostMapping("/yki/import/arvioija")
    fun fakeSolkiArvioijaPost(
        @RequestBody body: Map<String, Any?>,
    ): ResponseEntity<Map<String, String>> {
        logger.info(
            "Solki-stubi vastaanotti arvioijan {} ({} kenttaa)",
            body["arvioijaOid"],
            body.size,
        )
        // Map eika JSON-teksti Stringina: String serialisoituisi JSON-merkkijonoksi, jolloin
        // stubin lankamuoto poikkeaisi oikeasta Solkista eika tunnus loytyisi vastauksesta.
        return ResponseEntity.ok(mapOf("tunnus" to "A00001"))
    }
}
