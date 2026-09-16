package fi.oph.kitu.dev

import fi.oph.kitu.config.isProduction
import fi.oph.kitu.config.isQA
import jakarta.annotation.PostConstruct
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.boot.SpringApplication
import org.springframework.context.annotation.Profile
import org.springframework.core.env.Environment
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import org.springframework.web.context.WebApplicationContext
import java.util.concurrent.ConcurrentHashMap
import kotlin.system.exitProcess

@RestController
@RequestMapping("/dev")
@Profile("local", "test", "e2e")
class YkiController(
    private val environment: Environment,
    private val applicationContext: WebApplicationContext,
) {
    private val logger: Logger = LoggerFactory.getLogger(javaClass)

    @PostConstruct
    fun init() {
        if (environment.isProduction() || environment.isQA()) {
            logger.error("Fatal error: MockLoginController loaded in a prod-like environment")
            exitProcess(SpringApplication.exit(applicationContext))
        }
    }

    private val responses = ConcurrentHashMap<String, ResponseEntity<String>>()

    @GetMapping("/yki/import/suoritukset")
    fun fakeYkiSuorituksetImport(): ResponseEntity<String> =
        responses.getOrDefault(
            "/yki/import/suoritukset",
            ResponseEntity.ok(
                """
                "1.2.246.562.24.20281155246","010180-9026","N","Öhman-Testi","Ranja Testi","EST","Testikuja 5","40100","Testilä","testi@testi.fi",183424,2024-12-13T07:10:13Z,2024-09-01,"fin","YT","1.2.246.562.10.14893989377","Jyväskylän yliopisto, Soveltavan kielentutkimuksen keskus",2024-12-13,5,5,,5,5,,2024-12-14,"OPH-5000-1234",1,1,"Suorituksesta jäänyt viimeinen tehtävä arvioimatta. Arvioinnin jälkeen puhumisen taitotasoa 6.",2024-12-14
                "1.2.246.562.24.59267607404","010116A9518","M","Kivinen-Testi","Petro Testi","EST","Testikuja 10","40100","Testinsuu","testi.petro@testi.fi",183425,2024-12-13T06:54:38Z,2024-09-01,"fin","YT","1.2.246.562.10.14893989377","Jyväskylän yliopisto, Soveltavan kielentutkimuksen keskus",2024-12-09,6,6,,6,6,,2024-11-01,"OPH-5002-2024",4,0,,2024-12-09
                ,"010180-9026","N","Öhman-Testi","Ranja Testi","EST","Testikuja 5","40100","Testilä","testi@testi.fi",183424,2024-10-30T13:53:56Z,2024-09-01,"fin","YT","1.2.246.562.10.14893989377","Jyväskylän yliopisto, Soveltavan kielentutkimuksen keskus",2024-11-14,5,5,,5,5,,,,0,0,,
                "1.2.246.562.24.59267607404","010116A9518","CORRUPTED","Kivinen-Testi","Petro Testi","","Testikuja 10","40100","Testinsuu","testi.petro@testi.fi",183425,2024-10-30T13:55:09Z,2024-09-01,"fin","YT","1.2.246.562.10.14893989377","Jyväskylän yliopisto, Soveltavan kielentutkimuksen keskus",2024-10-30,6,6,,6,6,,,,0,0,,
                """.trimIndent(),
            ),
        )

    /**
     * Solki-stubi vaiheen 9 lahetykselle. Virhepolkuja ei ohjata taalta: client maarittaa URLin,
     * joten testi ei voi valittaa stubille haluttua statusta. Ne katetaan yksikkotesteissa.
     */
    @PostMapping("/yki/import/arvioija")
    fun fakeSolkiArvioijaPost(
        @RequestBody body: Map<String, Any?>,
    ): ResponseEntity<String> {
        logger.info(
            "Solki-stubi vastaanotti arvioijan {} ({} kenttaa)",
            body["arvioijanOppijanumero"],
            body.size,
        )
        return ResponseEntity.ok("""{"tunnus":"A00001"}""")
    }
}
