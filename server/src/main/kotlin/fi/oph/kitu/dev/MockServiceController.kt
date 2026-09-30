package fi.oph.kitu.dev

import fi.oph.kitu.dev.mockdata.generateRandomSsnBirthdayAndSex
import fi.oph.kitu.webmvc.ResourceNotFoundException
import fi.oph.kitu.yki.Sukupuoli
import org.springframework.context.annotation.Profile
import org.springframework.core.io.ClassPathResource
import org.springframework.http.HttpStatus
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.ResponseStatus
import org.springframework.web.bind.annotation.RestController
import java.time.format.DateTimeFormatter
import kotlin.random.Random

@RestController
@RequestMapping("/dev/mock")
@Profile("local", "e2e")
class MockServiceController {
    @GetMapping("oppijanumerorekisteri-service/henkilo/{oid}", produces = ["application/json"])
    fun getOnrHenkiloByOid(
        @PathVariable oid: String,
    ): String {
        val (hetu, syntymapaiva, sukupuoli) = generateRandomSsnBirthdayAndSex(random = getSeed(oid))
        return readMockResponse(
            "oppijanumerorekisteri-service/henkilo",
            "$oid.json",
            "default.json",
            replacements =
                mapOf(
                    "oidHenkilo" to oid,
                    "hetu" to hetu,
                    "sukupuoli" to (if (sukupuoli == Sukupuoli.M) "1" else "2"),
                    "syntymaaika" to syntymapaiva.format(DateTimeFormatter.ISO_LOCAL_DATE),
                ),
        )
    }

    private fun readMockResponse(
        servicePath: String,
        requestedResource: String,
        fallbackResource: String? = null,
        replacements: Map<String, String> = emptyMap(),
    ): String {
        val path = "/e2e-opintopolku-mocks/$servicePath"
        val resource =
            listOfNotNull(
                ClassPathResource("$path/$requestedResource"),
                fallbackResource?.let { ClassPathResource("$path/$it") },
            ).firstOrNull { it.exists() }
                ?.file
                ?.readText()
                ?: throw MockResourceNotFoundError()
        return replacements.entries.fold(resource) { acc, entry ->
            acc.replace("{{${entry.key}}}", entry.value)
        }
    }

    private fun getSeed(input: String): Random = Random(input.fold(0) { acc, char -> acc + char.code })
}

class MockResourceNotFoundError : ResourceNotFoundException("Mock-resurssia ei löytynyt")
