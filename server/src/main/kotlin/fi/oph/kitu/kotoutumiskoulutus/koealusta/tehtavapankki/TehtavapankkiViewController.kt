package fi.oph.kitu.kotoutumiskoulutus.koealusta.tehtavapankki

import fi.oph.kitu.tehtavapankki.TehtavapankkiRepository
import io.opentelemetry.api.trace.Span
import io.opentelemetry.instrumentation.annotations.WithSpan
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.stereotype.Controller
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.ResponseStatus

@Controller
@RequestMapping("/koto-tehtavapankki", produces = ["text/html"])
@ConditionalOnProperty(
    name = ["spring.cloud.aws.s3.enabled"],
    havingValue = "true",
    matchIfMissing = true,
)
class TehtavapankkiViewController(
    private val tehtavapankkiService: TehtavapankkiService,
    private val tehtavapankkiRepository: TehtavapankkiRepository,
) {
    @GetMapping("")
    fun listView(): ResponseEntity<String> {
        val tehtavapaketit = tehtavapankkiService.listTehtavapaketit()
        val paketitByS3Avain =
            tehtavapankkiRepository
                .findPaketitByS3Avaimet(tehtavapaketit.values.flatten().map { it.key })
                .associateBy { it.s3Avain!! }
        val pakettiIdsByS3Avain = paketitByS3Avain.mapValues { (_, paketti) -> paketti.id!! }
        // Ryhmän tiivistelmä kuvaa juuri sen kansion uusinta versiota, jotta saman
        // courseid:n eri kansiot eivät näytä samaa (väärää) lähdetietoa.
        val latestPakettiByGroup =
            tehtavapaketit.mapValues { (_, objects) ->
                objects.firstNotNullOfOrNull { paketitByS3Avain[it.key] }
            }
        return ResponseEntity.ok(
            TehtavapankkiPage.render(tehtavapaketit, pakettiIdsByS3Avain, latestPakettiByGroup),
        )
    }

    @WithSpan
    @GetMapping("/paketti/{id}")
    fun pakettiView(
        @PathVariable id: Int,
    ): ResponseEntity<String> {
        Span.current().setAttribute("paketti.id", id.toLong())
        val paketti = tehtavapankkiRepository.findPakettiById(id) ?: throw TehtavapankkiNotFoundError()
        val ryhmat = tehtavapankkiRepository.findRyhmatByPakettiId(id)
        val tehtavat = tehtavapankkiRepository.findTehtavatByPakettiId(id)
        val tehtavaIds = tehtavat.mapNotNull { it.id }
        val vastauksetByTehtava = tehtavapankkiRepository.findVastauksetByTehtavaIds(tehtavaIds)
        val tiedostotByTehtava = tehtavapankkiRepository.findTiedostotByTehtavaIds(tehtavaIds)
        val tehtavatByRyhma = tehtavat.groupBy { it.ryhmaId }
        return ResponseEntity.ok(
            TehtavapakettiPage.render(
                paketti = paketti,
                ryhmat = ryhmat,
                tehtavatByRyhma = tehtavatByRyhma,
                vastauksetByTehtava = vastauksetByTehtava,
                tiedostotByTehtava = tiedostotByTehtava,
            ),
        )
    }

    @WithSpan
    @GetMapping("/lataa")
    fun downloadRedirect(
        @RequestParam key: String,
    ): ResponseEntity<Void> {
        val url = tehtavapankkiService.getTemporaryDownloadUrl(key) ?: throw TehtavapankkiNotFoundError()
        return ResponseEntity.status(HttpStatus.FOUND).location(url.toURI()).build()
    }
}

@ResponseStatus(value = HttpStatus.NOT_FOUND, reason = "Tehtäväpankin sisältöä ei löytynyt")
class TehtavapankkiNotFoundError : RuntimeException()
