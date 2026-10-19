package fi.oph.kitu.kotoutumiskoulutus

import fi.oph.kitu.i18n.isoDate
import fi.oph.kitu.jdbc.SortDirection
import fi.oph.kitu.kotoutumiskoulutus.suoritukset.KielitestiSuorituksetParams
import fi.oph.kitu.kotoutumiskoulutus.suoritukset.KielitestiSuoritusColumn
import fi.oph.kitu.kotoutumiskoulutus.suoritukset.KielitestiSuoritusService
import fi.oph.kitu.kotoutumiskoulutus.suoritukset.error.KielitestiErrorService
import fi.oph.kitu.kotoutumiskoulutus.suoritukset.error.KielitestiSuoritusErrorColumn
import fi.oph.kitu.kotoutumiskoulutus.suoritukset.error.KielitestiSuoritusErrorCsvColumn
import fi.oph.kitu.util.TimeService
import fi.oph.kitu.webmvc.csvAttachmentResponse
import io.swagger.v3.oas.annotations.tags.Tag
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.ModelAttribute
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody

@RestController
@RequestMapping("/koto-kielitesti/api")
@Tag(name = "Kotoutumiskoulutuksen kielitesti, sisäiset rajapinnat")
class KielitestiApiController(
    private val suoritusService: KielitestiSuoritusService,
    private val errorService: KielitestiErrorService,
    private val timeService: TimeService,
) {
    @GetMapping("/suoritukset", produces = ["text/csv"])
    fun getSuorituksetAsCsv(
        @ModelAttribute params: KielitestiSuorituksetParams = KielitestiSuorituksetParams(),
    ): ResponseEntity<StreamingResponseBody> =
        csvAttachmentResponse<KielitestiSuoritusColumn, _>(
            filename = params.csvFileName(),
            data = suoritusService.getSuorituksetForCsv(params.toFilter()),
            excludeTags = params.excludeTags(),
        )

    @GetMapping("/suoritukset/virheet", produces = ["text/csv"])
    fun getErrorsAsCsv(): ResponseEntity<StreamingResponseBody> =
        csvAttachmentResponse<KielitestiSuoritusErrorCsvColumn, _>(
            filename = "koto-virheet-${timeService.today().isoDate()}.csv",
            data = errorService.getErrors(KielitestiSuoritusErrorColumn.VirheenLuontiaika, SortDirection.DESC),
        )
}
