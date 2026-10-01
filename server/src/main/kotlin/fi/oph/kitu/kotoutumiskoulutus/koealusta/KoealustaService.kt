package fi.oph.kitu.kotoutumiskoulutus.koealusta

import fi.oph.kitu.auditlogs.AuditLogger
import fi.oph.kitu.auditlogs.PeerService
import fi.oph.kitu.kotoutumiskoulutus.suoritukset.CustomKielitestiSuoritusRepository
import fi.oph.kitu.kotoutumiskoulutus.suoritukset.KielitestiSuoritusRepository
import fi.oph.kitu.kotoutumiskoulutus.suoritukset.error.KielitestiSuoritusErrorRepository
import fi.oph.kitu.observability.setAttribute
import fi.oph.kitu.restclient.withLenientStringConverter
import fi.oph.kitu.util.TimeService
import io.opentelemetry.api.trace.Span
import io.opentelemetry.instrumentation.annotations.WithSpan
import org.springframework.beans.factory.annotation.Value
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import org.springframework.web.client.RestClient
import org.springframework.web.client.toEntity
import java.time.Instant
import kotlin.time.Duration

@Service
class KoealustaService(
    val restClientBuilder: RestClient.Builder,
    private val kielitestiSuoritusRepository: KielitestiSuoritusRepository,
    private val customKielitestiSuoritusRepository: CustomKielitestiSuoritusRepository,
    private val mappingService: KoealustaMappingService,
    private val auditLogger: AuditLogger,
    private val kielitestiSuoritusErrorRepository: KielitestiSuoritusErrorRepository,
    private val timeService: TimeService,
) {
    @Value($$"${kitu.kotoutumiskoulutus.koealusta.wstoken}")
    lateinit var koealustaToken: String

    @Value($$"${kitu.kotoutumiskoulutus.koealusta.baseurl}")
    lateinit var koealustaBaseUrl: String

    @Value($$"${kitu.kotoutumiskoulutus.koealusta.keskeneraiset.enrolmentWindow}")
    lateinit var keskeneraisetEnrolmentWindow: String

    private val restClient by lazy {
        restClientBuilder
            .baseUrl(koealustaBaseUrl)
            .withLenientStringConverter()
            .build()
    }

    @WithSpan("koealusta.import.suoritukset")
    fun importValmiitSuoritukset(from: Instant): Instant {
        val span = Span.current()

        val response =
            makeMoodleRequest(
                "local_completion_export_get_completions",
                "from" to from.epochSecond,
            )

        if (response.body == null) {
            return from
        }

        val (suoritukset, validationFailure) = mappingService.responseStringToKielitestiSuoritus(response.body!!)

        val validationErrors = mappingService.convertErrors(validationFailure?.validationErrors.orEmpty())
        val oppijanumeroErrors = mappingService.convertErrors(validationFailure?.oppijanumeroExceptions.orEmpty())
        kielitestiSuoritusErrorRepository.deleteAllByCompleted(true)
        kielitestiSuoritusErrorRepository.saveAll(validationErrors + oppijanumeroErrors)

        val savedSuoritukset =
            suoritukset
                .mapNotNull {
                    if (!customKielitestiSuoritusRepository.exists(it)) {
                        kielitestiSuoritusRepository.save(it)
                    } else {
                        null
                    }
                }.also {
                    auditLogger.logAllInternalOnly("Kielitesti suoritus imported", it) { suoritus ->
                        arrayOf(
                            "suoritus.id" to suoritus.id,
                            "principal.name" to "koealusta.import",
                            "peer.service" to PeerService.Koealusta.value,
                        )
                    }
                }

        span.setAttribute("db.saved", savedSuoritukset.count())

        if (validationFailure?.isNotEmpty() == true) {
            span.setAttribute("db.saved.error.validation", validationErrors.count())
            span.setAttribute("db.saved.error.onr", oppijanumeroErrors.count())
            return from
        }

        return suoritukset.mapNotNull { it.suoritusaika }.maxOrNull() ?: from
    }

    @WithSpan("koealusta.import.keskeneraiset")
    @Transactional
    fun importKeskeneraisetSuoritukset() {
        val span = Span.current()

        val body =
            makeMoodleRequest(
                "local_completion_export_get_incomplete_course_participants",
                enrolledFromParam(),
            ).body ?: return

        val (suoritukset, validationFailure) = mappingService.responseStringToKeskenerainenSuoritus(body)

        val validationErrors =
            mappingService
                .convertErrors(validationFailure?.validationErrors.orEmpty())
                .map { it.copy(completed = false) }
        kielitestiSuoritusErrorRepository.deleteAllByCompleted(false)
        kielitestiSuoritusErrorRepository.saveAll(validationErrors)

        customKielitestiSuoritusRepository.deleteAllKeskeneraiset()

        val savedSuoritukset =
            kielitestiSuoritusRepository
                .saveAll(suoritukset)
                .also {
                    auditLogger.logAllInternalOnly("Kielitesti keskeneräinen suoritus imported", it) { suoritus ->
                        arrayOf(
                            "suoritus.id" to suoritus.id,
                            "principal.name" to "koealusta.import",
                            "peer.service" to PeerService.Koealusta.value,
                        )
                    }
                }

        span.setAttribute("db.saved", savedSuoritukset.count())
        span.setAttribute("db.saved.error.validation", validationErrors.count())
    }

    private fun enrolledFromParam(): Pair<String, Any>? =
        keskeneraisetEnrolmentWindow
            .takeIf { it.isNotBlank() }
            ?.let { window ->
                "enrolledfrom" to timeService.now().minusSeconds(Duration.parse(window).inWholeSeconds).epochSecond
            }

    private fun makeMoodleRequest(
        remoteFunction: String,
        vararg params: Pair<String, Any>?,
    ): ResponseEntity<String> {
        val span = Span.current()
        span.setAttribute("function", remoteFunction)
        params.filterNotNull().forEach { (key, value) -> span.setAttribute(key, value.toString()) }

        return restClient
            .get()
            .uri { builder ->
                builder
                    .path("/webservice/rest/server.php")
                    .apply {
                        queryParam("wstoken", koealustaToken)
                        queryParam("wsfunction", remoteFunction)
                        queryParam("moodlewsrestformat", "json")
                        params.filterNotNull().forEach { (key, value) -> queryParam(key, value) }
                    }.build()
            }.accept(MediaType.APPLICATION_JSON)
            .retrieve()
            .toEntity<String>()
    }
}
