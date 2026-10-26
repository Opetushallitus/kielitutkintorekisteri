package fi.oph.kitu.kotoutumiskoulutus.suoritukset.error

import fi.oph.kitu.auditlogs.AuditLogger
import fi.oph.kitu.jdbc.SortDirection
import fi.oph.kitu.jdbc.findAllSorted
import fi.oph.kitu.organisaatiot.OrganisaatioService
import io.opentelemetry.instrumentation.annotations.WithSpan
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional

@Service
class KielitestiErrorService(
    private val kielitestiSuoritusErrorRepository: KielitestiSuoritusErrorRepository,
    private val organisaatioService: OrganisaatioService,
    private val auditLogger: AuditLogger,
) {
    @WithSpan
    fun getErrors(
        sortColumn: KielitestiSuoritusErrorColumn,
        sortDirection: SortDirection,
    ): List<KielitestiSuoritusError> {
        val errors = kielitestiSuoritusErrorRepository.findAllSorted(sortColumn.entityName, sortDirection)
        auditLogger.logAllInternalOnly("Kielitesti suoritus error viewed", errors) { error ->
            arrayOf("suoritus.error.id" to error.id)
        }
        val organisaatiot = organisaatioService.getOrganisaatiot()
        return errors.map { it.copy(organisaatio = organisaatiot.nimet[it.schoolOid]) }
    }

    fun countErrors(): Long = kielitestiSuoritusErrorRepository.count()

    @WithSpan
    @Transactional
    fun syncErrors(
        completed: Boolean,
        errors: Iterable<KielitestiSuoritusError>,
    ) {
        val vanhat =
            kielitestiSuoritusErrorRepository
                .findAllByCompleted(completed)
                .groupByTo(mutableMapOf()) { it.tunniste() }

        val tallennettavat =
            errors.mapNotNull { error ->
                val uusi = error.copy(completed = completed)
                val vanha = vanhat[uusi.tunniste()]?.removeFirstOrNull()
                if (vanha == null) {
                    uusi
                } else {
                    uusi.copy(id = vanha.id, virheenLuontiaika = vanha.virheenLuontiaika).takeIf { it != vanha }
                }
            }

        kielitestiSuoritusErrorRepository.deleteAll(vanhat.values.flatten())
        kielitestiSuoritusErrorRepository.saveAll(tallennettavat)
    }

    private data class Tunniste(
        val hetu: String?,
        val viesti: String,
    )

    private fun KielitestiSuoritusError.tunniste() = Tunniste(hetu, viesti)
}
