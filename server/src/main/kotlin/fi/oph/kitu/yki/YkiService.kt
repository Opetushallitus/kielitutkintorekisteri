package fi.oph.kitu.yki

import fi.oph.kitu.auditlogs.AuditLogOperation
import fi.oph.kitu.auditlogs.AuditLogger
import fi.oph.kitu.oppijanumero.OppijanumeroService
import fi.oph.kitu.util.result.getOrThrow
import fi.oph.kitu.yki.suoritukset.YkiSuoritusEntity
import fi.oph.kitu.yki.suoritukset.YkiSuoritusFilter
import fi.oph.kitu.yki.suoritukset.YkiSuoritusOrder
import fi.oph.kitu.yki.suoritukset.YkiSuoritusRepository
import io.opentelemetry.instrumentation.annotations.WithSpan
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service

data class ExtendedFilter(
    val filter: YkiSuoritusFilter,
    val oppijanumeroUnavailable: Boolean,
)

@Service
class YkiService(
    private val suoritusRepository: YkiSuoritusRepository,
    private val auditLogger: AuditLogger,
    private val oppijanumeroService: OppijanumeroService,
) {
    private val logger: Logger = LoggerFactory.getLogger(javaClass)

    @WithSpan
    fun extendFilterWithLinkedOids(filter: YkiSuoritusFilter): ExtendedFilter =
        filter.extendHenkiloOids(oppijanumeroService).fold(
            ifLeft = { error ->
                logger.warn("Oppijanumeropalvelu ei vastannut, YKI-haku tehdään vain annetuilla oideilla", error)
                ExtendedFilter(filter, oppijanumeroUnavailable = true)
            },
            ifRight = { extended -> ExtendedFilter(extended, oppijanumeroUnavailable = false) },
        )

    @WithSpan
    fun extendFilterWithLinkedOidsOrThrow(filter: YkiSuoritusFilter): YkiSuoritusFilter =
        filter.extendHenkiloOids(oppijanumeroService).getOrThrow()

    fun findSuoritusById(id: Int): YkiSuoritusEntity? =
        suoritusRepository.findById(id).also { suoritus ->
            suoritus?.suorittajanOID?.let { oid ->
                auditLogger.log(AuditLogOperation.YkiSuoritusViewed, oid)
            }
        }

    @WithSpan
    fun allSuoritukset(
        versionHistory: Boolean,
        filter: YkiSuoritusFilter = YkiSuoritusFilter(),
    ): List<YkiSuoritusEntity> =
        suoritusRepository
            .find(distinct = !versionHistory, filter = filter)
            .toList()
            .also {
                auditLogger.logAllInternalOnly("Yki suoritus viewed", it) { suoritus ->
                    arrayOf(
                        "suoritus.id" to suoritus.id,
                    )
                }
            }

    @WithSpan
    fun allSuorituksetIncludingOpiskeluoikeusOid(
        versionHistory: Boolean,
        filter: YkiSuoritusFilter = YkiSuoritusFilter(),
    ): List<YkiSuoritusEntity> {
        val suoritukset = allSuoritukset(versionHistory, filter)
        val opiskeluoikeusOidit =
            suoritusRepository.findOpiskeluoikeusOidsBySolkiIds(
                suoritukset.filter { it.koskiOpiskeluoikeus == null }.map { it.solkiId },
            )
        return suoritukset.map { suoritus ->
            if (suoritus.koskiOpiskeluoikeus == null) {
                suoritus.copy(koskiOpiskeluoikeus = opiskeluoikeusOidit[suoritus.solkiId])
            } else {
                suoritus
            }
        }
    }

    @WithSpan
    fun countSuoritukset(
        filter: YkiSuoritusFilter = YkiSuoritusFilter(),
        versionHistory: Boolean = false,
    ): Long = suoritusRepository.countSuoritukset(filter = filter, distinct = !versionHistory)

    @WithSpan
    fun findSuorituksetPaged(
        filter: YkiSuoritusFilter = YkiSuoritusFilter(),
        order: YkiSuoritusOrder = YkiSuoritusOrder(),
        versionHistory: Boolean = false,
        limit: Int,
        offset: Int,
    ): List<YkiSuoritusEntity> =
        suoritusRepository
            .findForListView(
                filter = filter,
                order = order,
                distinct = !versionHistory,
                limit = limit,
                offset = offset,
            ).toList()
            .also {
                auditLogger.logAllInternalOnly("Yki suoritus viewed", it) { suoritus ->
                    arrayOf(
                        "suoritus.id" to suoritus.id,
                    )
                }
            }
}
