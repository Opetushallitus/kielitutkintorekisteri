package fi.oph.kitu.vkt.uusintaoikeus

import fi.oph.kitu.auditlogs.AuditLogger
import fi.oph.kitu.i18n.LocalizationService
import fi.oph.kitu.util.TimeService
import io.opentelemetry.instrumentation.annotations.WithSpan
import org.springframework.stereotype.Service

@Service
class VktUusintaoikeusService(
    private val repository: VktUusintaoikeusRepository,
    private val timeService: TimeService,
    private val localizationService: LocalizationService,
    private val auditLogger: AuditLogger,
) {
    @WithSpan("VktUusintaoikeusService.findUusintaanOikeutetut")
    fun findUusintaanOikeutetut(params: VktUusintaoikeusParams): VktUusintaoikeusraportti {
        val tarkastelupaiva = params.tarkastelupaiva ?: timeService.today()
        val translations =
            localizationService
                .translationBuilder()
                .koodistot("vktosakoe", "vktkielitaito")
                .build()

        val rivit =
            params.rajaa(repository.findUusintaanOikeutetut(tarkastelupaiva)).map { rivi ->
                rivi.copy(
                    osakoeNimi = translations.get(rivi.osakoe),
                    tutkinnotNimet = rivi.tutkinnot.joinToString(", ") { translations.get(it) },
                )
            }

        return VktUusintaoikeusraportti(
            tarkastelupaiva = tarkastelupaiva,
            rivit = VktUusintaoikeusColumn.jarjesta(rivit, params.sortColumn, params.sortDirection),
        )
    }

    @WithSpan("VktUusintaoikeusService.findForCsv")
    fun findForCsv(params: VktUusintaoikeusParams): VktUusintaoikeusraportti =
        findUusintaanOikeutetut(params).also { raportti ->
            auditLogger.logAllInternalOnly("VKT uusintaoikeudet viewed", raportti.rivit) { rivi ->
                arrayOf("suoritus.oppijanumero" to rivi.suorittajanOid)
            }
        }
}
