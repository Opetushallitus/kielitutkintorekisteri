package fi.oph.kitu.vkt.uusintaoikeus

import fi.oph.kitu.auditlogs.AuditLogger
import fi.oph.kitu.i18n.LocalizationService
import fi.oph.kitu.oid.Oid
import fi.oph.kitu.oppijanumero.OppijanumeroService
import fi.oph.kitu.oppijanumero.OppijanumerorekisteriHenkilo
import fi.oph.kitu.oppijanumero.yhteystiedot
import fi.oph.kitu.util.TimeService
import fi.oph.kitu.util.result.getOrThrow
import io.opentelemetry.instrumentation.annotations.WithSpan
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service

@Service
class VktUusintaoikeusService(
    private val repository: VktUusintaoikeusRepository,
    private val timeService: TimeService,
    private val localizationService: LocalizationService,
    private val auditLogger: AuditLogger,
    private val oppijanumeroService: OppijanumeroService,
) {
    private val logger: Logger = LoggerFactory.getLogger(javaClass)

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
    fun findForCsv(params: VktUusintaoikeusParams): VktUusintaoikeusraportti {
        val raportti = findUusintaanOikeutetut(params)
        if (params.piilotaHenkilotiedot) {
            return raportti.also { kirjaaKatselu("VKT uusintaoikeudet viewed", it) }
        }

        val henkilot = haeHenkilot(raportti.rivit.mapNotNull { it.oid() }.toSet())
        return raportti
            .copy(rivit = raportti.rivit.map { it.copy(yhteystiedot = henkilot[it.oid()]?.yhteystiedot()) })
            .also { kirjaaKatselu("VKT uusintaoikeudet ja yhteystiedot viewed", it) }
    }

    private fun haeHenkilot(oidit: Set<Oid>): Map<Oid, OppijanumerorekisteriHenkilo> =
        runCatching { oppijanumeroService.getHenkilotByHenkiloOids(oidit).getOrThrow() }
            .getOrElse { virhe ->
                logger.warn("Uusintaan oikeutettujen yhteystietoja ei saatu oppijanumerorekisteristä", virhe)
                emptyMap()
            }

    private fun kirjaaKatselu(
        viesti: String,
        raportti: VktUusintaoikeusraportti,
    ) {
        auditLogger.logAllInternalOnly(viesti, raportti.rivit) { rivi ->
            arrayOf("suoritus.oppijanumero" to rivi.suorittajanOid)
        }
    }

    private fun VktUusintaoikeus.oid(): Oid? = Oid.parse(suorittajanOid).getOrNull()
}
