package fi.oph.kitu.vkt

import arrow.core.getOrElse
import fi.oph.kitu.auditlogs.AuditLogOperation
import fi.oph.kitu.auditlogs.AuditLogger
import fi.oph.kitu.html.Pagination
import fi.oph.kitu.html.table.httpParams
import fi.oph.kitu.i18n.LocalizationService
import fi.oph.kitu.jdbc.PAGINATED_DEFAULT_PAGE_SIZE
import fi.oph.kitu.jdbc.toMap
import fi.oph.kitu.koodisto.Koodisto
import fi.oph.kitu.oid.Oid
import fi.oph.kitu.oppijanumero.OppijanumeroService
import fi.oph.kitu.tiedontuontischema.VktHenkilosuoritus
import fi.oph.kitu.util.cache.InMemoryCache
import fi.oph.kitu.vkt.CustomVktSuoritusRepository.Tutkintoryhma
import io.opentelemetry.instrumentation.annotations.WithSpan
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service
import java.time.LocalDate
import kotlin.jvm.optionals.getOrNull
import kotlin.time.Duration
import kotlin.time.Duration.Companion.minutes

@Service
class VktSuoritusService(
    private val suoritusRepository: VktSuoritusRepository,
    private val customSuoritusRepository: CustomVktSuoritusRepository,
    private val osakoeRepository: VktOsakoeRepository,
    private val auditLogger: AuditLogger,
    private val oppijanumeroService: OppijanumeroService,
    private val localizationService: LocalizationService,
) {
    @Value($$"${kitu.vkt.scheduling.cleanup.retentionTime}")
    lateinit var retentionTimeForDeletedSetting: String
    val retentionTimeForDeletedSeconds by lazy { Duration.parse(retentionTimeForDeletedSetting).inWholeSeconds }

    @WithSpan("VktSuoritusService.getSuorituksetAndPagination")
    fun getSuorituksetAndPagination(
        filter: VktSuoritusFilter,
        order: VktSuoritusOrder,
    ) = Pair(
        customSuoritusRepository.find(filter, order),
        getPagination(filter, order),
    )

    @WithSpan("VktSuoritusService.getVarhaisimmatTutkintopaivat")
    fun getVarhaisimmatTutkintopaivat(): Map<Koodisto.VktTaitotaso, LocalDate> =
        customSuoritusRepository.findVarhaisimmatTutkintopaivat()

    @WithSpan("VktSuoritusService.getPagination")
    fun getPagination(
        filter: VktSuoritusFilter,
        order: VktSuoritusOrder,
    ): Pagination =
        Pagination.valueOf(
            currentPageNumber = order.pageNumber ?: 0,
            numberOfRows = listRowCounts.get(filter)!!,
            pageSize = PAGINATED_DEFAULT_PAGE_SIZE,
            url = { pageNumber -> httpParams(order.copy(pageNumber = pageNumber).toMap() + filter.toMap()) },
        )

    @WithSpan("VktSuoritusService.getOppijanSuoritukset")
    fun getOppijanSuoritukset(
        id: Tutkintoryhma,
        includeSuorituksenVastaanottajat: Boolean = true,
    ): VktHenkilosuoritus? {
        val ids = customSuoritusRepository.getOppijanSuoritusIds(id)
        val suoritukset =
            ids
                .mapNotNull { suoritusRepository.findById(it).getOrNull() }
                .map { it.toHenkilosuoritus() }
                .also {
                    it.firstOrNull()?.henkilo?.let { henkilo ->
                        auditLogger.log(
                            operation = AuditLogOperation.VktSuoritusViewed,
                            oppijaHenkiloOid = henkilo.oid,
                        )
                    }
                }
        val suorituksenVastaanottajat =
            if (includeSuorituksenVastaanottajat) {
                suoritukset
                    .mapNotNull { it.suoritus.suorituksenVastaanottaja }
                    .toSet()
                    .associateBy({ it }, { oid ->
                        oppijanumeroService.getHenkiloByHenkiloOid(oid).getOrNull()?.kokoNimi() ?: oid.toString()
                    })
            } else {
                mapOf()
            }

        return if (suoritukset.isEmpty()) {
            null
        } else {
            mergeVktHenkilosuoritukset(suoritukset, suorituksenVastaanottajat)
                .getOrElse { error(it.message) }
        }
    }

    @WithSpan("VktSuoritusService.findEnrichedForCsv")
    fun findEnrichedForCsv(
        filter: VktSuoritusFilter,
        order: VktSuoritusOrder,
    ): List<VktSuoritusFlat> {
        val translations =
            localizationService
                .translationBuilder()
                .koodistot("kunta")
                .build()
        val suoritukset = customSuoritusRepository.find(filter, order).toList()

        val vastaanottajat: Map<Oid, String?> =
            suoritukset
                .mapNotNull { it.suorituksenVastaanottajanOid }
                .toSet()
                .associateWith { oppijanumeroService.getHenkiloByHenkiloOid(it).getOrNull()?.kokoNimi() }

        return suoritukset
            .map { suoritus ->
                suoritus.copy(
                    suorituksenVastaanottaja = vastaanottajat[suoritus.suorituksenVastaanottajanOid],
                    suorituspaikkakunta = translations.getByKoodiviite("kunta", suoritus.suorituspaikkakunta),
                )
            }.also {
                auditLogger.logAllInternalOnly("VKT suoritus viewed", it) { suoritus ->
                    arrayOf(
                        "suoritus.id" to suoritus.suoritusId,
                        "suoritus.oppijanumero" to suoritus.suorittajanOid,
                    )
                }
            }
    }

    @WithSpan("VktSuoritusService.setOsakoeArvosana")
    fun setOsakoeArvosana(
        osakoeId: Int,
        arvosana: Koodisto.VktArvosana?,
        arviointipaiva: LocalDate? = null,
    ) = osakoeRepository.updateArvosana(
        id = osakoeId,
        arvosana = arvosana,
        arviointipaiva = arviointipaiva,
    )

    @WithSpan("VktSuoritusService.deleteOsakoe")
    fun deleteOsakoe(osakoeId: Int) = osakoeRepository.delete(osakoeId, retentionTimeForDeletedSeconds)

    @WithSpan("VktSuoritusService.markKoskiTransferProcessed")
    fun markKoskiTransferProcessed(
        id: Tutkintoryhma,
        koskiOpiskeluoikeusOid: String? = null,
    ) = customSuoritusRepository.markSuoritusTransferredToKoski(id, koskiOpiskeluoikeusOid)

    @WithSpan("VktSuoritusService.requestTransferToKoski")
    fun requestTransferToKoski(id: Tutkintoryhma) = customSuoritusRepository.requestTransferToKoski(id)

    private val listRowCounts =
        InMemoryCache(ttl = 5.minutes) { filter: VktSuoritusFilter ->
            customSuoritusRepository.numberOfRowsForListView(filter)
        }

    @WithSpan("VktSuoritusService.cleanup")
    fun cleanup() {
        osakoeRepository.cleanup()
        customSuoritusRepository.cleanup()
    }
}
