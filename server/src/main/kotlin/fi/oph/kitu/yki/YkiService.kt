package fi.oph.kitu.yki

import arrow.core.Either
import arrow.core.raise.either
import arrow.core.raise.ensure
import arrow.core.raise.ensureNotNull
import fi.oph.kitu.auditlogs.AuditLogOperation
import fi.oph.kitu.auditlogs.AuditLogger
import fi.oph.kitu.oid.Oid
import fi.oph.kitu.oppijanumero.OppijanumeroException
import fi.oph.kitu.oppijanumero.OppijanumeroService
import fi.oph.kitu.util.TimeService
import fi.oph.kitu.util.cache.InMemoryCache
import fi.oph.kitu.util.result.getOrThrow
import fi.oph.kitu.yki.suoritukset.HyvaksyTarkistusarviointiError
import fi.oph.kitu.yki.suoritukset.PoistaSuoritusError
import fi.oph.kitu.yki.suoritukset.YkiSuoritusEntity
import fi.oph.kitu.yki.suoritukset.YkiSuoritusFilter
import fi.oph.kitu.yki.suoritukset.YkiSuoritusOrder
import fi.oph.kitu.yki.suoritukset.YkiSuoritusRepository
import fi.oph.kitu.yki.suoritukset.YkiSuoritusSivu
import fi.oph.kitu.yki.suoritukset.YkiSuoritusTilastoColumn
import fi.oph.kitu.yki.suoritukset.YkiSuoritusTilastoRivi
import fi.oph.kitu.yki.suoritukset.YkiTarkistusarvioinnitSivu
import fi.oph.kitu.yki.suoritukset.YkiTarkistusarviointiFilter
import io.opentelemetry.instrumentation.annotations.WithSpan
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.security.core.context.SecurityContextHolder
import org.springframework.security.oauth2.jwt.Jwt
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import java.time.LocalDate
import kotlin.time.Duration

data class ExtendedFilter<F>(
    val filter: F,
    val oppijanumeroUnavailable: Boolean,
)

@Service
class YkiService(
    private val suoritusRepository: YkiSuoritusRepository,
    private val auditLogger: AuditLogger,
    private val oppijanumeroService: OppijanumeroService,
    private val timeService: TimeService,
    @param:Value($$"${kitu.yki.varhaisinTutkintopaiva.cache.ttl}")
    varhaisinTutkintopaivaCacheTtl: String,
) {
    private val logger: Logger = LoggerFactory.getLogger(javaClass)

    private val varhaisinTutkintopaivaCache =
        InMemoryCache<Unit, LocalDate>(ttl = Duration.parse(varhaisinTutkintopaivaCacheTtl)) {
            suoritusRepository.findVarhaisinTutkintopaiva()
        }

    @WithSpan
    fun extendFilterWithLinkedOids(filter: YkiSuoritusFilter): ExtendedFilter<YkiSuoritusFilter> =
        filter.extendHenkiloOids(oppijanumeroService).toExtendedFilter(filter)

    @WithSpan
    fun extendFilterWithLinkedOids(filter: YkiTarkistusarviointiFilter): ExtendedFilter<YkiTarkistusarviointiFilter> =
        filter.extendHenkiloOids(oppijanumeroService).toExtendedFilter(filter)

    private fun <F> Either<OppijanumeroException, F>.toExtendedFilter(alkuperainen: F): ExtendedFilter<F> =
        fold(
            ifLeft = { error ->
                logger.warn("Oppijanumeropalvelu ei vastannut, YKI-haku tehdään vain annetuilla oideilla", error)
                ExtendedFilter(alkuperainen, oppijanumeroUnavailable = true)
            },
            ifRight = { extended -> ExtendedFilter(extended, oppijanumeroUnavailable = false) },
        )

    @WithSpan
    fun extendFilterWithLinkedOidsOrThrow(filter: YkiSuoritusFilter): YkiSuoritusFilter =
        filter.extendHenkiloOids(oppijanumeroService).getOrThrow()

    @WithSpan
    fun extendFilterWithLinkedOidsOrThrow(filter: YkiTarkistusarviointiFilter): YkiTarkistusarviointiFilter =
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
    fun findVarhaisinTutkintopaiva(): LocalDate? = varhaisinTutkintopaivaCache.get(Unit)

    @WithSpan
    fun countSuorituksetRyhmittain(
        filter: YkiSuoritusFilter = YkiSuoritusFilter(),
        versionHistory: Boolean = false,
        ryhmittely: List<YkiSuoritusTilastoColumn> = YkiSuoritusTilastoColumn.oletusryhmittely,
    ): List<YkiSuoritusTilastoRivi> =
        suoritusRepository.countSuorituksetRyhmittain(
            filter = filter,
            distinct = !versionHistory,
            ryhmittely = ryhmittely,
        )

    @WithSpan
    fun findSuorituksetSivu(
        filter: YkiSuoritusFilter = YkiSuoritusFilter(),
        order: YkiSuoritusOrder = YkiSuoritusOrder(),
        versionHistory: Boolean = false,
        limit: Int,
        offset: Int,
    ): YkiSuoritusSivu {
        val distinct = !versionHistory
        val sivu =
            if (filter.hasTextSearch()) {
                suoritusRepository.findSivuKokonaismaaralla(filter, order, distinct, limit, offset)
            } else {
                YkiSuoritusSivu(
                    suoritukset = suoritusRepository.findForListView(filter, order, distinct, limit, offset).toList(),
                    kokonaismaara = countSuoritukset(filter, versionHistory),
                )
            }
        auditLogger.logAllInternalOnly("Yki suoritus viewed", sivu.suoritukset) { suoritus ->
            arrayOf(
                "suoritus.id" to suoritus.id,
            )
        }
        return sivu
    }

    @Transactional
    @WithSpan
    fun poistaSuoritus(tunnus: String): Either<PoistaSuoritusError, Unit> =
        either {
            val suoritus =
                ensureNotNull(suoritusRepository.getLatestByLahdejarjestelmanTunnus(tunnus)) {
                    PoistaSuoritusError.SuoritustaEiLoydy(tunnus)
                }
            ensure(suoritus.arviointitila == Arviointitila.ILMOITTAUTUNUT) {
                PoistaSuoritusError.TilaEstaaPoiston(tunnus, suoritus.arviointitila)
            }

            val uusiVersio =
                suoritusRepository.save(
                    suoritus.eiSuoritettuna(timeService.now()),
                    updateOnConflict = false,
                )
            val jwtSubject = (SecurityContextHolder.getContext().authentication?.principal as? Jwt)?.subject
            auditLogger.logAllInternalOnly("Yki suoritus deleted", listOf(suoritus)) {
                arrayOf(
                    "suoritus.id" to uusiVersio,
                    "suoritus.solki_id" to it.solkiId,
                    "suoritus.lahdejarjestelmantunnus" to it.lahdejarjestelmanTunnus,
                    "suoritus.suorittajan_oid" to it.suorittajanOID,
                    "auth.principal_oid" to jwtSubject,
                )
            }
        }

    @WithSpan
    fun findViimeisinBySolkiId(solkiId: Int): YkiSuoritusEntity =
        suoritusRepository.findLatestBySolkiIds(listOf(solkiId)).first()

    @WithSpan
    fun findOpiskeluoikeusOid(solkiId: Int): Oid? =
        suoritusRepository.findOpiskeluoikeusOidsBySolkiIds(listOf(solkiId))[solkiId]

    @WithSpan
    fun findLatestBySolkiIds(solkiIds: List<Int>): List<YkiSuoritusEntity> =
        suoritusRepository.findLatestBySolkiIds(solkiIds)

    @WithSpan
    fun findTarkistusarvioidut(
        arviointitila: Arviointitila,
        filter: YkiTarkistusarviointiFilter,
    ): List<YkiSuoritusEntity> =
        suoritusRepository.findTarkistusarvoidutSuoritukset(arviointitila, filter).also(::auditoiNaytetyt)

    @WithSpan
    fun findTarkistusarvioidutSivu(
        arviointitila: Arviointitila,
        filter: YkiTarkistusarviointiFilter,
    ): YkiTarkistusarvioinnitSivu {
        val kokonaismaara = suoritusRepository.countTarkistusarvioidutSuoritukset(arviointitila, filter)
        val sivukoko = filter.rajaus.sivukoko()
        val viimeinenSivu = ((kokonaismaara - 1) / sivukoko + 1).coerceAtLeast(1)
        val sivunumero = filter.rajaus.page.coerceIn(1, viimeinenSivu)
        val suoritukset =
            suoritusRepository
                .findTarkistusarvoidutSuoritukset(arviointitila, filter, sivukoko, sivukoko * (sivunumero - 1))
                .also(::auditoiNaytetyt)
        return YkiTarkistusarvioinnitSivu(suoritukset, kokonaismaara, sivunumero)
    }

    private fun auditoiNaytetyt(suoritukset: List<YkiSuoritusEntity>) {
        auditLogger.logAllInternalOnly("Yki suoritus viewed", suoritukset) { suoritus ->
            arrayOf(
                "suoritus.id" to suoritus.id,
            )
        }
    }

    /**
     * Oletuspaiva tulee TimeServicelta, ei LocalDate.now():lta: kontti ajaa UTC:ssa ja
     * sovelluksen kasitys tasta paivasta on Europe/Helsinki, joten klo 21-24 UTC valilla
     * hyvaksyntapaiva olisi mennyt edelliselle paivalle.
     */
    @WithSpan
    fun hyvaksyTarkistusarvioinnit(
        suoritusIds: List<Int>,
        pvm: LocalDate?,
    ): Either<HyvaksyTarkistusarviointiError, Int> =
        suoritusRepository.hyvaksyTarkistusarvioinnit(
            suoritusIds = suoritusIds,
            pvm = pvm ?: timeService.today(),
        )
}
