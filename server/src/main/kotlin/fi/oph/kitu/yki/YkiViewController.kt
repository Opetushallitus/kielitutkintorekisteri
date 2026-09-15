package fi.oph.kitu.yki
import fi.oph.kitu.dev.mockdata.toInstant
import fi.oph.kitu.html.KituRequest
import fi.oph.kitu.html.Pagination
import fi.oph.kitu.html.ViewMessage
import fi.oph.kitu.html.ViewMessageData
import fi.oph.kitu.html.ViewMessageType
import fi.oph.kitu.html.errorTablePage
import fi.oph.kitu.html.table.httpParams
import fi.oph.kitu.i18n.LocalizationService
import fi.oph.kitu.i18n.UiText
import fi.oph.kitu.ilmoittautumisjarjestelma.IlmoittautumisjarjestelmaService
import fi.oph.kitu.jdbc.SortDirection
import fi.oph.kitu.koski.KoskiErrorService
import fi.oph.kitu.koski.KoskiYkiMappingError
import fi.oph.kitu.koski.KoskiYkiRequestMapper
import fi.oph.kitu.koski.YkiMappingId
import fi.oph.kitu.oppijanumero.OppijanumeroService
import fi.oph.kitu.util.result.splitIntoValuesAndErrors
import fi.oph.kitu.webmvc.Links
import fi.oph.kitu.yki.suoritukset.YkiSuorituksetPage
import fi.oph.kitu.yki.suoritukset.YkiSuoritusPage
import fi.oph.kitu.yki.suoritukset.YkiSuoritusRepository
import fi.oph.kitu.yki.suoritukset.YkiTarkistusarvioinnitPage
import fi.oph.kitu.yki.suoritukset.error.YkiKoskiErrors
import fi.oph.kitu.yki.suoritukset.error.YkiSuoritusErrorColumn
import fi.oph.kitu.yki.suoritukset.error.YkiSuoritusErrorService
import jakarta.servlet.http.HttpSession
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.security.web.csrf.CsrfToken
import org.springframework.stereotype.Controller
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.ModelAttribute
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.ResponseStatus
import org.springframework.web.servlet.view.RedirectView
import tools.jackson.databind.json.JsonMapper
import java.time.LocalDate

@Controller
@RequestMapping("/yki")
class YkiViewController(
    private val ykiService: YkiService,
    private val suoritusErrorService: YkiSuoritusErrorService,
    private val koskiErrorService: KoskiErrorService,
    private val ykiSuoritusRepository: YkiSuoritusRepository,
    private val koskiYkiRequestMapper: KoskiYkiRequestMapper,
    @param:Qualifier("koskiObjectMapper")
    private val koskiObjectMapper: JsonMapper,
    private val ilmoittautumisjarjestelma: IlmoittautumisjarjestelmaService,
    private val oppijanumeroService: OppijanumeroService,
    private val localizationService: LocalizationService,
) {
    @GetMapping("/suoritukset/{id}", produces = ["text/html"])
    fun suoritusView(
        @PathVariable id: Int,
    ): ResponseEntity<String> {
        val suoritus = ykiService.findSuoritusById(id)
        return suoritus?.let {
            val viimeisinSuoritus = ykiSuoritusRepository.findLatestBySolkiIds(listOf(suoritus.solkiId)).first()
            val opiskeluoikeusOid =
                ykiSuoritusRepository.findOpiskeluoikeusOidsBySolkiIds(listOf(suoritus.solkiId))[suoritus.solkiId]
            val (koskiError, koskiSiirronEstonSyyt) =
                if (suoritus.id == viimeisinSuoritus.id) {
                    Pair(
                        koskiErrorService.findById(YkiMappingId(suoritus.id)),
                        when (val left = koskiYkiRequestMapper.ykiSuoritusToKoskiRequest(suoritus).leftOrNull()) {
                            is KoskiYkiMappingError.EstoSyyt -> left.syyt
                            else -> null
                        },
                    )
                } else {
                    Pair(null, null)
                }
            val henkilo = oppijanumeroService.getHenkiloByHenkiloOid(suoritus.suorittajanOID)
            val t = localizationService.translationBuilder().koodistot("maatjavaltiot1", "maatjavaltiot2").build()
            ResponseEntity.ok(
                YkiSuoritusPage.render(
                    henkilo,
                    suoritus = it,
                    viimeisinSuoritus,
                    koskiError,
                    koskiSiirronEstonSyyt,
                    opiskeluoikeusOid,
                    t,
                ),
            )
        } ?: throw YkiSuoritusNotFoundError()
    }

    @GetMapping("/suoritukset", produces = ["text/html"])
    fun suorituksetGetView(
        @ModelAttribute params: YkiSuorituksetParams = YkiSuorituksetParams(),
        session: HttpSession? = null,
    ): ResponseEntity<String> =
        handleSuorituksetView(
            params.withRecalledSearch(session),
            KituRequest.currentCsrfToken(),
        )

    @PostMapping("/suoritukset", produces = ["text/html"])
    fun suorituksetPostView(
        @ModelAttribute params: YkiSuorituksetParams,
        csrfToken: CsrfToken? = KituRequest.currentCsrfToken(),
        session: HttpSession,
    ): ResponseEntity<String> {
        session.setAttribute(YKI_SEARCH_KEY, params.search)
        return handleSuorituksetView(params, csrfToken)
    }

    private fun handleSuorituksetView(
        params: YkiSuorituksetParams,
        csrfToken: CsrfToken?,
    ): ResponseEntity<String> {
        val extended = ykiService.extendFilterWithLinkedOids(params.toFilter())
        val totalSuoritukset = ykiService.countSuoritukset(extended.filter, params.versionHistory)
        return ResponseEntity.ok(
            YkiSuorituksetPage.render(
                suoritukset =
                    ykiService.findSuorituksetPaged(
                        extended.filter,
                        params.toOrder(),
                        params.versionHistory,
                        params.limit,
                        offset = params.limit * (params.page - 1),
                    ),
                totalSuoritukset = totalSuoritukset,
                params,
                pagination =
                    Pagination.valueOf(
                        currentPageNumber = params.page,
                        numberOfRows = totalSuoritukset.toInt(),
                        pageSize = params.limit,
                        url = { currentPage -> httpParams(params.toMap().plus("page" to currentPage)) },
                    ),
                errorsCount = suoritusErrorService.countErrors(),
                koskiErrorsCount = koskiErrorService.countByEntity("yki", false).toLong(),
                csrfToken = csrfToken,
                warning = if (extended.oppijanumeroUnavailable) ONR_UNAVAILABLE_WARNING else null,
            ),
        )
    }

    @GetMapping("/suoritukset/virheet", produces = ["text/html"])
    fun suorituksetVirheetView(
        sortColumn: YkiSuoritusErrorColumn = YkiSuoritusErrorColumn.VirheenLuontiaika,
        sortDirection: SortDirection = SortDirection.ASC,
    ): ResponseEntity<String> =
        ResponseEntity.ok(
            errorTablePage(
                title = UiText.Nav.yki.toString(),
                subtitle = UiText.Yki.suoritustenTuonninVirheet.toString(),
                sortColumn = sortColumn,
                sortDirection = sortDirection,
                rows = suoritusErrorService.getErrors(sortColumn, sortDirection),
            ),
        )

    @GetMapping("/koski-virheet", produces = ["text/html"])
    fun koskiVirheetView(hidden: Boolean = false): ResponseEntity<String> {
        val errors = koskiErrorService.findAllByEntity("yki", hidden)
        val hiddenCount = if (hidden) null else koskiErrorService.countByEntity("yki", true)
        val suoritusIds = errors.mapNotNull { YkiMappingId.parse(it.id)?.suoritusId }

        return ResponseEntity.ok(
            YkiKoskiErrors.render(
                errors = koskiErrorService.findAllByEntity("yki", hidden),
                suoritukset = ykiSuoritusRepository.findLatestBySolkiIds(suoritusIds),
                hiddenCount = hiddenCount,
            ),
        )
    }

    @GetMapping("/koski-virheet/piilota/{suoritusId}/{hidden}", produces = ["text/html"])
    fun hideKoskiVirheet(
        @PathVariable suoritusId: Int,
        @PathVariable hidden: Boolean,
    ): RedirectView {
        koskiErrorService.setHidden(
            id = YkiMappingId(suoritusId),
            hidden = hidden,
        )
        return RedirectView(Links.Yki.koskiVirheet())
    }

    @GetMapping("/koski-request/{suoritusId}", produces = ["application/json"])
    fun koskiRequestJson(
        @PathVariable suoritusId: Int,
    ): ResponseEntity<String> =
        ykiSuoritusRepository
            .findLatestBySolkiIds(listOf(suoritusId))
            .firstOrNull()
            ?.let {
                koskiYkiRequestMapper.ykiSuoritusToKoskiRequest(it)
            }?.let {
                ResponseEntity.ok(koskiObjectMapper.writeValueAsString(it))
            } ?: ResponseEntity.notFound().build()

    @GetMapping("/tarkistusarvioinnit", produces = ["text/html"])
    fun tarkistusArvioinnitView(viewMessage: ViewMessage? = null): ResponseEntity<String> =
        ykiSuoritusRepository.findTarkistusarvoidutSuoritukset(Arviointitila.TARKISTUSARVIOITU).let {
            ResponseEntity.ok(
                YkiTarkistusarvioinnitPage.render(
                    suoritukset = it.toList(),
                    message = viewMessage?.consume(),
                ),
            )
        }

    @GetMapping("/tarkistusarvioinnit/hyvaksytyt", produces = ["text/html"])
    fun hyvaksytytTarkistusArvioinnitView(viewMessage: ViewMessage? = null): ResponseEntity<String> =
        ykiSuoritusRepository.findTarkistusarvoidutSuoritukset(Arviointitila.TARKISTUSARVIOINTI_HYVAKSYTTY).let {
            ResponseEntity.ok(
                YkiTarkistusarvioinnitPage.renderHyvaksytyt(
                    suoritukset = it.toList(),
                    message = viewMessage?.consume(),
                ),
            )
        }

    @PostMapping("/tarkistusarvioinnit")
    fun hyvaksyTarkistusArvioinnit(
        @RequestParam suoritukset: List<Int>? = null,
        @RequestParam hyvaksyttyPvm: LocalDate? = null,
        viewMessage: ViewMessage? = null,
    ): RedirectView {
        hyvaksyTarkistusarvioinnit(suoritukset, hyvaksyttyPvm, viewMessage)
        return RedirectView(Links.Yki.tarkistusArvioinnit())
    }

    @PostMapping("/tarkistusarvioinnit/hyvaksytyt")
    fun korjaaHyvaksytytTarkistusArvioinnit(
        @RequestParam suoritukset: List<Int>? = null,
        @RequestParam hyvaksyttyPvm: LocalDate? = null,
        viewMessage: ViewMessage? = null,
    ): RedirectView {
        hyvaksyTarkistusarvioinnit(suoritukset, hyvaksyttyPvm, viewMessage)
        return RedirectView(Links.Yki.hyvaksytytTarkistusArvioinnit())
    }

    private fun hyvaksyTarkistusarvioinnit(
        suoritukset: List<Int>?,
        hyvaksyttyPvm: LocalDate?,
        viewMessage: ViewMessage?,
    ) {
        suoritukset?.let {
            ykiSuoritusRepository
                .hyvaksyTarkistusarvioinnit(
                    suoritusIds = suoritukset,
                    pvm = hyvaksyttyPvm ?: LocalDate.now(),
                ).fold(
                    ifLeft = { error -> viewMessage?.showError(error.message) },
                    ifRight = { updated ->
                        viewMessage?.showSuccess(
                            if (updated > 1) {
                                "$updated tarkistusarviointia merkitty hyväksytyksi"
                            } else {
                                "1 tarkistusarviointi merkitty hyväksytyksi"
                            },
                        )
                        ilmoittautumisjarjestelma.sendAllUpdatedArvioinninTilat()
                    },
                )
        }
    }

    // Väliaikainen rajapinta yki-import-ongelman selvittelyyn
    @GetMapping("/debug/import/{date}", produces = ["text/plain"])
    fun debugYkiImport(
        @PathVariable date: LocalDate,
    ): ResponseEntity<String> =
        ResponseEntity.ok(
            ykiService.debugImportSuoritukset(date.toInstant()),
        )

    companion object {
        const val YKI_SEARCH_KEY = "YkiSearch"

        private val ONR_UNAVAILABLE_WARNING: ViewMessageData
            get() = ViewMessageData(UiText.Yki.onrEiYhteytta.toString(), ViewMessageType.WARNING)
    }
}

fun Boolean?.toTrueOrNull(): String? = if (this == true) "true" else null

fun YkiSuorituksetParams.withRecalledSearch(session: HttpSession?): YkiSuorituksetParams =
    copy(
        search =
            if (recallSearch) {
                session?.getAttribute(YkiViewController.YKI_SEARCH_KEY) as? String ?: ""
            } else {
                ""
            },
    )

@ResponseStatus(value = HttpStatus.NOT_FOUND, reason = "YKI-suoritusta ei löytynyt")
class YkiSuoritusNotFoundError : RuntimeException()
