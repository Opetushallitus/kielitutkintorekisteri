package fi.oph.kitu.kotoutumiskoulutus

import fi.oph.kitu.html.Pagination
import fi.oph.kitu.html.table.httpParams
import fi.oph.kitu.jdbc.SortDirection
import fi.oph.kitu.kotoutumiskoulutus.suoritukset.KielitestiSuorituksetPage
import fi.oph.kitu.kotoutumiskoulutus.suoritukset.KielitestiSuorituksetParams
import fi.oph.kitu.kotoutumiskoulutus.suoritukset.KielitestiSuoritusColumn
import fi.oph.kitu.kotoutumiskoulutus.suoritukset.KielitestiSuoritusPage
import fi.oph.kitu.kotoutumiskoulutus.suoritukset.KielitestiSuoritusService
import fi.oph.kitu.kotoutumiskoulutus.suoritukset.error.KielitestiErrorService
import fi.oph.kitu.kotoutumiskoulutus.suoritukset.error.KielitestiSuoritusErrorColumn
import fi.oph.kitu.kotoutumiskoulutus.suoritukset.error.KielitestiSuoritusErrorPage
import fi.oph.kitu.kotoutumiskoulutus.suoritukset.sortByOrgName
import fi.oph.kitu.organisaatiot.OrganisaatioService
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.stereotype.Controller
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.ModelAttribute
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.ResponseStatus

@Controller
@RequestMapping("/koto-kielitesti", produces = ["text/html"])
class KielitestiViewController(
    private val suoritusService: KielitestiSuoritusService,
    private val errorService: KielitestiErrorService,
    private val organisaatioService: OrganisaatioService,
) {
    @GetMapping("/suoritukset")
    fun suorituksetView(
        @ModelAttribute
        params: KielitestiSuorituksetParams = KielitestiSuorituksetParams(),
    ): ResponseEntity<String> {
        val organisaatiot = organisaatioService.getOrganisaatiot()
        val filter = params.toFilter()
        val suoritukset = suoritusService.getSuoritukset(filter, params.toOrder())
        val numberOfSuoritukset = suoritusService.countSuoritukset(filter)

        return ResponseEntity.ok(
            KielitestiSuorituksetPage.render(
                suoritukset =
                    suoritukset
                        .map { it.copy(oppilaitos = organisaatiot.nimet[it.oppilaitosOid]?.fi) }
                        .let {
                            when (params.sortColumn) {
                                KielitestiSuoritusColumn.Oppilaitos -> {
                                    it.sortByOrgName(
                                        params.sortDirection,
                                        organisaatiot,
                                    )
                                }

                                else -> {
                                    it
                                }
                            }
                        },
                errorsCount =
                    errorService
                        .getErrors(KielitestiSuoritusErrorColumn.VirheenLuontiaika, params.sortDirection)
                        .count()
                        .toLong(),
                filterParams = params,
                numberOfSuoritukset = numberOfSuoritukset,
                pagination =
                    Pagination.valueOf(
                        currentPageNumber = params.page,
                        numberOfRows = numberOfSuoritukset,
                        pageSize = params.limit,
                        url = { currentPage -> httpParams(params.toMap().plus("page" to currentPage)) },
                    ),
            ),
        )
    }

    @GetMapping("/suoritukset/{id}", produces = ["text/html"])
    fun suoritusView(
        @PathVariable id: Int,
    ): ResponseEntity<String> {
        val suoritus = suoritusService.getSuoritusById(id)
        val organisaatiot = organisaatioService.getOrganisaatiot()
        return suoritus?.let {
            ResponseEntity.ok(KielitestiSuoritusPage.render(it, organisaatiot))
        } ?: throw KielitestiSuoritusNotFoundError()
    }

    @GetMapping("/suoritukset/virheet")
    fun virheetView(
        sortColumn: KielitestiSuoritusErrorColumn = KielitestiSuoritusErrorColumn.VirheenLuontiaika,
        sortDirection: SortDirection = SortDirection.DESC,
    ): ResponseEntity<String> =
        ResponseEntity.ok(
            KielitestiSuoritusErrorPage.render(
                sortColumn = sortColumn,
                sortDirection = sortDirection,
                errors = errorService.getErrors(sortColumn, sortDirection),
                organisaatioidenNimet = organisaatioService.getOrganisaatiot(),
            ),
        )
}

@ResponseStatus(value = HttpStatus.NOT_FOUND, reason = "Kielitestin suoritusta ei löytynyt")
class KielitestiSuoritusNotFoundError : RuntimeException()
