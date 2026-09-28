package fi.oph.kitu.yki.historia

import fi.oph.kitu.html.Pagination
import fi.oph.kitu.html.table.httpParams
import fi.oph.kitu.webmvc.Links
import org.springframework.http.ResponseEntity
import org.springframework.stereotype.Controller
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.ModelAttribute
import org.springframework.web.bind.annotation.RequestMapping

@Controller
@RequestMapping("/yki/historia-siirtymattomat")
class YkiHistoriaViewController(
    private val repository: YkiHistoriaSiirtymatonRepository,
) {
    @GetMapping("", produces = ["text/html"])
    fun siirtymattomatView(
        @ModelAttribute params: YkiHistoriaSiirtymatonParams,
    ): ResponseEntity<String> {
        val rivit = repository.haeSivullinen(params)
        val kokonaismaara = repository.laske(params)

        return ResponseEntity.ok(
            YkiHistoriaSiirtymatonPage.render(
                rivit = rivit,
                params = params,
                pagination =
                    Pagination.valueOf(
                        currentPageNumber = params.page,
                        numberOfRows = kokonaismaara,
                        pageSize = params.limit,
                        url = { page ->
                            Links.Yki.historiaSiirtymattomat() +
                                httpParams(params.toMap() + ("page" to page.toString()))
                        },
                    ),
            ),
        )
    }
}
