package fi.oph.kitu.yki

import fi.oph.kitu.koski.KoskiYkiRequestMapper
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.http.ResponseEntity
import org.springframework.stereotype.Controller
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.RequestMapping
import tools.jackson.databind.json.JsonMapper

/**
 * Koneluettava debug-reitti, jolla nahdaan mita kitu lahettaisi KOSKI-jarjestelmaan. Erillaan
 * YkiViewControllerista, koska yleiso ja vastausmuoto ovat eri: tama palauttaa JSONia
 * ja bodyttoman 404:n, kun virkailijanakymat heittavat YkiSuoritusNotFoundErrorin ja
 * saavat HTML-virhesivun.
 */
@Controller
@RequestMapping("/yki")
class YkiKoskiDebugController(
    private val ykiService: YkiService,
    private val koskiYkiRequestMapper: KoskiYkiRequestMapper,
    @param:Qualifier("koskiObjectMapper")
    private val koskiObjectMapper: JsonMapper,
) {
    @GetMapping("/koski-request/{suoritusId}", produces = ["application/json"])
    fun koskiRequestJson(
        @PathVariable suoritusId: Int,
    ): ResponseEntity<ByteArray> =
        ykiService
            .findLatestBySolkiIds(listOf(suoritusId))
            .firstOrNull()
            ?.let { koskiYkiRequestMapper.ykiSuoritusToKoskiRequest(it).fold(ifLeft = { it }, ifRight = { it }) }
            ?.let { ResponseEntity.ok(koskiObjectMapper.writeValueAsBytes(it)) }
            ?: ResponseEntity.notFound().build()
}
