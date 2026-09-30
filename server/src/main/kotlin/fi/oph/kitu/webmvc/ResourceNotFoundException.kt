package fi.oph.kitu.webmvc

import org.springframework.http.HttpStatus
import org.springframework.web.bind.annotation.ResponseStatus

/**
 * Virkailijanäkymä kertoo "ei löydy" heittämällä tämän. GlobalControllerExceptionHandler
 * tunnistaa koko hierarkian yhdellä haaralla, joten uusi domain ei enää vaadi muutosta
 * kahteen tiedostoon — aiemmin unohdus käsittelijän listasta renderöi hiljaa 500-sivun.
 *
 * Reason ei päädy käyttäjälle (käsittelijä renderöi ErrorPage.notFoundin), vaan se
 * dokumentoi aikeen lokeihin ja trace-tietoihin.
 */
@ResponseStatus(HttpStatus.NOT_FOUND)
open class ResourceNotFoundException(
    reason: String,
) : RuntimeException(reason)
