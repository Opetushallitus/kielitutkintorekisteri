package fi.oph.kitu.webmvc

import org.springframework.http.HttpStatus
import org.springframework.web.bind.annotation.ResponseStatus

/**
 * Virkailijanäkymä kertoo "ei löydy" heittämällä tämän. GlobalControllerExceptionHandler
 * tunnistaa koko hierarkian yhdellä haaralla, joten uusi domain ei vaadi muutosta käsittelijään.
 *
 * Reason ei päädy käyttäjälle (käsittelijä renderöi ErrorPage.notFoundin), vaan se
 * dokumentoi aikeen lokeihin ja trace-tietoihin.
 */
@ResponseStatus(HttpStatus.NOT_FOUND)
open class ResourceNotFoundException(
    reason: String,
) : RuntimeException(reason)
