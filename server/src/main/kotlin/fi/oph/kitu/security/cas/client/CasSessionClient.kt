package fi.oph.kitu.security.cas.client

import org.springframework.http.HttpHeaders
import org.springframework.http.MediaType
import org.springframework.util.LinkedMultiValueMap
import org.springframework.web.client.RestClient

/**
 * Palvelukayttajan CAS-kirjautuminen palveluun, joka ei hyvaksy OAuth2-tokenia (esim. ataru):
 * TGT → palvelukohtainen ST → palvelun oma sessioeväste. Kitun muut OPH-kutsut kulkevat
 * `oauth2RestClient`in kautta, joten tama on ainoa lahteva CAS-asiakas.
 */
class CasSessionClient(
    private val restClient: RestClient,
    private val casUrl: String,
    private val service: String,
    private val username: String,
    private val password: String,
    private val sessionCookieName: String,
) {
    private var grantingTicket: String? = null
    private var sessionCookie: String? = null

    @Synchronized
    fun sessionCookie(): String = sessionCookie ?: kirjaudu().also { sessionCookie = it }

    /** Vain vanhentunut eväste nollataan, jotta rinnakkainen kutsu ei heita juuri haettua pois. */
    @Synchronized
    fun invalidate(cookie: String) {
        if (sessionCookie == cookie) sessionCookie = null
    }

    private fun kirjaudu(): String {
        val serviceTicket =
            grantingTicket?.let { serviceTicket(it) }
                ?: serviceTicket(uusiGrantingTicket())
                ?: throw CasKirjautuminenEpaonnistui("Palvelutikettia ei saatu palvelulle $service")
        return sessio(serviceTicket)
    }

    private fun uusiGrantingTicket(): String {
        val response =
            restClient
                .post()
                .uri("$casUrl/v1/tickets")
                .contentType(MediaType.APPLICATION_FORM_URLENCODED)
                .body(lomake("username" to username, "password" to password))
                .exchange { _, res -> res.statusCode to res.headers.location }

        val location =
            response.second?.toString()?.takeIf { response.first.is2xxSuccessful }
                ?: throw CasKirjautuminenEpaonnistui("TGT:ta ei saatu (status ${response.first})")
        grantingTicket = location
        return location
    }

    /** Vanhentunut TGT palauttaa virhestatuksen, jolloin kutsuja hakee uuden. */
    private fun serviceTicket(tgt: String): String? =
        restClient
            .post()
            .uri(tgt)
            .contentType(MediaType.APPLICATION_FORM_URLENCODED)
            .body(lomake("service" to service))
            .exchange { _, res ->
                if (res.statusCode.is2xxSuccessful) res.bodyTo(String::class.java)?.trim() else null
            }?.takeIf { it.startsWith("ST-") }
            .also { if (it == null) grantingTicket = null }

    private fun sessio(serviceTicket: String): String =
        restClient
            .get()
            .uri("$service?ticket={ticket}", serviceTicket)
            .exchange { _, res -> evaste(res.headers) }
            ?: throw CasKirjautuminenEpaonnistui("Palvelu $service ei palauttanut evastetta $sessionCookieName")

    private fun evaste(headers: HttpHeaders): String? =
        headers[HttpHeaders.SET_COOKIE]
            .orEmpty()
            .map { it.substringBefore(';').trim() }
            .firstOrNull { it.startsWith("$sessionCookieName=") }

    private fun lomake(vararg kentat: Pair<String, String>) =
        LinkedMultiValueMap<String, String>().apply { kentat.forEach { (k, v) -> add(k, v) } }
}

class CasKirjautuminenEpaonnistui(
    message: String,
) : RuntimeException(message)
