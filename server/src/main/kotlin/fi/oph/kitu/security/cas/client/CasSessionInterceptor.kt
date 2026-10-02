package fi.oph.kitu.security.cas.client

import org.springframework.http.HttpHeaders
import org.springframework.http.HttpRequest
import org.springframework.http.HttpStatus
import org.springframework.http.client.ClientHttpRequestExecution
import org.springframework.http.client.ClientHttpRequestInterceptor
import org.springframework.http.client.ClientHttpResponse

/** Vanhentunut sessio nakyy 401:na, jolloin kirjaudutaan kerran uudelleen ja yritetaan uudestaan. */
class CasSessionInterceptor(
    private val cas: CasSessionClient,
) : ClientHttpRequestInterceptor {
    override fun intercept(
        request: HttpRequest,
        body: ByteArray,
        execution: ClientHttpRequestExecution,
    ): ClientHttpResponse {
        val cookie = cas.sessionCookie()
        request.headers.set(HttpHeaders.COOKIE, cookie)
        val response = execution.execute(request, body)
        if (response.statusCode != HttpStatus.UNAUTHORIZED) return response

        response.close()
        cas.invalidate(cookie)
        request.headers.set(HttpHeaders.COOKIE, cas.sessionCookie())
        return execution.execute(request, body)
    }
}
