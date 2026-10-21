package fi.oph.kitu.security.cas

import org.apereo.cas.client.validation.AssertionImpl
import org.springframework.security.cas.authentication.CasAuthenticationToken
import org.springframework.security.core.authority.AuthorityUtils
import org.springframework.security.core.userdetails.User
import kotlin.test.Test
import kotlin.test.assertNull
import kotlin.test.assertSame

class CasStatelessTicketCacheTest {
    private fun token(ticket: String): CasAuthenticationToken {
        val authorities = AuthorityUtils.createAuthorityList("ROLE_APP_KIELITUTKINTOREKISTERI")
        val user = User("palvelukayttaja", "", authorities)
        return CasAuthenticationToken("kitu", user, ticket, authorities, user, AssertionImpl("palvelukayttaja"))
    }

    @Test
    fun `merkinta loytyy palvelutiketilla, ei principalin merkkijonolla`() {
        val cache = CasStatelessTicketCache()
        val ticket = "ST-1-genuine-ticket"
        val token = token(ticket)

        cache.putTicketInCache(token)

        assertSame(token, cache.getByTicketId(ticket))
        // Avain on palvelutiketti eika principalin toString() - muuten hyokkaaja voisi
        // noutaa valmiiksi autentikoidun tokenin lahettamalla ?ticket=<principal.toString()>.
        assertNull(cache.getByTicketId(token.principal.toString()))
    }

    @Test
    fun `removeTicketFromCache tokenilla poistaa merkinnan`() {
        val cache = CasStatelessTicketCache()
        val ticket = "ST-2-genuine-ticket"
        val token = token(ticket)
        cache.putTicketInCache(token)

        cache.removeTicketFromCache(token)

        assertNull(cache.getByTicketId(ticket))
    }
}
