package fi.oph.kitu.oppijanumero

import arrow.core.Either
import arrow.core.flatMap
import arrow.core.left
import arrow.core.raise.either
import fi.oph.kitu.oid.Oid
import fi.oph.kitu.util.retry.RetryOutboundIntegration
import io.opentelemetry.api.trace.Span
import io.opentelemetry.instrumentation.annotations.WithSpan
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Service

interface OppijanumeroService {
    fun getMasterOid(oppija: Oppija): Either<OppijanumeroException, Oid>

    fun getMasterOid(henkiloOid: Oid): Either<OppijanumeroException, Oid>

    /**
     * Kuten [getMasterOid], mutta yksiloimaton henkilo on virhe: henkilo-OID ei kelpaa
     * oppijanumeron korvikkeeksi. Tarkoituksella ilman oletustoteutusta, jotta unohtunut
     * ylikirjoitus ei palauta [getMasterOid]in fallbackia hiljaa takaisin.
     */
    fun getOppijanumero(henkiloOid: Oid): Either<OppijanumeroException, Oid>

    fun getHenkiloByMasterOid(masterOid: Oid): Either<OppijanumeroException, OppijanumerorekisteriHenkilo>

    fun getLinkedOids(henkiloOid: Oid): Either<OppijanumeroException, Set<Oid>>

    fun getHenkiloByHenkiloOid(henkiloOid: Oid): Either<OppijanumeroException, OppijanumerorekisteriHenkilo> =
        either {
            val masterOid = getMasterOid(henkiloOid).bind()
            getHenkiloByMasterOid(masterOid).bind()
        }

    fun parseOid(source: String?): Either<OppijanumeroException, Oid> =
        Oid
            .parse(source)
            .mapLeft { OppijanumeroException.MalformedOppijanumero(oppijanumero = source) }
}

@Service
@Profile("!test && !e2e && !local-opintopolku")
class OppijanumeroServiceImpl(
    val client: OppijanumerorekisteriClient,
) : OppijanumeroService {
    @WithSpan
    @RetryOutboundIntegration
    override fun getMasterOid(oppija: Oppija): Either<OppijanumeroException, Oid> {
        require(oppija.etunimet.isNotEmpty()) { "etunimet cannot be empty" }
        require(oppija.hetu.isNotEmpty()) { "hetu cannot be empty" }
        require(oppija.sukunimi.isNotEmpty()) { "sukunimi cannot be empty" }
        require(oppija.kutsumanimi.isNotEmpty()) { "kutsumanimi cannot be empty" }

        val requestBody =
            YleistunnisteHaeRequest(
                oppija.etunimet,
                oppija.hetu,
                oppija.kutsumanimi.split(" ").first(),
                oppija.sukunimi,
            )

        return client
            .onrPost("yleistunniste/hae", requestBody, YleistunnisteHaeResponse::class.java)
            .flatMap { body ->
                val span = Span.current()
                span.setAttribute("response.hasOppijanumero", body.oppijanumero.isNullOrEmpty())
                span.setAttribute("response.hasOid", body.oid.isEmpty())
                span.setAttribute("response.areOppijanumeroAndOidSame", (body.oppijanumero == body.oid))

                if (body.oppijanumero.isNullOrEmpty()) {
                    OppijanumeroException.OppijaNotIdentifiedException(requestBody).left()
                } else {
                    Oid
                        .parse(body.oppijanumero)
                        .mapLeft {
                            OppijanumeroException.MalformedOppijanumero(
                                requestBody,
                                body.oppijanumero,
                            )
                        }
                }
            }
    }

    @WithSpan
    @RetryOutboundIntegration
    override fun getHenkiloByMasterOid(oid: Oid): Either<OppijanumeroException, OppijanumerorekisteriHenkilo> =
        either {
            val masterOid = getMasterOid(oid).bind()
            client.onrGet("henkilo/$masterOid", OppijanumerorekisteriHenkilo::class.java).bind()
        }

    @WithSpan
    @RetryOutboundIntegration
    override fun getLinkedOids(henkiloOid: Oid): Either<OppijanumeroException, Set<Oid>> =
        either {
            val body = getYleistunniste(henkiloOid).bind()
            Span.current().setAttribute("response.linkedCount", body.linked.size.toLong())
            (body.linked + body.oid)
                .map { parseOid(it).bind() }
                .toSet()
        }

    @WithSpan
    @RetryOutboundIntegration
    override fun getMasterOid(henkiloOid: Oid): Either<OppijanumeroException, Oid> =
        either {
            val body = getYleistunniste(henkiloOid).bind()
            parseOid(body.oppijanumero ?: body.oid).bind()
        }

    @WithSpan
    @RetryOutboundIntegration
    override fun getOppijanumero(henkiloOid: Oid): Either<OppijanumeroException, Oid> =
        either {
            val body = getYleistunniste(henkiloOid).bind()
            val oppijanumero =
                body.oppijanumero?.takeIf { it.isNotEmpty() }
                    ?: raise(OppijanumeroException.OppijaNotIdentifiedException(EmptyRequest()))
            parseOid(oppijanumero).bind()
        }

    private fun getYleistunniste(oid: Oid): Either<OppijanumeroException, YleistunnisteOidResponse> =
        client.onrGet("yleistunniste/hae/$oid", YleistunnisteOidResponse::class.java)
}
