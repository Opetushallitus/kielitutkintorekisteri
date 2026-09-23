package fi.oph.kitu.oppijanumero

import arrow.core.Either
import arrow.core.left
import arrow.core.right
import fi.oph.kitu.oid.Oid
import io.opentelemetry.instrumentation.annotations.WithSpan
import org.springframework.http.HttpStatus
import org.springframework.stereotype.Service

@Service
class OppijanumeroHakuService(
    private val oppijanumeroService: OppijanumeroService,
    private val troubleshooting: OppijanumeroTroubleshootingService,
) {
    @WithSpan
    fun haeMasterOid(oppija: Oppija): Either<OppijanumeroException, Oid> =
        oppijanumeroService.getMasterOid(oppija).fold(
            ifLeft = { virhe ->
                if (virhe.voiJohtuaNimienMuodosta()) {
                    troubleshooting
                        .troubleshootOppijaNameCombinations(oppija)
                        ?.let { oppijanumeroService.getMasterOid(it).getOrNull() }
                        ?.right()
                        ?: virhe.left()
                } else {
                    virhe.left()
                }
            },
            ifRight = { it.right() },
        )

    private fun OppijanumeroException.voiJohtuaNimienMuodosta(): Boolean =
        when (this) {
            is OppijanumeroException.OppijaNotIdentifiedException,
            is OppijanumeroException.OppijaNotFoundException,
            -> true

            is OppijanumeroException.BadRequest -> response.statusCode == HttpStatus.BAD_REQUEST

            else -> false
        }

    fun oppijaOf(
        hetu: String,
        etunimet: String,
        sukunimi: String,
        kutsumanimi: String?,
    ): Oppija =
        Oppija(
            etunimet = etunimet.trim(),
            hetu = hetu.trim(),
            kutsumanimi =
                kutsumanimi
                    ?.trim()
                    ?.takeIf { it.isNotEmpty() }
                    ?: etunimet.trim().split(" ").first(),
            sukunimi = sukunimi.trim(),
        )
}
