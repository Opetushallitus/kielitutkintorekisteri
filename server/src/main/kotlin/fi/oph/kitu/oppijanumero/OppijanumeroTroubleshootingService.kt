package fi.oph.kitu.oppijanumero

import fi.oph.kitu.oid.Oid
import org.springframework.stereotype.Service

data class RatkennutNimiyhdistelma(
    val oppija: Oppija,
    val oppijanumero: Oid,
)

@Service
class OppijanumeroTroubleshootingService(
    private val oppijanumeroService: OppijanumeroService,
) {
    fun troubleshootOppijaNameCombinations(oppija: Oppija): RatkennutNimiyhdistelma? =
        tryEachEtunimiAsKutsumanimi(oppija) ?: switchEtunimetAndSukunimi(oppija)

    fun tryEachEtunimiAsKutsumanimi(oppija: Oppija): RatkennutNimiyhdistelma? =
        oppija.etunimet
            .split(" ")
            .firstNotNullOfOrNull { etunimi ->
                val yritetty = oppija.copy(kutsumanimi = etunimi)
                oppijanumeroService
                    .getMasterOid(yritetty)
                    .getOrNull()
                    ?.let { RatkennutNimiyhdistelma(yritetty, it) }
            }

    fun switchEtunimetAndSukunimi(oppija: Oppija): RatkennutNimiyhdistelma? =
        tryEachEtunimiAsKutsumanimi(oppija.copy(etunimet = oppija.sukunimi, sukunimi = oppija.etunimet))
}
