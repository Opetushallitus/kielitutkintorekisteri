package fi.oph.kitu.oppijanumero

import org.springframework.stereotype.Service

@Service
class OppijanumeroTroubleshootingService(
    private val oppijanumeroService: OppijanumeroService,
) {
    fun troubleshootOppijaNameCombinations(oppija: Oppija): Oppija? =
        tryEachEtunimiAsKutsumanimi(oppija) ?: switchEtunimetAndSukunimi(oppija)

    fun tryEachEtunimiAsKutsumanimi(oppija: Oppija): Oppija? =
        oppija.etunimet
            .split(" ")
            .map { etunimet -> oppija.copy(kutsumanimi = etunimet) }
            .firstOrNull { oppija -> oppijanumeroService.getMasterOid(oppija).isRight() }

    fun henkiloByHetu(hetu: String): OppijanumerorekisteriHenkilo? =
        runCatching { oppijanumeroService.getHenkiloByHetu(hetu).getOrNull() }.getOrNull()

    fun switchEtunimetAndSukunimi(oppija: Oppija): Oppija? =
        tryEachEtunimiAsKutsumanimi(oppija.copy(etunimet = oppija.sukunimi, sukunimi = oppija.etunimet))
}
