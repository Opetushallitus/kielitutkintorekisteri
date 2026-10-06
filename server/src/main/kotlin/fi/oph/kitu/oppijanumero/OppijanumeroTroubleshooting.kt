package fi.oph.kitu.oppijanumero

import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity

fun OppijanumeroTroubleshootingService.troubleshootOppijanumero(
    oppija: Oppija,
    response: ResponseEntity<String>?,
): String =
    if (response != null && (
            response.statusCode == HttpStatus.BAD_REQUEST ||
                response.statusCode == HttpStatus.NOT_FOUND ||
                response.statusCode == HttpStatus.CONFLICT
        )
    ) {
        val notFound =
            """
            Oppijanumerorekisteristä ei löytynyt oppijanumeroa, kun kaikkia etunimiä testattiin kutsumanimenä ja etu- ja sukunimi vaihdettiin päittäin.
            Mahdollisesti henkilötunnuksessa tai jossain nimistä on kirjoitusvirhe, joku nimi puuttuu, tai nimet ovat väärässä järjestyksessä.
            """.trimIndent()
        troubleshootOppijaNameCombinations(oppija)
            ?.let { success ->
                "etunimet: ${success.etunimet}, kutsumanimi: ${success.kutsumanimi}, sukunimi: ${success.sukunimi}"
            }
            ?: onrNimetHetulla(oppija.hetu)
            ?: notFound
    } else {
        "Oppijanumerorekisterin virhe ei viittaa virheellisiin oppijan nimitietoihin. Tarkista virheviesti."
    }

private fun OppijanumeroTroubleshootingService.onrNimetHetulla(hetu: String): String? =
    henkiloByHetu(hetu)?.let { henkilo ->
        "Oppijanumerorekisterissä on tällä henkilötunnuksella henkilö eri nimillä. " +
            "Nimet tulee kirjoittaa täsmälleen näin: " +
            "etunimet: \"${henkilo.etunimet.orEmpty()}\", " +
            "kutsumanimi: \"${henkilo.kutsumanimi.orEmpty()}\", " +
            "sukunimi: \"${henkilo.sukunimi.orEmpty()}\""
    }
