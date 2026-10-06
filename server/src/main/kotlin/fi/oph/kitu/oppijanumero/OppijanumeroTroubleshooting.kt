package fi.oph.kitu.oppijanumero

import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity

data class OppijanumeroRatkaisuehdotus(
    val viesti: String,
    val korjatutNimet: Oppija? = null,
)

fun OppijanumeroTroubleshootingService.troubleshootOppijanumero(
    oppija: Oppija,
    response: ResponseEntity<String>?,
): OppijanumeroRatkaisuehdotus =
    if (response != null && (
            response.statusCode == HttpStatus.BAD_REQUEST ||
                response.statusCode == HttpStatus.NOT_FOUND ||
                response.statusCode == HttpStatus.CONFLICT
        )
    ) {
        troubleshootOppijaNameCombinations(oppija)
            ?.let { OppijanumeroRatkaisuehdotus(NIMIYHDISTELMA_LOYTYI, it) }
            ?: onrNimetHetulla(oppija)
            ?: OppijanumeroRatkaisuehdotus(
                """
                Oppijanumerorekisteristä ei löytynyt oppijanumeroa, kun kaikkia etunimiä testattiin kutsumanimenä ja etu- ja sukunimi vaihdettiin päittäin.
                Mahdollisesti henkilötunnuksessa tai jossain nimistä on kirjoitusvirhe, joku nimi puuttuu, tai nimet ovat väärässä järjestyksessä.
                """.trimIndent(),
            )
    } else {
        OppijanumeroRatkaisuehdotus(
            "Oppijanumerorekisterin virhe ei viittaa virheellisiin oppijan nimitietoihin. Tarkista virheviesti.",
        )
    }

const val NIMIYHDISTELMA_LOYTYI =
    "Oppijanumero löytyi, kun nimet kirjoitettiin toisin. Korjaa nimet lähdejärjestelmään näin:"

const val HETULLA_LOYTYI =
    "Oppijanumerorekisterissä on tällä henkilötunnuksella henkilö eri nimillä. Korjaa nimet lähdejärjestelmään näin:"

private fun OppijanumeroTroubleshootingService.onrNimetHetulla(oppija: Oppija): OppijanumeroRatkaisuehdotus? =
    henkiloByHetu(oppija.hetu)?.let { henkilo ->
        OppijanumeroRatkaisuehdotus(
            HETULLA_LOYTYI,
            Oppija(
                etunimet = henkilo.etunimet.orEmpty(),
                hetu = oppija.hetu,
                kutsumanimi = henkilo.kutsumanimi.orEmpty(),
                sukunimi = henkilo.sukunimi.orEmpty(),
            ),
        )
    }
