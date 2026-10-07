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
            ?: tryAllNameCombinations(oppija)?.let { OppijanumeroRatkaisuehdotus(NIMIYHDISTELMA_LOYTYI, it) }
            ?: OppijanumeroRatkaisuehdotus(EI_LOYTYNYT)
    } else {
        OppijanumeroRatkaisuehdotus(
            "Oppijanumerorekisterin virhe ei viittaa virheellisiin oppijan nimitietoihin. Tarkista virheviesti.",
        )
    }

const val EI_LOYTYNYT =
    "Oppijanumerorekisteristä ei löytynyt oppijanumeroa henkilötunnuksella eikä kokeilemalla nimiä " +
        "eri järjestyksissä ja eri kutsumanimillä. Mahdollisesti henkilötunnuksessa tai jossain nimistä " +
        "on kirjoitusvirhe, tai joku nimi puuttuu."

const val NIMIYHDISTELMA_LOYTYI =
    "Oppijanumero löytyi, kun nimet kirjoitettiin toisin. Korjaa nimet lähdejärjestelmään näin:"

const val HETULLA_LOYTYI =
    "Oppijanumerorekisterissä on tällä henkilötunnuksella henkilö eri nimillä. Korjaa nimet lähdejärjestelmään näin:"

const val HETULLA_LOYTYI_ERI_HENKILO =
    "Nimi oppijanumerorekisterissä ei vastaa suorituksella olevaa nimeä. Tarkista henkilötunnus. " +
        "Oppijanumerorekisterissä tällä henkilötunnuksella on:"

private fun OppijanumeroTroubleshootingService.onrNimetHetulla(oppija: Oppija): OppijanumeroRatkaisuehdotus? =
    henkiloByHetu(oppija.hetu)?.let { henkilo ->
        val onrNimet =
            Oppija(
                etunimet = henkilo.etunimet.orEmpty(),
                hetu = oppija.hetu,
                kutsumanimi = henkilo.kutsumanimi.orEmpty(),
                sukunimi = henkilo.sukunimi.orEmpty(),
            )
        OppijanumeroRatkaisuehdotus(
            if (nimetVastaavat(oppija.etunimet, oppija.sukunimi, onrNimet.etunimet, onrNimet.sukunimi)) {
                HETULLA_LOYTYI
            } else {
                HETULLA_LOYTYI_ERI_HENKILO
            },
            onrNimet,
        )
    }

fun nimetVastaavat(
    etunimet: String?,
    sukunimi: String?,
    onrEtunimet: String?,
    onrSukunimi: String?,
): Boolean {
    val nimet = nimiosat(etunimet, sukunimi)
    val onrNimiosat = nimiosat(onrEtunimet, onrSukunimi)
    return nimet.any { nimi -> onrNimiosat.any { lahes(nimi, it) } }
}

private fun nimiosat(vararg nimet: String?): List<String> =
    nimet
        .filterNotNull()
        .flatMap { it.lowercase().split(Regex("[\\s-]+")) }
        .filter { it.isNotEmpty() }

private fun lahes(
    a: String,
    b: String,
): Boolean {
    if (a == b) return true
    if (minOf(a.length, b.length) < 3) return false
    val sallittu = if (maxOf(a.length, b.length) >= 8) 2 else 1
    return editointietaisyys(a, b) <= sallittu
}

private fun editointietaisyys(
    a: String,
    b: String,
): Int {
    var edellinen = IntArray(b.length + 1) { it }
    for (i in a.indices) {
        val nykyinen = IntArray(b.length + 1)
        nykyinen[0] = i + 1
        for (j in b.indices) {
            val vaihto = edellinen[j] + if (a[i] == b[j]) 0 else 1
            nykyinen[j + 1] = minOf(vaihto, edellinen[j + 1] + 1, nykyinen[j] + 1)
        }
        edellinen = nykyinen
    }
    return edellinen[b.length]
}
