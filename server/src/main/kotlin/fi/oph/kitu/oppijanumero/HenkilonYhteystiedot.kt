package fi.oph.kitu.oppijanumero

import fi.oph.kitu.oppijanumero.OppijanumerorekisteriHenkilo.Yhteystietoryhma

data class HenkilonYhteystiedot(
    val puhelinnumero: String?,
    val sahkoposti: String?,
    val katuosoite: String?,
    val postinumero: String?,
    val postitoimipaikka: String?,
    val maa: String?,
)

private const val KOTIOSOITE = "yhteystietotyyppi1"
private const val TYOOSOITE = "yhteystietotyyppi2"
private const val VTJ_VAKINAINEN_KOTIMAINEN_OSOITE = "yhteystietotyyppi4"
private const val VTJ_VAKINAINEN_ULKOMAINEN_OSOITE = "yhteystietotyyppi5"
private const val HAKEMUKSELTA_TULEVA_YHTEYSTIETO = "yhteystietotyyppi6"
private const val VTJ_SAHKOINEN_OSOITE = "yhteystietotyyppi8"

private const val SAHKOPOSTI = "YHTEYSTIETO_SAHKOPOSTI"
private const val PUHELINNUMERO = "YHTEYSTIETO_PUHELINNUMERO"
private const val MATKAPUHELINNUMERO = "YHTEYSTIETO_MATKAPUHELINNUMERO"
private const val KATUOSOITE = "YHTEYSTIETO_KATUOSOITE"
private const val POSTINUMERO = "YHTEYSTIETO_POSTINUMERO"
private const val KAUPUNKI = "YHTEYSTIETO_KAUPUNKI"
private const val KUNTA = "YHTEYSTIETO_KUNTA"
private const val MAA = "YHTEYSTIETO_MAA"

private val KOTIOSOITTEEN_RYHMAT =
    listOf(VTJ_VAKINAINEN_KOTIMAINEN_OSOITE, VTJ_VAKINAINEN_ULKOMAINEN_OSOITE, KOTIOSOITE)

fun OppijanumerorekisteriHenkilo.yhteystiedot(): HenkilonYhteystiedot {
    val ryhmat = yhteystiedotRyhma.orEmpty()
    val kotiosoite =
        KOTIOSOITTEEN_RYHMAT.firstNotNullOfOrNull { kuvaus ->
            ryhmat.firstOrNull { it.ryhmaKuvaus == kuvaus && it.arvo(KATUOSOITE) != null }
        }
    val yhteydenottoryhmat = ryhmat.sortedBy { it.yhteydenotonJarjestys() }

    fun ensimmainen(vararg tyypit: String): String? =
        yhteydenottoryhmat.firstNotNullOfOrNull { ryhma -> tyypit.firstNotNullOfOrNull { ryhma.arvo(it) } }

    return HenkilonYhteystiedot(
        puhelinnumero = ensimmainen(MATKAPUHELINNUMERO, PUHELINNUMERO),
        sahkoposti = ensimmainen(SAHKOPOSTI),
        katuosoite = kotiosoite?.arvo(KATUOSOITE),
        postinumero = kotiosoite?.arvo(POSTINUMERO),
        postitoimipaikka = kotiosoite?.let { it.arvo(KAUPUNKI) ?: it.arvo(KUNTA) },
        maa = kotiosoite?.arvo(MAA),
    )
}

private fun Yhteystietoryhma.yhteydenotonJarjestys(): Int =
    when (ryhmaKuvaus) {
        KOTIOSOITE -> 0
        VTJ_SAHKOINEN_OSOITE -> 1
        HAKEMUKSELTA_TULEVA_YHTEYSTIETO -> 2
        TYOOSOITE -> 4
        else -> 3
    }

private fun Yhteystietoryhma.arvo(tyyppi: String): String? =
    yhteystieto
        .orEmpty()
        .firstNotNullOfOrNull { yhteystieto ->
            yhteystieto.yhteystietoArvo
                ?.trim()
                ?.takeIf { yhteystieto.yhteystietoTyyppi == tyyppi && it.isNotEmpty() }
        }
