package fi.oph.kitu.yki

import fi.oph.kitu.html.DisplayEnum
import fi.oph.kitu.i18n.UiText
import java.time.LocalDate

// Solki-arviointitilat + Kielitutkintorekisterin omat tilat
enum class Arviointitila : DisplayEnum {
    ILMOITTAUTUNUT,
    PERUTTU,
    EI_SUORITUSTA,
    ARVIOITAVA,
    ARVIOITU,
    TARKISTUSARVIOITAVA,
    TARKISTUSARVIOITU,
    TARKISTUSARVIOINTI_HYVAKSYTTY,
    ;

    val viewText: String
        get() =
            when (this) {
                ILMOITTAUTUNUT -> UiText.Yki.Arviointitila.ilmoittautunut
                PERUTTU -> UiText.Yki.Arviointitila.ilmoittautuminenPeruttu
                EI_SUORITUSTA -> UiText.Yki.Arviointitila.eiSuoritusta
                ARVIOITAVA -> UiText.Yki.Arviointitila.suoritusArvioitavana
                ARVIOITU -> UiText.Yki.Arviointitila.arviointiValmis
                TARKISTUSARVIOITAVA -> UiText.Yki.Arviointitila.suoritusTarkistusarvioitavana
                TARKISTUSARVIOITU -> UiText.Yki.Arviointitila.tarkistusarviointiTehty
                TARKISTUSARVIOINTI_HYVAKSYTTY -> UiText.Yki.Arviointitila.tarkistusarviointiHyvaksytty
            }.toString()

    fun arvioitu() = listOf(ARVIOITU, TARKISTUSARVIOITAVA).contains(this) || tarkistusarvioitu()

    fun tarkistusarvioitu() = listOf(TARKISTUSARVIOITU, TARKISTUSARVIOINTI_HYVAKSYTTY).contains(this)

    fun pelkkäIlmoittautuminen() = ilmoittautumistilat.contains(this)

    override fun displayText(): String = viewText

    companion object {
        val ilmoittautumistilat = listOf(ILMOITTAUTUNUT, PERUTTU)
    }
}

fun laskeArviointitila(
    nykyinen: Arviointitila,
    osakoeCount: Int,
    arvosanaPuuttuu: Int,
    oikeitaArvosanoja: Int,
    onTarkistusarviointi: Boolean,
    tarkistuksenKasittelypaiva: LocalDate?,
): Arviointitila =
    when (nykyinen) {
        Arviointitila.ILMOITTAUTUNUT,
        Arviointitila.PERUTTU,
        Arviointitila.TARKISTUSARVIOINTI_HYVAKSYTTY,
        -> {
            nykyinen
        }

        else -> {
            when {
                onTarkistusarviointi && tarkistuksenKasittelypaiva != null -> Arviointitila.TARKISTUSARVIOITU
                onTarkistusarviointi -> Arviointitila.TARKISTUSARVIOITAVA
                osakoeCount == 0 -> nykyinen
                arvosanaPuuttuu > 0 -> Arviointitila.ARVIOITAVA
                oikeitaArvosanoja == 0 -> Arviointitila.EI_SUORITUSTA
                else -> Arviointitila.ARVIOITU
            }
        }
    }
