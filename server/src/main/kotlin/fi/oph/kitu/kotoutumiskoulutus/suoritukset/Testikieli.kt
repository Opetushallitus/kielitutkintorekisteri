package fi.oph.kitu.kotoutumiskoulutus.suoritukset

import fi.oph.kitu.html.table.Nimetty
import fi.oph.kitu.i18n.LocalizedString
import fi.oph.kitu.i18n.UiText

enum class Testikieli : Nimetty {
    FIN,
    SWE,
    ;

    override val nimi: LocalizedString
        get() =
            when (this) {
                FIN -> UiText.Koto.Kieli.fin
                SWE -> UiText.Koto.Kieli.swe
            }

    companion object {
        fun fromString(str: String): Testikieli =
            when (str) {
                "fi" -> FIN
                "sv" -> SWE
                else -> throw IllegalArgumentException("Invalid language code $str")
            }
    }
}
