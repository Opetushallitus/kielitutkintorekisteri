package fi.oph.kitu.yki.suoritukset

import fi.oph.kitu.i18n.LocalizedString
import fi.oph.kitu.i18n.UiText

enum class YkiSuoritusAikaryhmittely(
    val sarake: YkiSuoritusTilastoColumn?,
) {
    Ei(null),
    Tutkintopaiva(YkiSuoritusTilastoColumn.Tutkintopaiva),
    Tutkintovuosi(YkiSuoritusTilastoColumn.Tutkintovuosi),
    ;

    val nimi: LocalizedString
        get() =
            when (this) {
                Ei -> UiText.Yki.Tilastot.eiAikaryhmittelya
                Tutkintopaiva -> UiText.Yki.Sarake.tutkintopaiva
                Tutkintovuosi -> UiText.Yki.Sarake.tutkintovuosi
            }
}
