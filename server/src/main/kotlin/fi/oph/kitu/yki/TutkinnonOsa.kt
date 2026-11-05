package fi.oph.kitu.yki

import fi.oph.kitu.html.DisplayEnum
import fi.oph.kitu.html.table.HideInTableFilter
import fi.oph.kitu.i18n.UiText

enum class TutkinnonOsa : DisplayEnum {
    PU,
    KI,
    TY,
    PY,

    @HideInTableFilter
    RS,

    @HideInTableFilter
    YL,
    ;

    val viewText: String
        get() =
            when (this) {
                PU -> UiText.Yki.Sarake.puhuminen
                KI -> UiText.Yki.Sarake.kirjoittaminen
                TY -> UiText.Yki.Sarake.tekstinYmmartaminen
                PY -> UiText.Yki.Sarake.puheenYmmartaminen
                RS -> UiText.Yki.Sarake.rakenteetJaSanasto
                YL -> UiText.Yki.Sarake.yleisarvosana
            }.toString()

    override fun displayText(): String = viewText

    companion object {
        // Solki-koodit avattuna:
        val puhuminen = PU
        val kirjoittaminen = KI
        val tekstinYmmartaminen = TY
        val puheenYmmartaminen = PY
        val rakenteetJaSanasto = RS
        val yleisarvosana = YL
    }
}
