package fi.oph.kitu.yki

import com.fasterxml.jackson.annotation.JsonValue
import fi.oph.kitu.html.table.HideInTableFilter
import fi.oph.kitu.html.table.Nimetty
import fi.oph.kitu.i18n.LocalizedString
import fi.oph.kitu.i18n.UiText

/** ISO 639-2 Alpha 3
 *  Legacy language codes:
 *  10,Svenska,svenska,Swedish
 *  11,Kaupallinen englanti,företagsengelska,English for business
 *  12,Tekninen englanti,teknisk engelska,English for technology
 *  */
enum class Tutkintokieli(
    @get:JsonValue
    val solkiCode: String,
) : Nimetty {
    DEU("deu"),
    ENG("eng"),
    FIN("fin"),
    FRA("fra"),
    ITA("ita"),
    RUS("rus"),
    SME("sme"),
    SPA("spa"),
    SWE("swe"),

    @HideInTableFilter
    SWE10("swe10"),

    @HideInTableFilter
    ENG11("eng11"),

    @HideInTableFilter
    ENG12("eng12"),
    ;

    override val nimi: LocalizedString
        get() =
            when (this) {
                DEU -> UiText.Yki.Kieli.saksa
                ENG -> UiText.Yki.Kieli.englanti
                FIN -> UiText.Yki.Kieli.suomi
                FRA -> UiText.Yki.Kieli.ranska
                ITA -> UiText.Yki.Kieli.italia
                RUS -> UiText.Yki.Kieli.venaja
                SME -> UiText.Yki.Kieli.pohjoissaame
                SPA -> UiText.Yki.Kieli.espanja
                SWE -> UiText.Yki.Kieli.ruotsi
                SWE10 -> UiText.Yki.Kieli.ruotsiVanha
                ENG11 -> UiText.Yki.Kieli.kaupallinenEnglanti
                ENG12 -> UiText.Yki.Kieli.tekninenEnglanti
            }

    fun isLegacy(): Boolean = this in legacyEntries

    companion object {
        val legacyEntries = setOf(SWE10, ENG11, ENG12)
    }
}
