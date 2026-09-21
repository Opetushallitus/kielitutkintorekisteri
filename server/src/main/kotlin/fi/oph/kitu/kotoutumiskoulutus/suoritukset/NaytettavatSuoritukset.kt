package fi.oph.kitu.kotoutumiskoulutus.suoritukset

import fi.oph.kitu.html.table.Nimetty
import fi.oph.kitu.i18n.LocalizedString
import fi.oph.kitu.i18n.UiText

enum class NaytettavatSuoritukset : Nimetty {
    VALMIIT,
    KESKENERAISET,
    KAIKKI,
    ;

    override val nimi: LocalizedString
        get() =
            when (this) {
                VALMIIT -> UiText.Filter.valmiit
                KESKENERAISET -> UiText.Filter.keskeneraiset
                KAIKKI -> UiText.Filter.kaikki
            }

    val whereSql: String?
        get() =
            when (this) {
                VALMIIT -> "completed"
                KESKENERAISET -> "NOT completed"
                KAIKKI -> null
            }
}
