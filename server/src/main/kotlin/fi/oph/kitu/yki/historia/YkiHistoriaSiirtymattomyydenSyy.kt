package fi.oph.kitu.yki.historia

import fi.oph.kitu.html.table.Nimetty
import fi.oph.kitu.i18n.LocalizedString
import fi.oph.kitu.i18n.UiText

/**
 * Migraatioskriptin kirjaamasta vapaasta syytekstista johdettu luokka. Vastaa tietokannan
 * tyyppia yki_historia_siirtymattomyyden_syy; arvot kirjoittaa latausskripti.
 */
enum class YkiHistoriaSiirtymattomyydenSyy : Nimetty {
    EI_OPPIJANUMEROA,
    PAIKALLINEN_VALIDOINTI,
    API_HYLKASI,
    RIKKINAINEN_RIVI,
    LAST_MODIFIED_EI_JASENNY,
    MUU,
    ;

    override val nimi: LocalizedString
        get() =
            when (this) {
                EI_OPPIJANUMEROA -> UiText.Yki.Historia.syyEiOppijanumeroa
                PAIKALLINEN_VALIDOINTI -> UiText.Yki.Historia.syyPaikallinenValidointi
                API_HYLKASI -> UiText.Yki.Historia.syyApiHylkasi
                RIKKINAINEN_RIVI -> UiText.Yki.Historia.syyRikkinainenRivi
                LAST_MODIFIED_EI_JASENNY -> UiText.Yki.Historia.syyLastModifiedEiJasenny
                MUU -> UiText.Yki.Historia.syyMuu
            }
}
