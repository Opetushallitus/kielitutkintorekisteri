package fi.oph.kitu.yki.arvioijat.hakemus

import fi.oph.kitu.i18n.LocalizedString
import fi.oph.kitu.i18n.UiText
import fi.oph.kitu.jdbc.getEnum
import fi.oph.kitu.jdbc.getLocalDateOrNull
import fi.oph.kitu.yki.arvioijat.TallennaArvioija
import org.springframework.jdbc.core.RowMapper
import java.time.LocalDate
import java.time.OffsetDateTime

/** Hakemuksen tiedot kartoitettuna; puuttuvat yhteystiedot taydennetaan ONR:sta. */
data class Arvioijahakemus(
    val hakemusOid: String,
    val henkiloOid: String,
    val arviointioikeudet: List<TallennaArvioija.Arviointioikeus>,
    val sahkopostiosoite: String?,
    val katuosoite: String?,
    val postinumero: String?,
    val postitoimipaikka: String?,
)

enum class ArvioijahakemuksenTila {
    KASITELTY,
    HYLATTY,
    EI_TAYTA_EHTOJA,
    ODOTTAA_YKSILOINTIA,
    KASITTELYSSA,
    ;

    val nimi: LocalizedString
        get() =
            when (this) {
                KASITELTY -> UiText.Yki.Arvioijahakemus.Tila.kasitelty
                HYLATTY -> UiText.Yki.Arvioijahakemus.Tila.hylatty
                EI_TAYTA_EHTOJA -> UiText.Yki.Arvioijahakemus.Tila.eiTaytaEhtoja
                ODOTTAA_YKSILOINTIA -> UiText.Yki.Arvioijahakemus.Tila.odottaaYksilointia
                KASITTELYSSA -> UiText.Yki.Arvioijahakemus.Tila.kasittelyssa
            }
}

data class ArvioijahakemusEntity(
    val hakemusOid: String,
    val henkiloOid: String?,
    val tila: ArvioijahakemuksenTila,
    val syy: String?,
    val arvioijaId: Int?,
    val kaudenAlkupaiva: LocalDate?,
    val kasitelty: OffsetDateTime,
) {
    companion object {
        val fromRow =
            RowMapper { rs, _ ->
                ArvioijahakemusEntity(
                    hakemusOid = rs.getString("hakemus_oid"),
                    henkiloOid = rs.getString("henkilo_oid"),
                    tila = rs.getEnum("tila"),
                    syy = rs.getString("syy"),
                    arvioijaId = rs.getObject("arvioija_id") as Int?,
                    kaudenAlkupaiva = rs.getLocalDateOrNull("kauden_alkupaiva"),
                    kasitelty = rs.getObject("kasitelty", OffsetDateTime::class.java),
                )
            }
    }
}
