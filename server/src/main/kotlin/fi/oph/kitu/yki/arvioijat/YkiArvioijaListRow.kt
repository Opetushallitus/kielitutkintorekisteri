package fi.oph.kitu.yki.arvioijat

import fi.oph.kitu.i18n.LocalizedString
import fi.oph.kitu.i18n.UiText
import fi.oph.kitu.jdbc.getEnum
import fi.oph.kitu.jdbc.getLocalDateOrNull
import fi.oph.kitu.jdbc.getOffsetDateTimeOrNull
import fi.oph.kitu.jdbc.getTypedArray
import fi.oph.kitu.oid.Oid
import fi.oph.kitu.oid.getOid
import fi.oph.kitu.yki.Tutkintokieli
import fi.oph.kitu.yki.Tutkintotaso
import org.springframework.jdbc.core.RowMapper
import java.time.LocalDate
import java.time.OffsetDateTime

/**
 * Litteä listanäkymän projektio: yksi rivi per arvioija × arviointioikeus, jotta kieli ja
 * tasot ovat omia lajiteltavia sarakkeitaan. Henkilötiedot toistuvat riveillä.
 */
data class YkiArvioijaListRow(
    val arvioijaId: Int,
    val arvioijaOid: Oid,
    val sukunimi: String,
    val etunimet: String,
    val sahkopostiosoite: String?,
    val katuosoite: String,
    val postinumero: String,
    val postitoimipaikka: String,
    val ashaNumero: String?,
    val kieli: Tutkintokieli,
    val tasot: Set<Tutkintotaso>,
    val tila: Rekisterointitila,
    val kaudenAlkupaiva: LocalDate?,
    val kaudenPaattymispaiva: LocalDate?,
    val jatkorekisterointi: Boolean,
    val ensimmainenRekisterointipaiva: LocalDate,
    val muokattu: OffsetDateTime?,
    val solkiinLahetetty: OffsetDateTime?,
    val solkiLahetysvirhe: String?,
    val solkiLahetysyritykset: Int,
) {
    val osoite: String get() = "$katuosoite, $postinumero $postitoimipaikka"

    val solkiTila: LocalizedString
        get() =
            when {
                solkiLahetysvirhe != null -> UiText.Yki.solkiLahetysEpaonnistui
                solkiinLahetetty == null -> UiText.Yki.odottaaLahetysta
                else -> UiText.Yki.solkiLahetysOnnistui
            }

    companion object {
        val fromRow =
            RowMapper { rs, _ ->
                YkiArvioijaListRow(
                    arvioijaId = rs.getInt("arvioija_id"),
                    arvioijaOid = rs.getOid("arvioija_oid"),
                    sukunimi = rs.getString("sukunimi"),
                    etunimet = rs.getString("etunimet"),
                    sahkopostiosoite = rs.getString("sahkopostiosoite"),
                    katuosoite = rs.getString("katuosoite"),
                    postinumero = rs.getString("postinumero"),
                    postitoimipaikka = rs.getString("postitoimipaikka"),
                    ashaNumero = rs.getString("asha_numero"),
                    kieli = rs.getEnum<Tutkintokieli>("kieli"),
                    tasot = rs.getTypedArray("tasot") { taso -> Tutkintotaso.valueOf(taso) }.toSet(),
                    tila = rs.getEnum<Rekisterointitila>("tila"),
                    kaudenAlkupaiva = rs.getLocalDateOrNull("kauden_alkupaiva"),
                    kaudenPaattymispaiva = rs.getLocalDateOrNull("kauden_paattymispaiva"),
                    jatkorekisterointi = rs.getBoolean("jatkorekisterointi"),
                    ensimmainenRekisterointipaiva = rs.getDate("ensimmainen_rekisterointipaiva").toLocalDate(),
                    muokattu = rs.getOffsetDateTimeOrNull("muokattu"),
                    solkiinLahetetty = rs.getOffsetDateTimeOrNull("solkiin_lahetetty"),
                    solkiLahetysvirhe = rs.getString("solki_lahetysvirhe"),
                    solkiLahetysyritykset = rs.getInt("solki_lahetysyritykset"),
                )
            }
    }
}
