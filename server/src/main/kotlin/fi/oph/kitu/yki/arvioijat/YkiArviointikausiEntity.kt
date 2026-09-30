package fi.oph.kitu.yki.arvioijat

import fi.oph.kitu.i18n.LocalizedString
import fi.oph.kitu.i18n.UiText
import fi.oph.kitu.jdbc.getEnum
import fi.oph.kitu.jdbc.getLocalDateOrNull
import fi.oph.kitu.jdbc.getOffsetDateTimeOrNull
import fi.oph.kitu.jdbc.getTypedArray
import fi.oph.kitu.oid.Oid
import fi.oph.kitu.oid.getOidOrNull
import fi.oph.kitu.yki.Tutkintokieli
import fi.oph.kitu.yki.Tutkintotaso
import org.springframework.data.annotation.Id
import org.springframework.data.relational.core.mapping.MappedCollection
import org.springframework.data.relational.core.mapping.Table
import org.springframework.jdbc.core.RowMapper
import java.time.LocalDate
import java.time.OffsetDateTime

/**
 * Yksi arviointikausi. Kausi on arvioijakohtainen: kaikilla kielilla on sama kausi, ja
 * [oikeudet] kertoo mihin kieliin ja tasoihin se kohdistuu.
 */
@Table("yki_arvioija_arviointikausi")
data class YkiArviointikausiEntity(
    @Id
    val id: Number?,
    val arvioijaId: Number?,
    val alkupaiva: LocalDate,
    val paattymispaiva: LocalDate?,
    /** Kautta koskevan hallintopaatoksen viite; jokainen kausi on oma paatoksensa. */
    val ashaNumero: String? = null,
    /** Asetettu jos kausi katkaistiin kesken kauden, ks. [Arviointikausi]. */
    val passivoitu: OffsetDateTime? = null,
    val passivoijaOid: Oid? = null,
    val luotu: OffsetDateTime? = null,
    val luojaOid: Oid? = null,
    val muokattu: OffsetDateTime? = null,
    val muokkaajaOid: Oid? = null,
    @MappedCollection(keyColumn = "id", idColumn = "kausi_id")
    val oikeudet: List<YkiArviointikausiOikeusEntity> = emptyList(),
) {
    companion object {
        val fromRow =
            RowMapper { rs, _ ->
                YkiArviointikausiEntity(
                    id = rs.getInt("id"),
                    arvioijaId = rs.getInt("arvioija_id"),
                    alkupaiva = rs.getDate("alkupaiva").toLocalDate(),
                    paattymispaiva = rs.getLocalDateOrNull("paattymispaiva"),
                    ashaNumero = rs.getString("asha_numero"),
                    passivoitu = rs.getOffsetDateTimeOrNull("passivoitu"),
                    passivoijaOid = rs.getOidOrNull("passivoija_oid"),
                    luotu = rs.getOffsetDateTimeOrNull("luotu"),
                    luojaOid = rs.getOidOrNull("luoja_oid"),
                    muokattu = rs.getOffsetDateTimeOrNull("muokattu"),
                    muokkaajaOid = rs.getOidOrNull("muokkaaja_oid"),
                    oikeudet = emptyList(),
                )
            }
    }
}

@Table("yki_arvioija_arviointikausi_oikeus")
data class YkiArviointikausiOikeusEntity(
    @Id
    val id: Number?,
    val kausiId: Number?,
    val kieli: Tutkintokieli,
    val tasot: Set<Tutkintotaso>,
) {
    companion object {
        val fromRow =
            RowMapper { rs, _ ->
                YkiArviointikausiOikeusEntity(
                    id = rs.getInt("id"),
                    kausiId = rs.getInt("kausi_id"),
                    kieli = rs.getEnum<Tutkintokieli>("kieli"),
                    tasot = rs.getTypedArray("tasot") { taso -> Tutkintotaso.valueOf(taso) }.toSet(),
                )
            }
    }
}

/** Mika toimenpide kirjasi muutoslokirivin. */
enum class Kausitoimenpide {
    LISAYS,
    MUOKKAUS,
    PASSIVOINTI,
    POISTO,
    TALLENNUS,
    ;

    val nimi: LocalizedString
        get() =
            when (this) {
                LISAYS -> UiText.Yki.Arvioija.Kausi.lisays
                MUOKKAUS -> UiText.Yki.Arvioija.Kausi.muokkaus
                PASSIVOINTI -> UiText.Yki.Arvioija.Kausi.passivointi
                POISTO -> UiText.Yki.Arvioija.Kausi.poisto
                TALLENNUS -> UiText.Yki.Arvioija.Kausi.tallennus
            }
}
