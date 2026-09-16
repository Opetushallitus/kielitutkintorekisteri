package fi.oph.kitu.yki.arvioijat

import fi.oph.kitu.jdbc.getTypedArray
import fi.oph.kitu.oid.Oid
import fi.oph.kitu.oid.getOid
import fi.oph.kitu.oid.getOidOrNull
import fi.oph.kitu.yki.Tutkintokieli
import fi.oph.kitu.yki.Tutkintotaso
import org.springframework.data.annotation.Id
import org.springframework.data.relational.core.mapping.MappedCollection
import org.springframework.data.relational.core.mapping.Table
import org.springframework.jdbc.core.RowMapper
import java.time.LocalDate
import java.time.OffsetDateTime

@Table(name = "yki_arvioija")
data class YkiArvioijaEntity(
    @Id
    val id: Number?,
    val arvioijaOid: Oid,
    val henkilotunnus: String?,
    val sukunimi: String,
    val etunimet: String,
    val sahkopostiosoite: String?,
    val katuosoite: String,
    val postinumero: String,
    val postitoimipaikka: String,
    val arvioijanEnsimmainenRekisterointipaiva: LocalDate? = null,
    val passivoitu: OffsetDateTime? = null,
    val luotu: OffsetDateTime? = null,
    val luojaOid: Oid? = null,
    val muokattu: OffsetDateTime? = null,
    val muokkaajaOid: Oid? = null,
    val solkiinLahetetty: OffsetDateTime? = null,
    val solkiLahetysvirhe: String? = null,
    val solkiLahetysyritykset: Int = 0,
    val solkiViimeisinLahetysyritys: OffsetDateTime? = null,
    val solkiTunnus: String? = null,
    @MappedCollection(keyColumn = "id", idColumn = "arvioija_id")
    val arviointioikeudet: List<YkiArviointioikeusEntity>,
) {
    companion object {
        val fromRow =
            RowMapper { rs, _ ->
                YkiArvioijaEntity(
                    id = rs.getInt("id"),
                    arvioijaOid = rs.getOid("arvioija_oid"),
                    henkilotunnus = rs.getString("henkilotunnus"),
                    sukunimi = rs.getString("sukunimi"),
                    etunimet = rs.getString("etunimet"),
                    sahkopostiosoite = rs.getString("sahkopostiosoite"),
                    katuosoite = rs.getString("katuosoite"),
                    postinumero = rs.getString("postinumero"),
                    postitoimipaikka = rs.getString("postitoimipaikka"),
                    arvioijanEnsimmainenRekisterointipaiva =
                        rs.getDate("arvioijan_ensimmainen_rekisterointipaiva")?.toLocalDate(),
                    passivoitu = rs.getObject("passivoitu", OffsetDateTime::class.java),
                    luotu = rs.getObject("luotu", OffsetDateTime::class.java),
                    luojaOid = rs.getOidOrNull("luoja_oid"),
                    muokattu = rs.getObject("muokattu", OffsetDateTime::class.java),
                    muokkaajaOid = rs.getOidOrNull("muokkaaja_oid"),
                    solkiinLahetetty = rs.getObject("solkiin_lahetetty", OffsetDateTime::class.java),
                    solkiLahetysvirhe = rs.getString("solki_lahetysvirhe"),
                    solkiLahetysyritykset = rs.getInt("solki_lahetysyritykset"),
                    solkiViimeisinLahetysyritys =
                        rs.getObject("solki_viimeisin_lahetysyritys", OffsetDateTime::class.java),
                    solkiTunnus = rs.getString("solki_tunnus"),
                    arviointioikeudet = emptyList(),
                )
            }
    }
}

@Table("yki_arviointioikeus")
data class YkiArviointioikeusEntity(
    @Id
    val id: Number?,
    val arvioijaId: Number?,
    val kieli: Tutkintokieli,
    val tasot: Set<Tutkintotaso>,
    val tila: YkiArvioijaTila?,
    val kaudenAlkupaiva: LocalDate?,
    val kaudenPaattymispaiva: LocalDate?,
    val jatkorekisterointi: Boolean,
    val ensimmainenRekisterointipaiva: LocalDate,
    val rekisteriintuontiaika: OffsetDateTime?,
    val ashaNumero: String? = null,
) {
    companion object {
        val fromRow =
            RowMapper { rs, _ ->
                YkiArviointioikeusEntity(
                    id = rs.getInt("id"),
                    arvioijaId = rs.getInt("arvioija_id"),
                    kieli = Tutkintokieli.valueOf(rs.getString("kieli")),
                    tasot = rs.getTypedArray("tasot") { taso -> Tutkintotaso.valueOf(taso) }.toSet(),
                    tila = rs.getString("tila")?.let(YkiArvioijaTila::valueOf),
                    kaudenAlkupaiva = rs.getDate("kauden_alkupaiva")?.toLocalDate(),
                    kaudenPaattymispaiva = rs.getDate("kauden_paattymispaiva")?.toLocalDate(),
                    jatkorekisterointi = rs.getBoolean("jatkorekisterointi"),
                    rekisteriintuontiaika = rs.getObject("rekisteriintuontiaika", OffsetDateTime::class.java),
                    ensimmainenRekisterointipaiva = rs.getDate("ensimmainen_rekisterointipaiva").toLocalDate(),
                    ashaNumero = rs.getString("asha_numero"),
                )
            }
    }
}

/**
 * Yksi muutoslokirivi. Append-only: rivi kirjataan vain kun kausi tosiasiassa muuttuu. Kaudet
 * itse elavat [YkiArviointikausiEntity]-rivilla.
 */
@Table("yki_arvioija_kausi")
data class YkiArvioijaKausiEntity(
    @Id
    val id: Number?,
    val arvioijaId: Number?,
    val kieli: Tutkintokieli,
    val tasot: Set<Tutkintotaso>,
    val tila: YkiArvioijaTila?,
    val kaudenAlkupaiva: LocalDate?,
    val kaudenPaattymispaiva: LocalDate?,
    val jatkorekisterointi: Boolean,
    val ashaNumero: String?,
    val toimenpide: Kausitoimenpide?,
    val kausiId: Number?,
    val kirjattu: OffsetDateTime?,
    val kirjaajaOid: Oid?,
) {
    companion object {
        val fromRow =
            RowMapper { rs, _ ->
                YkiArvioijaKausiEntity(
                    id = rs.getInt("id"),
                    arvioijaId = rs.getInt("arvioija_id"),
                    kieli = Tutkintokieli.valueOf(rs.getString("kieli")),
                    tasot = rs.getTypedArray("tasot") { taso -> Tutkintotaso.valueOf(taso) }.toSet(),
                    tila = rs.getString("tila")?.let(YkiArvioijaTila::valueOf),
                    kaudenAlkupaiva = rs.getDate("kauden_alkupaiva")?.toLocalDate(),
                    kaudenPaattymispaiva = rs.getDate("kauden_paattymispaiva")?.toLocalDate(),
                    jatkorekisterointi = rs.getBoolean("jatkorekisterointi"),
                    ashaNumero = rs.getString("asha_numero"),
                    toimenpide = rs.getString("toimenpide")?.let(Kausitoimenpide::valueOf),
                    kausiId = rs.getObject("kausi_id", Integer::class.java),
                    kirjattu = rs.getObject("kirjattu", OffsetDateTime::class.java),
                    kirjaajaOid = rs.getOidOrNull("kirjaaja_oid"),
                )
            }
    }
}
