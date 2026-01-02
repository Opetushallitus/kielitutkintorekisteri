package fi.oph.kitu.yki.suoritukset

import fi.oph.kitu.Oid
import fi.oph.kitu.i18n.finnishDate
import fi.oph.kitu.jdbc.getTypedArrayOrNull
import fi.oph.kitu.yki.KituArviointitila
import fi.oph.kitu.yki.Sukupuoli
import fi.oph.kitu.yki.TutkinnonOsa
import fi.oph.kitu.yki.Tutkintokieli
import fi.oph.kitu.yki.Tutkintotaso
import jakarta.persistence.EnumType
import jakarta.persistence.Enumerated
import org.springframework.data.annotation.Id
import org.springframework.data.relational.core.mapping.Table
import org.springframework.jdbc.core.RowMapper
import java.time.Instant
import java.time.LocalDate

@Table(name = "yki_suoritus")
data class YkiSuoritusEntity(
    @Id
    val id: Int?,
    val suorittajanOID: Oid,
    val hetu: String,
    @Enumerated(EnumType.STRING)
    val sukupuoli: Sukupuoli,
    val sukunimi: String,
    val etunimet: String,
    val kansalaisuus: String,
    val katuosoite: String,
    val postinumero: String,
    val postitoimipaikka: String,
    val email: String?,
    val suoritusId: Int,
    val lastModified: Instant,
    val tutkintopaiva: LocalDate,
    @Enumerated(EnumType.STRING)
    val tutkintokieli: Tutkintokieli,
    @Enumerated(EnumType.STRING)
    val tutkintotaso: Tutkintotaso,
    val jarjestajanTunnusOid: Oid,
    val jarjestajanNimi: String,
    val arviointipaiva: LocalDate?,
    val tekstinYmmartaminen: Int?,
    val kirjoittaminen: Int?,
    val rakenteetJaSanasto: Int?,
    val puheenYmmartaminen: Int?,
    val puhuminen: Int?,
    val yleisarvosana: Int?,
    val tarkistusarvioinninSaapumisPvm: LocalDate?,
    val tarkistusarvioinninAsiatunnus: String?,
    val tarkistusarvioidutOsakokeet: Set<TutkinnonOsa>?,
    val arvosanaMuuttui: Set<TutkinnonOsa>?,
    val perustelu: String?,
    val tarkistusarvioinninKasittelyPvm: LocalDate?,
    val tarkistusarviointiHyvaksyttyPvm: LocalDate?,
    val koskiOpiskeluoikeus: Oid?,
    val koskiSiirtoKasitelty: Boolean?,
    @Enumerated(EnumType.STRING)
    val arviointitila: KituArviointitila,
) {
    fun arvosana(osakoe: TutkinnonOsa): Int? =
        when (osakoe) {
            TutkinnonOsa.PU -> puhuminen
            TutkinnonOsa.KI -> kirjoittaminen
            TutkinnonOsa.TY -> tekstinYmmartaminen
            TutkinnonOsa.PY -> puheenYmmartaminen
            TutkinnonOsa.RS -> rakenteetJaSanasto
            TutkinnonOsa.YL -> yleisarvosana
        }

    fun tarkistusarviointiHyvaksyttyViewText(): String? =
        tarkistusarviointiHyvaksyttyPvm?.finnishDate()
            ?: if (arviointitila == KituArviointitila.TARKISTUSARVIOINTI_HYVAKSYTTY) {
                "Ennen 14.11.2025"
            } else {
                null
            }

    fun osakokeet(): List<Osakoe> =
        listOfNotNull(
            puhuminen?.let { Osakoe(TutkinnonOsa.PU, it, arviointipaiva) },
            kirjoittaminen?.let { Osakoe(TutkinnonOsa.KI, it, arviointipaiva) },
            tekstinYmmartaminen?.let { Osakoe(TutkinnonOsa.TY, it, arviointipaiva) },
            puheenYmmartaminen?.let { Osakoe(TutkinnonOsa.PY, it, arviointipaiva) },
            rakenteetJaSanasto?.let { Osakoe(TutkinnonOsa.RS, it, arviointipaiva) },
            yleisarvosana?.let { Osakoe(TutkinnonOsa.YL, it, arviointipaiva) },
        )

    companion object {
        val fromRow: RowMapper<YkiSuoritusEntity> =
            RowMapper { rs, _ ->
                YkiSuoritusEntity(
                    rs.getInt("id"),
                    Oid.parse(rs.getString("suorittajan_oid")).getOrThrow(),
                    rs.getString("hetu"),
                    Sukupuoli.valueOf(rs.getString("sukupuoli")),
                    rs.getString("sukunimi"),
                    rs.getString("etunimet"),
                    rs.getString("kansalaisuus"),
                    rs.getString("katuosoite"),
                    rs.getString("postinumero"),
                    rs.getString("postitoimipaikka"),
                    rs.getString("email"),
                    rs.getInt("suoritus_id"),
                    rs.getTimestamp("last_modified").toInstant(),
                    rs.getObject("tutkintopaiva", LocalDate::class.java),
                    Tutkintokieli.valueOf(rs.getString("tutkintokieli")),
                    Tutkintotaso.valueOf(rs.getString("tutkintotaso")),
                    Oid.parse(rs.getString("jarjestajan_tunnus_oid")).getOrThrow(),
                    rs.getString("jarjestajan_nimi"),
                    rs.getObject("arviointipaiva", LocalDate::class.java),
                    rs.getObject("tekstin_ymmartaminen", Integer::class.java)?.toInt(),
                    rs.getObject("kirjoittaminen", Integer::class.java)?.toInt(),
                    rs.getObject("rakenteet_ja_sanasto", Integer::class.java)?.toInt(),
                    rs.getObject("puheen_ymmartaminen", Integer::class.java)?.toInt(),
                    rs.getObject("puhuminen", Integer::class.java)?.toInt(),
                    rs.getObject("yleisarvosana", Integer::class.java)?.toInt(),
                    rs.getObject("tarkistusarvioinnin_saapumis_pvm", LocalDate::class.java),
                    rs.getString("tarkistusarvioinnin_asiatunnus"),
                    rs
                        .getTypedArrayOrNull(
                            "tarkistusarvioidut_osakokeet",
                        ) { taso -> TutkinnonOsa.valueOf(taso) }
                        ?.toSet(),
                    rs.getTypedArrayOrNull("arvosana_muuttui") { taso -> TutkinnonOsa.valueOf(taso) }?.toSet(),
                    rs.getString("perustelu"),
                    rs.getObject("tarkistusarvioinnin_kasittely_pvm", LocalDate::class.java),
                    rs.getObject("tarkistusarviointi_hyvaksytty_pvm", LocalDate::class.java),
                    Oid.parse(rs.getString("koski_opiskeluoikeus")).getOrNull(),
                    rs.getBoolean("koski_siirto_kasitelty"),
                    KituArviointitila.valueOf(rs.getString("arviointitila")),
                )
            }
    }
}

data class Osakoe(
    val tyyppi: TutkinnonOsa,
    val arvosana: Int?,
    val arviointipaiva: LocalDate?,
)
