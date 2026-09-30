package fi.oph.kitu.yki.historia

import fi.oph.kitu.jdbc.getEnum
import fi.oph.kitu.jdbc.getOffsetDateTimeOrNull
import org.springframework.jdbc.core.RowMapper
import java.time.OffsetDateTime

/**
 * Siirtymatta jaanyt YKI-historiarivi sellaisenaan. Kaikki lahdekentat ovat merkkijonoja,
 * koska juuri naissa riveissa on ne arvot joita ei voitu jasentaa - ks. V127.
 */
data class YkiHistoriaSiirtymatonEntity(
    val id: Int? = null,
    val suorittajanOid: String? = null,
    val hetu: String? = null,
    val sukupuoli: String? = null,
    val sukunimi: String? = null,
    val etunimet: String? = null,
    val kansalaisuus: String? = null,
    val katuosoite: String? = null,
    val postinumero: String? = null,
    val postitoimipaikka: String? = null,
    val email: String? = null,
    val solkiId: String? = null,
    val lastModified: String? = null,
    val tutkintopaiva: String? = null,
    val tutkintokieli: String? = null,
    val tutkintotaso: String? = null,
    val jarjestajanOid: String? = null,
    val jarjestajanNimi: String? = null,
    val arviointipaiva: String? = null,
    val asTy: String? = null,
    val asKi: String? = null,
    val asRs: String? = null,
    val asPy: String? = null,
    val asPu: String? = null,
    val asYl: String? = null,
    val tarkSaapumisPvm: String? = null,
    val tarkAsiatunnus: String? = null,
    val tarkOsakokeet: String? = null,
    val arvosanaMuuttui: String? = null,
    val perustelu: String? = null,
    val tarkKasittelyPvm: String? = null,
    val syy: String,
    val syyluokka: YkiHistoriaSiirtymattomyydenSyy,
    val oidHaunSyy: String? = null,
    val rawRivi: String? = null,
    val lahdetiedosto: String,
    val ladattu: OffsetDateTime? = null,
) {
    companion object {
        val fromRow: RowMapper<YkiHistoriaSiirtymatonEntity> =
            RowMapper { rs, _ ->
                YkiHistoriaSiirtymatonEntity(
                    id = rs.getInt("id"),
                    suorittajanOid = rs.getString("suorittajan_oid"),
                    hetu = rs.getString("hetu"),
                    sukupuoli = rs.getString("sukupuoli"),
                    sukunimi = rs.getString("sukunimi"),
                    etunimet = rs.getString("etunimet"),
                    kansalaisuus = rs.getString("kansalaisuus"),
                    katuosoite = rs.getString("katuosoite"),
                    postinumero = rs.getString("postinumero"),
                    postitoimipaikka = rs.getString("postitoimipaikka"),
                    email = rs.getString("email"),
                    solkiId = rs.getString("solki_id"),
                    lastModified = rs.getString("last_modified"),
                    tutkintopaiva = rs.getString("tutkintopaiva"),
                    tutkintokieli = rs.getString("tutkintokieli"),
                    tutkintotaso = rs.getString("tutkintotaso"),
                    jarjestajanOid = rs.getString("jarjestajan_oid"),
                    jarjestajanNimi = rs.getString("jarjestajan_nimi"),
                    arviointipaiva = rs.getString("arviointipaiva"),
                    asTy = rs.getString("as_ty"),
                    asKi = rs.getString("as_ki"),
                    asRs = rs.getString("as_rs"),
                    asPy = rs.getString("as_py"),
                    asPu = rs.getString("as_pu"),
                    asYl = rs.getString("as_yl"),
                    tarkSaapumisPvm = rs.getString("tark_saapumis_pvm"),
                    tarkAsiatunnus = rs.getString("tark_asiatunnus"),
                    tarkOsakokeet = rs.getString("tark_osakokeet"),
                    arvosanaMuuttui = rs.getString("arvosana_muuttui"),
                    perustelu = rs.getString("perustelu"),
                    tarkKasittelyPvm = rs.getString("tark_kasittely_pvm"),
                    syy = rs.getString("syy"),
                    syyluokka = rs.getEnum<YkiHistoriaSiirtymattomyydenSyy>("syyluokka"),
                    oidHaunSyy = rs.getString("oid_haun_syy"),
                    rawRivi = rs.getString("raw_rivi"),
                    lahdetiedosto = rs.getString("lahdetiedosto"),
                    ladattu = rs.getOffsetDateTimeOrNull("ladattu"),
                )
            }
    }
}
