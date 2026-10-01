package fi.oph.kitu.yki.suoritukset

import fi.oph.kitu.i18n.finnishDate
import fi.oph.kitu.jdbc.getEnum
import fi.oph.kitu.jdbc.getEnumOrNull
import fi.oph.kitu.jdbc.getInstant
import fi.oph.kitu.jdbc.getTypedArrayOrNull
import fi.oph.kitu.koodisto.Koodisto
import fi.oph.kitu.oid.Oid
import fi.oph.kitu.oid.getOidOrNull
import fi.oph.kitu.tiedontuontischema.Henkilosuoritus
import fi.oph.kitu.tiedontuontischema.Lahdejarjestelma
import fi.oph.kitu.tiedontuontischema.YkiSuoritus
import fi.oph.kitu.util.IgnoreForEquality
import fi.oph.kitu.util.result.getOrThrow
import fi.oph.kitu.yki.Arviointitila
import fi.oph.kitu.yki.Sukupuoli
import fi.oph.kitu.yki.TutkinnonOsa
import fi.oph.kitu.yki.Tutkintokieli
import fi.oph.kitu.yki.Tutkintotaso
import org.springframework.data.annotation.Id
import org.springframework.data.relational.core.mapping.Table
import org.springframework.jdbc.core.RowMapper
import java.lang.IllegalArgumentException
import java.sql.ResultSet
import java.sql.Timestamp
import java.time.Instant
import java.time.LocalDate

@Table(name = "yki_suoritus")
data class YkiSuoritusEntity(
    @Id
    @IgnoreForEquality("DB")
    val id: Int?,
    val suorittajanOID: Oid?,
    // Hetuja ei ole enää tallennettu Kielitutkintorekisteriin 1.1.2026 alkaen
    val hetu: String?,
    val sukupuoli: Sukupuoli,
    val sukunimi: String,
    val etunimet: String,
    val kansalaisuus: String,
    val katuosoite: String,
    val postinumero: String,
    val postitoimipaikka: String,
    val maa: String?, // ISO 3166-1 mukainen kolmikirjaiminen lyhenne
    val email: String?,
    val solkiId: Int,
    @IgnoreForEquality("DB")
    val lastModified: Instant,
    @IgnoreForEquality("DB")
    val receivedAt: Instant,
    val tutkintopaiva: LocalDate,
    val tutkintokieli: Tutkintokieli,
    val tutkintotaso: Tutkintotaso,
    val todistuskieli: Todistuskieli?,
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
    @IgnoreForEquality("DB")
    val tarkistusarviointiHyvaksyttyPvm: LocalDate?,
    @IgnoreForEquality("DB")
    val koskiOpiskeluoikeus: Oid?,
    @IgnoreForEquality("DB")
    val koskiSiirtoKasitelty: Boolean?,
    val arviointitila: Arviointitila,
    @IgnoreForEquality("DB")
    val arviointitilaLahetetty: Timestamp?,
    @IgnoreForEquality("DB")
    val arviointitilanLahetysvirhe: String?,
    val lahdejarjestelmanTunnus: String = "yki.$solkiId",
) {
    @IgnoreForEquality("DB")
    var ilmoitetutOsakokeet: Set<TutkinnonOsa>? = null

    fun arvosana(osakoe: TutkinnonOsa): Int? =
        when (osakoe) {
            TutkinnonOsa.PU -> puhuminen
            TutkinnonOsa.KI -> kirjoittaminen
            TutkinnonOsa.TY -> tekstinYmmartaminen
            TutkinnonOsa.PY -> puheenYmmartaminen
            TutkinnonOsa.RS -> rakenteetJaSanasto
            TutkinnonOsa.YL -> yleisarvosana
        }

    fun osakokeet(): List<Osakoe> {
        val tyypit = ilmoitetutOsakokeet ?: TutkinnonOsa.entries.filter { arvosana(it) != null }.toSet()
        return TutkinnonOsa.entries
            .filter { it in tyypit }
            .map { Osakoe(it, arvosana(it), arviointipaiva) }
    }

    fun isOphTesti(): Boolean = Lahdejarjestelma.ofTunnus(lahdejarjestelmanTunnus) == Lahdejarjestelma.OPHTesti

    fun kokoNimi() = "$sukunimi $etunimet"

    fun isVilppi() =
        kaikkiArvosanat().contains(
            Koodisto.YkiArvosana.Vilppi.koodiarvo
                .toInt(),
        )

    private fun kaikkiArvosanat(): List<Int> =
        listOfNotNull(
            puhuminen,
            kirjoittaminen,
            tekstinYmmartaminen,
            puheenYmmartaminen,
            rakenteetJaSanasto,
            yleisarvosana,
        )

    companion object {
        val fromRow: RowMapper<YkiSuoritusEntity> =
            RowMapper { rs, _ ->
                buildEntity(
                    rs = rs,
                    arviointipaiva = rs.getObject("arviointipaiva", LocalDate::class.java),
                    tekstinYmmartaminen = rs.getObject("tekstin_ymmartaminen", Int::class.javaObjectType),
                    kirjoittaminen = rs.getObject("kirjoittaminen", Int::class.javaObjectType),
                    rakenteetJaSanasto = rs.getObject("rakenteet_ja_sanasto", Int::class.javaObjectType),
                    puheenYmmartaminen = rs.getObject("puheen_ymmartaminen", Int::class.javaObjectType),
                    puhuminen = rs.getObject("puhuminen", Int::class.javaObjectType),
                    yleisarvosana = rs.getObject("yleisarvosana", Int::class.javaObjectType),
                    tarkistusarvioinninSaapumisPvm =
                        rs.getObject("tarkistusarvioinnin_saapumis_pvm", LocalDate::class.java),
                    tarkistusarvioinninAsiatunnus = rs.getString("tarkistusarvioinnin_asiatunnus"),
                    tarkistusarvioidutOsakokeet =
                        rs
                            .getTypedArrayOrNull("tarkistusarvioidut_osakokeet") { TutkinnonOsa.valueOf(it) }
                            ?.toSet(),
                    arvosanaMuuttui =
                        rs.getTypedArrayOrNull("arvosana_muuttui") { TutkinnonOsa.valueOf(it) }?.toSet(),
                    perustelu = rs.getString("perustelu"),
                    tarkistusarvioinninKasittelyPvm =
                        rs.getObject("tarkistusarvioinnin_kasittely_pvm", LocalDate::class.java),
                    tarkistusarviointiHyvaksyttyPvm =
                        rs.getObject("tarkistusarviointi_hyvaksytty_pvm", LocalDate::class.java),
                    koskiOpiskeluoikeus = Oid.parse(rs.getString("koski_opiskeluoikeus")).getOrNull(),
                    koskiSiirtoKasitelty = rs.getBoolean("koski_siirto_kasitelty"),
                    arviointitilaLahetetty = rs.getTimestamp("arviointitila_lahetetty"),
                    arviointitilanLahetysvirhe = rs.getString("arviointitilan_lahetysvirhe"),
                    osakoeTyypit =
                        rs.getTypedArrayOrNull("osakokeet_tyypit") { TutkinnonOsa.valueOf(it) }?.toList(),
                )
            }

        val fromRootRow: RowMapper<YkiSuoritusEntity> = RowMapper { rs, _ -> buildEntity(rs) }

        fun from(hs: Henkilosuoritus<YkiSuoritus>): YkiSuoritusEntity {
            val henkilo = hs.henkilo
            return with(hs.suoritus) {
                fun arvosana(tyyppi: TutkinnonOsa): Int? =
                    osat
                        .firstOrNull { it.tyyppi == tyyppi }
                        ?.arvosana

                YkiSuoritusEntity(
                    id = internalId,
                    suorittajanOID = henkilo.oid,
                    hetu = henkilo.hetu,
                    sukupuoli = henkilo.sukupuoli ?: throw IllegalArgumentException("Sukupuoli puuttuu"),
                    sukunimi = henkilo.sukunimi ?: throw IllegalArgumentException("Sukunimi puuttuu"),
                    etunimet = henkilo.etunimet ?: throw IllegalArgumentException("Etunimet puuttuu"),
                    kansalaisuus = henkilo.kansalaisuus ?: throw IllegalArgumentException("Kansalaisuus puuttuu"),
                    katuosoite = henkilo.katuosoite ?: throw IllegalArgumentException("Katuosoite puuttuu"),
                    postinumero = henkilo.postinumero ?: throw IllegalArgumentException("Postinumero puuttuu"),
                    postitoimipaikka =
                        henkilo.postitoimipaikka ?: throw IllegalArgumentException("Postitoimipaikka puuttuu"),
                    maa = henkilo.maa,
                    email = henkilo.email,
                    solkiId = lahdejarjestelmanId.id.toInt(),
                    lastModified = Instant.now(),
                    receivedAt = Instant.now(),
                    tutkintopaiva = tutkintopaiva,
                    tutkintokieli = kieli,
                    tutkintotaso = tutkintotaso,
                    todistuskieli = todistuskieli,
                    jarjestajanTunnusOid = jarjestaja.oid,
                    jarjestajanNimi = jarjestaja.nimi,
                    arviointipaiva = arviointipaiva,
                    tekstinYmmartaminen = arvosana(TutkinnonOsa.tekstinYmmartaminen),
                    kirjoittaminen = arvosana(TutkinnonOsa.kirjoittaminen),
                    rakenteetJaSanasto = arvosana(TutkinnonOsa.rakenteetJaSanasto),
                    puheenYmmartaminen = arvosana(TutkinnonOsa.puheenYmmartaminen),
                    puhuminen = arvosana(TutkinnonOsa.puhuminen),
                    yleisarvosana = arvosana(TutkinnonOsa.yleisarvosana),
                    tarkistusarvioinninSaapumisPvm = tarkistusarviointi?.saapumispaiva,
                    tarkistusarvioinninAsiatunnus = tarkistusarviointi?.asiatunnus,
                    tarkistusarvioidutOsakokeet = tarkistusarviointi?.tarkistusarvioidutOsakokeet?.toSet(),
                    arvosanaMuuttui = tarkistusarviointi?.arvosanaMuuttui?.toSet(),
                    perustelu = tarkistusarviointi?.perustelu,
                    tarkistusarvioinninKasittelyPvm = tarkistusarviointi?.kasittelypaiva,
                    tarkistusarviointiHyvaksyttyPvm = null,
                    koskiOpiskeluoikeus = koskiOpiskeluoikeusOid,
                    koskiSiirtoKasitelty = koskiSiirtoKasitelty,
                    arviointitila = arviointitila,
                    arviointitilaLahetetty = null,
                    arviointitilanLahetysvirhe = null,
                    lahdejarjestelmanTunnus = lahdejarjestelmanId.toTunnus(),
                ).also { it.ilmoitetutOsakokeet = osat.map { osa -> osa.tyyppi }.toSet() }
            }
        }

        private fun buildEntity(
            rs: ResultSet,
            arviointipaiva: LocalDate? = null,
            tekstinYmmartaminen: Int? = null,
            kirjoittaminen: Int? = null,
            rakenteetJaSanasto: Int? = null,
            puheenYmmartaminen: Int? = null,
            puhuminen: Int? = null,
            yleisarvosana: Int? = null,
            tarkistusarvioinninSaapumisPvm: LocalDate? = null,
            tarkistusarvioinninAsiatunnus: String? = null,
            tarkistusarvioidutOsakokeet: Set<TutkinnonOsa>? = null,
            arvosanaMuuttui: Set<TutkinnonOsa>? = null,
            perustelu: String? = null,
            tarkistusarvioinninKasittelyPvm: LocalDate? = null,
            tarkistusarviointiHyvaksyttyPvm: LocalDate? = null,
            koskiOpiskeluoikeus: Oid? = null,
            koskiSiirtoKasitelty: Boolean? = null,
            arviointitilaLahetetty: Timestamp? = null,
            arviointitilanLahetysvirhe: String? = null,
            osakoeTyypit: List<TutkinnonOsa>? = null,
        ): YkiSuoritusEntity =
            YkiSuoritusEntity(
                id = rs.getInt("id"),
                suorittajanOID = rs.getOidOrNull("suorittajan_oid"),
                hetu = rs.getString("hetu"),
                sukupuoli = rs.getEnum<Sukupuoli>("sukupuoli"),
                sukunimi = rs.getString("sukunimi"),
                etunimet = rs.getString("etunimet"),
                kansalaisuus = rs.getString("kansalaisuus"),
                katuosoite = rs.getString("katuosoite"),
                postinumero = rs.getString("postinumero"),
                postitoimipaikka = rs.getString("postitoimipaikka"),
                maa = rs.getString("maa"),
                email = rs.getString("email"),
                solkiId = rs.getInt("solki_id"),
                lastModified = rs.getInstant("last_modified"),
                receivedAt = rs.getInstant("received_at"),
                tutkintopaiva = rs.getObject("tutkintopaiva", LocalDate::class.java),
                tutkintokieli = rs.getEnum<Tutkintokieli>("tutkintokieli"),
                tutkintotaso = rs.getEnum<Tutkintotaso>("tutkintotaso"),
                todistuskieli = rs.getEnumOrNull<Todistuskieli>("todistuskieli"),
                jarjestajanTunnusOid = Oid.parse(rs.getString("jarjestajan_tunnus_oid")).getOrThrow(),
                jarjestajanNimi = rs.getString("jarjestajan_nimi"),
                arviointipaiva = arviointipaiva,
                tekstinYmmartaminen = tekstinYmmartaminen,
                kirjoittaminen = kirjoittaminen,
                rakenteetJaSanasto = rakenteetJaSanasto,
                puheenYmmartaminen = puheenYmmartaminen,
                puhuminen = puhuminen,
                yleisarvosana = yleisarvosana,
                tarkistusarvioinninSaapumisPvm = tarkistusarvioinninSaapumisPvm,
                tarkistusarvioinninAsiatunnus = tarkistusarvioinninAsiatunnus,
                tarkistusarvioidutOsakokeet = tarkistusarvioidutOsakokeet,
                arvosanaMuuttui = arvosanaMuuttui,
                perustelu = perustelu,
                tarkistusarvioinninKasittelyPvm = tarkistusarvioinninKasittelyPvm,
                tarkistusarviointiHyvaksyttyPvm = tarkistusarviointiHyvaksyttyPvm,
                koskiOpiskeluoikeus = koskiOpiskeluoikeus,
                koskiSiirtoKasitelty = koskiSiirtoKasitelty,
                arviointitila = rs.getEnum<Arviointitila>("arviointitila"),
                arviointitilaLahetetty = arviointitilaLahetetty,
                arviointitilanLahetysvirhe = arviointitilanLahetysvirhe,
                lahdejarjestelmanTunnus = rs.getString("lahdejarjestelmantunnus"),
            ).also { it.ilmoitetutOsakokeet = osakoeTyypit?.toSet() }
    }
}

data class Osakoe(
    val tyyppi: TutkinnonOsa,
    val arvosana: Int?,
    val arviointipaiva: LocalDate?,
)

fun List<YkiSuoritusEntity>.latestVersions(): List<YkiSuoritusEntity> =
    this
        .groupBy { it.solkiId }
        .map { (_, entities) -> entities.maxByOrNull { it.lastModified }!! }
