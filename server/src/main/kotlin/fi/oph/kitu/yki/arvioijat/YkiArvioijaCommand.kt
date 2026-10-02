package fi.oph.kitu.yki.arvioijat

import fi.oph.kitu.oid.Oid
import fi.oph.kitu.yki.Tutkintokieli
import fi.oph.kitu.yki.Tutkintotaso
import java.time.LocalDate

data class TallennaArvioija(
    val arvioijaOid: Oid,
    val sukunimi: String,
    val etunimet: String,
    val sahkopostiosoite: String?,
    val katuosoite: String,
    val postinumero: String,
    val postitoimipaikka: String,
    val kaudenAlkupaiva: LocalDate,
    val ashaNumero: String?,
    val arviointioikeudet: List<Arviointioikeus>,
    /**
     * Automaattisesti luotu jatkokausi, joka alkaa heti olemassa olevan kauden jalkeen. Silloin
     * alkupaiva voi olla yli vuoden paassa, joten vuoden rajaa ei sovelleta — validointi vaatii
     * sen sijaan, etta jokin olemassa oleva kausi paattyy alkupaivaa edeltavana paivana.
     */
    val automaattinenJatkokausi: Boolean = false,
) {
    val kaudenPaattymispaiva: LocalDate get() = Arviointikausi.paattymispaiva(kaudenAlkupaiva)

    /**
     * Tila jatetaan tyhjaksi: se lasketaan kauden paivista, ks. [Rekisterointitila].
     * Jatkorekisterointi johdetaan samasta datasta, joten se ei voi olla ristiriidassa
     * merkinnan historian kanssa eika muuttumaton tallennus kasvata kausihistoriaa.
     */
    fun toEntity(ensimmainenRekisterointipaiva: LocalDate = kaudenAlkupaiva): YkiArvioijaEntity =
        YkiArvioijaEntity(
            id = null,
            arvioijaOid = arvioijaOid,
            henkilotunnus = null,
            sukunimi = sukunimi,
            etunimet = etunimet,
            sahkopostiosoite = sahkopostiosoite,
            katuosoite = katuosoite,
            postinumero = postinumero,
            postitoimipaikka = postitoimipaikka,
            arviointioikeudet =
                arviointioikeudet.map { oikeus ->
                    YkiArviointioikeusEntity(
                        id = null,
                        arvioijaId = null,
                        kieli = oikeus.kieli,
                        tasot = oikeus.tasot,
                        tila = null,
                        kaudenAlkupaiva = kaudenAlkupaiva,
                        kaudenPaattymispaiva = kaudenPaattymispaiva,
                        jatkorekisterointi = kaudenAlkupaiva > ensimmainenRekisterointipaiva,
                        ensimmainenRekisterointipaiva = ensimmainenRekisterointipaiva,
                        rekisteriintuontiaika = null,
                    )
                },
        )

    data class Arviointioikeus(
        val kieli: Tutkintokieli,
        val tasot: Set<Tutkintotaso>,
    )
}

/**
 * Henkilotiedot tulevat ONR:sta, rekisterimerkinnan tiedot kitusta. Jos arvioija loytyy jo
 * rekisterista, [olemassaolevaMerkinta] kantaa hanen nykyiset arviointioikeutensa, ASHA-numeronsa
 * ja kautensa lomakkeelle — muuten tallennus pyyhkisi ne.
 */
data class ArvioijanEsitaytto(
    val arvioijaOid: Oid,
    val sukunimi: String,
    val etunimet: String,
    val sahkopostiosoite: String?,
    val katuosoite: String?,
    val postinumero: String?,
    val postitoimipaikka: String?,
    val turvakielto: Turvakieltotieto,
    val olemassaolevaMerkinta: YkiArvioijaEntity?,
)

/**
 * Yhden arviointikauden tallennus. [kausiId] on null uutta kautta lisattaessa.
 * [paattymispaiva] taytetaan validoinnin `enrich`-vaiheessa.
 */
data class TallennaKausi(
    val arvioijaId: Int,
    val kausiId: Int?,
    val alkupaiva: LocalDate,
    val arviointioikeudet: List<Kausioikeus>,
    val ashaNumero: String?,
    val paattymispaiva: LocalDate? = null,
)

/**
 * Arvioijan yhteystietojen paivitys. Kaudet eivat kulje taman kautta vaan
 * [TallennaKausi]n, joten muokkauslomake ei voi vahingossa luoda uutta kautta.
 */
data class PaivitaArvioijanTiedot(
    val arvioijaOid: Oid,
    val sukunimi: String,
    val etunimet: String,
    val sahkopostiosoite: String?,
    val katuosoite: String,
    val postinumero: String,
    val postitoimipaikka: String,
)
