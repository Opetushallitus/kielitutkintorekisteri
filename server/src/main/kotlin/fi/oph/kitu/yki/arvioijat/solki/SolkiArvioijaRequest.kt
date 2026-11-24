package fi.oph.kitu.yki.arvioijat.solki

import com.fasterxml.jackson.annotation.JsonFormat
import com.fasterxml.jackson.annotation.JsonInclude
import fi.oph.kitu.tietosisalto.Kuvaus
import fi.oph.kitu.yki.Tutkintokieli
import fi.oph.kitu.yki.Tutkintotaso
import fi.oph.kitu.yki.arvioijat.YkiArvioijaEntity
import java.time.LocalDate
import java.time.OffsetDateTime

/**
 * Kenttajoukko vastaa poistunutta CSV-tuontia, jotta Solkille ei synny kartoitustyota (suunnitelma
 * §5.1). Henkilotunnusta ei laheteta 1.1.2026 lainmuutoksen takia.
 *
 * [syntymapaiva] on ainoa kentta jota CSV:ssa ei ollut: Solki johti sen henkilotunnuksesta, joten
 * hetun poisto vei silta pohjan. Kitu ei sailyta sita vaan hakee sen ONR:sta lahetyshetkella.
 */
data class SolkiArvioijaRequest(
    @Kuvaus("Arvioijan oppijanumero")
    val arvioijaOid: String,
    @Kuvaus("Hetki, jona arvioijan tietoja on viimeksi muutettu Kielitutkintorekisterissä")
    @param:JsonFormat(shape = JsonFormat.Shape.STRING)
    val versio: OffsetDateTime?,
    @Kuvaus(
        "Arvioijan tila",
        lisatieto =
            "ARVIOIJA on rekisteriin merkitty arvioija. KOULUTETTAVA on henkilö, jolla ei vielä ole " +
                "arviointioikeutta, mutta joka viedään Solkiin arvioijakoulutusta varten.",
    )
    val tila: Tila,
    @Kuvaus("Arvioijan sukunimi")
    val sukunimi: String,
    @Kuvaus("Arvioijan etunimet")
    val etunimet: String,
    /** Muista kentista poiketen jatetaan pois kokonaan kun tietoa ei ole, ks. [of]. */
    @Kuvaus("Arvioijan syntymäpäivä, joka haetaan oppijanumerorekisteristä lähetyshetkellä")
    @param:JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd")
    @get:JsonInclude(JsonInclude.Include.NON_NULL)
    val syntymapaiva: LocalDate?,
    @Kuvaus("Arvioijan sähköpostiosoite")
    val sahkopostiosoite: String?,
    @Kuvaus("Arvioijan katuosoite")
    val katuosoite: String,
    @Kuvaus("Arvioijan postinumero")
    val postinumero: String,
    @Kuvaus("Arvioijan postitoimipaikka")
    val postitoimipaikka: String,
    @Kuvaus("Päivä, jona arvioija on ensimmäisen kerran merkitty arvioijarekisteriin")
    @param:JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd")
    val ensimmainenRekisterointipaiva: LocalDate?,
    @Kuvaus(
        "Arvioijan arviointikausi",
        lisatieto =
            "Lähetyksessä on yksi kausi: voimassa oleva, tai sen puuttuessa seuraava tuleva tai viimeisin " +
                "päättynyt. Tyhjä, jos arvioijalla on arviointioikeus vain käytöstä poistuneeseen kieleen.",
    )
    val arviointikausi: Arviointikausi?,
) {
    enum class Tila {
        ARVIOIJA,
        KOULUTETTAVA,
        POISTETTU,
    }

    /**
     * Kauden voimassaoloa (aktiivinen, passivoitu) ei laheteta: se vanhenee kauden umpeutuessa,
     * joten vastaanottaja johtaa sen kauden paivista. Paattymispaiva on inklusiivinen eli kauden
     * viimeinen voimassaolopaiva.
     */
    data class Arviointikausi(
        @Kuvaus("Arviointikauden ensimmäinen voimassaolopäivä", lisatieto = "Tyhjä koulutettavalla.")
        @param:JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd")
        val alkupaiva: LocalDate?,
        @Kuvaus("Arviointikauden viimeinen voimassaolopäivä", lisatieto = "Tyhjä koulutettavalla.")
        @param:JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd")
        val paattymispaiva: LocalDate?,
        @Kuvaus("Onko arviointikausi jatkoa arvioijan aiemmalle kaudelle")
        val jatkorekisterointi: Boolean,
        @Kuvaus("Arviointikauden arviointioikeudet", lisatieto = "Jokaisesta kielestä on oma arviointioikeutensa.")
        val arviointioikeudet: List<Arviointioikeus>,
    )

    data class Arviointioikeus(
        @Kuvaus("Kieli, jonka tutkintoja arvioija saa arvioida")
        val kieli: Tutkintokieli,
        @Kuvaus("Tutkintotasot, joita arvioija saa arvioida tällä kielellä")
        val tasot: List<Tutkintotaso>,
    )

    companion object {
        /**
         * @param syntymaaika ONR:sta lahetyshetkella. Tyhja vain jos ONR:ssakaan ei ole tietoa;
         *   ONR-katko on eri asia eika saa paatya tanne, ks. [SolkiArvioijaServiceImpl].
         */
        fun of(
            arvioija: YkiArvioijaEntity,
            syntymaaika: LocalDate?,
        ): SolkiArvioijaRequest =
            SolkiArvioijaRequest(
                arvioijaOid = arvioija.arvioijaOid.toString(),
                versio = arvioija.muokattu,
                tila = Tila.ARVIOIJA,
                sukunimi = arvioija.sukunimi,
                etunimet = arvioija.etunimet,
                syntymapaiva = syntymaaika,
                sahkopostiosoite = arvioija.sahkopostiosoite,
                katuosoite = arvioija.katuosoite,
                postinumero = arvioija.postinumero,
                postitoimipaikka = arvioija.postitoimipaikka,
                ensimmainenRekisterointipaiva =
                    arvioija.arvioijanEnsimmainenRekisterointipaiva
                        ?: arvioija.arviointioikeudet.minOfOrNull { it.ensimmainenRekisterointipaiva },
                arviointikausi = arviointikausi(arvioija),
            )

        private fun arviointikausi(arvioija: YkiArvioijaEntity): Arviointikausi? {
            val oikeudet = arvioija.arviointioikeudet.filterNot { it.kieli.isLegacy() }.sortedBy { it.kieli }
            if (oikeudet.isEmpty()) return null
            return Arviointikausi(
                alkupaiva = oikeudet.mapNotNull { it.kaudenAlkupaiva }.minOrNull(),
                paattymispaiva =
                    oikeudet
                        .map { it.kaudenPaattymispaiva }
                        .takeIf { paivat -> paivat.none { it == null } }
                        ?.filterNotNull()
                        ?.max(),
                jatkorekisterointi = oikeudet.any { it.jatkorekisterointi },
                arviointioikeudet = oikeudet.map { Arviointioikeus(kieli = it.kieli, tasot = it.tasot.sorted()) },
            )
        }
    }
}
