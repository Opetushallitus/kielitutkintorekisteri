package fi.oph.kitu.yki.arvioijat.solki

import com.fasterxml.jackson.annotation.JsonFormat
import com.fasterxml.jackson.annotation.JsonInclude
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
    val arvioijaOid: String,
    @param:JsonFormat(shape = JsonFormat.Shape.STRING)
    val versio: OffsetDateTime?,
    val sukunimi: String,
    val etunimet: String,
    /** Muista kentista poiketen jatetaan pois kokonaan kun tietoa ei ole, ks. [of]. */
    @param:JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd")
    @get:JsonInclude(JsonInclude.Include.NON_NULL)
    val syntymapaiva: LocalDate?,
    val sahkopostiosoite: String?,
    val katuosoite: String,
    val postinumero: String,
    val postitoimipaikka: String,
    @param:JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd")
    val ensimmainenRekisterointipaiva: LocalDate?,
    val arviointioikeudet: List<Arviointioikeus>,
) {
    /**
     * Tilaa ei laheteta: se vanhenee kauden umpeutuessa, joten vastaanottaja johtaa sen kauden
     * paivista. Paattymispaiva on inklusiivinen eli kauden viimeinen voimassaolopaiva.
     */
    data class Arviointioikeus(
        val kieli: Tutkintokieli,
        val tasot: List<Tutkintotaso>,
        @param:JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd")
        val kaudenAlkupaiva: LocalDate?,
        @param:JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd")
        val kaudenPaattymispaiva: LocalDate?,
        val jatkorekisterointi: Boolean,
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
                arviointioikeudet =
                    arvioija.arviointioikeudet.map { oikeus ->
                        Arviointioikeus(
                            kieli = oikeus.kieli,
                            tasot = oikeus.tasot.sorted(),
                            kaudenAlkupaiva = oikeus.kaudenAlkupaiva,
                            kaudenPaattymispaiva = oikeus.kaudenPaattymispaiva,
                            jatkorekisterointi = oikeus.jatkorekisterointi,
                        )
                    },
            )
    }
}
