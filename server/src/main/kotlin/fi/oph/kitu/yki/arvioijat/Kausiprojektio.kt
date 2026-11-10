package fi.oph.kitu.yki.arvioijat

import java.time.LocalDate

/**
 * `yki_arviointioikeus` on johdettu `yki_arvioija_arviointikausi`sta. Valinta riippuu kuluvasta
 * paivasta, joten projektio vanhenee kun tuleva kausi alkaa; sen korjaa yollinen ajo.
 */
object Kausiprojektio {
    /**
     * Voimassa oleva kausi voittaa tulevan ja tuleva paattyneen: arvioija jolla on tuleva kausi on
     * "Alkaa myohemmin", ei "Passivoitu". Passivointi katkaisee myos tulevan kauden tahan paivaan,
     * jolloin se paattyy ennen alkuaan; sellainen kausi ei ole koskaan ollut voimassa eika kelpaa.
     */
    fun projisoitava(
        kaikki: List<YkiArviointikausiEntity>,
        tanaan: LocalDate,
    ): YkiArviointikausiEntity? {
        val kaudet = kaikki.filterNot { it.paattymispaiva?.isBefore(it.alkupaiva) == true }.ifEmpty { kaikki }
        val voimassa = kaudet.filter { alkanut(it, tanaan) && !paattynyt(it, tanaan) }
        if (voimassa.isNotEmpty()) return voimassa.maxWith(compareBy({ it.alkupaiva }, { it.tunniste }))

        val tulevat = kaudet.filter { !alkanut(it, tanaan) }
        if (tulevat.isNotEmpty()) return tulevat.minWith(compareBy({ it.alkupaiva }, { it.tunniste }))

        return kaudet.maxWithOrNull(
            compareBy<YkiArviointikausiEntity, LocalDate?>(nullsFirst()) { it.paattymispaiva }
                .thenBy { it.tunniste },
        )
    }

    fun projisoi(
        kaudet: List<YkiArviointikausiEntity>,
        ensimmainenRekisterointipaiva: LocalDate?,
        nykyiset: List<YkiArviointioikeusEntity>,
        tanaan: LocalDate,
    ): List<YkiArviointioikeusEntity> {
        val kausi = projisoitava(kaudet, tanaan) ?: return emptyList()
        val ensimmainen =
            listOfNotNull(ensimmainenRekisterointipaiva, kaudet.minOfOrNull { it.alkupaiva })
                .minOrNull() ?: kausi.alkupaiva

        return kausi.oikeudet.map { oikeus ->
            YkiArviointioikeusEntity(
                id = null,
                arvioijaId = null,
                kieli = oikeus.kieli,
                tasot = oikeus.tasot,
                tila = null,
                kaudenAlkupaiva = kausi.alkupaiva,
                kaudenPaattymispaiva = kausi.paattymispaiva,
                jatkorekisterointi = kausi.alkupaiva > ensimmainen,
                ensimmainenRekisterointipaiva = ensimmainen,
                rekisteriintuontiaika = null,
                ashaNumero = kausi.ashaNumero,
            )
        }
    }

    /**
     * Tila jatetaan vertailun ulkopuolelle vaikka projektio kirjoittaa siihen NULLin: muuten
     * pelkka yollinen ajo pyyhkisi Solkin kirjaaman PASSIVOITU-tilan ja elvyttaisi merkinnan.
     * Kun kausi tosiasiassa muuttuu, kirjoitus tapahtuu ja tila nollautuu — uusi rekisterointi
     * saa aktivoida merkinnan.
     */
    fun onMuuttunut(
        nykyiset: List<YkiArviointioikeusEntity>,
        tavoite: List<YkiArviointioikeusEntity>,
    ): Boolean {
        val odotettu = (tavoite + sailytettavat(nykyiset, tavoite)).associateBy { it.kieli }
        val nyt = nykyiset.associateBy { it.kieli }

        if (odotettu.keys != nyt.keys) return true
        return odotettu.any { (kieli, rivi) -> !vastaa(nyt.getValue(kieli), rivi) }
    }

    /** Arviointioikeusmatriisi ei renderoi vanhentuneita kielia, joten niita ei myoskaan poisteta. */
    fun sailytettavat(
        nykyiset: List<YkiArviointioikeusEntity>,
        tavoite: List<YkiArviointioikeusEntity>,
    ): List<YkiArviointioikeusEntity> =
        nykyiset.filter { rivi -> rivi.kieli.isLegacy() && tavoite.none { it.kieli == rivi.kieli } }

    private fun vastaa(
        a: YkiArviointioikeusEntity,
        b: YkiArviointioikeusEntity,
    ): Boolean =
        a.tasot == b.tasot &&
            a.kaudenAlkupaiva == b.kaudenAlkupaiva &&
            a.kaudenPaattymispaiva == b.kaudenPaattymispaiva &&
            a.jatkorekisterointi == b.jatkorekisterointi &&
            a.ensimmainenRekisterointipaiva == b.ensimmainenRekisterointipaiva &&
            a.ashaNumero == b.ashaNumero

    private fun alkanut(
        kausi: YkiArviointikausiEntity,
        tanaan: LocalDate,
    ): Boolean = !kausi.alkupaiva.isAfter(tanaan)

    private fun paattynyt(
        kausi: YkiArviointikausiEntity,
        tanaan: LocalDate,
    ): Boolean = kausi.paattymispaiva != null && kausi.paattymispaiva.isBefore(tanaan)

    private val YkiArviointikausiEntity.tunniste: Int get() = id?.toInt() ?: 0
}
