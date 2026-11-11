package fi.oph.kitu.yki.arvioijat.hakemus

import fi.oph.kitu.ataru.OptionAnswer
import fi.oph.kitu.ataru.StatesAndFilters

/**
 * Ehdot, joilla hakemus haetaan kituun virkailijan tarkistettavaksi.
 *
 * [atarunTilat] ja [atarunSuodatus] ajetaan jo atarussa, joten ehdot tayttamattomia hakemuksia ei
 * siirry kituun lainkaan: vain virkailijan atarussa kasitellyksi ja hakukelpoiseksi merkitsemat,
 * passivoimattomat hakemukset. Muuttuvat ehdot kuuluvat atarun puolelle, koska vahvistettua hakemusta ei
 * haeta uudelleen ja atarun listaukselta poistunut tarkistettava hakemus poistuu kitusta seuraavalla haulla.
 * Siirto jattaa lisaksi itse pois passivoidut ja henkilottomat hakemukset.
 */
object ArvioijahakemusKriteeri {
    val atarunSuodatus: List<OptionAnswer> = emptyList()

    val atarunTilat: StatesAndFilters =
        StatesAndFilters(
            filters =
                StatesAndFilters.PERUSSUODATTIMET +
                    mapOf(
                        "active-status" to mapOf("active" to true, "passive" to false),
                        "eligibility-state" to
                            mapOf(
                                "unreviewed" to false,
                                "eligible" to true,
                                "uneligible" to false,
                                "conditionally-eligible" to false,
                            ),
                    ),
            processingStatesToInclude = listOf("processed"),
        )
}
