package fi.oph.kitu.yki.arvioijat.hakemus

import fi.oph.kitu.ataru.OptionAnswer
import fi.oph.kitu.ataru.SiirtoHakemus

/**
 * Ehdot, joilla hakemuksesta syntyy arvioijamerkinta. PAIKKAMERKKI, kunnes ehdot tiedetaan.
 *
 * [atarunSuodatus] ajetaan jo atarussa (`option-answers`: valintakenttien tarkka vastaavuus),
 * joten ehdot tayttamattomia hakemuksia ei siirry kituun lainkaan. [tayttyy] kattaa sen mihin
 * atarun suodatus ei pysty; sen hylkaamat kirjataan tilaan [ArvioijahakemuksenTila.EI_TAYTA_EHTOJA].
 */
object ArvioijahakemusKriteeri {
    val atarunSuodatus: List<OptionAnswer> = emptyList()

    fun tayttyy(hakemus: SiirtoHakemus): Boolean = hakemus.state != "inactivated"
}
