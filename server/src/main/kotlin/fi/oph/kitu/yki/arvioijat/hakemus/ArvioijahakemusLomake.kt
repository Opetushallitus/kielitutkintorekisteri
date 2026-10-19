package fi.oph.kitu.yki.arvioijat.hakemus

import arrow.core.Either
import arrow.core.left
import arrow.core.right
import fi.oph.kitu.ataru.SiirtoHakemus
import fi.oph.kitu.yki.Tutkintokieli
import fi.oph.kitu.yki.Tutkintotaso
import fi.oph.kitu.yki.arvioijat.TallennaArvioija
import org.springframework.boot.context.properties.ConfigurationProperties

/**
 * Atarun arvioijahakemuslomakkeen kartoitus arvioijamerkinnaksi. Ymparistokohtainen asetus, koska
 * jokaisessa ymparistossa on oma lomake ja ataru generoi kenttien id:t ja vaihtoehtojen arvot lomakkeelle.
 *
 * [kielet]- ja [tasot]-avaimet ovat siirron litistettyja vastausavaimia `<kentan id>_<vaihtoehdon arvo>`:
 * monivalinnan valittu vaihtoehto nakyy `keyValues`-avaimena, esim. `kielet[<kentan id>_0]=FIN`.
 */
@ConfigurationProperties("kitu.ataru.arvioijahakemus")
class ArvioijahakemusLomake(
    val lomakkeenAvain: String = "",
    val kielet: Map<String, Tutkintokieli> = emptyMap(),
    val tasot: Map<String, Tutkintotaso> = emptyMap(),
    val kentat: Yhteystietokentat = Yhteystietokentat(),
) {
    class Yhteystietokentat(
        val sahkoposti: String = "email",
        val katuosoite: String = "address",
        val postinumero: String = "postal-code",
        val postitoimipaikka: String = "postal-office",
    )

    fun puutteet(): List<String> =
        listOfNotNull(
            "lomakkeenAvain".takeIf { lomakkeenAvain.isBlank() },
            "kielet".takeIf { kielet.isEmpty() },
            "tasot".takeIf { tasot.isEmpty() },
        ) + kielet.filterValues { it.isLegacy() }.map { (avain, kieli) -> "kielet[$avain]: vanhentunut kieli $kieli" }

    fun kartoita(hakemus: SiirtoHakemus): Either<String, Arvioijahakemus> {
        val henkiloOid =
            hakemus.personOid?.takeIf { it.isNotBlank() }
                ?: return "Hakemuksella ei ole henkilöä".left()

        val tuntemattomat = tuntemattomatVaihtoehdot(hakemus)
        if (tuntemattomat.isNotEmpty()) return "Tuntematon kieli tai taso: ${tuntemattomat.joinToString()}".left()

        val valitutKielet = kielet.filterKeys(hakemus.keyValues::containsKey).values.toSortedSet()
        val tasojoukko = tasot.filterKeys(hakemus.keyValues::containsKey).values.toSet()
        if (valitutKielet.isEmpty() || tasojoukko.isEmpty()) return "Hakemuksella ei ole kieliä tai tasoja".left()

        return Arvioijahakemus(
            hakemusOid = hakemus.hakemusOid,
            henkiloOid = henkiloOid,
            arviointioikeudet = valitutKielet.map { TallennaArvioija.Arviointioikeus(it, tasojoukko) },
            sahkopostiosoite = arvo(hakemus, kentat.sahkoposti),
            katuosoite = arvo(hakemus, kentat.katuosoite),
            postinumero = arvo(hakemus, kentat.postinumero),
            postitoimipaikka = arvo(hakemus, kentat.postitoimipaikka),
        ).right()
    }

    private fun tuntemattomatVaihtoehdot(hakemus: SiirtoHakemus): List<String> {
        val kysymykset = (kielet.keys + tasot.keys).map(::kentanId).toSet()
        return hakemus.keyValues.keys.filter { avain ->
            kentanId(avain) in kysymykset && avain !in kielet && avain !in tasot
        }
    }

    private fun kentanId(vastausavain: String): String = vastausavain.substringBeforeLast('_')

    private fun arvo(
        hakemus: SiirtoHakemus,
        kentta: String,
    ): String? =
        hakemus.keyValues[kentta]
            ?.takeUnless { it.isNull }
            ?.asString()
            ?.trim()
            ?.takeIf { it.isNotEmpty() }
}
