package fi.oph.kitu.yki.arvioijat.hakemus

import arrow.core.Either
import arrow.core.NonEmptyList
import arrow.core.raise.Raise
import arrow.core.raise.accumulate
import arrow.core.raise.either
import arrow.core.raise.ensure
import fi.oph.kitu.ataru.SiirtoHakemus
import fi.oph.kitu.yki.Tutkintokieli
import fi.oph.kitu.yki.Tutkintotaso
import fi.oph.kitu.yki.arvioijat.TallennaArvioija
import org.springframework.boot.context.properties.ConfigurationProperties

/**
 * Atarun arvioijahakemuslomakkeen kartoitus arvioijamerkinnaksi, ymparistoittain tiedostossa
 * `arvioijahakemuslomake.yml`: jokaisessa ymparistossa on oma lomake, ja ataru generoi kenttien id:t
 * ja vaihtoehtojen arvot lomakkeelle.
 *
 * [kielet]- ja [Kielivaihtoehto.tasot]-avaimet ovat monivalinnan vaihtoehtojen arvoja. Siirto
 * litistaa valitun vaihtoehdon avaimeksi `<kysymyksen id>_<vaihtoehdon arvo>`.
 */
@ConfigurationProperties("kitu.ataru.arvioijahakemus")
class ArvioijahakemusLomake(
    val lomakkeenAvain: String = "",
    val kielikysymys: String = "",
    val kielet: Map<String, Kielivaihtoehto> = emptyMap(),
    val kentat: Yhteystietokentat = Yhteystietokentat(),
) {
    class Kielivaihtoehto(
        val kieli: Tutkintokieli? = null,
        val tasokysymys: String = "",
        val tasot: Map<String, Set<Tutkintotaso>> = emptyMap(),
    )

    class Yhteystietokentat(
        val sahkoposti: String = "email",
        val katuosoite: String = "address",
        val postinumero: String = "postal-code",
        val postitoimipaikka: String = "postal-office",
    )

    fun puutteet(): List<String> =
        listOfNotNull(
            "lomakkeenAvain".takeIf { lomakkeenAvain.isBlank() },
            "kielikysymys".takeIf { kielikysymys.isBlank() },
            "kielet".takeIf { kielet.isEmpty() },
        ) +
            kielet.flatMap { (arvo, vaihtoehto) ->
                listOfNotNull(
                    "kielet[$arvo].kieli".takeIf { vaihtoehto.kieli == null },
                    vaihtoehto.kieli?.takeIf { it.isLegacy() }?.let { "kielet[$arvo]: vanhentunut kieli $it" },
                    "kielet[$arvo].tasokysymys".takeIf { vaihtoehto.tasokysymys.isBlank() },
                    "kielet[$arvo].tasot".takeIf { vaihtoehto.tasot.isEmpty() },
                ) +
                    vaihtoehto.tasot
                        .filterValues { it.isEmpty() }
                        .keys
                        .map { "kielet[$arvo].tasot[$it]" }
            }

    fun kartoita(hakemus: SiirtoHakemus): Either<NonEmptyList<ArvioijahakemuksenVirhe>, Arvioijahakemus> =
        either {
            accumulate {
                tuntemattomatVastaukset(hakemus).forEach { avain ->
                    accumulating {
                        raise(
                            ArvioijahakemuksenVirhe(
                                ArvioijahakemuksenVirhetyyppi.TUNTEMATON_VAIHTOEHTO,
                                kentta = avain,
                                arvo = hakemus.keyValues[avain],
                            ),
                        )
                    }
                }

                val valitut = kielet.filterKeys { valittu(hakemus, kielikysymys, it) }.values
                accumulating {
                    ensure(valitut.isNotEmpty()) {
                        ArvioijahakemuksenVirhe(ArvioijahakemuksenVirhetyyppi.EI_KIELIA, kentta = kielikysymys)
                    }
                }
                valitut.forEach { vaihtoehto ->
                    accumulating {
                        ensure(tasot(hakemus, vaihtoehto).isNotEmpty()) {
                            ArvioijahakemuksenVirhe(
                                ArvioijahakemuksenVirhetyyppi.KIELELTA_PUUTTUU_TASO,
                                kentta = vaihtoehto.tasokysymys,
                            )
                        }
                    }
                }

                val sahkopostiosoite by accumulating { yhteystieto(hakemus, kentat.sahkoposti) }
                val katuosoite by accumulating { yhteystieto(hakemus, kentat.katuosoite) }
                val postinumero by accumulating { yhteystieto(hakemus, kentat.postinumero) }
                val postitoimipaikka by accumulating { yhteystieto(hakemus, kentat.postitoimipaikka) }

                Arvioijahakemus(
                    arviointioikeudet =
                        valitut
                            .mapNotNull { vaihtoehto -> vaihtoehto.kieli?.let { it to tasot(hakemus, vaihtoehto) } }
                            .groupBy({ it.first }, { it.second })
                            .toSortedMap()
                            .map { (kieli, tasot) -> TallennaArvioija.Arviointioikeus(kieli, tasot.flatten().toSet()) },
                    sahkopostiosoite = sahkopostiosoite,
                    katuosoite = katuosoite,
                    postinumero = postinumero,
                    postitoimipaikka = postitoimipaikka,
                )
            }
        }

    private fun tasot(
        hakemus: SiirtoHakemus,
        vaihtoehto: Kielivaihtoehto,
    ): Set<Tutkintotaso> =
        vaihtoehto.tasot
            .filterKeys { valittu(hakemus, vaihtoehto.tasokysymys, it) }
            .values
            .flatten()
            .toSet()

    private fun valittu(
        hakemus: SiirtoHakemus,
        kysymys: String,
        arvo: String,
    ): Boolean = "${kysymys}_$arvo" in hakemus.keyValues

    private fun tuntemattomatVastaukset(hakemus: SiirtoHakemus): List<String> {
        val tunnetut =
            kielet.values
                .groupBy({ it.tasokysymys }, { it.tasot.keys })
                .mapValues { (_, arvot) -> arvot.flatten().toSet() } + (kielikysymys to kielet.keys)
        return hakemus.keyValues.keys.filter { avain ->
            tunnetut.any { (kysymys, arvot) ->
                avain.startsWith("${kysymys}_") && avain.removePrefix("${kysymys}_") !in arvot
            }
        }
    }

    private fun Raise<ArvioijahakemuksenVirhe>.yhteystieto(
        hakemus: SiirtoHakemus,
        kentta: String,
    ): String? {
        val arvo = hakemus.keyValues[kentta]?.takeUnless { it.isNull } ?: return null
        ensure(arvo.isString) {
            ArvioijahakemuksenVirhe(ArvioijahakemuksenVirhetyyppi.VAARA_TIETOTYYPPI, kentta = kentta, arvo = arvo)
        }
        return arvo.stringValue().trim().takeIf { it.isNotEmpty() }
    }
}
