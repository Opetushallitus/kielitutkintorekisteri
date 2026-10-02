package fi.oph.kitu.yki.arvioijat.hakemus

import arrow.core.Either
import arrow.core.left
import arrow.core.right
import fi.oph.kitu.ataru.SiirtoHakemus
import fi.oph.kitu.yki.Tutkintokieli
import fi.oph.kitu.yki.Tutkintotaso
import fi.oph.kitu.yki.arvioijat.TallennaArvioija
import tools.jackson.databind.JsonNode

/**
 * Atarun arvioijahakemuslomakkeen rakenne. Lomakkeen avain ja kenttien id:t ovat
 * PAIKKAMERKKEJA, kunnes lomake on julkaistu: vaihda ne tahan, kun ne tiedetaan.
 * Henkilotietomoduulin kentat (`address`, `postal-code`, ...) ovat atarun vakioavaimia.
 */
object ArvioijahakemusLomake {
    const val LOMAKKEEN_AVAIN = "yki-arvioijahakemus"

    object Kentat {
        const val KIELET = "yki-arvioija-kielet"
        const val TASOT = "yki-arvioija-tasot"
        const val SAHKOPOSTI = "email"
        const val KATUOSOITE = "address"
        const val POSTINUMERO = "postal-code"
        const val POSTITOIMIPAIKKA = "postal-office"
    }

    fun kartoita(hakemus: SiirtoHakemus): Either<String, Arvioijahakemus> {
        val henkiloOid =
            hakemus.personOid?.takeIf { it.isNotBlank() }
                ?: return "Hakemuksella ei ole henkilöä".left()

        val kielet = arvot(hakemus, Kentat.KIELET)
        val tasot = arvot(hakemus, Kentat.TASOT)
        val tuntemattomat =
            kielet.filter { koodi -> kieli(koodi) == null } + tasot.filter { koodi -> taso(koodi) == null }
        if (tuntemattomat.isNotEmpty()) return "Tuntematon kieli tai taso: ${tuntemattomat.joinToString()}".left()
        if (kielet.isEmpty() || tasot.isEmpty()) return "Hakemuksella ei ole kieliä tai tasoja".left()

        val tasojoukko = tasot.mapNotNull(::taso).toSet()
        return Arvioijahakemus(
            hakemusOid = hakemus.hakemusOid,
            henkiloOid = henkiloOid,
            arviointioikeudet =
                kielet
                    .mapNotNull(::kieli)
                    .distinct()
                    .map { TallennaArvioija.Arviointioikeus(it, tasojoukko) },
            sahkopostiosoite = arvo(hakemus, Kentat.SAHKOPOSTI),
            katuosoite = arvo(hakemus, Kentat.KATUOSOITE),
            postinumero = arvo(hakemus, Kentat.POSTINUMERO),
            postitoimipaikka = arvo(hakemus, Kentat.POSTITOIMIPAIKKA),
        ).right()
    }

    private fun kieli(koodi: String): Tutkintokieli? =
        Tutkintokieli.entries.firstOrNull { it.solkiCode.equals(koodi, ignoreCase = true) && !it.isLegacy() }

    private fun taso(koodi: String): Tutkintotaso? =
        Tutkintotaso.entries.firstOrNull {
            it.name.equals(koodi, ignoreCase = true)
        }

    private fun arvo(
        hakemus: SiirtoHakemus,
        kentta: String,
    ): String? = arvot(hakemus, kentta).firstOrNull()

    private fun arvot(
        hakemus: SiirtoHakemus,
        kentta: String,
    ): List<String> = hakemus.keyValues[kentta]?.let(::litistetty).orEmpty()

    private fun litistetty(node: JsonNode): List<String> =
        when {
            node.isArray -> node.values().flatMap(::litistetty)
            node.isNull -> emptyList()
            else -> listOf(node.asString().trim()).filter { it.isNotEmpty() }
        }
}
