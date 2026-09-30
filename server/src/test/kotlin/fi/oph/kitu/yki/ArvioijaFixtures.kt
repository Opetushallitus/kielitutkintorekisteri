package fi.oph.kitu.yki

import fi.oph.kitu.oid.Oid
import fi.oph.kitu.util.result.getOrThrow
import fi.oph.kitu.yki.arvioijat.YkiArvioijaEntity
import fi.oph.kitu.yki.arvioijat.YkiArvioijaTila
import fi.oph.kitu.yki.arvioijat.YkiArviointioikeusEntity
import java.time.LocalDate
import java.time.OffsetDateTime

/**
 * Yhteiset arvioijafikstuurit. Ennen jokaisella arvioijatestilla oli oma
 * private fun arvioija(...), jotka toistivat samat literaalit ja erosivat vain
 * siina mitka kentat oli parametroitu.
 */

val OLETUSKAUDEN_ALKU: LocalDate = LocalDate.of(2021, 1, 1)
val OLETUSKAUDEN_LOPPU: LocalDate = LocalDate.of(2026, 1, 1)

fun arvioijaOid(i: Int): Oid = Oid.parse("1.2.246.562.24.1000000000$i").getOrThrow()

fun arviointioikeus(
    kieli: Tutkintokieli = Tutkintokieli.FIN,
    tasot: Set<Tutkintotaso> = setOf(Tutkintotaso.PT, Tutkintotaso.KT),
    tila: YkiArvioijaTila? = null,
    kaudenAlkupaiva: LocalDate? = OLETUSKAUDEN_ALKU,
    kaudenPaattymispaiva: LocalDate? = OLETUSKAUDEN_LOPPU,
    jatkorekisterointi: Boolean = false,
    ensimmainenRekisterointipaiva: LocalDate = kaudenAlkupaiva ?: OLETUSKAUDEN_ALKU,
    rekisteriintuontiaika: OffsetDateTime? = null,
    ashaNumero: String? = null,
) = YkiArviointioikeusEntity(
    id = null,
    arvioijaId = null,
    kieli = kieli,
    tasot = tasot,
    tila = tila,
    kaudenAlkupaiva = kaudenAlkupaiva,
    kaudenPaattymispaiva = kaudenPaattymispaiva,
    jatkorekisterointi = jatkorekisterointi,
    ensimmainenRekisterointipaiva = ensimmainenRekisterointipaiva,
    rekisteriintuontiaika = rekisteriintuontiaika,
    ashaNumero = ashaNumero,
)

fun arvioija(
    oid: Oid = arvioijaOid(1),
    sukunimi: String = "Testi-Testi",
    etunimet: String = "Testi",
    henkilotunnus: String? = null,
    sahkopostiosoite: String? = "testi@testi.fi",
    katuosoite: String = "Testikuja 5",
    postinumero: String = "40100",
    postitoimipaikka: String = "Testilä",
    oikeudet: List<YkiArviointioikeusEntity> = listOf(arviointioikeus()),
) = YkiArvioijaEntity(
    id = null,
    arvioijaOid = oid,
    henkilotunnus = henkilotunnus,
    sukunimi = sukunimi,
    etunimet = etunimet,
    sahkopostiosoite = sahkopostiosoite,
    katuosoite = katuosoite,
    postinumero = postinumero,
    postitoimipaikka = postitoimipaikka,
    arviointioikeudet = oikeudet,
)
