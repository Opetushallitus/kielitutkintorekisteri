package fi.oph.kitu.yki.arvioijat

import arrow.core.raise.Raise
import arrow.core.raise.RaiseAccumulate
import arrow.core.raise.ensure
import fi.oph.kitu.i18n.finnishDate
import fi.oph.kitu.util.validation.Validation.ValidationError
import fi.oph.kitu.yki.Tutkintokieli
import fi.oph.kitu.yki.Tutkintotaso
import java.time.LocalDate

internal fun Raise<ValidationError>.validateKaudenAlkupaiva(
    alkupaiva: LocalDate,
    tanaan: LocalDate,
    kentta: String,
) {
    ensure(!alkupaiva.isAfter(tanaan.plusYears(1))) {
        ValidationError(listOf(kentta), "Kauden alkupäivä ei voi olla yli vuotta tulevaisuudessa")
    }
}

internal fun RaiseAccumulate<ValidationError>.validateArviointioikeudet(
    oikeudet: List<Pair<Tutkintokieli, Set<Tutkintotaso>>>,
) {
    accumulating {
        ensure(oikeudet.isNotEmpty()) {
            ValidationError(
                listOf("arviointioikeus"),
                "Valitse vähintään yksi tutkintokieli ja tutkintotaso",
            )
        }
    }
    accumulating {
        ensure(oikeudet.all { (_, tasot) -> tasot.isNotEmpty() }) {
            ValidationError(
                listOf("arviointioikeus"),
                "Valitse jokaiselle valitulle tutkintokielelle vähintään yksi tutkintotaso",
            )
        }
    }
    accumulating {
        val kielet = oikeudet.map { it.first }
        ensure(kielet.size == kielet.distinct().size) {
            ValidationError(listOf("arviointioikeus"), "Sama tutkintokieli on valittu useaan kertaan")
        }
    }
}

internal fun RaiseAccumulate<ValidationError>.validatePakollisetYhteystiedot(
    sukunimi: String,
    etunimet: String,
    katuosoite: String,
    postinumero: String,
    postitoimipaikka: String,
) {
    pakollinen(sukunimi, "sukunimi", "Sukunimi")
    pakollinen(etunimet, "etunimet", "Etunimet")
    pakollinen(katuosoite, "katuosoite", "Katuosoite")
    pakollinen(postinumero, "postinumero", "Postinumero")
    pakollinen(postitoimipaikka, "postitoimipaikka", "Postitoimipaikka")
}

private fun RaiseAccumulate<ValidationError>.pakollinen(
    arvo: String,
    kentta: String,
    otsikko: String,
) {
    accumulating {
        ensure(arvo.isNotBlank()) {
            ValidationError(listOf(kentta), "$otsikko on pakollinen tieto")
        }
    }
}

internal fun Raise<ValidationError>.validatePostinumero(postinumero: String) {
    ensure(postinumero.isBlank() || POSTINUMERO.matches(postinumero)) {
        ValidationError(listOf("postinumero"), "Postinumeron on oltava viisi numeroa")
    }
}

internal fun Raise<ValidationError>.validateSahkopostiosoite(sahkoposti: String?) {
    ensure(sahkoposti.isNullOrBlank() || SAHKOPOSTI.matches(sahkoposti)) {
        ValidationError(listOf("sahkopostiosoite"), "Sähköpostiosoite on virheellinen")
    }
}

internal fun Raise<ValidationError>.validatePuhelinnumero(puhelinnumero: String?) {
    ensure(puhelinnumero.isNullOrBlank() || onKelvollinenPuhelinnumero(puhelinnumero)) {
        ValidationError(listOf("puhelinnumero"), "Puhelinnumero on virheellinen")
    }
}

internal fun onKelvollinenPuhelinnumero(puhelinnumero: String): Boolean = PUHELINNUMERO.matches(puhelinnumero)

private val POSTINUMERO = Regex("""\d{5}""")
private val PUHELINNUMERO = Regex("""\+?[0-9 ()-]{5,30}""")
private val SAHKOPOSTI = Regex("""[^@\s]+@[^@\s.]+(\.[^@\s.]+)+""")

/**
 * Henkilolla voi olla samaan aikaan useampi arviointikausi, kunhan niilla ei ole yhteista kielta.
 * Kahta kautta samoilla paivilla ei sallita: ne olisivat yksi kausi, ja kannan uniikkiehto kattaa
 * paivaparin. Tarkistus on kummallakin kirjoituspolulla, koska lisayslomake toimii myos
 * jatkokauden kirjaamisena.
 */
internal fun Raise<ValidationError>.validateEiPaallekkaisiaKausia(
    kaudet: List<YkiArviointikausiEntity>,
    alkupaiva: LocalDate,
    paattymispaiva: LocalDate?,
    kielet: Set<Tutkintokieli>,
    ohitaKausiId: Int? = null,
    kentta: String,
) {
    val muut = kaudet.filter { it.id?.toInt() != ohitaKausiId }

    val samatPaivat = muut.firstOrNull { it.alkupaiva == alkupaiva && it.paattymispaiva == paattymispaiva }
    ensure(samatPaivat == null) {
        ValidationError(
            listOf(kentta),
            "Henkilöllä on jo arviointikausi ${jakso(samatPaivat)}. Lisää kieli olemassa olevaan kauteen.",
        )
    }

    val paallekkainen =
        muut.firstOrNull { kausi ->
            kausi.oikeudet.any { it.kieli in kielet } &&
                !paattyyEnnen(paattymispaiva, kausi.alkupaiva) &&
                !paattyyEnnen(kausi.paattymispaiva, alkupaiva)
        }

    ensure(paallekkainen == null) {
        val yhteiset =
            paallekkainen.oikeudet
                .map { it.kieli }
                .filter { it in kielet }
                .sorted()
                .joinToString(", ") { it.nimi.toString() }
        ValidationError(
            listOf(kentta),
            "Kausi menee päällekkäin arviointikauden ${jakso(paallekkainen)} kanssa. " +
                "Henkilöllä ei voi olla samalla kielellä päällekkäisiä arviointikausia ($yhteiset).",
        )
    }
}

private fun jakso(kausi: YkiArviointikausiEntity): String =
    "${kausi.alkupaiva.finnishDate()}–${kausi.paattymispaiva?.finnishDate() ?: ""}"

/** Tyhja paattymispaiva tarkoittaa toistaiseksi voimassa olevaa, joten se ei paaty ennen mitaan. */
private fun paattyyEnnen(
    paattymispaiva: LocalDate?,
    alkupaiva: LocalDate,
): Boolean = paattymispaiva != null && paattymispaiva < alkupaiva
