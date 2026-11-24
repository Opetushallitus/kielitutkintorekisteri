package fi.oph.kitu.tietosisalto

import arrow.core.NonEmptyList
import com.fasterxml.jackson.annotation.JsonInclude
import fi.oph.kitu.html.DisplayEnum
import fi.oph.kitu.html.table.Nimetty
import fi.oph.kitu.i18n.Language
import fi.oph.kitu.koodisto.Koodisto
import fi.oph.kitu.koodisto.KoskiKoodiviite
import fi.oph.kitu.oid.Oid
import tools.jackson.databind.JsonNode
import tools.jackson.databind.json.JsonMapper
import java.time.LocalDate
import java.time.OffsetDateTime
import kotlin.reflect.KClass
import kotlin.reflect.KProperty1
import kotlin.reflect.full.allSuperclasses
import kotlin.reflect.full.findAnnotation
import kotlin.reflect.full.memberProperties
import kotlin.reflect.full.primaryConstructor

data class Kentta(
    val polku: String,
    val luokka: KClass<*>,
    val nimi: String,
    val kuvaus: String?,
    val lisatieto: String?,
    val pakollisuus: Pakollisuus,
    val lista: Lista,
    val muoto: Muoto,
    val lapset: List<Kentta>,
) {
    val kaytossa get() = muoto != Muoto.EiKaytossa

    fun kaikki(): List<Kentta> = listOf(this) + lapset.flatMap { it.kaikki() }
}

enum class Pakollisuus { AINA, VOI_OLLA_TYHJA, VOI_PUUTTUA }

enum class Lista { EI, LISTA, VAHINTAAN_YKSI }

data class Arvo(
    val koodi: String,
    val nimi: String?,
)

sealed interface Muoto {
    data object Teksti : Muoto

    data object Tunniste : Muoto

    data object Paivamaara : Muoto

    data object Aikaleima : Muoto

    data object KyllaEi : Muoto

    data object Kokonaisluku : Muoto

    data object EiKaytossa : Muoto

    data class Valinta(
        val arvot: List<Arvo>,
    ) : Muoto

    data class Koodi(
        val koodistoUri: String,
        val arvot: List<Arvo>,
        val selite: String? = null,
    ) : Muoto

    data class Tietoryhma(
        val luokka: KClass<*>,
    ) : Muoto
}

fun kentat(
    virta: Tietovirta,
    mapper: JsonMapper,
): List<Kentta> = luokanKentat(virta.juuri, "", virta, mapper, 0)

private fun luokanKentat(
    luokka: KClass<*>,
    etuliite: String,
    virta: Tietovirta,
    mapper: JsonMapper,
    syvyys: Int,
): List<Kentta> {
    check(syvyys < 10) { "Tietomalli on liian syvä tai syklinen: $etuliite" }
    val ominaisuudet = luokka.memberProperties.associateBy { it.name }
    val konstruktori = checkNotNull(luokka.primaryConstructor) { "Luokalla $luokka ei ole pääkonstruktoria" }

    return konstruktori.parameters.map { parametri ->
        val ominaisuus = ominaisuudet.getValue(parametri.name!!)
        val tyyppi = ominaisuus.returnType
        val lista =
            when (tyyppi.classifier) {
                List::class -> Lista.LISTA
                NonEmptyList::class -> Lista.VAHINTAAN_YKSI
                else -> Lista.EI
            }
        val alkio = if (lista == Lista.EI) tyyppi else tyyppi.arguments.single().type!!
        val alkioluokka = alkio.classifier as KClass<*>
        val polku = etuliite + ominaisuus.name + if (lista == Lista.EI) "" else "[]"
        val tarkennus = virta.tarkennukset[polku]
        val muoto = muoto(alkioluokka, tarkennus, polku, virta, mapper)
        val kuvaus = kuvaus(luokka, ominaisuus)

        Kentta(
            polku = polku,
            luokka = luokka,
            nimi = ominaisuus.name,
            kuvaus = kuvaus?.teksti,
            lisatieto = kuvaus?.lisatieto?.ifBlank { null },
            pakollisuus =
                when {
                    jatetaanPoisTyhjana(ominaisuus) -> Pakollisuus.VOI_PUUTTUA
                    tyyppi.isMarkedNullable -> Pakollisuus.VOI_OLLA_TYHJA
                    else -> Pakollisuus.AINA
                },
            lista = lista,
            muoto = muoto,
            lapset =
                if (muoto is Muoto.Tietoryhma) {
                    luokanKentat(muoto.luokka, "$polku.", virta, mapper, syvyys + 1)
                } else {
                    emptyList()
                },
        )
    }
}

private fun muoto(
    luokka: KClass<*>,
    tarkennus: Tarkennus?,
    polku: String,
    virta: Tietovirta,
    mapper: JsonMapper,
): Muoto {
    fun eiTarkennusta(muoto: Muoto): Muoto {
        check(tarkennus == null) { "Tarkennus $tarkennus ei sovi kenttään $polku" }
        return muoto
    }

    return when {
        tarkennus == Tarkennus.EiKaytossa -> {
            Muoto.EiKaytossa
        }

        luokka == Oid::class -> {
            eiTarkennusta(Muoto.Tunniste)
        }

        luokka == String::class -> {
            eiTarkennusta(Muoto.Teksti)
        }

        luokka == Boolean::class -> {
            eiTarkennusta(Muoto.KyllaEi)
        }

        luokka == Int::class -> {
            eiTarkennusta(Muoto.Kokonaisluku)
        }

        luokka == LocalDate::class -> {
            eiTarkennusta(Muoto.Paivamaara)
        }

        luokka == OffsetDateTime::class -> {
            eiTarkennusta(Muoto.Aikaleima)
        }

        luokka == KoskiKoodiviite::class -> {
            when (tarkennus) {
                is Tarkennus.Koodit -> koodi(tarkennus.arvot)
                is Tarkennus.UlkoinenKoodisto -> Muoto.Koodi(tarkennus.koodistoUri, emptyList(), tarkennus.selite)
                else -> error("Kentälle $polku on kerrottava koodisto tietovirran tarkennuksissa")
            }
        }

        luokka.java.isEnum -> {
            val vakiot = luokka.java.enumConstants.map { it as Enum<*> }
            if (vakiot.first() is Koodisto.Koodiviite) {
                check(tarkennus == null || tarkennus is Tarkennus.Koodit) { "Tarkennus ei sovi kenttään $polku" }
                koodi((tarkennus as? Tarkennus.Koodit)?.arvot ?: vakiot.map { it as Koodisto.Koodiviite })
            } else {
                check(tarkennus == null || tarkennus is Tarkennus.Arvot) { "Tarkennus ei sovi kenttään $polku" }
                Muoto.Valinta(
                    ((tarkennus as? Tarkennus.Arvot)?.arvot ?: vakiot).map {
                        Arvo(mapper.valueToTree<JsonNode>(it).asString(), nimi(it))
                    },
                )
            }
        }

        luokka.isSealed -> {
            eiTarkennusta(
                Muoto.Tietoryhma(
                    checkNotNull(virta.alatyypit[luokka]) { "Tietovirralta puuttuu alatyyppi kentälle $polku" },
                ),
            )
        }

        luokka.isData -> {
            eiTarkennusta(Muoto.Tietoryhma(luokka))
        }

        else -> {
            error("Tuntematon tyyppi $luokka kentässä $polku")
        }
    }
}

private fun koodi(arvot: List<Koodisto.Koodiviite>): Muoto.Koodi =
    Muoto.Koodi(
        koodistoUri = arvot.map { it.koodistoUri }.distinct().single(),
        arvot = arvot.map { Arvo(it.koodiarvo, nimi(it)) },
    )

private fun nimi(arvo: Any): String? =
    when (arvo) {
        is Nimetty -> arvo.nimi.get(Language.FI)
        is Koodisto.KoodiviiteNimella -> arvo.nimi.get(Language.FI)
        is Koodisto.YkiArvosana -> arvo.viewText
        is DisplayEnum -> arvo.displayText()
        else -> null
    }

private fun kuvaus(
    luokka: KClass<*>,
    ominaisuus: KProperty1<out Any, *>,
): Kuvaus? =
    (
        listOf(ominaisuus) +
            luokka.allSuperclasses.mapNotNull { yla -> yla.memberProperties.find { it.name == ominaisuus.name } }
    ).firstNotNullOfOrNull { it.findAnnotation<Kuvaus>() }

private fun jatetaanPoisTyhjana(ominaisuus: KProperty1<out Any, *>): Boolean =
    ominaisuus.getter.findAnnotation<JsonInclude>()?.value == JsonInclude.Include.NON_NULL
