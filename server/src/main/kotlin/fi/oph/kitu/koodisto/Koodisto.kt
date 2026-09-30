package fi.oph.kitu.koodisto

import arrow.core.Either
import arrow.core.left
import arrow.core.right
import com.fasterxml.jackson.annotation.JsonValue
import fi.oph.kitu.html.table.HideInTableFilter
import fi.oph.kitu.i18n.LocalizedString
import fi.oph.kitu.organisaatiot.KoodiviiteUri
import fi.oph.kitu.yki.Tutkintotaso

object Koodisto {
    interface Koodiviite {
        @get:JsonValue
        val koodiarvo: String
        val koodistoUri: String

        fun toKoski(): KoskiKoodiviite = KoskiKoodiviite(koodiarvo, koodistoUri)
    }

    interface KoodiviiteNimella : Koodiviite {
        val nimi: LocalizedString
    }

    interface ArvosanaKoodiviite : Koodiviite {
        val order: Int

        companion object {
            fun <T : ArvosanaKoodiviite> compare(
                a: T,
                b: T,
            ): Int = a.order - b.order
        }
    }

    enum class OpiskeluoikeudenTyyppi(
        override val koodiarvo: String,
    ) : Koodiviite {
        Kielitutkinto("kielitutkinto"),
        ;

        override val koodistoUri: String = "opiskeluoikeudentyyppi"
    }

    enum class LahdeJarjestelma(
        override val koodiarvo: String = "kielitutkintorekisteri",
    ) : Koodiviite {
        Kielitutkintorekisteri,
        ;

        override val koodistoUri: String = "lahdejarjestelma"
    }

    enum class SuorituksenTyyppi(
        override val koodiarvo: String,
    ) : Koodiviite {
        YleinenKielitutkinto("yleinenkielitutkinto"),
        YleisenKielitutkinnonOsa("yleisenkielitutkinnonosa"),
        ValtionhallinnonKielitutkinto("valtionhallinnonkielitutkinto"),
        ValtionhallinnonKielitaito("valtionhallinnonkielitaito"),
        ValtionhallinnonKielitutkinnonOsakoe("valtionhallinnonkielitutkinnonosakoe"),
        ;

        override val koodistoUri: String = "suorituksentyyppi"
    }

    enum class OpiskeluoikeudenTila(
        override val koodiarvo: String,
    ) : Koodiviite {
        Lasna("lasna"),
        Paattynyt("paattynyt"),
        Mitatoity("mitatoity"),
        ;

        override val koodistoUri: String = "koskiopiskeluoikeudentila"
    }

    enum class YkiTutkintotaso(
        override val koodiarvo: String,
    ) : Koodiviite {
        PT("pt"),
        KT("kt"),
        YT("yt"),
        ;

        override val koodistoUri: String = "ykitutkintotaso"

        companion object {
            fun fromName(name: String): Either<InvalidKoodistoValueError, YkiTutkintotaso> =
                entries.firstOrNull { it.name == name }?.right()
                    ?: InvalidKoodistoValueError("ykitutkintotaso", name).left()
        }
    }

    enum class Tutkintokieli(
        override val koodiarvo: String,
        override val nimi: LocalizedString,
    ) : KoodiviiteNimella {
        @HideInTableFilter DEU("DE", LocalizedString("Saksa")),

        @HideInTableFilter ENG("EN", LocalizedString("Englanti")),

        FIN("FI", LocalizedString("Suomi")),

        @HideInTableFilter FRA("FR", LocalizedString("Ranska")),

        @HideInTableFilter ITA("IT", LocalizedString("Italia")),

        @HideInTableFilter RUS("RU", LocalizedString("Venäjä")),

        @HideInTableFilter SME("SE", LocalizedString("Pohjoissaame")),

        @HideInTableFilter SPA("ES", LocalizedString("Espanja")),

        SWE("SV", LocalizedString("Ruotsi")),
        ;

        override val koodistoUri: String = "kieli"

        companion object {
            fun fromName(name: String): Either<InvalidKoodistoValueError, Tutkintokieli> =
                entries.firstOrNull { it.name == name }?.right()
                    ?: InvalidKoodistoValueError("kieli", name).left()
        }
    }

    enum class YkiSuorituksenOsa(
        override val koodiarvo: String,
    ) : Koodiviite {
        TekstinYmmartaminen("tekstinymmartaminen"),
        Kirjoittaminen("kirjoittaminen"),
        PuheenYmmartaminen("puheenymmartaminen"),
        Puhuminen("puhuminen"),
        RakenteetJaSanasto("rakenteetjasanasto"),
        ;

        override val koodistoUri: String = "ykisuorituksenosa"
    }

    enum class YkiArvosana(
        override val koodiarvo: String,
        val viewText: String,
    ) : Koodiviite {
        PT1("1", "1"),
        PT2("2", "2"),
        KT3("3", "3"),
        KT4("4", "4"),
        YT5("5", "5"),
        YT6("6", "6"),
        ALLE1("alle1", "Alle 1"),
        ALLE3("alle3", "Alle 3"),
        ALLE5("alle5", "Alle 5"),
        EiVoiArvioida("9", "Ei voi arvioida"),
        Keskeytetty("10", "Keskeytetty"),
        Vilppi("11", "Vilppi"),
        EiSuoritusta("12", "Ei suoritusta"),
        ;

        override val koodistoUri: String = "ykiarvosana"

        companion object {
            fun of(
                arvosana: Int,
                tutkintotaso: Tutkintotaso,
            ): Either<InvalidYkiArvosanaError, YkiArvosana> =
                when (tutkintotaso) {
                    Tutkintotaso.PT -> {
                        when (arvosana) {
                            0 -> ALLE1.right()
                            1 -> PT1.right()
                            2 -> PT2.right()
                            9 -> EiVoiArvioida.right()
                            10 -> Keskeytetty.right()
                            11 -> Vilppi.right()
                            12 -> EiSuoritusta.right()
                            else -> InvalidYkiArvosanaError(arvosana, tutkintotaso).left()
                        }
                    }

                    Tutkintotaso.KT -> {
                        when (arvosana) {
                            3 -> KT3.right()
                            4 -> KT4.right()
                            0, 1, 2 -> ALLE3.right()
                            9 -> EiVoiArvioida.right()
                            10 -> Keskeytetty.right()
                            11 -> Vilppi.right()
                            12 -> EiSuoritusta.right()
                            else -> InvalidYkiArvosanaError(arvosana, tutkintotaso).left()
                        }
                    }

                    Tutkintotaso.YT -> {
                        when (arvosana) {
                            5 -> YT5.right()
                            6 -> YT6.right()
                            0, 1, 2, 3, 4 -> ALLE5.right()
                            9 -> EiVoiArvioida.right()
                            10 -> Keskeytetty.right()
                            11 -> Vilppi.right()
                            12 -> EiSuoritusta.right()
                            else -> InvalidYkiArvosanaError(arvosana, tutkintotaso).left()
                        }
                    }
                }

            fun validIntegersFor(tutkintotaso: Tutkintotaso): Set<Int> =
                when (tutkintotaso) {
                    Tutkintotaso.PT -> setOf(0, 1, 2, 9, 10, 11, 12)
                    Tutkintotaso.KT -> setOf(0, 1, 2, 3, 4, 9, 10, 11, 12)
                    Tutkintotaso.YT -> setOf(0, 1, 2, 3, 4, 5, 6, 9, 10, 11, 12)
                }
        }
    }

    enum class VktTaitotaso(
        override val koodiarvo: String,
        override val nimi: LocalizedString,
    ) : KoodiviiteNimella {
        Erinomainen("erinomainen", LocalizedString("Erinomainen")),
        HyväJaTyydyttävä("hyvajatyydyttava", LocalizedString("Hyvä ja tyydyttävä")),
        ;

        override val koodistoUri: String = "vkttutkintotaso"
    }

    enum class VktKielitaito(
        override val koodiarvo: String,
    ) : Koodiviite {
        Kirjallinen("kirjallinen"),
        Suullinen("suullinen"),
        Ymmärtäminen("ymmartaminen"),
        ;

        override val koodistoUri: String = "vktkielitaito"
    }

    enum class VktOsakoe(
        override val koodiarvo: String,
    ) : Koodiviite {
        Kirjoittaminen("kirjoittaminen"),
        PuheenYmmärtäminen("puheenymmartaminen"),
        Puhuminen("puhuminen"),
        TekstinYmmärtäminen("tekstinymmartaminen"),
        ;

        override val koodistoUri: String = "vktosakoe"
    }

    enum class VktArvosana(
        override val koodiarvo: String,
        override val order: Int,
    ) : ArvosanaKoodiviite {
        Erinomainen("erinomainen", 3),
        Hyvä("hyva", 2),
        Tyydyttävä("tyydyttava", 1),
        Hylätty("hylatty", -1),
        EiSuoritusta("ei_suoritusta", -2),
        ;

        override val koodistoUri: String = "vktarvosana"
    }
}

data class InvalidYkiArvosanaError(
    val arvosana: Int,
    val tutkintotaso: Tutkintotaso,
) : Exception("Virheellinen arvosana $arvosana tutkintotasolle $tutkintotaso")

data class InvalidKoodistoValueError(
    val koodistoUri: String,
    val name: String,
) : Exception("Tuntematon arvo \"$name\" koodistossa $koodistoUri")
