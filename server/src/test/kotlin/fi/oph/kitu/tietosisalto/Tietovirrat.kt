package fi.oph.kitu.tietosisalto

import fi.oph.kitu.ilmoittautumisjarjestelma.YkiArvioinninTilaRequest
import fi.oph.kitu.koodisto.Koodisto
import fi.oph.kitu.koski.KoskiRequest
import fi.oph.kitu.koski.KoskiRequest.Opiskeluoikeus.KielitutkintoSuoritus.Vahvistus
import fi.oph.kitu.koski.KoskiRequest.Opiskeluoikeus.KielitutkintoSuoritus.VahvistusImpl
import fi.oph.kitu.koski.KoskiRequest.Opiskeluoikeus.KielitutkintoSuoritus.VahvistusPaikkakunnalla
import fi.oph.kitu.koski.Osasuoritus
import fi.oph.kitu.koski.VktKielitaito
import fi.oph.kitu.koski.YkiOsasuoritus
import fi.oph.kitu.yki.TutkinnonOsa
import fi.oph.kitu.yki.Tutkintokieli
import fi.oph.kitu.yki.arvioijat.solki.SolkiArvioijaPoistoRequest
import fi.oph.kitu.yki.arvioijat.solki.SolkiArvioijaRequest
import kotlin.reflect.KClass

data class Tietovirta(
    val ankkuri: String,
    val otsikko: String,
    val vastaanottaja: String,
    val milloin: String,
    val miksi: String,
    val juuri: KClass<*>,
    val alatyypit: Map<KClass<*>, KClass<*>> = emptyMap(),
    val tarkennukset: Map<String, Tarkennus> = emptyMap(),
    val huomiot: List<String> = emptyList(),
)

sealed interface Tarkennus {
    data class Koodit(
        val arvot: List<Koodisto.Koodiviite>,
    ) : Tarkennus {
        constructor(vararg arvot: Koodisto.Koodiviite) : this(arvot.toList())
    }

    data class Arvot(
        val arvot: List<Enum<*>>,
    ) : Tarkennus

    data class UlkoinenKoodisto(
        val koodistoUri: String,
        val selite: String,
    ) : Tarkennus

    data object EiKaytossa : Tarkennus
}

object Tietovirrat {
    private const val JAKSON_TILA = "opiskeluoikeudet[].tila.opiskeluoikeusjaksot[].tila"
    private const val SUORITUS = "opiskeluoikeudet[].suoritukset[]"
    private const val OSASUORITUS = "$SUORITUS.osasuoritukset[]"
    private const val OSAKOE = "$OSASUORITUS.osasuoritukset[]"

    private val nykyisetKielet = Tarkennus.Arvot(Tutkintokieli.entries - Tutkintokieli.legacyEntries)

    private val jaksonTilat =
        Tarkennus.Koodit(Koodisto.OpiskeluoikeudenTila.Lasna, Koodisto.OpiskeluoikeudenTila.Paattynyt)

    val koskiYki =
        Tietovirta(
            ankkuri = "koski-yki",
            otsikko = "YKI-suoritukset KOSKI-järjestelmään",
            vastaanottaja = "Opetushallituksen KOSKI-järjestelmä",
            milloin =
                "Ajastetusti sen jälkeen, kun suoritus on arvioitu. " +
                    "Suoritus lähetetään uudelleen, jos sen tiedot muuttuvat.",
            miksi =
                "Suorittaja näkee suorituksensa Oma Opintopolussa, ja KOSKI-järjestelmä tekee siitä todistuksen.",
            juuri = KoskiRequest::class,
            alatyypit = mapOf(Vahvistus::class to VahvistusImpl::class, Osasuoritus::class to YkiOsasuoritus::class),
            tarkennukset =
                mapOf(
                    JAKSON_TILA to jaksonTilat,
                    "$SUORITUS.tyyppi" to Tarkennus.Koodit(Koodisto.SuorituksenTyyppi.YleinenKielitutkinto),
                    "$SUORITUS.koulutusmoduuli.tunniste" to Tarkennus.Koodit(Koodisto.YkiTutkintotaso.entries),
                    "$OSASUORITUS.tyyppi" to Tarkennus.Koodit(Koodisto.SuorituksenTyyppi.YleisenKielitutkinnonOsa),
                    "$OSASUORITUS.koulutusmoduuli.tunniste" to Tarkennus.Koodit(Koodisto.YkiSuorituksenOsa.entries),
                    "$OSASUORITUS.arviointi[].arvosana" to
                        Tarkennus.Koodit(
                            Koodisto.YkiArvosana.entries -
                                setOf(
                                    Koodisto.YkiArvosana.Keskeytetty,
                                    Koodisto.YkiArvosana.Vilppi,
                                    Koodisto.YkiArvosana.EiSuoritusta,
                                ),
                        ),
                    "$OSASUORITUS.alkamispäivä" to Tarkennus.EiKaytossa,
                    "$SUORITUS.yleisarvosana" to Tarkennus.Koodit(Koodisto.YkiArvosana.entries),
                ),
            huomiot =
                listOf(
                    "Suorittajasta lähetetään vain oppijanumero. Nimeä, henkilötunnusta tai yhteystietoja ei lähetetä.",
                    "Suoritusta ei lähetetä, jos sitä ei ole arvioitu, sillä ei ole oppijanumeroa tai jonkin " +
                        "osakokeen arvosana on vilppi.",
                    "Keskeytettyjä ja suorittamatta jääneitä osakokeita ei lähetetä.",
                ),
        )

    val koskiVkt =
        Tietovirta(
            ankkuri = "koski-vkt",
            otsikko = "VKT-suoritukset KOSKI-järjestelmään",
            vastaanottaja = "Opetushallituksen KOSKI-järjestelmä",
            milloin =
                "Ajastetusti sen jälkeen, kun suorituksen kaikki osakokeet on arvioitu. " +
                    "Suoritus lähetetään uudelleen, jos sen tiedot muuttuvat.",
            miksi = "Suorittaja näkee suorituksensa Oma Opintopolussa.",
            juuri = KoskiRequest::class,
            alatyypit =
                mapOf(Vahvistus::class to VahvistusPaikkakunnalla::class, Osasuoritus::class to VktKielitaito::class),
            tarkennukset =
                mapOf(
                    JAKSON_TILA to jaksonTilat,
                    "$SUORITUS.tyyppi" to Tarkennus.Koodit(Koodisto.SuorituksenTyyppi.ValtionhallinnonKielitutkinto),
                    "$SUORITUS.koulutusmoduuli.tunniste" to Tarkennus.Koodit(Koodisto.VktTaitotaso.entries),
                    "$SUORITUS.koulutusmoduuli.kieli" to
                        Tarkennus.Koodit(Koodisto.Tutkintokieli.FIN, Koodisto.Tutkintokieli.SWE),
                    "$SUORITUS.vahvistus.paikkakunta" to Tarkennus.UlkoinenKoodisto("kunta", "Suomen kunnat"),
                    "$OSASUORITUS.tyyppi" to Tarkennus.Koodit(Koodisto.SuorituksenTyyppi.ValtionhallinnonKielitaito),
                    "$OSASUORITUS.koulutusmoduuli.tunniste" to Tarkennus.Koodit(Koodisto.VktKielitaito.entries),
                    "$OSASUORITUS.arviointi[].arvosana" to Tarkennus.Koodit(Koodisto.VktArvosana.entries),
                    "$OSAKOE.tyyppi" to
                        Tarkennus.Koodit(Koodisto.SuorituksenTyyppi.ValtionhallinnonKielitutkinnonOsakoe),
                    "$OSAKOE.koulutusmoduuli.tunniste" to Tarkennus.Koodit(Koodisto.VktOsakoe.entries),
                    "$OSAKOE.arviointi[].arvosana" to Tarkennus.Koodit(Koodisto.VktArvosana.entries),
                    "$SUORITUS.järjestäjä" to Tarkennus.EiKaytossa,
                    "$SUORITUS.yleisarvosana" to Tarkennus.EiKaytossa,
                ),
            huomiot =
                listOf(
                    "Suorittajasta lähetetään vain oppijanumero. Nimeä, henkilötunnusta tai yhteystietoja ei lähetetä.",
                    "Suoritusta ei lähetetä, jos jonkin osakokeen arviointi tai suorituspaikkakunta puuttuu.",
                    "Lähetykseen tulevat vain ne kielitaidot, joiden kaikki osakokeet on suoritettu ja arvioitu.",
                ),
        )

    val solkiArvioija =
        Tietovirta(
            ankkuri = "solki-arvioija",
            otsikko = "YKI-arvioijat Solki-järjestelmään",
            vastaanottaja = "Jyväskylän yliopiston Solki-järjestelmä",
            milloin =
                "Aina kun arvioijan tietoja tallennetaan Kielitutkintorekisterissä. " +
                    "Epäonnistunut lähetys yritetään ajastetusti uudelleen.",
            miksi = "Solki tarvitsee ajantasaiset tiedot arvioijista ja heidän arviointioikeuksistaan.",
            juuri = SolkiArvioijaRequest::class,
            tarkennukset =
                mapOf(
                    "tila" to
                        Tarkennus.Arvot(
                            listOf(SolkiArvioijaRequest.Tila.ARVIOIJA, SolkiArvioijaRequest.Tila.KOULUTETTAVA),
                        ),
                    "arviointikaudet[].arviointioikeudet[].kieli" to nykyisetKielet,
                ),
            huomiot =
                listOf(
                    "Lähetyksessä ovat joka kerta arvioijan kaikki arviointikaudet. Kausi tai arviointioikeus, " +
                        "jota lähetyksessä ei enää ole, on poistettu Kielitutkintorekisteristä.",
                    "Arvioijalla voi olla samaan aikaan voimassa useampi arviointikausi eri kielille. Samalla " +
                        "kielellä kaudet eivät ole päällekkäin.",
                    "Koulutettavan arviointikaudessa ei ole päiviä, ja sen arviointioikeudet ovat kielet ja " +
                        "tasot, joihin henkilö koulutetaan. Kielitutkintorekisteri ei vielä lähetä koulutettavia.",
                    "Henkilötunnusta ei lähetetä.",
                    "Puhelinnumeroa ei lähetetä.",
                    "Arviointikauden tilaa (aktiivinen, passivoitu) ei lähetetä, koska se vanhenisi kauden " +
                        "päättyessä. Vastaanottaja päättelee tilan arviointikauden päivistä; kauden " +
                        "päättymispäivä on kauden viimeinen voimassaolopäivä.",
                    "Käytöstä poistuneiden kielten arviointioikeuksia ei lähetetä.",
                    "Syntymäpäivää ei säilytetä Kielitutkintorekisterissä, vaan se haetaan " +
                        "oppijanumerorekisteristä jokaista lähetystä varten.",
                ),
        )

    val solkiArvioijanPoisto =
        Tietovirta(
            ankkuri = "solki-arvioija-poisto",
            otsikko = "Poistetut YKI-arvioijat Solki-järjestelmään",
            vastaanottaja = "Jyväskylän yliopiston Solki-järjestelmä",
            milloin =
                "Kun arvioijan merkintä poistetaan Kielitutkintorekisteristä säilytysajan päätyttyä. " +
                    "Epäonnistunut lähetys yritetään ajastetusti uudelleen.",
            miksi = "Solki saa tiedon, ettei henkilö ole enää arvioijarekisterissä.",
            juuri = SolkiArvioijaPoistoRequest::class,
            tarkennukset = mapOf("tila" to Tarkennus.Arvot(listOf(SolkiArvioijaRequest.Tila.POISTETTU))),
            huomiot =
                listOf(
                    "Poistetusta arvioijasta ei lähetetä nimeä, yhteystietoja eikä arviointioikeuksia.",
                    "Ilmoitus lähetetään samaan osoitteeseen kuin arvioijan tiedot.",
                ),
        )

    val kiosArviointitila =
        Tietovirta(
            ankkuri = "ilmoittautumisjarjestelma-arviointitila",
            otsikko = "YKI-arviointitilat ilmoittautumisjärjestelmään",
            vastaanottaja = "YKI-tutkintojen ilmoittautumisjärjestelmä (KIOS)",
            milloin =
                "Heti kun suorituksen tiedot päivittyvät Kielitutkintorekisterissä. " +
                    "Ajastettu lähetys varmistaa, että tila menee perille myös katkoksen jälkeen.",
            miksi = "Suorittaja näkee arvioinnin etenemisen omalla sivullaan ilmoittautumisjärjestelmässä.",
            juuri = YkiArvioinninTilaRequest::class,
            tarkennukset =
                mapOf(
                    "tilat[].suoritus.tutkintokieli" to nykyisetKielet,
                    "tilat[].suoritus.osakokeet[]" to
                        Tarkennus.Arvot(listOf(TutkinnonOsa.PU, TutkinnonOsa.KI, TutkinnonOsa.TY, TutkinnonOsa.PY)),
                ),
            huomiot =
                listOf(
                    "Arvosanoja ei lähetetä, vain arvioinnin tila.",
                    "Suorituksesta ei lähetetä mitään, jos sillä ei ole oppijanumeroa tai siihen liittyy vilppi.",
                ),
        )

    val kaikki = listOf(koskiYki, koskiVkt, solkiArvioija, solkiArvioijanPoisto, kiosArviointitila)
}
