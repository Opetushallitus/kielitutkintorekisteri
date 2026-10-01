package fi.oph.kitu.yki.suoritukset

import arrow.core.Either
import fi.oph.kitu.dev.mockdata.generateRandomYkiSuoritusEntity
import fi.oph.kitu.i18n.Language
import fi.oph.kitu.i18n.Translations
import fi.oph.kitu.i18n.UiText
import fi.oph.kitu.i18n.finnishDateTime
import fi.oph.kitu.oppijanumero.OppijanumerorekisteriHenkilo
import fi.oph.kitu.yki.Arviointitila
import fi.oph.kitu.yki.TutkinnonOsa
import kotlinx.html.div
import kotlinx.html.stream.createHTML
import org.junit.jupiter.api.Test
import java.time.Instant
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class YkiSuoritusPageTest {
    private fun renderHenkilonTiedot(
        suoritus: YkiSuoritusEntity,
        henkilo: OppijanumerorekisteriHenkilo?,
    ) = with(YkiSuoritusPage) {
        createHTML()
            .div {
                henkilonTiedot(
                    henkilo?.let { Either.Right(it) },
                    suoritus,
                    Translations(Language.FI, emptyMap()),
                )
            }.replace(Regex(">\\s+<"), "><")
    }

    private fun onrHenkilo(
        hetu: String? = null,
        sukunimi: String? = null,
        etunimet: String? = null,
    ) = OppijanumerorekisteriHenkilo(
        oidHenkilo = null,
        hetu = hetu,
        kaikkiHetut = null,
        passivoitu = null,
        etunimet = etunimet,
        kutsumanimi = null,
        sukunimi = sukunimi,
        aidinkieli = null,
        asiointiKieli = null,
        kansalaisuus = null,
        kasittelijaOid = null,
        syntymaaika = null,
        sukupuoli = null,
        kotikunta = null,
        oppijanumero = null,
        turvakielto = null,
        eiSuomalaistaHetua = null,
        yksiloity = null,
        yksiloityVTJ = null,
        yksilointiYritetty = null,
        duplicate = null,
        created = null,
        modified = null,
        vtjsynced = null,
        yhteystiedotRyhma = null,
        yksilointivirheet = null,
        passinumerot = null,
    )

    private fun renderArviointi(suoritus: YkiSuoritusEntity) =
        with(YkiSuoritusPage) {
            createHTML().div { arviointi(suoritus) }
        }

    private fun renderIntegraatiot(suoritus: YkiSuoritusEntity) =
        with(YkiSuoritusPage) {
            createHTML().div { integraatiot(suoritus, null, null, null) }
        }

    private fun ilmoittautunut(vararg osakokeet: TutkinnonOsa) =
        generateRandomYkiSuoritusEntity()
            .copy(
                arviointitila = Arviointitila.ILMOITTAUTUNUT,
                arviointipaiva = null,
                tekstinYmmartaminen = null,
                kirjoittaminen = null,
                rakenteetJaSanasto = null,
                puheenYmmartaminen = null,
                puhuminen = null,
                yleisarvosana = null,
                tarkistusarvioinninSaapumisPvm = null,
                tarkistusarvioinninAsiatunnus = null,
                tarkistusarvioidutOsakokeet = null,
                arvosanaMuuttui = null,
                perustelu = null,
                tarkistusarvioinninKasittelyPvm = null,
            ).also { it.ilmoitetutOsakokeet = osakokeet.toSet() }

    @Test
    fun `arviointi renders registered osakokeet even without arvosana`() {
        val html = renderArviointi(ilmoittautunut(TutkinnonOsa.TY, TutkinnonOsa.KI))

        assertTrue(html.contains(TutkinnonOsa.TY.viewText), "missing tekstin ymmärtäminen row:\n$html")
        assertTrue(html.contains(TutkinnonOsa.KI.viewText), "missing kirjoittaminen row:\n$html")
    }

    @Test
    fun `arviointi does not render osakokeet that were not registered`() {
        val html = renderArviointi(ilmoittautunut(TutkinnonOsa.TY))

        assertTrue(html.contains(TutkinnonOsa.TY.viewText), "missing tekstin ymmärtäminen row:\n$html")
        assertFalse(html.contains(TutkinnonOsa.YL.viewText), "unregistered yleisarvosana should not render:\n$html")
    }

    @Test
    fun `integraatiot renders rekisteriintuontiaika from receivedAt`() {
        val suoritus =
            generateRandomYkiSuoritusEntity().copy(
                lastModified = Instant.parse("2026-01-01T10:00:00Z"),
                receivedAt = Instant.parse("2025-06-15T08:30:00Z"),
            )

        val html = renderIntegraatiot(suoritus)

        assertTrue(
            html.contains(
                UiText.Yki.Sarake.rekisteriintuontiaika
                    .toString(),
            ),
            "missing rekisteriintuontiaika header:\n$html",
        )
        assertTrue(
            html.contains(suoritus.receivedAt.finnishDateTime(includeTimeZone = false)),
            "missing receivedAt value:\n$html",
        )
    }

    @Test
    fun `henkilotunnus renders a dash for a hetuton suoritus instead of falling back to the ONR hetu`() {
        val suoritus = generateRandomYkiSuoritusEntity().copy(hetu = null)
        val henkilo = onrHenkilo(hetu = "010180-9026", sukunimi = suoritus.sukunimi, etunimet = suoritus.etunimet)

        val html = renderHenkilonTiedot(suoritus, henkilo)

        assertTrue(
            html.contains("<tr><th>Henkilötunnus</th><td>–</td><td>010180-9026</td></tr>"),
            "hetu row should show a dash for the suoritus and the ONR hetu without a diff highlight:\n$html",
        )
    }

    @Test
    fun `henkilotunnus row is not highlighted as a diff even when the hetut differ`() {
        val suoritus = generateRandomYkiSuoritusEntity().copy(hetu = "111111-111C")
        val henkilo = onrHenkilo(hetu = "222222-222D", sukunimi = suoritus.sukunimi, etunimet = suoritus.etunimet)

        val html = renderHenkilonTiedot(suoritus, henkilo)

        assertTrue(
            html.contains("<tr><th>Henkilötunnus</th><td>111111-111C</td><td>222222-222D</td></tr>"),
            "differing hetut should render without a diff highlight:\n$html",
        )
    }

    @Test
    fun `henkilotunnus renders a dash for the ONR column when ONR has no hetu`() {
        val suoritus = generateRandomYkiSuoritusEntity().copy(hetu = "111111-111C")
        val henkilo = onrHenkilo(hetu = null, sukunimi = suoritus.sukunimi, etunimet = suoritus.etunimet)

        val html = renderHenkilonTiedot(suoritus, henkilo)

        assertTrue(
            html.contains("<tr><th>Henkilötunnus</th><td>111111-111C</td><td>–</td></tr>"),
            "missing ONR hetu should render as a dash without a diff highlight:\n$html",
        )
    }

    @Test
    fun `differing sukunimi is still highlighted as a diff`() {
        val suoritus = generateRandomYkiSuoritusEntity().copy(sukunimi = "Meikäläinen", hetu = null)
        val henkilo = onrHenkilo(hetu = "010180-9026", sukunimi = "Möykäläinen", etunimet = suoritus.etunimet)

        val html = renderHenkilonTiedot(suoritus, henkilo)

        assertTrue(
            html.contains("""<tr class="diff"><th>Sukunimi</th><td>Meikäläinen</td><td>Möykäläinen</td></tr>"""),
            "differing sukunimi should keep the diff highlight:\n$html",
        )
    }

    @Test
    fun `oppijanumeroton historiarivi nakyy eika tarjoa yksilointilinkkia`() {
        val suoritus = generateRandomYkiSuoritusEntity().copy(suorittajanOID = null)

        val html = renderHenkilonTiedot(suoritus, null)

        assertTrue(html.contains(UiText.Yki.eiOppijanumeroa.toString()), html)
        assertFalse(html.contains(UiText.Yki.teeYksilointi.toString()), "yksilointilinkki nakyi")
        assertFalse(html.contains("/henkilo-ui/oppija/"), "ONR-linkki nakyi")
    }

    @Test
    fun `yksiloimaton henkilo tarjoaa yhä yksilointilinkin`() {
        val suoritus = generateRandomYkiSuoritusEntity()

        val html = renderHenkilonTiedot(suoritus, onrHenkilo())

        assertTrue(html.contains(UiText.Yki.teeYksilointi.toString()), html)
        assertFalse(html.contains(UiText.Yki.eiOppijanumeroa.toString()), html)
    }

    @Test
    fun `koko tietosivu renderoityy ilman oppijanumeroa ja ilman ONR-vastausta`() {
        val suoritus = generateRandomYkiSuoritusEntity().copy(id = 1, suorittajanOID = null)

        val html =
            YkiSuoritusPage.render(
                henkilo = null,
                suoritus = suoritus,
                viimeisinSuoritus = suoritus,
                koskiError = null,
                koskiSiirronEstonSyyt = listOf("Suorituksella ei ole oppijanumeroa"),
                opiskeluoikeusOid = null,
                t = Translations(Language.FI, emptyMap()),
            )

        assertTrue(html.contains(UiText.Yki.eiOppijanumeroa.toString()), "oppijanumeron puute ei nakynyt")
        assertTrue(html.contains("Suorituksella ei ole oppijanumeroa"), "KOSKI-eston syy ei nakynyt")
    }
}
