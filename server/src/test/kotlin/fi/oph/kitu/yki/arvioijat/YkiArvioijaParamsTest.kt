package fi.oph.kitu.yki.arvioijat

import fi.oph.kitu.i18n.UiText
import fi.oph.kitu.yki.Tutkintokieli
import fi.oph.kitu.yki.Tutkintotaso
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class YkiArvioijaParamsTest {
    @Test
    fun `rajauskuvaus näyttää valinnat valikon niminä`() {
        val params =
            YkiArvioijaParams(
                tila = listOf(Rekisterointitila.PASSIVOITU, Rekisterointitila.AKTIIVINEN),
                kieli = listOf(Tutkintokieli.SWE, Tutkintokieli.FIN),
                taso = listOf(Tutkintotaso.PT),
            )

        assertEquals(
            listOf(
                "${UiText.Yki.Sarake.tila}: ${UiText.Yki.ArvioijaTila.aktiivinen}, ${UiText.Yki.ArvioijaTila.passivoitu}",
                "${UiText.Yki.Sarake.kieli}: ${UiText.Yki.Kieli.suomi}, ${UiText.Yki.Kieli.ruotsi}",
                "${UiText.Yki.Sarake.taso}: ${UiText.Yki.Taso.perustaso}",
            ),
            params.filterDescriptions(),
        )
    }
}
