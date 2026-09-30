package fi.oph.kitu.koodisto

import arrow.core.Either
import fi.oph.kitu.organisaatiot.KoodiviiteUri
import fi.oph.kitu.yki.Tutkintotaso
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource
import org.junit.jupiter.params.provider.MethodSource
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertNull

class KoodistoEnumsTest {
    @Test
    fun `YkiTutkintotaso fromName palauttaa tunnetun arvon Right-haarana`() {
        assertEquals(
            Koodisto.YkiTutkintotaso.PT,
            (Koodisto.YkiTutkintotaso.fromName("PT") as Either.Right).value,
        )
        assertEquals(
            Koodisto.YkiTutkintotaso.YT,
            (Koodisto.YkiTutkintotaso.fromName("YT") as Either.Right).value,
        )
    }

    @Test
    fun `YkiTutkintotaso fromName erottelee kirjainkoon`() {
        val result = Koodisto.YkiTutkintotaso.fromName("pt")
        val left = assertIs<Either.Left<InvalidKoodistoValueError>>(result).value
        assertEquals("ykitutkintotaso", left.koodistoUri)
        assertEquals("pt", left.name)
    }

    @Test
    fun `YkiTutkintotaso fromName palauttaa Left-virheen tuntemattomasta arvosta`() {
        val result = Koodisto.YkiTutkintotaso.fromName("X1")
        val left = assertIs<Either.Left<InvalidKoodistoValueError>>(result).value
        assertEquals("ykitutkintotaso", left.koodistoUri)
        assertEquals("X1", left.name)
    }

    @Test
    fun `Tutkintokieli fromName palauttaa tunnetun arvon`() {
        assertEquals(
            Koodisto.Tutkintokieli.FIN,
            (Koodisto.Tutkintokieli.fromName("FIN") as Either.Right).value,
        )
    }

    @Test
    fun `Tutkintokieli fromName palauttaa Leftin tuntemattomasta arvosta`() {
        val result = Koodisto.Tutkintokieli.fromName("XXX")
        val left = assertIs<Either.Left<InvalidKoodistoValueError>>(result).value
        assertEquals("kieli", left.koodistoUri)
        assertEquals("XXX", left.name)
    }

    @ParameterizedTest
    @MethodSource("ptCases")
    fun `YkiArvosana of PT-tasolla`(
        input: Int,
        expected: Koodisto.YkiArvosana,
    ) {
        assertEquals(
            expected,
            (Koodisto.YkiArvosana.of(input, Tutkintotaso.PT) as Either.Right).value,
        )
    }

    @ParameterizedTest
    @MethodSource("ktCases")
    fun `YkiArvosana of KT-tasolla`(
        input: Int,
        expected: Koodisto.YkiArvosana,
    ) {
        assertEquals(
            expected,
            (Koodisto.YkiArvosana.of(input, Tutkintotaso.KT) as Either.Right).value,
        )
    }

    @ParameterizedTest
    @MethodSource("ytCases")
    fun `YkiArvosana of YT-tasolla`(
        input: Int,
        expected: Koodisto.YkiArvosana,
    ) {
        assertEquals(
            expected,
            (Koodisto.YkiArvosana.of(input, Tutkintotaso.YT) as Either.Right).value,
        )
    }

    @ParameterizedTest
    @EnumSource(Tutkintotaso::class)
    fun `YkiArvosana of palauttaa Leftin sallitun alueen ulkopuolelta`(taso: Tutkintotaso) {
        val result = Koodisto.YkiArvosana.of(99, taso)
        val left = assertIs<Either.Left<InvalidYkiArvosanaError>>(result).value
        assertEquals(99, left.arvosana)
        assertEquals(taso, left.tutkintotaso)
    }

    @Test
    fun `YkiArvosana validIntegersFor antaa kullekin tasolle sallitut numerot`() {
        assertEquals(setOf(0, 1, 2, 9, 10, 11, 12), Koodisto.YkiArvosana.validIntegersFor(Tutkintotaso.PT))
        assertEquals(setOf(0, 1, 2, 3, 4, 9, 10, 11, 12), Koodisto.YkiArvosana.validIntegersFor(Tutkintotaso.KT))
        assertEquals(setOf(0, 1, 2, 3, 4, 5, 6, 9, 10, 11, 12), Koodisto.YkiArvosana.validIntegersFor(Tutkintotaso.YT))
    }

    @Test
    fun `toKoski tuottaa KoskiKoodiviite-objektin samalla arvolla ja URIlla`() {
        val koski = Koodisto.YkiTutkintotaso.KT.toKoski()
        assertEquals("kt", koski.koodiarvo)
        assertEquals("ykitutkintotaso", koski.koodistoUri)
    }

    @Test
    fun `VktArvosana jarjestys vastaa order-kenttaa`() {
        val sorted = Koodisto.VktArvosana.entries.sortedWith(Koodisto.ArvosanaKoodiviite::compare)
        assertEquals(
            listOf(
                Koodisto.VktArvosana.EiSuoritusta,
                Koodisto.VktArvosana.Hylätty,
                Koodisto.VktArvosana.Tyydyttävä,
                Koodisto.VktArvosana.Hyvä,
                Koodisto.VktArvosana.Erinomainen,
            ),
            sorted,
        )
    }

    companion object {
        @JvmStatic
        fun ptCases(): List<Array<Any>> =
            listOf(
                arrayOf(0, Koodisto.YkiArvosana.ALLE1),
                arrayOf(1, Koodisto.YkiArvosana.PT1),
                arrayOf(2, Koodisto.YkiArvosana.PT2),
                arrayOf(9, Koodisto.YkiArvosana.EiVoiArvioida),
                arrayOf(10, Koodisto.YkiArvosana.Keskeytetty),
                arrayOf(11, Koodisto.YkiArvosana.Vilppi),
            )

        @JvmStatic
        fun ktCases(): List<Array<Any>> =
            listOf(
                arrayOf(0, Koodisto.YkiArvosana.ALLE3),
                arrayOf(1, Koodisto.YkiArvosana.ALLE3),
                arrayOf(2, Koodisto.YkiArvosana.ALLE3),
                arrayOf(3, Koodisto.YkiArvosana.KT3),
                arrayOf(4, Koodisto.YkiArvosana.KT4),
                arrayOf(9, Koodisto.YkiArvosana.EiVoiArvioida),
                arrayOf(10, Koodisto.YkiArvosana.Keskeytetty),
                arrayOf(11, Koodisto.YkiArvosana.Vilppi),
            )

        @JvmStatic
        fun ytCases(): List<Array<Any>> =
            listOf(
                arrayOf(0, Koodisto.YkiArvosana.ALLE5),
                arrayOf(1, Koodisto.YkiArvosana.ALLE5),
                arrayOf(2, Koodisto.YkiArvosana.ALLE5),
                arrayOf(3, Koodisto.YkiArvosana.ALLE5),
                arrayOf(4, Koodisto.YkiArvosana.ALLE5),
                arrayOf(5, Koodisto.YkiArvosana.YT5),
                arrayOf(6, Koodisto.YkiArvosana.YT6),
                arrayOf(9, Koodisto.YkiArvosana.EiVoiArvioida),
                arrayOf(10, Koodisto.YkiArvosana.Keskeytetty),
                arrayOf(11, Koodisto.YkiArvosana.Vilppi),
            )
    }
}
