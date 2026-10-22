package fi.oph.kitu.oppijanumero

import fi.oph.kitu.util.defaultObjectMapper
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class HenkilonYhteystiedotTest {
    @Test
    fun `VTJn vakinainen kotimainen osoite ohittaa kotiosoitteen`() {
        val yhteystiedot =
            henkilo(
                ryhma(KOTIOSOITE, KATUOSOITE to "Kotikatu 1", POSTINUMERO to "00100", KAUPUNKI to "Helsinki"),
                ryhma(
                    VTJ_KOTIMAINEN,
                    KATUOSOITE to "Vtj-katu 2",
                    POSTINUMERO to "02100",
                    KAUPUNKI to "Espoo",
                    MAA to "Suomi",
                ),
            ).yhteystiedot()

        assertEquals("Vtj-katu 2", yhteystiedot.katuosoite)
        assertEquals("02100", yhteystiedot.postinumero)
        assertEquals("Espoo", yhteystiedot.postitoimipaikka)
        assertEquals("Suomi", yhteystiedot.maa)
    }

    @Test
    fun `VTJn ulkomainen osoite ohittaa kotiosoitteen`() {
        val yhteystiedot =
            henkilo(
                ryhma(KOTIOSOITE, KATUOSOITE to "Kotikatu 1"),
                ryhma(VTJ_ULKOMAINEN, KATUOSOITE to "Rue de Test 5", KAUPUNKI to "Paris", MAA to "Ranska"),
            ).yhteystiedot()

        assertEquals("Rue de Test 5", yhteystiedot.katuosoite)
        assertEquals("Ranska", yhteystiedot.maa)
    }

    @Test
    fun `kotiosoitetta kaytetaan kun VTJ-osoitetta ei ole`() {
        val yhteystiedot =
            henkilo(ryhma(KOTIOSOITE, KATUOSOITE to "Kotikatu 1", POSTINUMERO to "00100", KAUPUNKI to "Helsinki"))
                .yhteystiedot()

        assertEquals("Kotikatu 1", yhteystiedot.katuosoite)
        assertEquals("00100", yhteystiedot.postinumero)
        assertEquals("Helsinki", yhteystiedot.postitoimipaikka)
    }

    @Test
    fun `osoitteen kentat tulevat aina samasta ryhmasta`() {
        val yhteystiedot =
            henkilo(
                ryhma(VTJ_KOTIMAINEN, POSTINUMERO to "02100"),
                ryhma(KOTIOSOITE, KATUOSOITE to "Kotikatu 1", POSTINUMERO to "00100"),
                ryhma(TYOOSOITE, KATUOSOITE to "Tyokatu 9", POSTINUMERO to "00900", KAUPUNKI to "Helsinki"),
            ).yhteystiedot()

        assertEquals("Kotikatu 1", yhteystiedot.katuosoite)
        assertEquals("00100", yhteystiedot.postinumero)
        assertEquals(null, yhteystiedot.postitoimipaikka)
    }

    @Test
    fun `postitoimipaikaksi kelpaa kunta kun kaupunki puuttuu`() {
        assertEquals(
            "Vantaa",
            henkilo(
                ryhma(VTJ_KOTIMAINEN, KATUOSOITE to "Vtj-katu 2", KUNTA to "Vantaa"),
            ).yhteystiedot().postitoimipaikka,
        )
    }

    @Test
    fun `matkapuhelin ennen lankapuhelinta ja henkilokohtaiset ennen tyoyhteystietoja`() {
        val yhteystiedot =
            henkilo(
                ryhma(TYOOSOITE, MATKAPUHELIN to "040 999 9999", SAHKOPOSTI to "tyo@example.com"),
                ryhma(KOTIOSOITE, PUHELIN to "09 123 456", MATKAPUHELIN to "050 123 4567"),
            ).yhteystiedot()

        assertEquals("050 123 4567", yhteystiedot.puhelinnumero)
        assertEquals("tyo@example.com", yhteystiedot.sahkoposti)
    }

    @Test
    fun `henkilokohtainen lankapuhelin ohittaa tyon matkapuhelimen`() {
        assertEquals(
            "09 123 456",
            henkilo(
                ryhma(TYOOSOITE, MATKAPUHELIN to "040 999 9999"),
                ryhma(HAKEMUKSELTA, PUHELIN to "09 123 456"),
            ).yhteystiedot().puhelinnumero,
        )
    }

    @Test
    fun `tyhjat arvot ohitetaan`() {
        val yhteystiedot =
            henkilo(
                ryhma(KOTIOSOITE, SAHKOPOSTI to "  ", KATUOSOITE to ""),
                ryhma(VTJ_SAHKOINEN, SAHKOPOSTI to "vtj@example.com"),
            ).yhteystiedot()

        assertEquals("vtj@example.com", yhteystiedot.sahkoposti)
        assertEquals(null, yhteystiedot.katuosoite)
    }

    @Test
    fun `ilman yhteystietoja kaikki kentat ovat tyhjia`() {
        assertEquals(
            HenkilonYhteystiedot(null, null, null, null, null, null),
            defaultObjectMapper
                .convertValue(mapOf("oidHenkilo" to "1.2.246.562.24.1"), OppijanumerorekisteriHenkilo::class.java)
                .yhteystiedot(),
        )
    }

    private fun ryhma(
        kuvaus: String,
        vararg yhteystiedot: Pair<String, String>,
    ): Map<String, Any> =
        mapOf(
            "ryhmaKuvaus" to kuvaus,
            "yhteystieto" to
                yhteystiedot.map { (tyyppi, arvo) -> mapOf("yhteystietoTyyppi" to tyyppi, "yhteystietoArvo" to arvo) },
        )

    private fun henkilo(vararg ryhmat: Map<String, Any>): OppijanumerorekisteriHenkilo =
        defaultObjectMapper.convertValue(
            mapOf("yhteystiedotRyhma" to ryhmat.toList()),
            OppijanumerorekisteriHenkilo::class.java,
        )

    companion object {
        private const val KOTIOSOITE = "yhteystietotyyppi1"
        private const val TYOOSOITE = "yhteystietotyyppi2"
        private const val VTJ_KOTIMAINEN = "yhteystietotyyppi4"
        private const val VTJ_ULKOMAINEN = "yhteystietotyyppi5"
        private const val HAKEMUKSELTA = "yhteystietotyyppi6"
        private const val VTJ_SAHKOINEN = "yhteystietotyyppi8"

        private const val SAHKOPOSTI = "YHTEYSTIETO_SAHKOPOSTI"
        private const val PUHELIN = "YHTEYSTIETO_PUHELINNUMERO"
        private const val MATKAPUHELIN = "YHTEYSTIETO_MATKAPUHELINNUMERO"
        private const val KATUOSOITE = "YHTEYSTIETO_KATUOSOITE"
        private const val POSTINUMERO = "YHTEYSTIETO_POSTINUMERO"
        private const val KAUPUNKI = "YHTEYSTIETO_KAUPUNKI"
        private const val KUNTA = "YHTEYSTIETO_KUNTA"
        private const val MAA = "YHTEYSTIETO_MAA"
    }
}
