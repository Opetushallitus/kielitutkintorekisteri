package fi.oph.kitu.yki.arvioijat.hakemus

import fi.oph.kitu.yki.arvioijat.hakemus.ArvioijahakemuksenVirhetyyppi.TUNTEMATON_VAIHTOEHTO
import fi.oph.kitu.yki.arvioijat.hakemus.ArvioijahakemuksenVirhetyyppi.VAARA_TIETOTYYPPI
import fi.oph.kitu.yki.arvioijat.hakemus.ArvioijahakemuksenVirhetyyppi.VALIDOINTIVIRHE
import fi.oph.kitu.yki.arvioijat.hakemus.ArvioijahakemuksenVirhetyyppi.VIRHEELLINEN_RAKENNE
import tools.jackson.databind.node.StringNode
import kotlin.test.Test
import kotlin.test.assertEquals

class ArvioijahakemusVirheTest {
    @Test
    fun `tavallinen arvo ja viesti talletetaan sellaisenaan`() {
        val virhe = ArvioijahakemuksenVirhe(TUNTEMATON_VAIHTOEHTO, "kielet_7", StringNode("7"))
        assertEquals(virhe, virhe.tallennettava())

        val validointi = ArvioijahakemuksenVirhe(VALIDOINTIVIRHE, "postinumero", viesti = "Postinumero on virheellinen")
        assertEquals(validointi, validointi.tallennettava())
    }

    @Test
    fun `hetua ei talleteta arvona eika viestina`() {
        assertEquals(
            null,
            ArvioijahakemuksenVirhe(VAARA_TIETOTYYPPI, "email", StringNode("hetu 010101-123N")).tallennettava().arvo,
        )
        assertEquals(
            null,
            ArvioijahakemuksenVirhe(VALIDOINTIVIRHE, "x", viesti = "Arvo 010101a123N").tallennettava().viesti,
        )
    }

    @Test
    fun `henkilotunnuskentan, pitkan arvon ja rakennevirheen arvoa ei talleteta`() {
        assertEquals(null, ArvioijahakemuksenVirhe(VAARA_TIETOTYYPPI, "ssn", StringNode("x")).tallennettava().arvo)
        assertEquals(
            null,
            ArvioijahakemuksenVirhe(VAARA_TIETOTYYPPI, "email", StringNode("x".repeat(501))).tallennettava().arvo,
        )
        assertEquals(
            null,
            ArvioijahakemuksenVirhe(VIRHEELLINEN_RAKENNE, "keyValues", StringNode("x")).tallennettava().arvo,
        )
    }
}
