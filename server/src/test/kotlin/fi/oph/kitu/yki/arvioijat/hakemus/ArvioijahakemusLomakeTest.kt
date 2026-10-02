package fi.oph.kitu.yki.arvioijat.hakemus

import fi.oph.kitu.ataru.SiirtoHakemus
import fi.oph.kitu.util.defaultObjectMapper
import fi.oph.kitu.yki.Tutkintokieli
import fi.oph.kitu.yki.Tutkintotaso
import fi.oph.kitu.yki.arvioijat.TallennaArvioija
import fi.oph.kitu.yki.arvioijat.hakemus.ArvioijahakemusLomake.Kentat
import kotlin.test.Test
import kotlin.test.assertContains
import kotlin.test.assertEquals

class ArvioijahakemusLomakeTest {
    private fun hakemus(
        keyValues: String,
        personOid: String? = "1.2.246.562.24.59267607404",
        state: String = "active",
    ) = SiirtoHakemus(
        hakemusOid = "1.2.246.562.11.1",
        personOid = personOid,
        state = state,
        keyValues =
            defaultObjectMapper
                .readTree(keyValues)
                .properties()
                .associate { it.key to it.value },
    )

    @Test
    fun `kielet ja tasot kartoittuvat arviointioikeuksiksi`() {
        val tulos =
            ArvioijahakemusLomake
                .kartoita(
                    hakemus(
                        """{"${Kentat.KIELET}":["fin","SWE"],"${Kentat.TASOT}":["PT","kt"],
                            "${Kentat.KATUOSOITE}":"Katu 1","${Kentat.SAHKOPOSTI}":"a@b.fi"}""",
                    ),
                ).getOrNull()!!

        assertEquals(
            listOf(
                TallennaArvioija.Arviointioikeus(Tutkintokieli.FIN, setOf(Tutkintotaso.PT, Tutkintotaso.KT)),
                TallennaArvioija.Arviointioikeus(Tutkintokieli.SWE, setOf(Tutkintotaso.PT, Tutkintotaso.KT)),
            ),
            tulos.arviointioikeudet,
        )
        assertEquals("Katu 1", tulos.katuosoite)
        assertEquals("a@b.fi", tulos.sahkopostiosoite)
        assertEquals(null, tulos.postinumero)
    }

    @Test
    fun `tuntematon tai vanhentunut kieli hylataan`() {
        val syy =
            ArvioijahakemusLomake
                .kartoita(hakemus("""{"${Kentat.KIELET}":["swe10"],"${Kentat.TASOT}":["PT"]}"""))
                .leftOrNull()!!

        assertContains(syy, "swe10")
    }

    @Test
    fun `puuttuvat kielet hylataan`() {
        val syy = ArvioijahakemusLomake.kartoita(hakemus("""{"${Kentat.TASOT}":["PT"]}""")).leftOrNull()

        assertEquals("Hakemuksella ei ole kieliä tai tasoja", syy)
    }

    @Test
    fun `hakemus ilman henkiloa hylataan`() {
        val syy =
            ArvioijahakemusLomake
                .kartoita(hakemus("""{"${Kentat.KIELET}":["fin"],"${Kentat.TASOT}":["PT"]}""", personOid = null))
                .leftOrNull()

        assertEquals("Hakemuksella ei ole henkilöä", syy)
    }

    @Test
    fun `inaktivoitu hakemus ei tayta ehtoja`() {
        assertEquals(false, ArvioijahakemusKriteeri.tayttyy(hakemus("{}", state = "inactivated")))
        assertEquals(true, ArvioijahakemusKriteeri.tayttyy(hakemus("{}")))
    }
}
