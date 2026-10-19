package fi.oph.kitu.yki.arvioijat.hakemus

import fi.oph.kitu.ataru.SiirtoHakemus
import fi.oph.kitu.util.defaultObjectMapper
import fi.oph.kitu.yki.Tutkintokieli
import fi.oph.kitu.yki.Tutkintotaso
import fi.oph.kitu.yki.arvioijat.TallennaArvioija
import org.springframework.boot.context.properties.bind.Binder
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource
import tools.jackson.databind.JsonNode
import kotlin.test.Test
import kotlin.test.assertContains
import kotlin.test.assertEquals

class ArvioijahakemusLomakeTest {
    private val kielet = "a886d639-ef13-4442-9b0f-f0818c849450"
    private val tasot = "03b86401-c42a-487a-a220-b46b9752e46c"

    private val lomake =
        ArvioijahakemusLomake(
            lomakkeenAvain = "lomake",
            kielet = mapOf("${kielet}_0" to Tutkintokieli.FIN, "${kielet}_1" to Tutkintokieli.SWE),
            tasot = mapOf("${tasot}_0" to Tutkintotaso.PT, "${tasot}_1" to Tutkintotaso.KT),
        )

    private fun hakemus(
        vararg vastaukset: Pair<String, String>,
        personOid: String? = "1.2.246.562.24.59267607404",
        state: String = "active",
    ) = SiirtoHakemus(
        hakemusOid = "1.2.246.562.11.1",
        personOid = personOid,
        state = state,
        keyValues = vastaukset.associate { (avain, arvo) -> avain to defaultObjectMapper.valueToTree<JsonNode>(arvo) },
    )

    @Test
    fun `valitut kielet ja tasot kartoittuvat arviointioikeuksiksi`() {
        val tulos =
            lomake
                .kartoita(
                    hakemus(
                        "${kielet}_1" to "1",
                        "${kielet}_0" to "0",
                        "${tasot}_0" to "0",
                        "${tasot}_1" to "1",
                        "address" to "Katu 1",
                        "email" to "a@b.fi",
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
    fun `asetuksista puuttuva vaihtoehto hylataan`() {
        val syy = lomake.kartoita(hakemus("${kielet}_7" to "7", "${tasot}_0" to "0")).leftOrNull()!!

        assertContains(syy, "${kielet}_7")
    }

    @Test
    fun `kysymysryhman ja muiden kysymysten vastaukset ohitetaan`() {
        val tulos =
            lomake.kartoita(
                hakemus(
                    "${kielet}_0" to "0",
                    "${tasot}_0" to "0",
                    "${kielet}_group0_1" to "1",
                    "aed87e9e-7cbf-43d1-a96c-97697c06b43d" to "0",
                ),
            )

        assertEquals(listOf(Tutkintokieli.FIN), tulos.getOrNull()?.arviointioikeudet?.map { it.kieli })
    }

    @Test
    fun `puuttuvat kielet hylataan`() {
        val syy = lomake.kartoita(hakemus("${tasot}_0" to "0")).leftOrNull()

        assertEquals("Hakemuksella ei ole kieliä tai tasoja", syy)
    }

    @Test
    fun `hakemus ilman henkiloa hylataan`() {
        val syy = lomake.kartoita(hakemus("${kielet}_0" to "0", "${tasot}_0" to "0", personOid = null)).leftOrNull()

        assertEquals("Hakemuksella ei ole henkilöä", syy)
    }

    @Test
    fun `puutteet kertoo puuttuvat asetukset ja vanhentuneet kielet`() {
        assertEquals(emptyList(), lomake.puutteet())
        assertEquals(listOf("lomakkeenAvain", "kielet", "tasot"), ArvioijahakemusLomake().puutteet())

        val vanhentunut =
            ArvioijahakemusLomake(
                lomakkeenAvain = "lomake",
                kielet = mapOf("${kielet}_9" to Tutkintokieli.SWE10),
                tasot = lomake.tasot,
            )
        assertEquals(listOf("kielet[${kielet}_9]: vanhentunut kieli SWE10"), vanhentunut.puutteet())
    }

    @Test
    fun `asetukset sitoutuvat vastausavaimineen`() {
        val sidottu =
            Binder(
                MapConfigurationPropertySource(
                    mapOf(
                        "kitu.ataru.arvioijahakemus.lomakkeenAvain" to "lomake",
                        "kitu.ataru.arvioijahakemus.kielet[${kielet}_0]" to "fin",
                        "kitu.ataru.arvioijahakemus.tasot[${tasot}_0]" to "PT",
                        "kitu.ataru.arvioijahakemus.kentat.sahkoposti" to "oma-email",
                    ),
                ),
            ).bind("kitu.ataru.arvioijahakemus", ArvioijahakemusLomake::class.java).get()

        assertEquals("lomake", sidottu.lomakkeenAvain)
        assertEquals(mapOf("${kielet}_0" to Tutkintokieli.FIN), sidottu.kielet)
        assertEquals(mapOf("${tasot}_0" to Tutkintotaso.PT), sidottu.tasot)
        assertEquals("oma-email", sidottu.kentat.sahkoposti)
        assertEquals("address", sidottu.kentat.katuosoite)
    }

    @Test
    fun `inaktivoitu hakemus ei tayta ehtoja`() {
        assertEquals(false, ArvioijahakemusKriteeri.tayttyy(hakemus(state = "inactivated")))
        assertEquals(true, ArvioijahakemusKriteeri.tayttyy(hakemus()))
    }

    @Test
    fun `atarusta haetaan vain kasitellyt hakukelpoiset passivoimattomat hakemukset`() {
        val tilat = defaultObjectMapper.valueToTree<JsonNode>(ArvioijahakemusKriteeri.atarunTilat)

        assertEquals(listOf("processed"), tilat["processing-states-to-include"].values().map { it.asString() })
        assertEquals(false, tilat["filters"]["active-status"]["passive"].asBoolean())
        assertEquals(true, tilat["filters"]["eligibility-state"]["eligible"].asBoolean())
        assertEquals(false, tilat["filters"]["eligibility-state"]["conditionally-eligible"].asBoolean())
        assertEquals(true, tilat["filters"]["only-ssn"]["without-ssn"].asBoolean(), "perussuodattimet sailyvat")
    }
}
