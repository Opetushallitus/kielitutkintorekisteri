package fi.oph.kitu.yki.arvioijat.hakemus

import fi.oph.kitu.ataru.SiirtoHakemus
import fi.oph.kitu.util.defaultObjectMapper
import fi.oph.kitu.yki.Tutkintokieli
import fi.oph.kitu.yki.Tutkintotaso.KT
import fi.oph.kitu.yki.Tutkintotaso.PT
import fi.oph.kitu.yki.Tutkintotaso.YT
import fi.oph.kitu.yki.arvioijat.TallennaArvioija
import fi.oph.kitu.yki.arvioijat.hakemus.ArvioijahakemusLomake.Kielivaihtoehto
import org.springframework.boot.context.properties.bind.Binder
import org.springframework.boot.context.properties.source.ConfigurationPropertySource
import org.springframework.boot.env.YamlPropertySourceLoader
import org.springframework.core.io.ByteArrayResource
import org.springframework.core.io.ClassPathResource
import tools.jackson.databind.JsonNode
import kotlin.test.Test
import kotlin.test.assertContains
import kotlin.test.assertEquals

class ArvioijahakemusLomakeTest {
    private val kielet = "a886d639-ef13-4442-9b0f-f0818c849450"
    private val suomenTasot = "aa5ef81b-181f-4d7a-9e55-4b54338b6903"
    private val englanninTasot = "19cde2b6-66c8-4c2e-aaff-013114945cda"

    private val lomake =
        ArvioijahakemusLomake(
            lomakkeenAvain = "lomake",
            kielikysymys = kielet,
            kielet =
                mapOf(
                    "0" to
                        Kielivaihtoehto(Tutkintokieli.FIN, suomenTasot, mapOf("0" to setOf(PT, KT), "1" to setOf(YT))),
                    "4" to
                        Kielivaihtoehto(
                            Tutkintokieli.ENG,
                            englanninTasot,
                            mapOf("1" to setOf(PT, KT), "0" to setOf(YT)),
                        ),
                ),
        )

    private fun hakemus(vararg vastaukset: Pair<String, String>) =
        SiirtoHakemus(
            hakemusOid = "1.2.246.562.11.1",
            personOid = "1.2.246.562.24.59267607404",
            keyValues =
                vastaukset.associate { (avain, arvo) ->
                    avain to defaultObjectMapper.valueToTree<JsonNode>(arvo)
                },
        )

    private fun binder(yaml: String) =
        Binder(
            YamlPropertySourceLoader()
                .load("lomake", ByteArrayResource(yaml.toByteArray()))
                .map { ConfigurationPropertySource.from(it)!! },
        )

    @Test
    fun `jokainen kieli saa omat tasonsa`() {
        val tulos =
            lomake
                .kartoita(
                    hakemus(
                        "${kielet}_4" to "4",
                        "${englanninTasot}_0" to "0",
                        "${kielet}_0" to "0",
                        "${suomenTasot}_0" to "0",
                        "${suomenTasot}_1" to "1",
                        "address" to "Katu 1",
                        "email" to "a@b.fi",
                    ),
                ).getOrNull()!!

        assertEquals(
            listOf(
                TallennaArvioija.Arviointioikeus(Tutkintokieli.ENG, setOf(YT)),
                TallennaArvioija.Arviointioikeus(Tutkintokieli.FIN, setOf(PT, KT, YT)),
            ),
            tulos.arviointioikeudet,
        )
        assertEquals("Katu 1", tulos.katuosoite)
        assertEquals("a@b.fi", tulos.sahkopostiosoite)
        assertEquals(null, tulos.postinumero)
    }

    @Test
    fun `asetuksista puuttuva kieli tai taso hylataan`() {
        val tuntematonKieli =
            lomake.kartoita(
                hakemus(
                    "${kielet}_7" to "7",
                    "${kielet}_0" to "0",
                    "${suomenTasot}_0" to "0",
                ),
            )
        assertContains(tuntematonKieli.leftOrNull()!!, "${kielet}_7")

        val tuntematonTaso = lomake.kartoita(hakemus("${kielet}_0" to "0", "${suomenTasot}_2" to "2"))
        assertContains(tuntematonTaso.leftOrNull()!!, "${suomenTasot}_2")

        val kysymysryhma = lomake.kartoita(hakemus("${kielet}_0" to "0", "${suomenTasot}_group0_0" to "0"))
        assertContains(kysymysryhma.leftOrNull()!!, "${suomenTasot}_group0_0")
    }

    @Test
    fun `muiden kysymysten vastaukset ohitetaan`() {
        val tulos =
            lomake.kartoita(
                hakemus(
                    "${kielet}_0" to "0",
                    "${suomenTasot}_1" to "1",
                    "aed87e9e-7cbf-43d1-a96c-97697c06b43d" to "0",
                    "9d16eb91-ed15-4e26-84f0-41d0ff7cbcff_2" to "2",
                ),
            )

        assertEquals(
            listOf(TallennaArvioija.Arviointioikeus(Tutkintokieli.FIN, setOf(YT))),
            tulos.getOrNull()?.arviointioikeudet,
        )
    }

    @Test
    fun `kieli ilman tasoa ja hakemus ilman kielia hylataan`() {
        assertEquals(
            "Kielelle ei ole valittu tasoa: ENG",
            lomake
                .kartoita(
                    hakemus("${kielet}_0" to "0", "${suomenTasot}_0" to "0", "${kielet}_4" to "4"),
                ).leftOrNull(),
        )
        assertEquals("Hakemuksella ei ole kieliä", lomake.kartoita(hakemus("${suomenTasot}_0" to "0")).leftOrNull())
    }

    @Test
    fun `puutteet kertoo puuttuvat asetukset ja vanhentuneet kielet`() {
        assertEquals(emptyList(), lomake.puutteet())
        assertEquals(listOf("lomakkeenAvain", "kielikysymys", "kielet"), ArvioijahakemusLomake().puutteet())

        val puutteellinen =
            ArvioijahakemusLomake(
                lomakkeenAvain = "lomake",
                kielikysymys = kielet,
                kielet =
                    mapOf(
                        "1" to Kielivaihtoehto(),
                        "2" to Kielivaihtoehto(Tutkintokieli.SWE10, "tasot", mapOf("0" to emptySet())),
                    ),
            )
        assertEquals(
            listOf(
                "kielet[1].kieli",
                "kielet[1].tasokysymys",
                "kielet[1].tasot",
                "kielet[2]: vanhentunut kieli SWE10",
                "kielet[2].tasot[0]",
            ),
            puutteellinen.puutteet(),
        )
    }

    @Test
    fun `sisakkainen yaml sitoutuu lomakkeeksi`() {
        val sidottu =
            binder(
                """
                kitu.ataru.arvioijahakemus:
                  lomakkeenAvain: lomake
                  kielikysymys: $kielet
                  kielet:
                    "4": { kieli: eng, tasokysymys: $englanninTasot, tasot: { "1": [PT, KT], "0": [YT] } }
                  kentat:
                    sahkoposti: oma-email
                """.trimIndent(),
            ).bind("kitu.ataru.arvioijahakemus", ArvioijahakemusLomake::class.java).get()

        assertEquals("lomake", sidottu.lomakkeenAvain)
        val englanti = sidottu.kielet.getValue("4")
        assertEquals(Tutkintokieli.ENG, englanti.kieli)
        assertEquals(mapOf("1" to setOf(PT, KT), "0" to setOf(YT)), englanti.tasot)
        assertEquals("oma-email", sidottu.kentat.sahkoposti)
        assertEquals("address", sidottu.kentat.katuosoite)
    }

    @Test
    fun `jokaisen ympariston lomakekartoitus sitoutuu ehjana`() {
        val profiilit =
            YamlPropertySourceLoader()
                .load("arvioijahakemuslomake", ClassPathResource("arvioijahakemuslomake.yml"))
                .mapNotNull { dokumentti ->
                    val profiili = dokumentti.getProperty("spring.config.activate.on-profile")?.toString()
                    profiili?.also {
                        val sidottu =
                            Binder(ConfigurationPropertySource.from(dokumentti)!!)
                                .bind("kitu.ataru.arvioijahakemus", ArvioijahakemusLomake::class.java)
                                .get()
                        assertEquals(emptyList(), sidottu.puutteet(), it)
                    }
                }

        assertEquals(listOf("e2e | local-opintopolku", "qa"), profiilit)
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
