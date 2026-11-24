package fi.oph.kitu.yki.arvioijat

import fi.oph.kitu.yki.Tutkintokieli
import fi.oph.kitu.yki.Tutkintotaso
import fi.oph.kitu.yki.arvioijat.Kausiprojektio
import fi.oph.kitu.yki.arvioijat.YkiArvioijaTila
import fi.oph.kitu.yki.arvioijat.YkiArviointikausiEntity
import fi.oph.kitu.yki.arvioijat.YkiArviointikausiOikeusEntity
import fi.oph.kitu.yki.arvioijat.YkiArviointioikeusEntity
import java.time.LocalDate
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class KausiprojektioTest {
    private val tanaan = LocalDate.of(2026, 6, 1)

    private fun kausi(
        id: Int,
        alku: LocalDate,
        loppu: LocalDate?,
        vararg kielet: Tutkintokieli,
    ) = YkiArviointikausiEntity(
        id = id,
        arvioijaId = 1,
        alkupaiva = alku,
        paattymispaiva = loppu,
        oikeudet =
            kielet.mapIndexed { i, kieli ->
                YkiArviointikausiOikeusEntity(i, id, kieli, setOf(Tutkintotaso.PT))
            },
    )

    private fun projisoi(
        kaudet: List<YkiArviointikausiEntity>,
        ensimmainen: LocalDate? = null,
        nykyiset: List<YkiArviointioikeusEntity> = emptyList(),
    ) = Kausiprojektio.projisoi(kaudet, ensimmainen, nykyiset, tanaan)

    @Test
    fun `voimassa oleva kausi voittaa paattyneen`() {
        val voimassa = kausi(2, LocalDate.of(2025, 1, 1), LocalDate.of(2030, 1, 1), Tutkintokieli.FIN)
        val paattynyt = kausi(1, LocalDate.of(2020, 1, 1), LocalDate.of(2025, 1, 1), Tutkintokieli.FIN)

        assertEquals(voimassa, Kausiprojektio.projisoitava(listOf(paattynyt, voimassa), tanaan))
    }

    @Test
    fun `passivoinnissa katkaistua tulevaa kautta ei projisoida`() {
        val passivointipaiva = tanaan.minusDays(1)
        val katkaistu = kausi(1, LocalDate.of(2025, 1, 1), passivointipaiva, Tutkintokieli.FIN)
        val tulevaKatkaistu = kausi(2, LocalDate.of(2026, 12, 1), passivointipaiva, Tutkintokieli.FIN)

        assertEquals(katkaistu, Kausiprojektio.projisoitava(listOf(katkaistu, tulevaKatkaistu), tanaan))
    }

    @Test
    fun `tuleva kausi voittaa paattyneen`() {
        val tuleva = kausi(2, LocalDate.of(2027, 1, 1), LocalDate.of(2032, 1, 1), Tutkintokieli.FIN)
        val paattynyt = kausi(1, LocalDate.of(2015, 1, 1), LocalDate.of(2020, 1, 1), Tutkintokieli.FIN)

        assertEquals(
            tuleva,
            Kausiprojektio.projisoitava(listOf(paattynyt, tuleva), tanaan),
            "Merkinta jolla on tuleva kausi ei ole passivoitu vaan alkaa myohemmin",
        )
    }

    @Test
    fun `voimassa oleva kausi voittaa etukateen kirjatun jatkokauden`() {
        val voimassa = kausi(1, LocalDate.of(2025, 1, 1), LocalDate.of(2030, 1, 1), Tutkintokieli.FIN)
        val jatko = kausi(2, LocalDate.of(2030, 1, 1), LocalDate.of(2035, 1, 1), Tutkintokieli.FIN)

        assertEquals(
            voimassa,
            Kausiprojektio.projisoitava(listOf(voimassa, jatko), tanaan),
            "Etukateen kirjattu jatkokausi ei saa nayttaa arvioijaa passiivisena kesken kauden",
        )
    }

    @Test
    fun `paattyneista valitaan viimeisin`() {
        val vanha = kausi(1, LocalDate.of(2010, 1, 1), LocalDate.of(2015, 1, 1), Tutkintokieli.FIN)
        val uudempi = kausi(2, LocalDate.of(2015, 1, 1), LocalDate.of(2020, 1, 1), Tutkintokieli.FIN)

        assertEquals(uudempi, Kausiprojektio.projisoitava(listOf(vanha, uudempi), tanaan))
    }

    @Test
    fun `jatkokausi johdetaan ensimmaisesta rekisterointipaivasta`() {
        val kaudet =
            listOf(
                kausi(1, LocalDate.of(2020, 1, 1), LocalDate.of(2025, 1, 1), Tutkintokieli.FIN),
                kausi(2, LocalDate.of(2025, 1, 1), LocalDate.of(2030, 1, 1), Tutkintokieli.FIN),
            )

        val rivi = projisoi(kaudet).single()

        assertTrue(rivi.jatkorekisterointi, "Toinen kausi on jatkorekisterointi")
        assertEquals(LocalDate.of(2020, 1, 1), rivi.ensimmainenRekisterointipaiva)
    }

    @Test
    fun `tuotu ensimmainen rekisterointipaiva sailyy vaikka se edeltaa kausia`() {
        val kaudet = listOf(kausi(1, LocalDate.of(2020, 1, 1), LocalDate.of(2025, 1, 1), Tutkintokieli.FIN))

        val rivi = projisoi(kaudet, ensimmainen = LocalDate.of(2004, 5, 6)).single()

        assertEquals(
            LocalDate.of(2004, 5, 6),
            rivi.ensimmainenRekisterointipaiva,
            "Solki-tuonnin paiva on vanhempi kuin yksikaan rekonstruoitava kausi",
        )
    }

    @Test
    fun `kaikki kauden kielet saavat saman kauden`() {
        val kaudet =
            listOf(
                kausi(
                    1,
                    LocalDate.of(2025, 1, 1),
                    LocalDate.of(2030, 1, 1),
                    Tutkintokieli.FIN,
                    Tutkintokieli.SWE,
                ),
            )

        val paivat = projisoi(kaudet).map { it.kaudenAlkupaiva to it.kaudenPaattymispaiva }.distinct()

        assertEquals(1, paivat.size, "Kausi on arvioijakohtainen, joten kielet eivat voi erota")
    }

    @Test
    fun `eri kielilla voi olla samaan aikaan eri kausi`() {
        val suomi = kausi(1, LocalDate.of(2025, 1, 1), LocalDate.of(2029, 12, 31), Tutkintokieli.FIN)
        val ruotsi = kausi(2, LocalDate.of(2026, 3, 1), LocalDate.of(2031, 2, 28), Tutkintokieli.SWE)

        val projektio = projisoi(listOf(suomi, ruotsi)).associateBy { it.kieli }

        assertEquals(setOf(Tutkintokieli.FIN, Tutkintokieli.SWE), projektio.keys)
        assertEquals(LocalDate.of(2025, 1, 1), projektio.getValue(Tutkintokieli.FIN).kaudenAlkupaiva)
        assertEquals(LocalDate.of(2029, 12, 31), projektio.getValue(Tutkintokieli.FIN).kaudenPaattymispaiva)
        assertEquals(LocalDate.of(2026, 3, 1), projektio.getValue(Tutkintokieli.SWE).kaudenAlkupaiva)
        assertEquals(LocalDate.of(2031, 2, 28), projektio.getValue(Tutkintokieli.SWE).kaudenPaattymispaiva)
    }

    @Test
    fun `jatkokaudesta pois jaanyt kieli sailyy paattyneen kautensa paivilla`() {
        val paattynyt =
            kausi(1, LocalDate.of(2020, 1, 1), LocalDate.of(2024, 12, 31), Tutkintokieli.FIN, Tutkintokieli.ENG)
        val jatko = kausi(2, LocalDate.of(2025, 1, 1), LocalDate.of(2029, 12, 31), Tutkintokieli.FIN)

        val projektio = projisoi(listOf(paattynyt, jatko)).associateBy { it.kieli }

        assertEquals(LocalDate.of(2025, 1, 1), projektio.getValue(Tutkintokieli.FIN).kaudenAlkupaiva)
        assertEquals(LocalDate.of(2020, 1, 1), projektio.getValue(Tutkintokieli.ENG).kaudenAlkupaiva)
        assertEquals(LocalDate.of(2024, 12, 31), projektio.getValue(Tutkintokieli.ENG).kaudenPaattymispaiva)
    }

    @Test
    fun `projektio ei muutu kun tallennettu tila poikkeaa`() {
        val kaudet = listOf(kausi(1, LocalDate.of(2025, 1, 1), LocalDate.of(2030, 1, 1), Tutkintokieli.FIN))
        val tavoite = projisoi(kaudet)
        val nykyiset = tavoite.map { it.copy(tila = YkiArvioijaTila.PASSIVOITU) }

        assertFalse(
            Kausiprojektio.onMuuttunut(nykyiset, tavoite),
            "Solkin kirjaamaa passivointia ei saa pyyhkia pelkalla uudelleenlaskennalla",
        )
    }

    @Test
    fun `vanhentunut kieli sailytetaan projektion ulkopuolella`() {
        val kaudet = listOf(kausi(1, LocalDate.of(2025, 1, 1), LocalDate.of(2030, 1, 1), Tutkintokieli.FIN))
        val tavoite = projisoi(kaudet)
        val legacy =
            tavoite.single().copy(kieli = Tutkintokieli.SWE10, kaudenAlkupaiva = LocalDate.of(2001, 1, 1))

        assertEquals(listOf(legacy), Kausiprojektio.sailytettavat(listOf(legacy) + tavoite, tavoite))
        assertFalse(Kausiprojektio.onMuuttunut(listOf(legacy) + tavoite, tavoite))
    }
}
