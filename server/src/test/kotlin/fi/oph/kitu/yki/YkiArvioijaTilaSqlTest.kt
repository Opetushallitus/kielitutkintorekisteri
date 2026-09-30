package fi.oph.kitu.yki

import fi.oph.kitu.DBContainerConfiguration
import fi.oph.kitu.yki.arvioijat.Rekisterointitila
import fi.oph.kitu.yki.arvioijat.YkiArvioijaParams
import fi.oph.kitu.yki.arvioijat.YkiArvioijaRepository
import fi.oph.kitu.yki.arvioijat.YkiArvioijaTila
import org.junit.jupiter.api.BeforeEach
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import java.time.LocalDate
import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * Laskentasaanto on kirjoitettu kahdesti — Kotliniksi ja SQL:ksi — joten ne on pakotettava
 * pysymaan yhtenevina. Tama on ainoa testi joka vartioi sita.
 */
@SpringBootTest
@Import(DBContainerConfiguration::class)
class YkiArvioijaTilaSqlTest(
    @param:Autowired private val repository: YkiArvioijaRepository,
) {
    private val tanaan = LocalDate.of(2026, 6, 1)

    private val tapaukset =
        listOf(
            Triple(LocalDate.of(2021, 1, 1), tanaan.minusDays(1), null),
            Triple(LocalDate.of(2021, 1, 1), tanaan, null),
            Triple(LocalDate.of(2021, 1, 1), tanaan.plusYears(5), null),
            Triple(tanaan.plusDays(1), tanaan.plusYears(5), null),
            Triple(tanaan, tanaan.plusYears(5), null),
            Triple(LocalDate.of(2021, 1, 1), null, null),
            Triple(null, null, null),
            Triple(LocalDate.of(2021, 1, 1), tanaan.plusYears(5), YkiArvioijaTila.PASSIVOITU),
            Triple(LocalDate.of(2021, 1, 1), tanaan.minusDays(1), YkiArvioijaTila.AKTIIVINEN),
            Triple(tanaan.plusDays(1), tanaan.plusYears(5), YkiArvioijaTila.PASSIVOITU),
        )

    @BeforeEach
    fun seed() {
        repository.deleteAll()
        tapaukset.forEachIndexed { i, (alku, loppu, tallennettu) ->
            repository.tallenna(testiarvioija(i, alku, loppu, tallennettu))
        }
    }

    @Test
    fun `SQL-lauseke ja laske-funktio antavat saman tilan`() {
        val rivit =
            repository
                .findForListView(YkiArvioijaParams(limit = tapaukset.size), tanaan)
                .associateBy { it.sukunimi }

        assertEquals(tapaukset.size, rivit.size, "jokaisen tapauksen on paadyttava listalle")

        tapaukset.forEachIndexed { i, (alku, loppu, tallennettu) ->
            assertEquals(
                Rekisterointitila.laske(tallennettu, alku, loppu, tanaan),
                rivit.getValue(sukunimi(i)).tila,
                "tapaus $i: alku=$alku loppu=$loppu tallennettu=$tallennettu",
            )
        }
    }

    @Test
    fun `suodatin kohdistuu laskettuun tilaan eika tallennettuun sarakkeeseen`() {
        Rekisterointitila.entries.forEach { tila ->
            val odotetut =
                tapaukset
                    .mapIndexedNotNull { i, (alku, loppu, tallennettu) ->
                        sukunimi(i).takeIf { Rekisterointitila.laske(tallennettu, alku, loppu, tanaan) == tila }
                    }.sorted()

            assertEquals(
                odotetut,
                repository
                    .findForListView(YkiArvioijaParams(tila = tila, limit = tapaukset.size), tanaan)
                    .map { it.sukunimi }
                    .sorted(),
                "suodatin $tila",
            )
            assertEquals(
                odotetut.size,
                repository.countForListView(YkiArvioijaParams(tila = tila), tanaan),
                "laskuri $tila",
            )
        }
    }

    private fun sukunimi(i: Int) = "Tapaus-$i"

    private fun testiarvioija(
        i: Int,
        alku: LocalDate?,
        loppu: LocalDate?,
        tallennettu: YkiArvioijaTila?,
    ) = arvioija(
        oid = arvioijaOid(i),
        sukunimi = sukunimi(i),
        sahkopostiosoite = null,
        oikeudet =
            listOf(
                arviointioikeus(
                    tasot = setOf(Tutkintotaso.PT),
                    tila = tallennettu,
                    kaudenAlkupaiva = alku,
                    kaudenPaattymispaiva = loppu,
                ),
            ),
    )
}
