package fi.oph.kitu.yki.suoritukset

import fi.oph.kitu.yki.TutkinnonOsa
import fi.oph.kitu.yki.Tutkintokieli
import fi.oph.kitu.yki.Tutkintotaso
import org.junit.jupiter.api.Test
import java.time.LocalDate
import kotlin.test.assertEquals
import fi.oph.kitu.yki.suoritukset.YkiTarkistusarviointiTilastoKohde as Kohde

class YkiTarkistusarviointiTilastotTest {
    private val eiRyhmaa = YkiTarkistusarviointiTilastoRyhma()

    private fun pohja(
        tarkistetut: Set<TutkinnonOsa>,
        muuttuneet: Set<TutkinnonOsa> = emptySet(),
        lukumaara: Long = 1,
        ryhma: YkiTarkistusarviointiTilastoRyhma = eiRyhmaa,
    ) = YkiTarkistusarviointiTilastoPohja(ryhma, tarkistetut, muuttuneet, lukumaara)

    private fun List<YkiTarkistusarviointiTilastoRivi>.luvut(kohde: Kohde) =
        single { it.kohde == kohde }.let { Triple(it.yhteensa, it.muuttui, it.eiMuuttunut) }

    private fun osa(osa: TutkinnonOsa) = Kohde.Osakoe(osa)

    @Test
    fun `sekamuotoinen tarkistusarviointi lasketaan kerran ylarivilla ja molempiin sarakkeisiin osakokeittain`() {
        val rivit =
            koostaTarkistusarviointiTilastot(
                listOf(
                    pohja(
                        tarkistetut = setOf(TutkinnonOsa.PU, TutkinnonOsa.KI),
                        muuttuneet = setOf(TutkinnonOsa.PU),
                    ),
                ),
            )

        assertEquals(Triple(1L, 1L, 0L), rivit.luvut(Kohde.Tarkistusarvioinnit))
        assertEquals(Triple(2L, 1L, 1L), rivit.luvut(Kohde.OsakokeetYhteensa))
        assertEquals(Triple(1L, 1L, 0L), rivit.luvut(osa(TutkinnonOsa.PU)))
        assertEquals(Triple(1L, 0L, 1L), rivit.luvut(osa(TutkinnonOsa.KI)))
    }

    @Test
    fun `tarkistusarviointi ilman muutoksia lasketaan muuttumattomaksi`() {
        val rivit =
            koostaTarkistusarviointiTilastot(
                listOf(pohja(setOf(TutkinnonOsa.PU, TutkinnonOsa.TY), lukumaara = 3)),
            )

        assertEquals(Triple(3L, 0L, 3L), rivit.luvut(Kohde.Tarkistusarvioinnit))
        assertEquals(Triple(6L, 0L, 6L), rivit.luvut(Kohde.OsakokeetYhteensa))
    }

    @Test
    fun `saman osakoejoukon eri jarjestykset ja lukumaarat summataan`() {
        val rivit =
            koostaTarkistusarviointiTilastot(
                listOf(
                    pohja(linkedSetOf(TutkinnonOsa.PU, TutkinnonOsa.KI), linkedSetOf(TutkinnonOsa.KI), lukumaara = 2),
                    pohja(linkedSetOf(TutkinnonOsa.KI, TutkinnonOsa.PU), linkedSetOf(TutkinnonOsa.KI), lukumaara = 3),
                ),
            )

        assertEquals(Triple(5L, 5L, 0L), rivit.luvut(Kohde.Tarkistusarvioinnit))
        assertEquals(Triple(10L, 5L, 5L), rivit.luvut(Kohde.OsakokeetYhteensa))
    }

    @Test
    fun `osakoeliitokseton rivi nakyy omana rivinaan eika sita lasketa muuttumattomaksi`() {
        val rivit =
            koostaTarkistusarviointiTilastot(
                listOf(
                    pohja(setOf(TutkinnonOsa.PU)),
                    pohja(tarkistetut = emptySet(), lukumaara = 2),
                ),
            )

        assertEquals(Triple(3L, 0L, 1L), rivit.luvut(Kohde.Tarkistusarvioinnit))
        assertEquals(Triple(2L, null, null), rivit.luvut(Kohde.IlmanOsakoetietoa))
        assertEquals(Triple(1L, 0L, 1L), rivit.luvut(Kohde.OsakokeetYhteensa))
    }

    @Test
    fun `osakoetiedottomien rivia ei nayteta kun niita ei ole`() {
        val rivit = koostaTarkistusarviointiTilastot(listOf(pohja(setOf(TutkinnonOsa.PU))))

        assertEquals(0, rivit.count { it.kohde == Kohde.IlmanOsakoetietoa })
    }

    @Test
    fun `neljan osakokeen rivit naytetaan aina ja rakenteet seka yleisarvosana vain kun niita on`() {
        val ilman = koostaTarkistusarviointiTilastot(listOf(pohja(setOf(TutkinnonOsa.PU))))
        assertEquals(
            listOf(
                Kohde.Tarkistusarvioinnit,
                Kohde.OsakokeetYhteensa,
                osa(TutkinnonOsa.PU),
                osa(TutkinnonOsa.KI),
                osa(TutkinnonOsa.TY),
                osa(TutkinnonOsa.PY),
            ),
            ilman.map { it.kohde },
        )

        val kanssa = koostaTarkistusarviointiTilastot(listOf(pohja(setOf(TutkinnonOsa.RS), setOf(TutkinnonOsa.RS))))
        assertEquals(Triple(1L, 1L, 0L), kanssa.luvut(osa(TutkinnonOsa.RS)))
        assertEquals(Triple(1L, 1L, 0L), kanssa.luvut(Kohde.OsakokeetYhteensa))
        assertEquals(0, kanssa.count { it.kohde == osa(TutkinnonOsa.YL) })
    }

    @Test
    fun `ryhmat toistavat lohkon ja uusin tutkinto tulee ensin`() {
        val vanha = YkiTarkistusarviointiTilastoRyhma(tutkintovuosi = 2023)
        val uusi = YkiTarkistusarviointiTilastoRyhma(tutkintovuosi = 2025)

        val rivit =
            koostaTarkistusarviointiTilastot(
                listOf(pohja(setOf(TutkinnonOsa.PU), ryhma = vanha), pohja(setOf(TutkinnonOsa.KI), ryhma = uusi)),
            )

        assertEquals(listOf(uusi, vanha), rivit.map { it.ryhma }.distinct())
        assertEquals(6, rivit.count { it.ryhma == uusi })
        assertEquals(6, rivit.count { it.ryhma == vanha })
    }

    @Test
    fun `saman paivan ryhmat jarjestetaan kielen nimen ja tason mukaan`() {
        val paiva = LocalDate.of(2025, 1, 1)

        fun ryhma(
            kieli: Tutkintokieli,
            taso: Tutkintotaso,
        ) = YkiTarkistusarviointiTilastoRyhma(tutkintopaiva = paiva, tutkintokieli = kieli, tutkintotaso = taso)

        val rivit =
            koostaTarkistusarviointiTilastot(
                listOf(
                    pohja(setOf(TutkinnonOsa.PU), ryhma = ryhma(Tutkintokieli.SWE, Tutkintotaso.PT)),
                    pohja(setOf(TutkinnonOsa.PU), ryhma = ryhma(Tutkintokieli.FIN, Tutkintotaso.YT)),
                    pohja(setOf(TutkinnonOsa.PU), ryhma = ryhma(Tutkintokieli.FIN, Tutkintotaso.PT)),
                ),
            )

        assertEquals(
            listOf(
                ryhma(Tutkintokieli.SWE, Tutkintotaso.PT),
                ryhma(Tutkintokieli.FIN, Tutkintotaso.PT),
                ryhma(Tutkintokieli.FIN, Tutkintotaso.YT),
            ),
            rivit.map { it.ryhma }.distinct(),
        )
    }

    @Test
    fun `ei pohjia tarkoittaa ei riveja`() {
        assertEquals(emptyList(), koostaTarkistusarviointiTilastot(emptyList()))
    }

    @Test
    fun `tiedostonimi kertoo tilan ryhmittelyn ja rajauksen eika henkilotiedoista`() {
        val nimi =
            YkiTarkistusarviointiTilastoValinnat(
                tila = listOf(Tarkistusarvioinnintila.Hyvaksytty),
                aikaryhmittely = YkiSuoritusAikaryhmittely.Tutkintovuosi,
                ryhmittely = listOf(YkiTarkistusarviointiTilastoColumn.Tutkintokieli),
            ).csvFileName(YkiTarkistusarvioinnitParams(tutkintoalku = LocalDate.of(2025, 1, 1)))

        assertEquals(
            "yki_tarkistusarviointitilastot_hyvaksytty_ryhmittely_tutkintovuosi-tutkintokieli_2025-01-01.csv",
            nimi,
        )
    }

    @Test
    fun `tyhja tilavalinta tarkoittaa molempia tiloja`() {
        assertEquals(
            Tarkistusarvioinnintila.entries.map { it.arviointitila },
            YkiTarkistusarviointiTilastoValinnat().tilat(),
        )
    }
}
