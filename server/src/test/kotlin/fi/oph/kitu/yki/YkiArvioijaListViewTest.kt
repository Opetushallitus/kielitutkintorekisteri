package fi.oph.kitu.yki

import fi.oph.kitu.DBContainerConfiguration
import fi.oph.kitu.html.table.ColumnTag
import fi.oph.kitu.html.table.RenderableDisplayTableEnum
import fi.oph.kitu.jdbc.SortDirection
import fi.oph.kitu.oid.Oid
import fi.oph.kitu.util.result.getOrThrow
import fi.oph.kitu.yki.arvioijat.Rekisterointitila
import fi.oph.kitu.yki.arvioijat.YkiArvioijaColumn
import fi.oph.kitu.yki.arvioijat.YkiArvioijaListRow
import fi.oph.kitu.yki.arvioijat.YkiArvioijaParams
import fi.oph.kitu.yki.arvioijat.YkiArvioijaRepository
import fi.oph.kitu.yki.arvioijat.YkiArvioijaTila
import org.junit.jupiter.api.BeforeEach
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.testcontainers.postgresql.PostgreSQLContainer
import java.time.LocalDate
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

@SpringBootTest
@Import(DBContainerConfiguration::class)
class YkiArvioijaListViewTest(
    @param:Autowired private val postgres: PostgreSQLContainer,
    @param:Autowired private val repository: YkiArvioijaRepository,
) {
    /** Kiinteä tarkasteluhetki, jotta laskettu tila ei riipu ajohetkestä. */
    private val tanaan = LocalDate.of(2025, 6, 1)

    @BeforeEach
    fun nukeDb() {
        repository.deleteAll()
        listOf(
            testiarvioija("1.2.246.562.24.20281155246", "Öhman-Testi", Tutkintokieli.FIN),
            testiarvioija("1.2.246.562.24.59267607404", "Andersson-Testi", Tutkintokieli.SWE),
            // Kausi on päättynyt ennen tarkasteluhetkeä, joten tila lasketaan passivoiduksi.
            testiarvioija(
                "1.2.246.562.24.74064782358",
                "Kivinen-Testi",
                Tutkintokieli.ENG,
                kaudenPaattymispaiva = LocalDate.of(2024, 1, 1),
            ),
        ).forEach { repository.tallenna(it) }
    }

    private fun testiarvioija(
        oid: String,
        sukunimi: String,
        kieli: Tutkintokieli,
        kaudenAlkupaiva: LocalDate = OLETUSKAUDEN_ALKU,
        kaudenPaattymispaiva: LocalDate = OLETUSKAUDEN_LOPPU,
        tila: YkiArvioijaTila? = null,
    ) = arvioija(
        oid = Oid.parse(oid).getOrThrow(),
        sukunimi = sukunimi,
        sahkopostiosoite = "$sukunimi@testi.fi".lowercase(),
        oikeudet =
            listOf(
                arviointioikeus(
                    kieli = kieli,
                    tila = tila,
                    kaudenAlkupaiva = kaudenAlkupaiva,
                    kaudenPaattymispaiva = kaudenPaattymispaiva,
                    ensimmainenRekisterointipaiva = OLETUSKAUDEN_ALKU,
                ),
            ),
    )

    @Test
    fun `listanäkymä palauttaa kaikki rivit oletusjärjestyksessä`() {
        val rows = repository.findForListView(YkiArvioijaParams(), tanaan)

        assertEquals(3, repository.countForListView(YkiArvioijaParams(), tanaan))
        assertEquals(
            listOf("Andersson-Testi", "Kivinen-Testi", "Öhman-Testi"),
            rows.map { it.sukunimi },
        )
    }

    @Test
    fun `hakusana suodattaa nimen ja oppijanumeron perusteella`() {
        assertEquals(1, repository.findForListView(YkiArvioijaParams(search = "Kivinen"), tanaan).size)
        assertEquals(1, repository.findForListView(YkiArvioijaParams(search = "59267607404"), tanaan).size)
        assertEquals(0, repository.findForListView(YkiArvioijaParams(search = "ei-lC6ydy"), tanaan).size)
    }

    @Test
    fun `monisanainen haku loytaa henkilon vaikka termit ovat eri sarakkeissa`() {
        // Sukunimi ja etunimi ovat eri sarakkeissa, joten yksi ILIKE ei riittaisi.
        assertEquals(
            listOf("Kivinen-Testi"),
            repository.findForListView(YkiArvioijaParams(search = "Kivinen Testi"), tanaan).map { it.sukunimi },
        )
        // Jarjestyksella ei ole valia.
        assertEquals(
            listOf("Kivinen-Testi"),
            repository.findForListView(YkiArvioijaParams(search = "Testi Kivinen"), tanaan).map { it.sukunimi },
        )
        // Kaikkien termien on osuttava.
        assertEquals(0, repository.findForListView(YkiArvioijaParams(search = "Kivinen Andersson"), tanaan).size)
        // Ylimaarainen valilyonti ei riko hakua.
        assertEquals(1, repository.findForListView(YkiArvioijaParams(search = "  Kivinen   "), tanaan).size)
    }

    @Test
    fun `haku ei ole kirjainkokoriippuvainen`() {
        assertEquals(1, repository.findForListView(YkiArvioijaParams(search = "kivinen"), tanaan).size)
        assertEquals(1, repository.findForListView(YkiArvioijaParams(search = "KIVINEN"), tanaan).size)
    }

    @Test
    fun `tila-, kieli- ja tasosuodattimet rajaavat tuloksia`() {
        assertEquals(
            listOf("Kivinen-Testi"),
            repository
                .findForListView(YkiArvioijaParams(tila = Rekisterointitila.PASSIVOITU), tanaan)
                .map { it.sukunimi },
        )
        assertEquals(
            listOf("Öhman-Testi"),
            repository.findForListView(YkiArvioijaParams(kieli = Tutkintokieli.FIN), tanaan).map { it.sukunimi },
        )
        assertEquals(3, repository.findForListView(YkiArvioijaParams(taso = Tutkintotaso.PT), tanaan).size)
        assertEquals(0, repository.findForListView(YkiArvioijaParams(taso = Tutkintotaso.YT), tanaan).size)
    }

    @Test
    fun `kausiPaattyyEnnen rajaa päättyvät kaudet`() {
        assertEquals(
            3,
            repository.findForListView(YkiArvioijaParams(kausiPaattyyEnnen = LocalDate.of(2027, 1, 1)), tanaan).size,
        )
        assertEquals(
            0,
            repository.findForListView(YkiArvioijaParams(kausiPaattyyEnnen = LocalDate.of(2020, 1, 1)), tanaan).size,
        )
    }

    @Test
    fun `vainSolkiVirheet rajaa riveihin joilla on lähetysvirhe`() {
        assertEquals(0, repository.findForListView(YkiArvioijaParams(vainSolkiVirheet = true), tanaan).size)
    }

    @Test
    fun `sivutus ja laskeva järjestys toimivat`() {
        val ekaSivu =
            repository.findForListView(
                YkiArvioijaParams(sortDirection = SortDirection.DESC, page = 1, limit = 2),
                tanaan,
            )
        assertEquals(listOf("Öhman-Testi", "Kivinen-Testi"), ekaSivu.map { it.sukunimi })

        val tokaSivu =
            repository.findForListView(
                YkiArvioijaParams(sortDirection = SortDirection.DESC, page = 2, limit = 2),
                tanaan,
            )
        assertEquals(listOf("Andersson-Testi"), tokaSivu.map { it.sukunimi })
    }

    @Test
    fun `CSV-vienti sisältää enemmän sarakkeita kuin listanäkymä ja henkilötiedot voi piilottaa`() {
        val listView =
            RenderableDisplayTableEnum.getByTags<YkiArvioijaColumn, YkiArvioijaListRow>(setOf(ColumnTag.LIST_VIEW))
        val csv =
            RenderableDisplayTableEnum.getByTags<YkiArvioijaColumn, YkiArvioijaListRow>(setOf(ColumnTag.CSV_EXPORT))
        val csvIlmanHenkilotietoja =
            RenderableDisplayTableEnum.getByTags<YkiArvioijaColumn, YkiArvioijaListRow>(
                setOf(ColumnTag.CSV_EXPORT),
                setOf(ColumnTag.PERSONAL_DATA),
            )

        assertTrue(csv.size > listView.size, "CSV-vienti sisaltaa myos vain-CSV-sarakkeet")
        assertTrue(csv.any { it == YkiArvioijaColumn.AshaNumero })
        assertTrue(listView.none { it == YkiArvioijaColumn.AshaNumero })
        assertTrue(csvIlmanHenkilotietoja.none { it == YkiArvioijaColumn.Oppijanumero })
    }

    @Test
    fun `linkkisarake nakyy listalla myos ilman henkilotietoja mutta ei CSV-viennissa`() {
        val listView =
            RenderableDisplayTableEnum.getByTags<YkiArvioijaColumn, YkiArvioijaListRow>(setOf(ColumnTag.LIST_VIEW))
        val listViewIlmanHenkilotietoja =
            RenderableDisplayTableEnum.getByTags<YkiArvioijaColumn, YkiArvioijaListRow>(
                setOf(ColumnTag.LIST_VIEW),
                setOf(ColumnTag.PERSONAL_DATA),
            )
        val csv =
            RenderableDisplayTableEnum.getByTags<YkiArvioijaColumn, YkiArvioijaListRow>(setOf(ColumnTag.CSV_EXPORT))

        assertTrue(listView.any { it == YkiArvioijaColumn.Linkki })
        assertTrue(
            listViewIlmanHenkilotietoja.any { it == YkiArvioijaColumn.Linkki },
            "rivi on avattavissa myos kun henkilotiedot on piilotettu",
        )
        assertTrue(csv.none { it == YkiArvioijaColumn.Linkki }, "linkki ei kuulu CSV-vientiin")
    }
}
