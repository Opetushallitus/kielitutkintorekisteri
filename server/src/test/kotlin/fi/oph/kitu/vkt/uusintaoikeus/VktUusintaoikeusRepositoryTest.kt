package fi.oph.kitu.vkt.uusintaoikeus

import fi.oph.kitu.DBContainerConfiguration
import fi.oph.kitu.koodisto.Koodisto
import fi.oph.kitu.koodisto.Koodisto.VktArvosana.Hylätty
import fi.oph.kitu.koodisto.Koodisto.VktArvosana.Hyvä
import fi.oph.kitu.koodisto.Koodisto.VktArvosana.Tyydyttävä
import fi.oph.kitu.koodisto.Koodisto.VktOsakoe.Kirjoittaminen
import fi.oph.kitu.koodisto.Koodisto.VktOsakoe.PuheenYmmärtäminen
import fi.oph.kitu.koodisto.Koodisto.VktOsakoe.Puhuminen
import fi.oph.kitu.koodisto.Koodisto.VktOsakoe.TekstinYmmärtäminen
import fi.oph.kitu.oid.Oid
import fi.oph.kitu.util.result.getOrThrow
import fi.oph.kitu.vkt.VktSuoritusEntity
import fi.oph.kitu.vkt.VktSuoritusRepository
import fi.oph.kitu.vkt.uusintaoikeus.VktUusintaoikeusRepository.Companion.UUSINTAOIKEUDEN_VIIMEINEN_PAIVA
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import java.time.LocalDate
import kotlin.test.Test
import kotlin.test.assertEquals

@SpringBootTest
@Import(DBContainerConfiguration::class)
class VktUusintaoikeusRepositoryTest(
    @param:Autowired private val suoritusRepository: VktSuoritusRepository,
    @param:Autowired private val repository: VktUusintaoikeusRepository,
) {
    private var ilmoittautumisia = 0

    @BeforeEach
    @AfterEach
    fun tyhjennaSuoritukset() {
        suoritusRepository.deleteAll()
    }

    @Test
    fun `hyvaksytty ja hylatty saman tutkinnon osakoe antaa oikeuden uusia hylatyn osakokeen`() {
        tallenna(
            osakoe(Kirjoittaminen, LocalDate.of(2025, 3, 1), Hylätty),
            osakoe(TekstinYmmärtäminen, LocalDate.of(2025, 3, 1), Hyvä),
        )

        assertEquals(
            listOf(
                VktUusintaoikeus(
                    suorittajanOid = HENKILO,
                    sukunimi = "Testinen",
                    etunimet = "Testi",
                    tutkintokieli = Koodisto.Tutkintokieli.FIN,
                    taitotaso = Koodisto.VktTaitotaso.HyväJaTyydyttävä,
                    osakoe = Kirjoittaminen,
                    tutkinnot = listOf(Koodisto.VktKielitaito.Kirjallinen),
                    hylkaamispaatos = LocalDate.of(2025, 3, 15),
                    oikeusPaattyy = LocalDate.of(2028, 3, 15),
                ),
            ),
            repository.findUusintaanOikeutetut(TARKASTELUPAIVA),
        )
    }

    @Test
    fun `ehto 1 - kun toinenkin osakoe on hylatty, kumpaakaan ei voi uusia`() {
        tallenna(
            osakoe(Kirjoittaminen, LocalDate.of(2025, 3, 1), Hylätty),
            osakoe(TekstinYmmärtäminen, LocalDate.of(2025, 3, 1), Hylätty),
        )

        assertEquals(emptyList(), repository.findUusintaanOikeutetut(TARKASTELUPAIVA))
    }

    @Test
    fun `ehto 2 - hyvaksyttyjen osakokeiden tulosta ei voi korottaa`() {
        tallenna(
            osakoe(Kirjoittaminen, LocalDate.of(2025, 3, 1), Tyydyttävä),
            osakoe(TekstinYmmärtäminen, LocalDate.of(2025, 3, 1), Hyvä),
        )

        assertEquals(emptyList(), repository.findUusintaanOikeutetut(TARKASTELUPAIVA))
    }

    @Test
    fun `ehto 3 - hyvaksytysti uusittu osakoe ei ole enaa uusittavissa`() {
        tallenna(
            osakoe(Kirjoittaminen, LocalDate.of(2025, 3, 1), Hylätty),
            osakoe(TekstinYmmärtäminen, LocalDate.of(2025, 3, 1), Hyvä),
        )
        tallenna(osakoe(Kirjoittaminen, LocalDate.of(2025, 9, 1), Tyydyttävä))

        assertEquals(emptyList(), repository.findUusintaanOikeutetut(TARKASTELUPAIVA))
    }

    @Test
    fun `ehto 4 - oikeus on voimassa tasan kolme vuotta hylkaamispaatoksesta`() {
        tallenna(
            osakoe(Kirjoittaminen, LocalDate.of(2025, 3, 1), Hylätty, arviointipaiva = LocalDate.of(2025, 3, 15)),
            osakoe(TekstinYmmärtäminen, LocalDate.of(2025, 3, 1), Hyvä),
        )

        assertEquals(1, repository.findUusintaanOikeutetut(LocalDate.of(2028, 3, 15)).size)
        assertEquals(emptyList(), repository.findUusintaanOikeutetut(LocalDate.of(2028, 3, 16)))
    }

    @Test
    fun `ehto 5 - oikeudet paattyvat viimeistaan 31-12-2028`() {
        tallenna(
            osakoe(Kirjoittaminen, LocalDate.of(2026, 6, 1), Hylätty),
            osakoe(TekstinYmmärtäminen, LocalDate.of(2026, 6, 1), Hyvä),
        )

        assertEquals(
            listOf(UUSINTAOIKEUDEN_VIIMEINEN_PAIVA),
            repository.findUusintaanOikeutetut(UUSINTAOIKEUDEN_VIIMEINEN_PAIVA).map { it.oikeusPaattyy },
        )
        assertEquals(
            emptyList(),
            repository.findUusintaanOikeutetut(UUSINTAOIKEUDEN_VIIMEINEN_PAIVA.plusDays(1)),
        )
    }

    @Test
    fun `ehto 6 - eri kielen osakoe ei anna uusintaoikeutta`() {
        tallenna(osakoe(Kirjoittaminen, LocalDate.of(2025, 3, 1), Hylätty), kieli = Koodisto.Tutkintokieli.FIN)
        tallenna(osakoe(TekstinYmmärtäminen, LocalDate.of(2025, 3, 1), Hyvä), kieli = Koodisto.Tutkintokieli.SWE)

        assertEquals(emptyList(), repository.findUusintaanOikeutetut(TARKASTELUPAIVA))
    }

    @Test
    fun `ehto 6 - eri taitotason osakoe ei anna uusintaoikeutta`() {
        tallenna(
            osakoe(Kirjoittaminen, LocalDate.of(2025, 3, 1), Hylätty),
            taso = Koodisto.VktTaitotaso.Erinomainen,
        )
        tallenna(
            osakoe(TekstinYmmärtäminen, LocalDate.of(2025, 3, 1), Hyvä),
            taso = Koodisto.VktTaitotaso.HyväJaTyydyttävä,
        )

        assertEquals(emptyList(), repository.findUusintaanOikeutetut(TARKASTELUPAIVA))
    }

    @Test
    fun `hylatty uusinta ei pidenna oikeutta`() {
        tallenna(
            osakoe(Kirjoittaminen, LocalDate.of(2024, 2, 1), Hylätty, arviointipaiva = LocalDate.of(2024, 2, 15)),
            osakoe(TekstinYmmärtäminen, LocalDate.of(2024, 2, 1), Hyvä),
        )
        tallenna(osakoe(Kirjoittaminen, LocalDate.of(2025, 6, 1), Hylätty, arviointipaiva = LocalDate.of(2025, 6, 15)))

        val oikeudet = repository.findUusintaanOikeutetut(TARKASTELUPAIVA)
        assertEquals(listOf(LocalDate.of(2024, 2, 15)), oikeudet.map { it.hylkaamispaatos })
        assertEquals(listOf(LocalDate.of(2027, 2, 15)), oikeudet.map { it.oikeusPaattyy })
        assertEquals(emptyList(), repository.findUusintaanOikeutetut(LocalDate.of(2027, 2, 16)))
    }

    @Test
    fun `aiempi kokonaan hylatty tutkintokerta ei aikaista hylkaamispaatosta`() {
        tallenna(
            osakoe(Kirjoittaminen, LocalDate.of(2024, 2, 1), Hylätty),
            osakoe(TekstinYmmärtäminen, LocalDate.of(2024, 2, 1), Hylätty),
        )
        tallenna(
            osakoe(Kirjoittaminen, LocalDate.of(2025, 2, 1), Hylätty, arviointipaiva = LocalDate.of(2025, 2, 15)),
            osakoe(TekstinYmmärtäminen, LocalDate.of(2025, 2, 1), Hyvä),
        )

        assertEquals(
            listOf(LocalDate.of(2025, 2, 15)),
            repository.findUusintaanOikeutetut(TARKASTELUPAIVA).map { it.hylkaamispaatos },
        )
    }

    @Test
    fun `yli kolmen vuoden paassa toisistaan suoritetut osakokeet eivat ole samaa tutkintoa`() {
        tallenna(osakoe(TekstinYmmärtäminen, LocalDate.of(2022, 1, 1), Hyvä))
        tallenna(osakoe(Kirjoittaminen, LocalDate.of(2025, 1, 2), Hylätty))

        assertEquals(emptyList(), repository.findUusintaanOikeutetut(TARKASTELUPAIVA))
    }

    @Test
    fun `kahden tutkinnon kautta uusittava osakoe on yksi rivi`() {
        tallenna(
            osakoe(Kirjoittaminen, LocalDate.of(2025, 3, 1), Hyvä),
            osakoe(TekstinYmmärtäminen, LocalDate.of(2025, 3, 1), Hylätty),
            osakoe(Puhuminen, LocalDate.of(2025, 3, 3), Hyvä),
            osakoe(PuheenYmmärtäminen, LocalDate.of(2025, 3, 3), Tyydyttävä),
        )

        val oikeudet = repository.findUusintaanOikeutetut(TARKASTELUPAIVA)
        assertEquals(listOf(TekstinYmmärtäminen), oikeudet.map { it.osakoe })
        assertEquals(
            listOf(listOf(Koodisto.VktKielitaito.Kirjallinen, Koodisto.VktKielitaito.Ymmärtäminen)),
            oikeudet.map { it.tutkinnot },
        )
    }

    @Test
    fun `ilmoittautumisen vanhentunut versio ei vaikuta`() {
        val ilmoittautumisenId = "KIOS:korjattu"
        tallenna(
            osakoe(Kirjoittaminen, LocalDate.of(2025, 3, 1), Hylätty),
            osakoe(TekstinYmmärtäminen, LocalDate.of(2025, 3, 1), Hyvä),
            ilmoittautumisenId = ilmoittautumisenId,
        )
        tallenna(
            osakoe(Kirjoittaminen, LocalDate.of(2025, 3, 1), Tyydyttävä),
            osakoe(TekstinYmmärtäminen, LocalDate.of(2025, 3, 1), Hyvä),
            ilmoittautumisenId = ilmoittautumisenId,
        )

        assertEquals(emptyList(), repository.findUusintaanOikeutetut(TARKASTELUPAIVA))
    }

    @Test
    fun `arvioimaton uusinta ei kumoa oikeutta`() {
        tallenna(
            osakoe(Kirjoittaminen, LocalDate.of(2025, 3, 1), Hylätty),
            osakoe(TekstinYmmärtäminen, LocalDate.of(2025, 3, 1), Hyvä),
        )
        tallenna(osakoe(Kirjoittaminen, LocalDate.of(2026, 10, 1), arvosana = null))

        assertEquals(listOf(Kirjoittaminen), repository.findUusintaanOikeutetut(TARKASTELUPAIVA).map { it.osakoe })
    }

    @Test
    fun `menneena tarkastelupaivana huomioidaan vain siihen mennessa arvioidut osakokeet`() {
        tallenna(
            osakoe(Kirjoittaminen, LocalDate.of(2025, 3, 1), Hylätty, arviointipaiva = LocalDate.of(2025, 3, 15)),
            osakoe(TekstinYmmärtäminen, LocalDate.of(2025, 3, 1), Hyvä, arviointipaiva = LocalDate.of(2025, 3, 15)),
        )
        tallenna(
            osakoe(Kirjoittaminen, LocalDate.of(2025, 9, 1), Tyydyttävä, arviointipaiva = LocalDate.of(2025, 9, 15)),
        )

        assertEquals(emptyList(), repository.findUusintaanOikeutetut(LocalDate.of(2025, 3, 14)))
        assertEquals(
            listOf(Kirjoittaminen),
            repository.findUusintaanOikeutetut(LocalDate.of(2025, 3, 15)).map { it.osakoe },
        )
        assertEquals(
            listOf(Kirjoittaminen),
            repository.findUusintaanOikeutetut(LocalDate.of(2025, 9, 14)).map { it.osakoe },
        )
        assertEquals(emptyList(), repository.findUusintaanOikeutetut(LocalDate.of(2025, 9, 15)))
    }

    private fun tallenna(
        vararg osakokeet: VktSuoritusEntity.VktOsakoe,
        kieli: Koodisto.Tutkintokieli = Koodisto.Tutkintokieli.FIN,
        taso: Koodisto.VktTaitotaso = Koodisto.VktTaitotaso.HyväJaTyydyttävä,
        ilmoittautumisenId: String = "KIOS:${++ilmoittautumisia}",
    ) {
        suoritusRepository.save(
            VktSuoritusEntity(
                ilmoittautumisenId = ilmoittautumisenId,
                suorittajanOid = Oid.parse(HENKILO).getOrThrow(),
                etunimet = "Testi",
                sukunimi = "Testinen",
                tutkintokieli = kieli,
                suorituspaikkakunta = "091",
                taitotaso = taso,
                suorituksenVastaanottaja = null,
                osakokeet = osakokeet.toSet(),
                tutkinnot = emptySet(),
            ),
        )
    }

    private fun osakoe(
        tyyppi: Koodisto.VktOsakoe,
        tutkintopaiva: LocalDate,
        arvosana: Koodisto.VktArvosana?,
        arviointipaiva: LocalDate? = arvosana?.let { tutkintopaiva.plusDays(14) },
    ) = VktSuoritusEntity.VktOsakoe(
        tyyppi = tyyppi,
        tutkintopaiva = tutkintopaiva,
        arviointipaiva = arviointipaiva,
        arvosana = arvosana,
        merkittyPoistettavaksi = null,
    )

    companion object {
        private const val HENKILO = "1.2.246.562.24.12345678910"
        private val TARKASTELUPAIVA = LocalDate.of(2026, 10, 21)
    }
}
