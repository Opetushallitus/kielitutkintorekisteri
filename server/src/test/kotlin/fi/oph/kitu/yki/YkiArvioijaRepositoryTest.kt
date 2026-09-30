package fi.oph.kitu.yki

import fi.oph.kitu.DBContainerConfiguration
import fi.oph.kitu.oid.Oid
import fi.oph.kitu.util.result.getOrThrow
import fi.oph.kitu.yki.arvioijat.Tallennuslahde
import fi.oph.kitu.yki.arvioijat.YkiArvioijaEntity
import fi.oph.kitu.yki.arvioijat.YkiArvioijaRepository
import fi.oph.kitu.yki.arvioijat.YkiArvioijaTila
import fi.oph.kitu.yki.arvioijat.YkiArviointioikeusEntity
import org.junit.jupiter.api.BeforeEach
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.dao.OptimisticLockingFailureException
import org.testcontainers.postgresql.PostgreSQLContainer
import java.time.LocalDate
import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter
import kotlin.jvm.optionals.getOrNull
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

@SpringBootTest
@Import(DBContainerConfiguration::class)
class YkiArvioijaRepositoryTest(
    @param:Autowired private val postgres: PostgreSQLContainer,
    @param:Autowired private val arvioijaRepository: YkiArvioijaRepository,
) {
    private val oid = Oid.parse("1.2.246.562.24.20281155246").getOrThrow()

    @BeforeEach
    fun nukeDb() {
        arvioijaRepository.deleteAll()
    }

    private fun arviointioikeus(
        kieli: Tutkintokieli = Tutkintokieli.SWE,
        kaudenAlkupaiva: LocalDate? = null,
        kaudenPaattymispaiva: LocalDate? = null,
        tila: YkiArvioijaTila = YkiArvioijaTila.AKTIIVINEN,
        jatkorekisterointi: Boolean = false,
        tasot: Set<Tutkintotaso> = setOf(Tutkintotaso.YT),
    ) = YkiArviointioikeusEntity(
        id = null,
        arvioijaId = null,
        kaudenAlkupaiva = kaudenAlkupaiva,
        kaudenPaattymispaiva = kaudenPaattymispaiva,
        jatkorekisterointi = jatkorekisterointi,
        tila = tila,
        kieli = kieli,
        tasot = tasot,
        ensimmainenRekisterointipaiva = LocalDate.of(2020, 1, 1),
        rekisteriintuontiaika = null,
    )

    private fun testiarvioija(vararg arviointioikeudet: YkiArviointioikeusEntity) =
        arvioija(
            oid = oid,
            henkilotunnus = "010180-9026",
            sukunimi = "Öhman-Testi",
            etunimet = "Ranja Testi",
            oikeudet = arviointioikeudet.toList(),
        )

    /** Kanta tayttaa luotu- ja muokattu-leimat, joten ne nollataan vertailua varten. */
    private fun YkiArvioijaEntity.normalisoi() =
        copy(
            id = null,
            luotu = null,
            muokattu = null,
            // Johdetaan kausista tallennuksen yhteydessa, ks. paivitaEnsimmainenRekisterointipaiva.
            arvioijanEnsimmainenRekisterointipaiva = null,
            arviointioikeudet =
                arviointioikeudet
                    .map { it.copy(id = null, arvioijaId = null, rekisteriintuontiaika = null) }
                    .sortedBy { it.kieli.name },
        )

    @Test
    fun `Uuden arviointioikeuden tallennus olemassaolevalle henkilölle lisää sen kyseiselle henkilölle`() {
        val swe = arviointioikeus(Tutkintokieli.SWE)
        val eng = arviointioikeus(Tutkintokieli.ENG)

        arvioijaRepository.tallenna(testiarvioija(swe))

        val savedId = arvioijaRepository.tallenna(testiarvioija(swe, eng))
        val saved = arvioijaRepository.findById(savedId).getOrNull()

        assertEquals(testiarvioija(swe, eng).normalisoi(), saved?.normalisoi())
        assertEquals(1, arvioijaRepository.findAll().count())
    }

    @Test
    fun `Payloadista puuttuva arviointioikeus poistetaan, koska kitu on rekisterin master`() {
        val swe = arviointioikeus(Tutkintokieli.SWE)
        val eng = arviointioikeus(Tutkintokieli.ENG)

        arvioijaRepository.tallenna(testiarvioija(swe, eng))
        assertEquals(2, arvioijaRepository.findByArvioijaOid(oid)?.arviointioikeudet?.size)

        // Ruotsin arviointioikeus perutaan: se katoaa payloadista, jolloin sen on kadottava kannastakin.
        arvioijaRepository.tallenna(testiarvioija(eng))

        val jaljella = arvioijaRepository.findByArvioijaOid(oid)?.arviointioikeudet.orEmpty()
        assertEquals(listOf(Tutkintokieli.ENG), jaljella.map { it.kieli })
    }

    @Test
    fun `Sisaantulevassa pushissa puuttuvia arviointioikeuksia ei poisteta`() {
        val swe = arviointioikeus(Tutkintokieli.SWE)
        val eng = arviointioikeus(Tutkintokieli.ENG)

        arvioijaRepository.tallenna(testiarvioija(swe, eng))
        arvioijaRepository.tallenna(testiarvioija(eng), lahde = Tallennuslahde.SOLKI)

        val jaljella = arvioijaRepository.findByArvioijaOid(oid)?.arviointioikeudet.orEmpty()
        assertEquals(
            listOf(Tutkintokieli.ENG, Tutkintokieli.SWE),
            jaljella.map { it.kieli }.sortedBy { it.name },
        )
    }

    @Test
    fun `Solkin push ei jata rivia lahetysjonoon`() {
        arvioijaRepository.tallenna(testiarvioija(arviointioikeus()), lahde = Tallennuslahde.SOLKI)
        // Toinen push samaan riviin kayttaa ON CONFLICT -haaraa.
        arvioijaRepository.tallenna(testiarvioija(arviointioikeus()), lahde = Tallennuslahde.SOLKI)

        val tallennettu = arvioijaRepository.findByArvioijaOid(oid)
        val lahetetty = tallennettu?.solkiinLahetetty
        assertNotNull(lahetetty, "Solkista tullut rivi on leimattava lahetetyksi")
        assertFalse(
            lahetetty.isBefore(tallennettu.muokattu),
            "leiman on oltava vahintaan muokkaushetki, muuten rivi jaa lahetysjonoon",
        )
    }

    @Test
    fun `Kitun oma tallennus jaa lahetysjonoon`() {
        arvioijaRepository.tallenna(testiarvioija(arviointioikeus()))

        assertNull(
            arvioijaRepository.findByArvioijaOid(oid)?.solkiinLahetetty,
            "kitussa tehty muutos on lahetettava Solkiin",
        )
    }

    @Test
    fun `Vanhentunut muokkaushetki estaa tallennuksen`() {
        arvioijaRepository.tallenna(testiarvioija(arviointioikeus(Tutkintokieli.SWE)))

        assertFailsWith<OptimisticLockingFailureException> {
            arvioijaRepository.tallenna(
                testiarvioija(arviointioikeus(Tutkintokieli.ENG)),
                odotettuMuokkaushetki = OffsetDateTime.parse("2020-01-01T00:00:00Z"),
            )
        }

        assertEquals(
            listOf(Tutkintokieli.SWE),
            arvioijaRepository.findByArvioijaOid(oid)?.arviointioikeudet?.map { it.kieli },
            "estetyn tallennuksen ei saa muuttaa mitaan",
        )
    }

    @Test
    fun `Ajantasainen muokkaushetki sallii tallennuksen`() {
        arvioijaRepository.tallenna(testiarvioija(arviointioikeus(Tutkintokieli.SWE)))
        val nykyinen = arvioijaRepository.findByArvioijaOid(oid)?.muokattu

        arvioijaRepository.tallenna(
            testiarvioija(arviointioikeus(Tutkintokieli.ENG)),
            odotettuMuokkaushetki = nykyinen,
        )

        assertEquals(
            listOf(Tutkintokieli.ENG),
            arvioijaRepository.findByArvioijaOid(oid)?.arviointioikeudet?.map { it.kieli },
        )
    }

    @Test
    fun `Solkin push ei siivoa kitun lahettamatonta muutosta jonosta`() {
        // Kitun oma tallennus jattaa rivin lahetysjonoon (solkiin_lahetetty = NULL).
        arvioijaRepository.tallenna(testiarvioija(arviointioikeus(Tutkintokieli.SWE)))

        arvioijaRepository.tallenna(testiarvioija(arviointioikeus(Tutkintokieli.SWE)), lahde = Tallennuslahde.SOLKI)

        assertNull(
            arvioijaRepository.findByArvioijaOid(oid)?.solkiinLahetetty,
            "jonossa ollut muutos on yha lahettamatta",
        )
    }

    @Test
    fun `Solkin push ei pyyhi kitussa syotettyja kenttia`() {
        val kitunMerkinta =
            testiarvioija(arviointioikeus(Tutkintokieli.SWE)).copy(
                passivoitu = OffsetDateTime.parse("2026-02-01T00:00:00Z"),
            )
        arvioijaRepository.tallenna(kitunMerkinta)

        // Solkin payload ei kanna tata kenttaa lainkaan.
        arvioijaRepository.tallenna(testiarvioija(arviointioikeus(Tutkintokieli.SWE)), lahde = Tallennuslahde.SOLKI)

        val tallennettu = arvioijaRepository.findByArvioijaOid(oid)
        assertNotNull(tallennettu?.passivoitu, "passivointihetki on sailytysajan laskennan alkupiste")
    }

    @Test
    fun `Tasomuutos saman kauden sisalla kirjataan historiaan`() {
        val arvioijaId = arvioijaRepository.tallenna(testiarvioija(arviointioikeus(tasot = setOf(Tutkintotaso.PT))))
        assertEquals(1, kausihistorianTasot(arvioijaId).size)

        arvioijaRepository.tallenna(
            testiarvioija(arviointioikeus(tasot = setOf(Tutkintotaso.PT, Tutkintotaso.KT))),
        )

        assertEquals(
            listOf(listOf("PT"), listOf("KT", "PT")),
            kausihistorianTasot(arvioijaId).sortedBy { it.size },
            "hallintopaatoksella myonnetyn tason on nayttava historiassa",
        )
    }

    @Test
    fun `Muuttumaton kausi ei kasvata historiaa vaikka tasot tulisivat eri jarjestyksessa`() {
        val arvioijaId =
            arvioijaRepository.tallenna(
                testiarvioija(arviointioikeus(tasot = linkedSetOf(Tutkintotaso.PT, Tutkintotaso.KT))),
            )

        arvioijaRepository.tallenna(
            testiarvioija(arviointioikeus(tasot = linkedSetOf(Tutkintotaso.KT, Tutkintotaso.PT))),
        )

        assertEquals(1, kausihistorianTasot(arvioijaId).size, "sama kausi, sama tasojoukko")
    }

    private fun kausihistorianTasot(arvioijaId: Int): List<List<String>> =
        arvioijaRepository.findKausihistoria(arvioijaId).map { it.tasot.map { taso -> taso.name }.sorted() }

    @Test
    fun `Duplikaatteja ei tallenneta`() {
        val arvioija = testiarvioija(arviointioikeus())

        arvioijaRepository.saveAllNewEntities(listOf(arvioija))
        assertEquals(1, arvioijaRepository.findAll().count())
        assertEquals(1, arvioijaRepository.findAll().sumOf { it.arviointioikeudet.size })

        arvioijaRepository.saveAllNewEntities(listOf(arvioija))
        assertEquals(1, arvioijaRepository.findAll().count())
        assertEquals(1, arvioijaRepository.findAll().sumOf { it.arviointioikeudet.size })
    }

    @Test
    fun `different versions of the same arvioija are saved`() {
        val dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
        val arvioija =
            testiarvioija(
                arviointioikeus(
                    kaudenAlkupaiva = LocalDate.parse("2024-09-01", dateFormatter),
                    kaudenPaattymispaiva = LocalDate.parse("2025-09-01", dateFormatter),
                ),
            )

        arvioijaRepository.saveAllNewEntities(listOf(arvioija))
        assertEquals(1, arvioijaRepository.findAll().count())

        val updatedArvioija =
            arvioija.copy(
                arviointioikeudet =
                    arvioija.arviointioikeudet.map {
                        it.copy(kaudenAlkupaiva = LocalDate.now(), jatkorekisterointi = true)
                    },
            )
        val savedIds = arvioijaRepository.saveAllNewEntities(listOf(updatedArvioija))
        val saved = arvioijaRepository.findById(savedIds.first()).getOrNull()

        assertEquals(1, savedIds.count())
        assertEquals(updatedArvioija.normalisoi(), saved?.normalisoi())
        assertEquals(1, arvioijaRepository.findAll().count())
    }

    @Test
    fun `Päivitys nollaa aiemmin tallennetun henkilötunnuksen, jos tulevassa datassa hetua ei ole`() {
        val arvioijaHetulla =
            testiarvioija(
                arviointioikeus(
                    kaudenAlkupaiva = LocalDate.of(2026, 3, 1),
                    kaudenPaattymispaiva = LocalDate.of(2027, 3, 1),
                ),
            )

        val firstId = arvioijaRepository.tallenna(arvioijaHetulla)
        assertEquals("010180-9026", arvioijaRepository.findById(firstId).getOrNull()?.henkilotunnus)

        // Päivitys ilman hetua (vastaa lainmuutoksen jälkeen validoinnin läpäissyttä syötettä):
        // tallennetun rivin henkilotunnus pitää nollautua, eikä se saa jäädä lojumaan.
        arvioijaRepository.tallenna(arvioijaHetulla.copy(henkilotunnus = null))

        assertNull(arvioijaRepository.findById(firstId).getOrNull()?.henkilotunnus)
    }

    @Test
    fun `Kausihistoriaan kirjataan rivi vain kun kausi muuttuu`() {
        val kausi1 =
            arviointioikeus(
                kaudenAlkupaiva = LocalDate.of(2021, 1, 1),
                kaudenPaattymispaiva = LocalDate.of(2026, 1, 1),
            )
        val id = arvioijaRepository.tallenna(testiarvioija(kausi1))
        assertEquals(1, arvioijaRepository.findKausihistoria(id).size)

        // Sama kausi uudelleen: historia ei kasva.
        arvioijaRepository.tallenna(testiarvioija(kausi1))
        assertEquals(1, arvioijaRepository.findKausihistoria(id).size)

        // Pelkka yhteystiedon korjaus ei myoskaan kasvata historiaa.
        arvioijaRepository.tallenna(testiarvioija(kausi1).copy(sahkopostiosoite = "uusi@testi.fi"))
        assertEquals(1, arvioijaRepository.findKausihistoria(id).size)

        // Uusi kausi kirjautuu historiaan omana rivinaan.
        val kausi2 =
            kausi1.copy(
                kaudenAlkupaiva = LocalDate.of(2026, 1, 1),
                kaudenPaattymispaiva = LocalDate.of(2031, 1, 1),
                jatkorekisterointi = true,
            )
        arvioijaRepository.tallenna(testiarvioija(kausi2))

        val historia = arvioijaRepository.findKausihistoria(id)
        assertEquals(2, historia.size)
        assertTrue(historia.any { it.kaudenAlkupaiva == LocalDate.of(2021, 1, 1) })
        assertTrue(historia.any { it.kaudenAlkupaiva == LocalDate.of(2026, 1, 1) })
    }

    @Test
    fun `Tallennus merkitsee rivin lähetettäväksi Solkiin`() {
        val id = arvioijaRepository.tallenna(testiarvioija(arviointioikeus()))
        val saved = arvioijaRepository.findById(id).getOrNull()

        assertNotNull(saved)
        assertNull(saved.solkiinLahetetty, "uusi merkinta on lahetettava Solkiin")
        assertNull(saved.solkiLahetysvirhe)
        assertEquals(0, saved.solkiLahetysyritykset)
        assertNotNull(saved.luotu)
        assertNotNull(saved.muokattu)
    }

    @Test
    fun `Uudet kentät tallentuvat ja luotu-leima säilyy päivityksessä`() {
        val tekija = Oid.parse("1.2.246.562.24.59267607404").getOrThrow()
        val id =
            arvioijaRepository.tallenna(testiarvioija(arviointioikeus()), tekija = tekija)
        val luotu = arvioijaRepository.findById(id).getOrNull()?.luotu
        assertNotNull(luotu)

        arvioijaRepository.tallenna(testiarvioija(arviointioikeus()), tekija = tekija)

        val saved = arvioijaRepository.findById(id).getOrNull()
        assertNotNull(saved)
        assertEquals(luotu, saved.luotu, "luotu-leima ei saa muuttua paivityksessa")
        assertEquals(tekija, saved.muokkaajaOid)
    }
}
