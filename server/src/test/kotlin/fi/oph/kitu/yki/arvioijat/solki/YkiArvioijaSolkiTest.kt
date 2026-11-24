package fi.oph.kitu.yki.arvioijat.solki

import arrow.core.Either
import arrow.core.left
import arrow.core.right
import fi.oph.kitu.DBContainerConfiguration
import fi.oph.kitu.TestTimeService
import fi.oph.kitu.oid.Oid
import fi.oph.kitu.oppijanumero.OppijanumeroService
import fi.oph.kitu.util.result.getOrThrow
import fi.oph.kitu.yki.Tutkintokieli
import fi.oph.kitu.yki.Tutkintotaso
import fi.oph.kitu.yki.arvioijat.ArvioijarekisteriAsetukset
import fi.oph.kitu.yki.arvioijat.Kausioikeus
import fi.oph.kitu.yki.arvioijat.Tallennuslahde
import fi.oph.kitu.yki.arvioijat.YkiArvioijaEntity
import fi.oph.kitu.yki.arvioijat.YkiArvioijaKausiRepository
import fi.oph.kitu.yki.arvioijat.YkiArvioijaPoistoRepository
import fi.oph.kitu.yki.arvioijat.YkiArvioijaRepository
import fi.oph.kitu.yki.arvioijat.YkiArviointioikeusEntity
import fi.oph.kitu.yki.arvioijat.arvioijaOid
import fi.oph.kitu.yki.arvioijat.solki.Lahetystulos
import fi.oph.kitu.yki.arvioijat.solki.SolkiArvioijaException
import fi.oph.kitu.yki.arvioijat.solki.SolkiArvioijaPoistoRequest
import fi.oph.kitu.yki.arvioijat.solki.SolkiArvioijaRequest
import fi.oph.kitu.yki.arvioijat.solki.SolkiArvioijaServiceImpl
import org.junit.jupiter.api.BeforeEach
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.http.ResponseEntity
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.web.client.ResourceAccessException
import java.time.Instant
import java.time.LocalDate
import kotlin.test.Test
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

@SpringBootTest
@Import(DBContainerConfiguration::class)
class YkiArvioijaSolkiTest(
    @param:Autowired private val repository: YkiArvioijaRepository,
    @param:Autowired private val poistoRepository: YkiArvioijaPoistoRepository,
    @param:Autowired private val kausiRepository: YkiArvioijaKausiRepository,
    @param:Autowired private val jdbcTemplate: JdbcTemplate,
    @param:Autowired private val timeService: TestTimeService,
    @param:Autowired private val oppijanumeroService: OppijanumeroService,
) {
    private val hetki = Instant.parse("2026-06-01T09:00:00Z")
    private val tanaan = LocalDate.of(2026, 6, 1)

    /** Kerää lähetetyt pyynnöt ja antaa testin päättää vastauksen. */
    private class Stub(
        var vastaus: (SolkiArvioijaRequest) -> Either<SolkiArvioijaException, String?> = { "A00001".right() },
    ) : fi.oph.kitu.yki.arvioijat.solki.SolkiArvioijaClient {
        val lahetetyt = mutableListOf<SolkiArvioijaRequest>()
        val poistot = mutableListOf<SolkiArvioijaPoistoRequest>()
        var poistonVastaus: Either<SolkiArvioijaException, Unit> = Unit.right()

        override fun lahetaPoisto(request: SolkiArvioijaPoistoRequest): Either<SolkiArvioijaException, Unit> {
            poistot += request
            return poistonVastaus
        }

        override fun laheta(request: SolkiArvioijaRequest): Either<SolkiArvioijaException, String?> {
            lahetetyt += request
            return vastaus(request)
        }
    }

    private lateinit var stub: Stub
    private lateinit var solki: SolkiArvioijaServiceImpl

    @BeforeEach
    fun setup() {
        repository.deleteAll()
        jdbcTemplate.update("DELETE FROM yki_arvioija_poisto")
        stub = Stub()
        solki =
            SolkiArvioijaServiceImpl(
                repository,
                poistoRepository,
                kausiRepository,
                stub,
                oppijanumeroService,
                ArvioijarekisteriAsetukset(muokkausKaytossa = true, integraatioKaytossa = true),
            )
    }

    @Test
    fun `kitun oma tallennus jaa lahetysjonoon ja lahtee`() {
        tallenna()

        val jonossa = repository.findLahetettavat()
        assertEquals(1, jonossa.size, "kitun tallennus jaa jonoon")

        timeService.runWithFixedClock(hetki) { solki.lahetaLahettamattomat() }

        assertEquals(1, stub.lahetetyt.size)
        assertEquals(0, repository.findLahetettavat().size, "onnistunut lahetys poistaa jonosta")
    }

    @Test
    fun `Solkin oma push ei paady takaisin Solkiin`() {
        tallenna(lahde = Tallennuslahde.SOLKI)

        assertEquals(0, repository.findLahetettavat().size, "Solkin dataa ei laheteta takaisin")
    }

    @Test
    fun `payload sisaltaa CSV-vastaavat kentat`() {
        tallenna()

        timeService.runWithFixedClock(hetki) { solki.lahetaLahettamattomat() }

        val request = stub.lahetetyt.single()
        assertEquals("1.2.246.562.24.20281155246", request.arvioijaOid)
        assertNotNull(request.versio, "versio = kitun muokattu")
        assertEquals(SolkiArvioijaRequest.Tila.ARVIOIJA, request.tila)
        val oikeus =
            request.arviointikaudet
                .single()
                .arviointioikeudet
                .single()
        assertEquals(Tutkintokieli.FIN, oikeus.kieli)
        assertEquals(listOf(Tutkintotaso.PT), oikeus.tasot)
        assertEquals(LocalDate.of(1980, 1, 1), request.syntymapaiva, "syntymaaika haetaan ONR:sta")
        assertEquals(
            LocalDate.of(2024, 1, 1),
            request.ensimmainenRekisterointipaiva,
            "ensimmainen rekisterointipaiva kuuluu dokumentin juureen, ei arviointioikeudelle",
        )
    }

    @Test
    fun `Solkin palauttama tunnus talletetaan riville`() {
        val id = tallenna()

        timeService.runWithFixedClock(hetki) { solki.lahetaLahettamattomat() }

        assertEquals("A00001", repository.findArvioijaById(id)!!.solkiTunnus)
    }

    @Test
    fun `tunnukseton vastaus ei nollaa jo tallennettua tunnusta`() {
        val id = tallenna()
        timeService.runWithFixedClock(hetki) { solki.lahetaLahettamattomat() }

        stub.vastaus = { null.right() }
        timeService.runWithFixedClock(hetki) {
            solki.lahetaArvioijaKasin(repository.findArvioijaById(id)!!)
        }

        assertEquals(
            "A00001",
            repository.findArvioijaById(id)!!.solkiTunnus,
            "kerran saatu tunnus ei saa kadota vastauksesta jossa sita ei ole",
        )
    }

    @Test
    fun `tulevan kauden paivat lahetetaan sellaisenaan`() {
        // Tilaa ei laheteta, joten vastaanottaja erottaa tulevan kauden voimassa olevasta
        // vain naista paivista.
        tallenna(
            kaudenAlkupaiva = LocalDate.of(2030, 1, 1),
            kaudenPaattymispaiva = LocalDate.of(2034, 12, 31),
        )

        timeService.runWithFixedClock(hetki) { solki.lahetaLahettamattomat() }

        val kausi =
            stub.lahetetyt
                .single()
                .arviointikaudet
                .single()
        assertEquals(LocalDate.of(2030, 1, 1), kausi.alkupaiva)
        assertEquals(LocalDate.of(2034, 12, 31), kausi.paattymispaiva)
    }

    @Test
    fun `kauden kielet lahetetaan yhdessa kaudessa omine tasoineen`() {
        tallenna(
            kielet =
                mapOf(
                    Tutkintokieli.SWE to setOf(Tutkintotaso.YT),
                    Tutkintokieli.FIN to setOf(Tutkintotaso.KT, Tutkintotaso.PT),
                ),
        )

        timeService.runWithFixedClock(hetki) { solki.lahetaLahettamattomat() }

        val kausi =
            stub.lahetetyt
                .single()
                .arviointikaudet
                .single()
        assertEquals(LocalDate.of(2024, 1, 1), kausi.alkupaiva)
        assertEquals(LocalDate.of(2029, 1, 1), kausi.paattymispaiva)
        assertEquals(
            listOf(
                Tutkintokieli.FIN to listOf(Tutkintotaso.PT, Tutkintotaso.KT),
                Tutkintokieli.SWE to listOf(Tutkintotaso.YT),
            ),
            kausi.arviointioikeudet.map { it.kieli to it.tasot },
        )
    }

    @Test
    fun `sanomassa on kauden ja arviointioikeuksien tunnisteet`() {
        val id =
            tallenna(
                kielet =
                    mapOf(
                        Tutkintokieli.FIN to setOf(Tutkintotaso.PT),
                        Tutkintokieli.SWE to setOf(Tutkintotaso.YT),
                    ),
            )
        val tallennettu = kausiRepository.findKaudet(id).single()

        timeService.runWithFixedClock(hetki) { solki.lahetaLahettamattomat() }

        val kausi =
            stub.lahetetyt
                .single()
                .arviointikaudet
                .single()
        assertEquals(tallennettu.id?.toInt(), kausi.id)
        assertEquals(
            tallennettu.oikeudet.associate { it.kieli to it.id?.toInt() },
            kausi.arviointioikeudet.associate { it.kieli to it.id },
        )
        assertTrue(kausi.arviointioikeudet.none { it.id == null })
    }

    @Test
    fun `tunnisteet ovat tyhjia kun kautta ei ole tallennettu kitussa`() {
        tallenna(lahde = Tallennuslahde.SOLKI)
        val id = repository.findByArvioijaOid(Oid.parse("1.2.246.562.24.20281155246").getOrThrow())!!.id!!.toInt()
        assertEquals(0, kausiRepository.findKaudet(id).size)

        solki.lahetaArvioijaKasin(repository.findArvioijaById(id)!!)

        val kausi =
            stub.lahetetyt
                .single()
                .arviointikaudet
                .single()
        assertNull(kausi.id)
        assertEquals(listOf(null), kausi.arviointioikeudet.map { it.id })
    }

    @Test
    fun `sanomassa ovat kaikki kaudet, myos paattyneet ja samanaikaiset`() {
        val id = tallenna(kaudenAlkupaiva = LocalDate.of(2019, 1, 1), kaudenPaattymispaiva = LocalDate.of(2023, 12, 31))
        kausiRepository.lisaaKausi(
            id,
            LocalDate.of(2024, 1, 1),
            LocalDate.of(2028, 12, 31),
            listOf(Kausioikeus(Tutkintokieli.FIN, setOf(Tutkintotaso.PT))),
            null,
            null,
        )
        kausiRepository.lisaaKausi(
            id,
            LocalDate.of(2025, 6, 1),
            LocalDate.of(2030, 5, 31),
            listOf(Kausioikeus(Tutkintokieli.SWE, setOf(Tutkintotaso.YT))),
            null,
            null,
        )

        solki.lahetaArvioijaKasin(repository.findArvioijaById(id)!!)

        val kaudet = stub.lahetetyt.single().arviointikaudet
        assertEquals(
            listOf(LocalDate.of(2019, 1, 1), LocalDate.of(2024, 1, 1), LocalDate.of(2025, 6, 1)),
            kaudet.map { it.alkupaiva },
        )
        assertEquals(
            listOf(listOf(Tutkintokieli.FIN), listOf(Tutkintokieli.FIN), listOf(Tutkintokieli.SWE)),
            kaudet.map { kausi -> kausi.arviointioikeudet.map { it.kieli } },
        )
        assertEquals(listOf(false, true, true), kaudet.map { it.jatkorekisterointi })
        assertEquals(
            kausiRepository.findKaudet(id).map { it.id?.toInt() }.toSet(),
            kaudet.map { it.id }.toSet(),
        )
    }

    @Test
    fun `ennen alkuaan paattyvaa kautta ei laheteta`() {
        val id = tallenna()
        kausiRepository.lisaaKausi(
            id,
            LocalDate.of(2030, 1, 1),
            tanaan,
            listOf(Kausioikeus(Tutkintokieli.SWE, setOf(Tutkintotaso.YT))),
            null,
            null,
        )

        solki.lahetaArvioijaKasin(repository.findArvioijaById(id)!!)

        assertEquals(
            listOf(LocalDate.of(2024, 1, 1)),
            stub.lahetetyt
                .single()
                .arviointikaudet
                .map { it.alkupaiva },
        )
    }

    @Test
    fun `arviointioikeuden tunniste sailyy kun arvioija tallennetaan uudelleen`() {
        val id =
            tallenna(
                kielet =
                    mapOf(
                        Tutkintokieli.FIN to setOf(Tutkintotaso.PT),
                        Tutkintokieli.SWE to setOf(Tutkintotaso.YT),
                    ),
            )
        val ennen = kausiRepository.findKaudet(id).single()

        tallenna(
            kielet =
                mapOf(
                    Tutkintokieli.FIN to setOf(Tutkintotaso.PT, Tutkintotaso.KT),
                    Tutkintokieli.ENG to setOf(Tutkintotaso.PT),
                ),
        )

        val jalkeen = kausiRepository.findKaudet(id).single()
        assertEquals(ennen.id, jalkeen.id)
        assertEquals(
            setOf(Tutkintokieli.FIN, Tutkintokieli.ENG, Tutkintokieli.SWE),
            jalkeen.oikeudet.map { it.kieli }.toSet(),
            "tallennus ei poista kauden muita kielia",
        )
        assertEquals(
            ennen.oikeudet.single { it.kieli == Tutkintokieli.SWE }.id,
            jalkeen.oikeudet.single { it.kieli == Tutkintokieli.SWE }.id,
        )
        val fin = jalkeen.oikeudet.single { it.kieli == Tutkintokieli.FIN }
        assertEquals(ennen.oikeudet.single { it.kieli == Tutkintokieli.FIN }.id, fin.id)
        assertEquals(setOf(Tutkintotaso.PT, Tutkintotaso.KT), fin.tasot)
    }

    @Test
    fun `arviointioikeuden tunniste sailyy kun kautta muokataan`() {
        val id =
            tallenna(
                kielet =
                    mapOf(
                        Tutkintokieli.FIN to setOf(Tutkintotaso.PT),
                        Tutkintokieli.SWE to setOf(Tutkintotaso.YT),
                    ),
            )
        val ennen = kausiRepository.findKaudet(id).single()

        kausiRepository.paivitaKausi(
            kausiId = ennen.id!!.toInt(),
            alkupaiva = ennen.alkupaiva,
            paattymispaiva = LocalDate.of(2030, 1, 1),
            oikeudet = listOf(Kausioikeus(Tutkintokieli.FIN, setOf(Tutkintotaso.KT))),
            ashaNumero = null,
            tekija = null,
        )

        val jalkeen = kausiRepository.findKaudet(id).single()
        assertEquals(ennen.id, jalkeen.id)
        val fin = jalkeen.oikeudet.single()
        assertEquals(ennen.oikeudet.single { it.kieli == Tutkintokieli.FIN }.id, fin.id)
        assertEquals(setOf(Tutkintotaso.KT), fin.tasot)
    }

    @Test
    fun `vanhentunutta kielta ei laheteta`() {
        val id =
            tallenna(
                lahde = Tallennuslahde.SOLKI,
                kielet =
                    mapOf(
                        Tutkintokieli.FIN to setOf(Tutkintotaso.PT),
                        Tutkintokieli.SWE10 to setOf(Tutkintotaso.PT),
                    ),
            )

        val request = SolkiArvioijaRequest.of(repository.findArvioijaById(id)!!, syntymaaika = null)

        assertEquals(
            listOf(Tutkintokieli.FIN),
            request.arviointikaudet
                .single()
                .arviointioikeudet
                .map { it.kieli },
        )
    }

    @Test
    fun `pelkka vanhentunut kieli ei muodosta kautta`() {
        val id = tallenna(lahde = Tallennuslahde.SOLKI, kielet = mapOf(Tutkintokieli.ENG11 to setOf(Tutkintotaso.PT)))

        val request = SolkiArvioijaRequest.of(repository.findArvioijaById(id)!!, syntymaaika = null)

        assertEquals(emptyList(), request.arviointikaudet)
    }

    @Test
    fun `ONR-katko jattaa rivin jonoon eika laheta puutteellista payloadia`() {
        // Fixtureton OID = ONR ei vastaa. Ilman tata rivi lahtisi ilman syntymaaikaa ja
        // merkittaisiin lahetetyksi, jolloin puute jaisi pysyvaksi seuraavaan muokkaukseen asti.
        val id = tallenna(oid = "1.2.246.562.24.99999999999")

        timeService.runWithFixedClock(hetki) { solki.lahetaLahettamattomat() }

        assertEquals(0, stub.lahetetyt.size, "puutteellista payloadia ei saa lahettaa")
        assertEquals(1, repository.findLahetettavat().size, "rivi jaa jonoon uusintaa varten")
        val rivi = repository.findArvioijaById(id)!!
        assertNull(rivi.solkiinLahetetty)
        assertContains(rivi.solkiLahetysvirhe!!, "Syntymaajan haku")
    }

    @Test
    fun `ONR-n tyhja syntymaaika ei esta lahetysta`() {
        // Yksiloimattomalla henkilolla ei ole syntymaaikaa. Se on eri asia kuin ONR-katko:
        // tieto puuttuu aidosti, joten kentta jatetaan pois ja rivi merkitaan lahetetyksi.
        val id = tallenna(oid = "1.2.246.562.24.10691606777")

        timeService.runWithFixedClock(hetki) { solki.lahetaLahettamattomat() }

        assertNull(stub.lahetetyt.single().syntymapaiva)
        assertNotNull(repository.findArvioijaById(id)!!.solkiinLahetetty)
        assertEquals(0, repository.findLahetettavat().size)
    }

    @Test
    fun `virhe kirjataan riville ja rivi jaa jonoon`() {
        val id = tallenna()
        stub.vastaus = { req ->
            SolkiArvioijaException
                .UnexpectedError(req.arvioijaOid, ResponseEntity.status(500).body("hajosi"))
                .left()
        }

        timeService.runWithFixedClock(hetki) { solki.lahetaLahettamattomat() }

        val rivi = repository.findArvioijaById(id)!!
        assertNotNull(rivi.solkiLahetysvirhe)
        assertEquals(1, rivi.solkiLahetysyritykset, "yrityslaskuri kasvaa virheesta")
        assertNull(rivi.solkiinLahetetty)
        assertEquals(1, repository.findLahetettavat().size, "epaonnistunut jaa jonoon")
    }

    @Test
    fun `virheilmoitus ei sisalla henkilotietoja`() {
        val id = tallenna()
        stub.vastaus = { req ->
            SolkiArvioijaException
                .BadRequest(req.arvioijaOid, ResponseEntity.badRequest().body("kentta puuttuu"))
                .left()
        }

        timeService.runWithFixedClock(hetki) { solki.lahetaLahettamattomat() }

        val virhe = repository.findArvioijaById(id)!!.solkiLahetysvirhe!!
        assertContains(virhe, "1.2.246.562.24.20281155246", message = "oppijanumero kuuluu virheeseen")
        assertTrue(
            listOf("Testikuja", "testi@testi.fi", "Testila").none { virhe.contains(it) },
            "osoite ja sahkoposti eivat saa vuotaa lokiin eivatka virhesarakkeeseen: $virhe",
        )
    }

    @Test
    fun `nopeat uusinnat rajautuvat yrityslaskurilla mutta yollinen ajo ei`() {
        val id = tallenna()
        repeat(3) { repository.merkitseLahetysvirhe(id, "virhe") }

        assertEquals(0, repository.findLahetettavat(maxYritykset = 3).size, "3 yritysta kaytetty")
        assertEquals(1, repository.findLahetettavat().size, "yollinen ajo yrittaa silti")
    }

    @Test
    fun `odottamaton poikkeus ei kaada tallennusta vaan kirjautuu riville`() {
        val id = tallenna()
        stub.vastaus = { throw ResourceAccessException("connection refused") }

        // Ei saa heittaa: tallennus on jo tehty, ja virkailija saisi 500:n valmiiseen riviin.
        timeService.runWithFixedClock(hetki) { solki.lahetaLahettamattomat() }

        val rivi = repository.findArvioijaById(id)!!
        assertContains(rivi.solkiLahetysvirhe!!, "Unexpected failure")
        assertEquals(1, repository.findLahetettavat().size, "rivi jaa jonoon uusintaa varten")
    }

    @Test
    fun `lahetyksen aikana muokattu rivi jaa jonoon`() {
        val id = tallenna()
        val lahetettava = repository.findLahetettavat().single()

        // Virkailija ehtii muokata rivia kesken lahetyksen.
        repository.tallenna(lahetettava.copy(postitoimipaikka = "TAMPERE"))
        repository.merkitseLahetetyksi(id, lahetettava.muokattu)

        assertEquals(
            1,
            repository.findLahetettavat().size,
            "vanhalla versiolla tehty leima ei saa pudottaa uutta muutosta jonosta",
        )
    }

    @Test
    fun `yhden rivin virhe ei keskeyta eraa`() {
        val hajoava = tallenna()
        val toimiva = tallenna(oid = "1.2.246.562.24.59267607404")
        stub.vastaus = { req ->
            if (req.arvioijaOid.endsWith("20281155246")) {
                throw IllegalStateException("hajosi")
            } else {
                "A00002".right()
            }
        }

        timeService.runWithFixedClock(hetki) { solki.lahetaLahettamattomat() }

        assertNotNull(repository.findArvioijaById(hajoava)!!.solkiLahetysvirhe)
        assertNotNull(
            repository.findArvioijaById(toimiva)!!.solkiinLahetetty,
            "eran muut rivit on lahetettava vaikka yksi hajoaa",
        )
    }

    @Test
    fun `kytkin pois estaa automaattisen lahetyksen`() {
        tallenna()

        val tulos =
            timeService.runWithFixedClock(
                hetki,
            ) { kytkinPois().lahetaArvioija(repository.findLahetettavat().single()) }

        assertEquals(Lahetystulos.EI_KAYTOSSA, tulos)
        assertEquals(0, stub.lahetetyt.size, "automaattilahetys ei saa ottaa yhteytta Solkiin")
        assertEquals(1, repository.findLahetettavat().size, "rivin on jaatava jonoon")
    }

    @Test
    fun `kytkin pois ei estä eraajoa hiljaisesti vaan jattaa rivit jonoon`() {
        tallenna()

        val onnistuneet = timeService.runWithFixedClock(hetki) { kytkinPois().lahetaLahettamattomat() }

        assertEquals(0, onnistuneet)
        assertEquals(0, stub.lahetetyt.size)
        assertEquals(1, repository.findLahetettavat().size)
    }

    /** Painike on ainoa tapa kokeilla integraatiota ennen kuin automaattinen liikenne avataan. */
    @Test
    fun `kytkin pois ei estä kasin lahetysta`() {
        val id = tallenna()

        val tulos =
            timeService.runWithFixedClock(hetki) {
                kytkinPois().lahetaArvioijaKasin(repository.findLahetettavat().single())
            }

        assertEquals(Lahetystulos.LAHETETTY, tulos)
        assertEquals(1, stub.lahetetyt.size, "kasin lahetyksen on mentava Solkiin asti")
        assertNotNull(repository.findArvioijaById(id)!!.solkiinLahetetty)
        assertEquals(0, repository.findLahetettavat().size, "onnistunut kasin lahetys poistaa jonosta")
    }

    @Test
    fun `kasin lahetyksen virhe kirjataan riville`() {
        val id = tallenna()
        stub.vastaus = { SolkiArvioijaException.NullResponse(it.arvioijaOid).left() }

        val tulos =
            timeService.runWithFixedClock(hetki) {
                kytkinPois().lahetaArvioijaKasin(repository.findLahetettavat().single())
            }

        assertEquals(Lahetystulos.VIRHE, tulos)
        assertNotNull(repository.findArvioijaById(id)!!.solkiLahetysvirhe)
        assertEquals(1, repository.findLahetettavat().size, "epaonnistunut rivi jaa jonoon")
    }

    @Test
    fun `poistoilmoitus lahtee ja poistuu jonosta`() {
        kirjaaPoisto()

        assertEquals(1, solki.lahetaPoistot())

        val ilmoitus = stub.poistot.single()
        assertEquals(POISTETTU_OID, ilmoitus.arvioijaOid)
        assertEquals(SolkiArvioijaRequest.Tila.POISTETTU, ilmoitus.tila)
        assertEquals(0, poistoRepository.findLahetettavat().size, "vastaanotettu ilmoitus ei jaa kantaan")
    }

    @Test
    fun `epaonnistunut poistoilmoitus jaa jonoon ja yritys kirjataan`() {
        kirjaaPoisto()
        stub.poistonVastaus = SolkiArvioijaException.NullResponse(POISTETTU_OID).left()

        assertEquals(0, solki.lahetaPoistot())

        assertEquals(1, poistoRepository.findLahetettavat().size)
        assertEquals(0, poistoRepository.findLahetettavat(maxYritykset = 1).size, "yrityskerta on kirjattu")
        assertNotNull(
            jdbcTemplate.queryForObject("SELECT solki_lahetysvirhe FROM yki_arvioija_poisto", String::class.java),
        )
    }

    @Test
    fun `poistoilmoitus odottaa jonossa kun integraatio on pois`() {
        kirjaaPoisto()

        assertEquals(0, kytkinPois().lahetaPoistot())

        assertEquals(0, stub.poistot.size)
        assertEquals(1, poistoRepository.findLahetettavat().size)
    }

    @Test
    fun `uudelleen lisatyn arvioijan poistoilmoitusta ei laheteta`() {
        kirjaaPoisto()
        tallenna(oid = POISTETTU_OID)

        assertEquals(0, solki.lahetaPoistot())

        assertEquals(0, stub.poistot.size, "ilmoitus poistaisi Solkista voimassa olevan arvioijan")
        assertEquals(0, poistoRepository.findLahetettavat().size)
    }

    private fun kirjaaPoisto() {
        jdbcTemplate.update("INSERT INTO yki_arvioija_poisto (arvioija_oid) VALUES (?)", POISTETTU_OID)
    }

    private fun kytkinPois() =
        SolkiArvioijaServiceImpl(
            repository,
            poistoRepository,
            kausiRepository,
            stub,
            oppijanumeroService,
            ArvioijarekisteriAsetukset(muokkausKaytossa = true, integraatioKaytossa = false),
        )

    private fun tallenna(
        lahde: Tallennuslahde = Tallennuslahde.KITU,
        oid: String = "1.2.246.562.24.20281155246",
        kaudenAlkupaiva: LocalDate = LocalDate.of(2024, 1, 1),
        kaudenPaattymispaiva: LocalDate = LocalDate.of(2029, 1, 1),
        kielet: Map<Tutkintokieli, Set<Tutkintotaso>> = mapOf(Tutkintokieli.FIN to setOf(Tutkintotaso.PT)),
    ): Int =
        repository.tallenna(
            YkiArvioijaEntity(
                id = null,
                arvioijaOid = Oid.parse(oid).getOrThrow(),
                henkilotunnus = null,
                sukunimi = "Öhman-Testi",
                etunimet = "Ranja Testi",
                sahkopostiosoite = "testi@testi.fi",
                katuosoite = "Testikuja 5",
                postinumero = "40100",
                postitoimipaikka = "Testila",
                arviointioikeudet =
                    kielet.map { (kieli, tasot) ->
                        YkiArviointioikeusEntity(
                            id = null,
                            arvioijaId = null,
                            kieli = kieli,
                            tasot = tasot,
                            tila = null,
                            kaudenAlkupaiva = kaudenAlkupaiva,
                            kaudenPaattymispaiva = kaudenPaattymispaiva,
                            jatkorekisterointi = false,
                            ensimmainenRekisterointipaiva = LocalDate.of(2024, 1, 1),
                            rekisteriintuontiaika = null,
                        )
                    },
            ),
            lahde = lahde,
        )

    companion object {
        private const val POISTETTU_OID = "1.2.246.562.24.20281155246"
    }
}
