package fi.oph.kitu.yki

import arrow.core.Either
import arrow.core.left
import arrow.core.right
import fi.oph.kitu.DBContainerConfiguration
import fi.oph.kitu.TestTimeService
import fi.oph.kitu.oid.Oid
import fi.oph.kitu.oppijanumero.OppijanumeroService
import fi.oph.kitu.util.result.getOrThrow
import fi.oph.kitu.yki.arvioijat.ArvioijarekisteriAsetukset
import fi.oph.kitu.yki.arvioijat.Tallennuslahde
import fi.oph.kitu.yki.arvioijat.YkiArvioijaEntity
import fi.oph.kitu.yki.arvioijat.YkiArvioijaRepository
import fi.oph.kitu.yki.arvioijat.YkiArviointioikeusEntity
import fi.oph.kitu.yki.arvioijat.solki.Lahetystulos
import fi.oph.kitu.yki.arvioijat.solki.SolkiArvioijaException
import fi.oph.kitu.yki.arvioijat.solki.SolkiArvioijaRequest
import fi.oph.kitu.yki.arvioijat.solki.SolkiArvioijaServiceImpl
import org.junit.jupiter.api.BeforeEach
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.http.ResponseEntity
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
        stub = Stub()
        solki =
            SolkiArvioijaServiceImpl(
                repository,
                stub,
                timeService,
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
    fun `payload sisaltaa CSV-vastaavat kentat ja lasketun tilan`() {
        tallenna()

        timeService.runWithFixedClock(hetki) { solki.lahetaLahettamattomat() }

        val request = stub.lahetetyt.single()
        assertEquals("1.2.246.562.24.20281155246", request.arvioijaOid)
        assertNotNull(request.versio, "versio = kitun muokattu")
        val oikeus = request.arviointioikeudet.single()
        assertEquals(Tutkintokieli.FIN, oikeus.kieli)
        assertEquals(listOf(Tutkintotaso.PT), oikeus.tasot)
        assertEquals(SolkiArvioijaRequest.Tila.AKTIIVINEN, oikeus.tila, "tila lasketaan lahetyshetkella")
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
    fun `tuleva kausi lahetetaan aktiivisena`() {
        // Solkin sanastossa on vain AKTIIVINEN ja PASSIVOITU; kauden alkupaiva kertoo lopun.
        tallenna(
            kaudenAlkupaiva = LocalDate.of(2030, 1, 1),
            kaudenPaattymispaiva = LocalDate.of(2035, 1, 1),
        )

        timeService.runWithFixedClock(hetki) { solki.lahetaLahettamattomat() }

        val oikeus =
            stub.lahetetyt
                .single()
                .arviointioikeudet
                .single()
        assertEquals(SolkiArvioijaRequest.Tila.AKTIIVINEN, oikeus.tila)
        assertEquals(LocalDate.of(2030, 1, 1), oikeus.kaudenAlkupaiva)
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

    private fun kytkinPois() =
        SolkiArvioijaServiceImpl(
            repository,
            stub,
            timeService,
            oppijanumeroService,
            ArvioijarekisteriAsetukset(muokkausKaytossa = true, integraatioKaytossa = false),
        )

    private fun tallenna(
        lahde: Tallennuslahde = Tallennuslahde.KITU,
        oid: String = "1.2.246.562.24.20281155246",
        kaudenAlkupaiva: LocalDate = LocalDate.of(2024, 1, 1),
        kaudenPaattymispaiva: LocalDate = LocalDate.of(2029, 1, 1),
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
                    listOf(
                        YkiArviointioikeusEntity(
                            id = null,
                            arvioijaId = null,
                            kieli = Tutkintokieli.FIN,
                            tasot = setOf(Tutkintotaso.PT),
                            tila = null,
                            kaudenAlkupaiva = kaudenAlkupaiva,
                            kaudenPaattymispaiva = kaudenPaattymispaiva,
                            jatkorekisterointi = false,
                            ensimmainenRekisterointipaiva = LocalDate.of(2024, 1, 1),
                            rekisteriintuontiaika = null,
                        ),
                    ),
            ),
            lahde = lahde,
        )
}
