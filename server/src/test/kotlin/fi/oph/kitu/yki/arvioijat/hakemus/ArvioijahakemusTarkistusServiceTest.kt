package fi.oph.kitu.yki.arvioijat.hakemus

import fi.oph.kitu.DBContainerConfiguration
import fi.oph.kitu.TestTimeService
import fi.oph.kitu.auditlogs.AuditLogger
import fi.oph.kitu.tyhjennaArvioijahakemukset
import fi.oph.kitu.util.result.getOrThrow
import fi.oph.kitu.yki.Tutkintokieli
import fi.oph.kitu.yki.Tutkintotaso
import fi.oph.kitu.yki.arvioijat.ArvioijaFormData
import fi.oph.kitu.yki.arvioijat.TallennaArvioija
import fi.oph.kitu.yki.arvioijat.TallennaArvioijaValidation
import fi.oph.kitu.yki.arvioijat.YkiArvioijaError
import fi.oph.kitu.yki.arvioijat.YkiArvioijaKausiRepository
import fi.oph.kitu.yki.arvioijat.YkiArvioijaRepository
import fi.oph.kitu.yki.arvioijat.YkiArvioijaService
import fi.oph.kitu.yki.arvioijat.hakemus.ArvioijahakemusTestidata.HETKI
import fi.oph.kitu.yki.arvioijat.hakemus.ArvioijahakemusTestidata.OSOITTEETON_OID
import fi.oph.kitu.yki.arvioijat.hakemus.ArvioijahakemusTestidata.PETRO_OID
import fi.oph.kitu.yki.arvioijat.hakemus.ArvioijahakemusTestidata.TANAAN
import fi.oph.kitu.yki.arvioijat.hakemus.ArvioijahakemusTestidata.oid
import fi.oph.kitu.yki.arvioijat.hakemus.ArvioijahakemusTestidata.tarkistettava
import org.junit.jupiter.api.BeforeEach
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import java.time.LocalDate
import java.time.OffsetDateTime
import java.time.ZoneOffset
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

@SpringBootTest
@Import(DBContainerConfiguration::class)
class ArvioijahakemusTarkistusServiceTest(
    @param:Autowired private val service: ArvioijahakemusTarkistusService,
    @param:Autowired private val repository: ArvioijahakemusRepository,
    @param:Autowired private val tarkistettavaRepository: ArvioijahakemusTarkistettavaRepository,
    @param:Autowired private val jdbcTemplate: JdbcTemplate,
    @param:Autowired private val arvioijaRepository: YkiArvioijaRepository,
    @param:Autowired private val kausiRepository: YkiArvioijaKausiRepository,
    @param:Autowired private val arvioijaService: YkiArvioijaService,
    @param:Autowired private val timeService: TestTimeService,
) {
    private val asha = "OPH-1234-2026"

    @BeforeEach
    fun setup() {
        jdbcTemplate.tyhjennaArvioijahakemukset()
        arvioijaRepository.deleteAll()
    }

    private fun <T> kello(f: () -> T): T = timeService.runWithFixedClock(HETKI, f)

    private fun porrasta(
        hakemusOid: String = "h1",
        hakemus: Arvioijahakemus = tarkistettava(hakemusOid).hakemus,
    ) = tarkistettavaRepository.tallenna(tarkistettava(hakemusOid, hakemus = hakemus), HETKI.atOffset(ZoneOffset.UTC))

    private fun lomake(hakemusOid: String = "h1"): ArvioijaFormData = kello { service.lomake(hakemusOid).getOrThrow() }

    private fun vahvista(
        form: ArvioijaFormData,
        hakemusOid: String = "h1",
        palvelu: ArvioijahakemusTarkistusService = service,
    ) = kello { palvelu.vahvista(hakemusOid, form, tekija = null) }

    private fun kaudet() =
        kausiRepository
            .findKaudet(assertNotNull(arvioijaRepository.findByArvioijaOid(oid(PETRO_OID))).id!!.toInt())
            .sortedBy { it.alkupaiva }

    private fun luoOlemassaoleva(alkupaiva: LocalDate) =
        kello {
            arvioijaService
                .luoArvioija(
                    TallennaArvioija(
                        arvioijaOid = oid(PETRO_OID),
                        sukunimi = "Kivinen-Testi",
                        etunimet = "Petro Testi",
                        sahkopostiosoite = null,
                        katuosoite = "Vanha katu 1",
                        postinumero = "00100",
                        postitoimipaikka = "HELSINKI",
                        kaudenAlkupaiva = alkupaiva,
                        ashaNumero = "VANHA-1",
                        arviointioikeudet =
                            listOf(TallennaArvioija.Arviointioikeus(Tutkintokieli.FIN, setOf(Tutkintotaso.PT))),
                    ),
                    tekija = null,
                ).getOrThrow()
        }

    private fun validointipolut(tulos: Any?): List<String> {
        val virhe = assertIs<Tarkistusvirhe.Arvioija>(tulos).virhe
        return assertIs<YkiArvioijaError.Validointivirheet>(virhe).virheet.map { it.path.joinToString(".") }
    }

    @Test
    fun `lomake esitaytetaan hakemukselta ja oppijanumerorekisterista ja kausi ehdotetaan alkamaan tanaan`() {
        porrasta(
            hakemus =
                tarkistettava("h1").hakemus.copy(sahkopostiosoite = "hakemus@oph.fi"),
        )

        val form = lomake()

        assertEquals(PETRO_OID, form.arvioijaOid)
        assertEquals("hakemus@oph.fi", form.sahkopostiosoite, "hakemuksen yhteystieto voittaa ONR:n")
        assertEquals("Kivinenkatu 2 A 3", form.katuosoite, "puuttuva yhteystieto taydennetaan ONR:sta")
        assertEquals(TANAAN, form.kaudenAlkupaiva)
        assertNull(form.ashaNumero)
        assertEquals(listOf("FIN:PT", "FIN:KT"), form.arviointioikeus)
        assertEquals(false, form.onOlemassa)
    }

    @Test
    fun `tuntematon tai jo vahvistettu hakemus ei ole tarkistettavana`() {
        assertEquals(Tarkistusvirhe.EiTarkistettavana, kello { service.lomake("h1") }.leftOrNull())

        porrasta()
        repository.varaa("h1", PETRO_OID, TANAAN, HETKI.atOffset(ZoneOffset.UTC))

        assertEquals(Tarkistusvirhe.JoKasittelyssa, kello { service.lomake("h1") }.leftOrNull())
    }

    @Test
    fun `vahvistus tallentaa arvioijan virkailijan alkupaivalla ja ASHA-numerolla`() {
        porrasta()
        val alkupaiva = TANAAN.minusDays(10)

        val arvioija = vahvista(lomake().copy(kaudenAlkupaiva = alkupaiva, ashaNumero = " $asha ")).getOrThrow()

        assertEquals(oid(PETRO_OID), arvioija.arvioijaOid)
        val kausi = kaudet().single()
        assertEquals(alkupaiva, kausi.alkupaiva)
        assertEquals(asha, kausi.ashaNumero)
        val kirjanpito = repository.haeKaikki().single()
        assertEquals(ArvioijahakemuksenTila.KASITELTY, kirjanpito.tila)
        assertEquals(arvioija.id?.toInt(), kirjanpito.arvioijaId)
        assertEquals(alkupaiva, kirjanpito.kaudenAlkupaiva)
        assertEquals(
            0,
            jdbcTemplate.queryForObject("SELECT count(*) FROM yki_arvioijahakemus_tarkistettava", Long::class.java),
        )
    }

    @Test
    fun `vahvistus vaatii ASHA-numeron eika tallenna mitaan`() {
        porrasta()

        val tulos = vahvista(lomake().copy(ashaNumero = "  ", postinumero = "abc"))

        assertEquals(listOf("ashaNumero", "postinumero"), validointipolut(tulos.leftOrNull()).sorted())
        assertNull(arvioijaRepository.findByArvioijaOid(oid(PETRO_OID)))
        assertEquals(emptyList(), repository.haeKaikki())
        assertNotNull(tarkistettavaRepository.hae("h1"))
    }

    @Test
    fun `validointivirhe vapauttaa varauksen ja hakemus jaa tarkistettavaksi`() {
        porrasta()

        val tulos = vahvista(lomake().copy(ashaNumero = asha, kaudenAlkupaiva = TANAAN.plusYears(2)))

        assertEquals(listOf("kaudenAlkupaiva"), validointipolut(tulos.leftOrNull()))
        assertEquals(emptyList(), repository.haeKaikki())
        assertNotNull(tarkistettavaRepository.hae("h1"))
    }

    @Test
    fun `jatkokaudelle ehdotetaan voimassa olevan kauden jalkeista paivaa joka saa olla yli vuoden paassa`() {
        luoOlemassaoleva(LocalDate.of(2025, 1, 1))
        porrasta()

        val form = lomake()
        assertEquals(LocalDate.of(2030, 1, 1), form.kaudenAlkupaiva)
        assertEquals(true, form.onOlemassa)

        vahvista(form.copy(ashaNumero = asha)).getOrThrow()

        assertEquals(
            listOf(LocalDate.of(2025, 1, 1) to "VANHA-1", LocalDate.of(2030, 1, 1) to asha),
            kaudet().map { it.alkupaiva to it.ashaNumero },
        )
    }

    @Test
    fun `muu kuin edellista kautta jatkava alkupaiva ei saa olla yli vuoden paassa`() {
        luoOlemassaoleva(LocalDate.of(2025, 1, 1))
        porrasta()

        val tulos = vahvista(lomake().copy(kaudenAlkupaiva = LocalDate.of(2030, 6, 1), ashaNumero = asha))

        assertEquals(listOf("kaudenAlkupaiva"), validointipolut(tulos.leftOrNull()))
        assertEquals(1, kaudet().size)
    }

    @Test
    fun `toinen vahvistus ei luo toista kautta`() {
        porrasta()
        val form = lomake().copy(ashaNumero = asha)
        vahvista(form).getOrThrow()

        assertEquals(Tarkistusvirhe.JoKasittelyssa, vahvista(form).leftOrNull())

        assertEquals(1, kaudet().size)
    }

    @Test
    fun `varaus onnistuu vain kerran`() {
        val hetki = HETKI.atOffset(ZoneOffset.UTC)

        assertTrue(repository.varaa("h1", PETRO_OID, TANAAN, hetki))
        assertEquals(false, repository.varaa("h1", PETRO_OID, TANAAN, hetki))
    }

    @Test
    fun `lomakkeen piilotettu oppijanumero ohitetaan`() {
        porrasta()

        vahvista(lomake().copy(arvioijaOid = OSOITTEETON_OID, ashaNumero = asha)).getOrThrow()

        assertNotNull(arvioijaRepository.findByArvioijaOid(oid(PETRO_OID)))
        assertNull(arvioijaRepository.findByArvioijaOid(oid(OSOITTEETON_OID)))
    }

    @Test
    fun `samanaikainen muokkaus vapauttaa varauksen`() {
        luoOlemassaoleva(LocalDate.of(2025, 1, 1))
        porrasta()
        val form = lomake().copy(ashaNumero = asha)
        jdbcTemplate.update("UPDATE yki_arvioija SET muokattu = muokattu + interval '1 second'")

        val tulos = vahvista(form)

        assertEquals(Tarkistusvirhe.Arvioija(YkiArvioijaError.MuokattuSamanaikaisesti), tulos.leftOrNull())
        assertEquals(emptyList(), repository.haeKaikki())
        assertNotNull(tarkistettavaRepository.hae("h1"))
    }

    @Test
    fun `kaatuminen tallennuksen jalkeen jattaa hakemuksen kasittelyyn eika uusi vahvistus luo toista kautta`(
        @Autowired jdbc: NamedParameterJdbcTemplate,
        @Autowired validation: TallennaArvioijaValidation,
        @Autowired auditLogger: AuditLogger,
    ) {
        val kaatuvaKirjanpito =
            object : ArvioijahakemusRepository(jdbc) {
                override fun merkitseKasitellyksi(
                    hakemusOid: String,
                    henkiloOid: String,
                    arvioijaId: Int?,
                    kaudenAlkupaiva: LocalDate,
                    hetki: OffsetDateTime,
                    syy: String?,
                ): Unit = throw IllegalStateException("kaatui kesken")
            }
        val kaatuva =
            ArvioijahakemusTarkistusService(
                tarkistettavaRepository,
                kaatuvaKirjanpito,
                arvioijaService,
                kausiRepository,
                validation,
                timeService,
                auditLogger,
            )
        porrasta()
        val form = lomake().copy(ashaNumero = asha)

        assertFailsWith<IllegalStateException> { vahvista(form, palvelu = kaatuva) }

        assertEquals(ArvioijahakemuksenTila.KASITTELYSSA, repository.haeKaikki().single().tila)
        assertNull(tarkistettavaRepository.hae("h1"), "keskeytynyt ei nay tarkistettavana")
        assertEquals(Tarkistusvirhe.JoKasittelyssa, vahvista(form).leftOrNull())
        assertEquals(1, kaudet().size)
    }
}
