package fi.oph.kitu.yki.arvioijat.hakemus

import arrow.core.Either
import arrow.core.left
import arrow.core.nonEmptyListOf
import fi.oph.kitu.DBContainerConfiguration
import fi.oph.kitu.TestTimeService
import fi.oph.kitu.ataru.SiirtoJasennysvirhe
import fi.oph.kitu.auditlogs.AuditLogger
import fi.oph.kitu.oid.Oid
import fi.oph.kitu.oppijanumero.EmptyRequest
import fi.oph.kitu.oppijanumero.OppijanumeroException
import fi.oph.kitu.oppijanumero.OppijanumeroService
import fi.oph.kitu.tyhjennaArvioijahakemukset
import fi.oph.kitu.util.defaultObjectMapper
import fi.oph.kitu.yki.Tutkintokieli
import fi.oph.kitu.yki.Tutkintotaso
import fi.oph.kitu.yki.arvioijat.PaivitaArvioijanTiedotValidation
import fi.oph.kitu.yki.arvioijat.TallennaArvioija
import fi.oph.kitu.yki.arvioijat.TallennaArvioijaValidation
import fi.oph.kitu.yki.arvioijat.YkiArvioijaKausiRepository
import fi.oph.kitu.yki.arvioijat.YkiArvioijaRepository
import fi.oph.kitu.yki.arvioijat.YkiArvioijaService
import fi.oph.kitu.yki.arvioijat.hakemus.ArvioijahakemuksenVirhetyyppi.HENKILOA_EI_LOYDY
import fi.oph.kitu.yki.arvioijat.hakemus.ArvioijahakemuksenVirhetyyppi.KIELELTA_PUUTTUU_TASO
import fi.oph.kitu.yki.arvioijat.hakemus.ArvioijahakemuksenVirhetyyppi.TUNTEMATON_VAIHTOEHTO
import fi.oph.kitu.yki.arvioijat.hakemus.ArvioijahakemuksenVirhetyyppi.VAARA_TIETOTYYPPI
import fi.oph.kitu.yki.arvioijat.hakemus.ArvioijahakemuksenVirhetyyppi.VALIDOINTIVIRHE
import fi.oph.kitu.yki.arvioijat.hakemus.ArvioijahakemuksenVirhetyyppi.VIRHEELLINEN_RAKENNE
import fi.oph.kitu.yki.arvioijat.hakemus.ArvioijahakemusTestidata.HETKI
import fi.oph.kitu.yki.arvioijat.hakemus.ArvioijahakemusTestidata.MYOHEMMIN
import fi.oph.kitu.yki.arvioijat.hakemus.ArvioijahakemusTestidata.OSOITTEETON_OID
import fi.oph.kitu.yki.arvioijat.hakemus.ArvioijahakemusTestidata.PETRO_OID
import fi.oph.kitu.yki.arvioijat.hakemus.ArvioijahakemusTestidata.TANAAN
import fi.oph.kitu.yki.arvioijat.hakemus.ArvioijahakemusTestidata.TUNTEMATON_OID
import fi.oph.kitu.yki.arvioijat.hakemus.ArvioijahakemusTestidata.YKSILOIMATON_OID
import fi.oph.kitu.yki.arvioijat.hakemus.ArvioijahakemusTestidata.kirjaaKasitellyksi
import fi.oph.kitu.yki.arvioijat.hakemus.ArvioijahakemusTestidata.lomake
import fi.oph.kitu.yki.arvioijat.hakemus.ArvioijahakemusTestidata.oid
import fi.oph.kitu.yki.arvioijat.hakemus.ArvioijahakemusTestidata.siirtoHakemus
import fi.oph.kitu.yki.arvioijat.hakemus.ArvioijahakemusTestidata.tarkistettava
import fi.oph.kitu.yki.arvioijat.solki.SolkiArvioijaService
import org.junit.jupiter.api.BeforeEach
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.jdbc.core.JdbcTemplate
import java.time.Instant
import java.time.ZoneOffset
import kotlin.test.Test
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNull

@SpringBootTest
@Import(DBContainerConfiguration::class)
class ArvioijahakemusHakuServiceTest(
    @param:Autowired private val repository: ArvioijahakemusRepository,
    @param:Autowired private val tarkistettavaRepository: ArvioijahakemusTarkistettavaRepository,
    @param:Autowired private val virheRepository: ArvioijahakemusVirheRepository,
    @param:Autowired private val jdbcTemplate: JdbcTemplate,
    @param:Autowired private val arvioijaRepository: YkiArvioijaRepository,
    @param:Autowired private val kausiRepository: YkiArvioijaKausiRepository,
    @param:Autowired private val arvioijaService: YkiArvioijaService,
    @param:Autowired private val auditLogger: AuditLogger,
) {
    private val ataru = FakeAtaruClient()

    @BeforeEach
    fun setup() {
        jdbcTemplate.tyhjennaArvioijahakemukset()
        arvioijaRepository.deleteAll()
        ataru.tyhjenna()
    }

    private fun hae(
        timeService: TestTimeService,
        asetukset: ArvioijahakemusLomake = lomake,
        arvioijat: YkiArvioijaService = arvioijaService,
        kello: Instant = HETKI,
    ): Hakuyhteenveto =
        timeService.runWithFixedClock(kello) {
            ArvioijahakemusHakuService(
                ataru,
                asetukset,
                repository,
                tarkistettavaRepository,
                virheRepository,
                arvioijat,
                timeService,
                auditLogger,
            ).hae()
        }

    private fun virheet() = virheRepository.haeKaikki().associateBy { it.hakemusOid }

    private fun tarkistettavat() = tarkistettavaRepository.haeKaikki().associateBy { it.hakemusOid }

    private fun tarkistettaviaKannassa() =
        jdbcTemplate.queryForObject("SELECT count(*) FROM yki_arvioijahakemus_tarkistettava", Long::class.java)

    @Test
    fun `uusi hakemus tulee tarkistettavaksi eika arvioijaa luoda`(
        @Autowired timeService: TestTimeService,
    ) {
        ataru.hakemukset =
            listOf(siirtoHakemus("h1", PETRO_OID, """{"kielet_0":"0","tasot_0":"0","email":"hakemus@oph.fi"}"""))

        assertEquals(Hakuyhteenveto(tarkistettavia = 1), hae(timeService))

        val rivi = tarkistettavat().getValue("h1")
        assertEquals(PETRO_OID, rivi.arvioijaOid)
        assertEquals("Kivinen-Testi", rivi.sukunimi)
        assertEquals("Petro Testi", rivi.etunimet)
        assertEquals(
            Arvioijahakemus(
                arviointioikeudet =
                    listOf(
                        TallennaArvioija.Arviointioikeus(Tutkintokieli.FIN, setOf(Tutkintotaso.PT, Tutkintotaso.KT)),
                    ),
                sahkopostiosoite = "hakemus@oph.fi",
                katuosoite = null,
                postinumero = null,
                postitoimipaikka = null,
            ),
            rivi.hakemus,
            "vain hakemuksen omat yhteystiedot talletetaan",
        )
        assertNull(arvioijaRepository.findByArvioijaOid(oid(PETRO_OID)))
        assertEquals(emptyList(), repository.haeKaikki())
        assertEquals(listOf("lomake"), ataru.listauskutsut)
    }

    @Test
    fun `puutteellinen lomakeasetus kaataa ajon ennen atarun kutsumista`(
        @Autowired timeService: TestTimeService,
    ) {
        ataru.hakemukset = listOf(siirtoHakemus("h1", PETRO_OID))

        val virhe =
            assertFailsWith<IllegalStateException> {
                hae(timeService, asetukset = ArvioijahakemusLomake(lomakkeenAvain = "lomake", kielikysymys = "kielet"))
            }

        assertContains(virhe.message.orEmpty(), "kielet")
        assertEquals(emptyList(), ataru.listauskutsut)
        assertEquals(emptyMap(), tarkistettavat())
    }

    @Test
    fun `tarkistettava haetaan joka haulla uudelleen ja sen tiedot paivittyvat`(
        @Autowired timeService: TestTimeService,
    ) {
        ataru.hakemukset = listOf(siirtoHakemus("h1", PETRO_OID))
        hae(timeService)

        ataru.hakemukset =
            listOf(siirtoHakemus("h1", PETRO_OID, """{"kielet_0":"0","tasot_1":"1","address":"Katu 9"}"""))
        assertEquals(Hakuyhteenveto(tarkistettavia = 1), hae(timeService, kello = MYOHEMMIN))

        assertEquals(listOf("h1"), ataru.siirtokutsut.last())
        val rivi = tarkistettavat().getValue("h1")
        assertEquals("Katu 9", rivi.hakemus.katuosoite)
        assertEquals(
            listOf(TallennaArvioija.Arviointioikeus(Tutkintokieli.FIN, setOf(Tutkintotaso.YT))),
            rivi.hakemus.arviointioikeudet,
        )
        assertEquals(HETKI, rivi.ensimmainenHaku.toInstant())
        assertEquals(MYOHEMMIN, rivi.viimeisinHaku.toInstant())
    }

    @Test
    fun `vahvistettua hakemusta ei haeta eika palauteta tarkistettavaksi`(
        @Autowired timeService: TestTimeService,
    ) {
        repository.kirjaaKasitellyksi("h1")
        ataru.hakemukset = listOf(siirtoHakemus("h1", PETRO_OID))

        assertEquals(Hakuyhteenveto(), hae(timeService))

        assertEquals(emptyList(), ataru.siirtokutsut, "siirtoa ei kutsuta kun uusia hakemuksia ei ole")
        assertEquals(emptyMap(), tarkistettavat())
    }

    @Test
    fun `haku ei kirjoita kasittelyssa olevan tai kasitellyn hakemuksen yli`() {
        val hetki = HETKI.atOffset(ZoneOffset.UTC)
        repository.varaa("h1", PETRO_OID, TANAAN, hetki)
        repository.kirjaaKasitellyksi("h2")

        repository.odottaaYksilointia("h1", YKSILOIMATON_OID, "syy", hetki)
        repository.odottaaYksilointia("h2", YKSILOIMATON_OID, "syy", hetki)
        tarkistettavaRepository.tallenna(tarkistettava("h1"), hetki)
        tarkistettavaRepository.tallenna(tarkistettava("h2"), hetki)

        val rivit = repository.haeKaikki().associateBy { it.hakemusOid }
        assertEquals(ArvioijahakemuksenTila.KASITTELYSSA, rivit.getValue("h1").tila)
        assertEquals(PETRO_OID, rivit.getValue("h1").henkiloOid)
        assertEquals(ArvioijahakemuksenTila.KASITELTY, rivit.getValue("h2").tila)
        assertEquals(0, tarkistettaviaKannassa())
    }

    @Test
    fun `kaatumisesta jaaneet virhe- ja tarkistettavarivit poistetaan kun hakemuksella on kirjanpitorivi`(
        @Autowired timeService: TestTimeService,
    ) {
        val hetki = HETKI.atOffset(ZoneOffset.UTC)
        tarkistettavaRepository.tallenna(tarkistettava("h1"), hetki)
        virheRepository.tallenna("h1", PETRO_OID, nonEmptyListOf(ArvioijahakemuksenVirhe(VALIDOINTIVIRHE, "x")), hetki)
        repository.varaa("h1", PETRO_OID, TANAAN, hetki)
        assertNull(tarkistettavaRepository.hae("h1"), "kirjanpitorivi piilottaa tarkistettavan")
        ataru.hakemukset = listOf(siirtoHakemus("h1", PETRO_OID))

        hae(timeService)

        assertEquals(emptyMap(), virheet())
        assertEquals(0, tarkistettaviaKannassa())
        assertEquals(ArvioijahakemuksenTila.KASITTELYSSA, repository.haeKaikki().single().tila)
    }

    @Test
    fun `yksiloimaton hakemus odottaa ja haetaan uudelleen jokaisella ajolla`(
        @Autowired timeService: TestTimeService,
    ) {
        ataru.hakemukset = listOf(siirtoHakemus("h1", YKSILOIMATON_OID))

        assertEquals(Hakuyhteenveto(odottaaYksilointia = 1), hae(timeService))
        val rivi = repository.haeKasittelemattomat().single()
        assertEquals(ArvioijahakemuksenTila.ODOTTAA_YKSILOINTIA, rivi.tila)
        assertContains(rivi.syy.orEmpty(), "yksilöity")

        ataru.hakemukset = listOf(siirtoHakemus("h1", YKSILOIMATON_OID), siirtoHakemus("h2", PETRO_OID))
        assertEquals(Hakuyhteenveto(tarkistettavia = 1, odottaaYksilointia = 1), hae(timeService))

        assertEquals(listOf("h1", "h2"), ataru.siirtokutsut.last(), "odottavat haetaan uudelleen")
        assertEquals(setOf("h2"), tarkistettavat().keys)
    }

    @Test
    fun `kartoitusvirhe kirjataan virheeksi ja hakemus yritetaan joka ajolla uudelleen`(
        @Autowired timeService: TestTimeService,
    ) {
        ataru.hakemukset =
            listOf(siirtoHakemus("h1", PETRO_OID, """{"kielet_0":"0","tasot_2":"2","email":["a@oph.fi","b@oph.fi"]}"""))

        assertEquals(Hakuyhteenveto(virheellisia = 1), hae(timeService))

        assertEquals(emptyList(), repository.haeKaikki())
        assertEquals(emptyMap(), tarkistettavat())
        val virhe = virheet().getValue("h1")
        assertEquals(PETRO_OID, virhe.henkiloOid)
        assertEquals(
            listOf(
                ArvioijahakemuksenVirhe(TUNTEMATON_VAIHTOEHTO, "tasot_2", defaultObjectMapper.valueToTree("2")),
                ArvioijahakemuksenVirhe(KIELELTA_PUUTTUU_TASO, "tasot"),
                ArvioijahakemuksenVirhe(
                    VAARA_TIETOTYYPPI,
                    "email",
                    defaultObjectMapper.valueToTree(listOf("a@oph.fi", "b@oph.fi")),
                ),
            ),
            virhe.virheet,
        )
        assertEquals(1, virhe.yrityksia)

        assertEquals(Hakuyhteenveto(virheellisia = 1), hae(timeService, kello = MYOHEMMIN))

        assertEquals(listOf("h1"), ataru.siirtokutsut.last(), "virheellinen hakemus haetaan uudelleen")
        val toinen = virheet().getValue("h1")
        assertEquals(2, toinen.yrityksia)
        assertEquals(HETKI, toinen.ensimmainenYritys.toInstant())
        assertEquals(MYOHEMMIN, toinen.viimeisinYritys.toInstant())
    }

    @Test
    fun `korjatun kartoituksen jalkeen hakemus tulee tarkistettavaksi ja virhe poistuu`(
        @Autowired timeService: TestTimeService,
    ) {
        ataru.hakemukset = listOf(siirtoHakemus("h1", PETRO_OID, """{"kielet_0":"0","tasot_2":"2"}"""))
        hae(timeService)

        val korjattu =
            ArvioijahakemusLomake(
                lomakkeenAvain = "lomake",
                kielikysymys = "kielet",
                kielet =
                    mapOf(
                        "0" to
                            ArvioijahakemusLomake.Kielivaihtoehto(
                                Tutkintokieli.FIN,
                                "tasot",
                                mapOf("0" to setOf(Tutkintotaso.PT), "2" to setOf(Tutkintotaso.YT)),
                            ),
                    ),
            )

        assertEquals(Hakuyhteenveto(tarkistettavia = 1), hae(timeService, asetukset = korjattu))

        assertEquals(emptyMap(), virheet())
        assertEquals(setOf("h1"), tarkistettavat().keys)
    }

    @Test
    fun `tarkistettava siirtyy virheisiin kun hakemuksesta tulee virheellinen`(
        @Autowired timeService: TestTimeService,
    ) {
        ataru.hakemukset = listOf(siirtoHakemus("h1", PETRO_OID))
        hae(timeService)

        ataru.hakemukset = listOf(siirtoHakemus("h1", PETRO_OID, """{"kielet_0":"0","tasot_2":"2"}"""))
        assertEquals(Hakuyhteenveto(virheellisia = 1), hae(timeService))

        assertEquals(emptyMap(), tarkistettavat())
        assertEquals(setOf("h1"), virheet().keys)
    }

    @Test
    fun `henkilo jota ONR ei tunne kirjataan virheeksi kunnes henkilo loytyy`(
        @Autowired timeService: TestTimeService,
    ) {
        ataru.hakemukset = listOf(siirtoHakemus("h1", TUNTEMATON_OID))

        assertEquals(Hakuyhteenveto(virheellisia = 1), hae(timeService))
        assertEquals(listOf(ArvioijahakemuksenVirhe(HENKILOA_EI_LOYDY)), virheet().getValue("h1").virheet)

        ataru.hakemukset = listOf(siirtoHakemus("h1", PETRO_OID))

        assertEquals(Hakuyhteenveto(tarkistettavia = 1), hae(timeService))
        assertEquals(emptyMap(), virheet())
    }

    @Test
    fun `puuttuva osoite ei esta tarkistettavaksi tuloa`(
        @Autowired timeService: TestTimeService,
    ) {
        ataru.hakemukset =
            listOf(siirtoHakemus("h1", OSOITTEETON_OID, """{"kielet_0":"0","tasot_0":"0","address":"Katu 1"}"""))

        assertEquals(Hakuyhteenveto(tarkistettavia = 1), hae(timeService))

        val rivi = tarkistettavat().getValue("h1")
        assertEquals("Katu 1", rivi.hakemus.katuosoite)
        assertNull(rivi.hakemus.postinumero)
        assertEquals(emptyMap(), virheet())
    }

    @Test
    fun `ohimeneva ONR-virhe ei muuta kirjattua virhetta`(
        @Autowired timeService: TestTimeService,
        @Autowired tallennaValidation: TallennaArvioijaValidation,
        @Autowired paivitaValidation: PaivitaArvioijanTiedotValidation,
        @Autowired oppijanumeroService: OppijanumeroService,
        @Autowired solki: SolkiArvioijaService,
    ) {
        ataru.hakemukset = listOf(siirtoHakemus("h1", TUNTEMATON_OID))
        hae(timeService)

        val ohimenevaOnr =
            object : OppijanumeroService by oppijanumeroService {
                override fun getOppijanumero(henkiloOid: Oid): Either<OppijanumeroException, Oid> =
                    OppijanumeroException.NullResponse(EmptyRequest()).left()
            }
        val arvioijat =
            YkiArvioijaService(
                arvioijaRepository,
                kausiRepository,
                tallennaValidation,
                paivitaValidation,
                ohimenevaOnr,
                auditLogger,
                timeService,
                solki,
            )

        assertEquals(
            Hakuyhteenveto(yritetaanUudelleen = 1),
            hae(timeService, arvioijat = arvioijat, kello = MYOHEMMIN),
        )

        val virhe = virheet().getValue("h1")
        assertEquals(1, virhe.yrityksia)
        assertEquals(listOf(ArvioijahakemuksenVirhe(HENKILOA_EI_LOYDY)), virhe.virheet)
    }

    @Test
    fun `listaukselta poistunut hakemus poistetaan kaikista odottavista tiloista mutta tyhja listaus ei poista mitaan`(
        @Autowired timeService: TestTimeService,
    ) {
        repository.kirjaaKasitellyksi("h0")
        ataru.hakemukset =
            listOf(
                siirtoHakemus("h1", TUNTEMATON_OID),
                siirtoHakemus("h2", PETRO_OID),
                siirtoHakemus("h3", YKSILOIMATON_OID),
            )
        hae(timeService)

        ataru.hakemukset = emptyList()
        hae(timeService)
        assertEquals(setOf("h1"), virheet().keys, "tyhja listaus voi johtua puuttuvasta oikeudesta")
        assertEquals(setOf("h2"), tarkistettavat().keys)
        assertEquals(
            setOf("h0", "h3"),
            repository.haeKaikki().map { it.hakemusOid }.toSet(),
        )

        ataru.hakemukset = listOf(siirtoHakemus("h4", PETRO_OID))
        hae(timeService)
        assertEquals(emptyMap(), virheet())
        assertEquals(setOf("h4"), tarkistettavat().keys)
        assertEquals(
            listOf("h0" to ArvioijahakemuksenTila.KASITELTY),
            repository.haeKaikki().map { it.hakemusOid to it.tila },
            "kasiteltya ei poisteta",
        )
    }

    @Test
    fun `siirron rakennevirhe kirjataan hakemukselle ja tunnistamaton kaataa ajon muita estamatta`(
        @Autowired timeService: TestTimeService,
    ) {
        ataru.hakemukset = listOf(siirtoHakemus("h1", PETRO_OID))
        ataru.jasentymattomat =
            listOf(
                SiirtoJasennysvirhe("h2", "1.2.246.562.24.2", "keyValues"),
                SiirtoJasennysvirhe("h3", null, "personOid"),
                SiirtoJasennysvirhe(null, null, ""),
            )

        assertFailsWith<IllegalStateException> { hae(timeService) }

        assertEquals(setOf("h1"), tarkistettavat().keys)
        val virheet = virheet()
        assertEquals(listOf(ArvioijahakemuksenVirhe(VIRHEELLINEN_RAKENNE, "keyValues")), virheet.getValue("h2").virheet)
        assertEquals("1.2.246.562.24.2", virheet.getValue("h2").henkiloOid)
        assertEquals(listOf(ArvioijahakemuksenVirhe(VIRHEELLINEN_RAKENNE, "personOid")), virheet.getValue("h3").virheet)
        assertEquals(null, virheet.getValue("h3").henkiloOid)
    }

    @Test
    fun `yksilointia odottanut hakemus siirtyy virheisiin ja vartioitu poisto ei koske kasiteltyyn`(
        @Autowired timeService: TestTimeService,
    ) {
        ataru.hakemukset = listOf(siirtoHakemus("h1", YKSILOIMATON_OID))
        hae(timeService)
        assertEquals(ArvioijahakemuksenTila.ODOTTAA_YKSILOINTIA, repository.haeKaikki().single().tila)

        ataru.hakemukset = listOf(siirtoHakemus("h1", YKSILOIMATON_OID, """{"kielet_0":"0","tasot_2":"2"}"""))
        assertEquals(Hakuyhteenveto(virheellisia = 1), hae(timeService))

        assertEquals(emptyList(), repository.haeKaikki())
        assertEquals(setOf("h1"), virheet().keys)

        repository.kirjaaKasitellyksi("h2")
        repository.poista("h2", ArvioijahakemuksenTila.ODOTTAA_YKSILOINTIA)
        assertEquals(ArvioijahakemuksenTila.KASITELTY, repository.haeKaikki().single().tila)
    }

    @Test
    fun `virheisiin ei talleteta hetua eika henkilotunnuskenttaa`(
        @Autowired timeService: TestTimeService,
    ) {
        val hetu = "010101-123N"
        val asetukset =
            ArvioijahakemusLomake(
                lomakkeenAvain = lomake.lomakkeenAvain,
                kielikysymys = lomake.kielikysymys,
                kielet = lomake.kielet,
                kentat = ArvioijahakemusLomake.Yhteystietokentat(sahkoposti = "ssn"),
            )
        ataru.hakemukset =
            listOf(
                siirtoHakemus("h1", PETRO_OID, """{"kielet_0":"0","tasot_0":"0","kielet_7":"$hetu","ssn":["$hetu"]}"""),
            )

        assertEquals(Hakuyhteenveto(virheellisia = 1), hae(timeService, asetukset = asetukset))

        val tallennettu =
            jdbcTemplate.queryForObject("SELECT virheet::text FROM yki_arvioijahakemus_virhe", String::class.java)!!
        assertFalse(tallennettu.contains(hetu), tallennettu)
        assertEquals(
            listOf(TUNTEMATON_VAIHTOEHTO, VAARA_TIETOTYYPPI),
            virheet().getValue("h1").virheet.map { it.tyyppi },
        )
    }
}
