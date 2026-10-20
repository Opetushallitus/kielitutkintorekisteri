package fi.oph.kitu.yki.arvioijat.hakemus

import arrow.core.Either
import arrow.core.left
import arrow.core.nonEmptyListOf
import arrow.core.right
import fi.oph.kitu.DBContainerConfiguration
import fi.oph.kitu.TestTimeService
import fi.oph.kitu.ataru.AtaruClient
import fi.oph.kitu.ataru.AtaruException
import fi.oph.kitu.ataru.HakemusOtsake
import fi.oph.kitu.ataru.OptionAnswer
import fi.oph.kitu.ataru.SiirtoHakemus
import fi.oph.kitu.ataru.SiirtoJasennysvirhe
import fi.oph.kitu.ataru.StatesAndFilters
import fi.oph.kitu.auditlogs.AuditLogger
import fi.oph.kitu.oid.Oid
import fi.oph.kitu.oppijanumero.EmptyRequest
import fi.oph.kitu.oppijanumero.OppijanumeroException
import fi.oph.kitu.oppijanumero.OppijanumeroService
import fi.oph.kitu.tyhjennaArvioijahakemukset
import fi.oph.kitu.util.defaultObjectMapper
import fi.oph.kitu.util.result.getOrThrow
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
import fi.oph.kitu.yki.arvioijat.hakemus.ArvioijahakemuksenVirhetyyppi.OSOITE_PUUTTUU
import fi.oph.kitu.yki.arvioijat.hakemus.ArvioijahakemuksenVirhetyyppi.TUNTEMATON_VAIHTOEHTO
import fi.oph.kitu.yki.arvioijat.hakemus.ArvioijahakemuksenVirhetyyppi.VAARA_TIETOTYYPPI
import fi.oph.kitu.yki.arvioijat.hakemus.ArvioijahakemuksenVirhetyyppi.VALIDOINTIVIRHE
import fi.oph.kitu.yki.arvioijat.hakemus.ArvioijahakemuksenVirhetyyppi.VIRHEELLINEN_RAKENNE
import fi.oph.kitu.yki.arvioijat.solki.SolkiArvioijaService
import org.junit.jupiter.api.BeforeEach
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import java.time.Instant
import java.time.LocalDate
import java.time.ZoneOffset
import kotlin.test.Test
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNotNull

@SpringBootTest
@Import(DBContainerConfiguration::class)
class ArvioijahakemusTuontiServiceTest(
    @param:Autowired private val repository: ArvioijahakemusRepository,
    @param:Autowired private val virheRepository: ArvioijahakemusVirheRepository,
    @param:Autowired private val jdbcTemplate: JdbcTemplate,
    @param:Autowired private val arvioijaRepository: YkiArvioijaRepository,
    @param:Autowired private val kausiRepository: YkiArvioijaKausiRepository,
    @param:Autowired private val arvioijaService: YkiArvioijaService,
    @param:Autowired private val auditLogger: AuditLogger,
) {
    /** Mock-ONR:n henkilo, jolla on osoite ja sahkoposti. */
    private val petronOid = "1.2.246.562.24.59267607404"

    /** Mock-ONR tuntee taman henkilon, mutta hanella ei ole oppijanumeroa. */
    private val yksiloimatonOid = "1.2.246.562.24.10691606777"

    /** Mock-ONR:n henkilo ilman yhteystietoja. */
    private val osoitteetonOid = "1.2.246.562.24.20281155246"

    /** Mock-ONR ei tunne tata henkiloa. */
    private val tuntematonOid = "1.2.246.562.24.00000000001"

    private val hetki = Instant.parse("2026-06-01T09:00:00Z")
    private val myohemmin = Instant.parse("2026-06-02T09:00:00Z")
    private val tanaan = LocalDate.of(2026, 6, 1)

    private val ataru = FakeAtaruClient()

    private val lomake =
        ArvioijahakemusLomake(
            lomakkeenAvain = "lomake",
            kielikysymys = "kielet",
            kielet =
                mapOf(
                    "0" to
                        ArvioijahakemusLomake.Kielivaihtoehto(
                            Tutkintokieli.FIN,
                            "tasot",
                            mapOf("0" to setOf(Tutkintotaso.PT, Tutkintotaso.KT), "1" to setOf(Tutkintotaso.YT)),
                        ),
                ),
        )

    @BeforeEach
    fun setup() {
        jdbcTemplate.tyhjennaArvioijahakemukset()
        arvioijaRepository.deleteAll()
        ataru.hakemukset = emptyList()
        ataru.jasentymattomat = emptyList()
        ataru.siirtokutsut.clear()
        ataru.listauskutsut.clear()
    }

    private fun tuo(
        timeService: TestTimeService,
        kirjanpito: ArvioijahakemusRepository = repository,
        asetukset: ArvioijahakemusLomake = lomake,
        arvioijat: YkiArvioijaService = arvioijaService,
        kello: Instant = hetki,
    ): Tuontiyhteenveto =
        timeService.runWithFixedClock(kello) {
            ArvioijahakemusTuontiService(
                ataru,
                asetukset,
                kirjanpito,
                virheRepository,
                arvioijat,
                kausiRepository,
                timeService,
                auditLogger,
            ).tuo()
        }

    private fun hakemus(
        oid: String,
        personOid: String,
        keyValues: String = """{"kielet_0":"0","tasot_0":"0"}""",
    ) = SiirtoHakemus(
        hakemusOid = oid,
        personOid = personOid,
        keyValues =
            defaultObjectMapper
                .readTree(keyValues)
                .properties()
                .associate { it.key to it.value },
    )

    private fun virheet() = virheRepository.haeKaikki().associateBy { it.hakemusOid }

    @Test
    fun `uudesta hakemuksesta syntyy arvioija ja kausi joka alkaa tanaan`(
        @Autowired timeService: TestTimeService,
    ) {
        ataru.hakemukset =
            listOf(
                hakemus(
                    "h1",
                    petronOid,
                    """{"kielet_0":"0","tasot_0":"0","email":"hakemus@oph.fi"}""",
                ),
            )

        val yhteenveto = tuo(timeService)

        assertEquals(Tuontiyhteenveto(kasitelty = 1), yhteenveto)
        val arvioija = assertNotNull(arvioijaRepository.findByArvioijaOid(oid(petronOid)))
        assertEquals("hakemus@oph.fi", arvioija.sahkopostiosoite, "hakemuksen yhteystieto voittaa ONR:n")
        assertEquals("Kivinenkatu 2 A 3", arvioija.katuosoite, "puuttuva yhteystieto taydennetaan ONR:sta")
        assertEquals(listOf(tanaan), kausiRepository.findKaudet(arvioija.id!!.toInt()).map { it.alkupaiva })

        val rivi = repository.haeKaikki().single()
        assertEquals(ArvioijahakemuksenTila.KASITELTY, rivi.tila)
        assertEquals(arvioija.id?.toInt(), rivi.arvioijaId)
        assertEquals(listOf("lomake"), ataru.listauskutsut)
    }

    @Test
    fun `puutteellinen lomakeasetus kaataa ajon ennen atarun kutsumista`(
        @Autowired timeService: TestTimeService,
    ) {
        ataru.hakemukset = listOf(hakemus("h1", petronOid))

        val virhe =
            assertFailsWith<IllegalStateException> {
                tuo(timeService, asetukset = ArvioijahakemusLomake(lomakkeenAvain = "lomake", kielikysymys = "kielet"))
            }

        assertContains(virhe.message.orEmpty(), "kielet")
        assertEquals(emptyList(), ataru.listauskutsut)
        assertEquals(emptyList(), repository.haeKaikki())
    }

    @Test
    fun `kasiteltya hakemusta ei haeta eika kasitella uudelleen`(
        @Autowired timeService: TestTimeService,
    ) {
        ataru.hakemukset = listOf(hakemus("h1", petronOid))
        tuo(timeService)

        val toinen = tuo(timeService)

        assertEquals(Tuontiyhteenveto(), toinen)
        assertEquals(1, ataru.siirtokutsut.size, "siirtoa ei kutsuta kun uusia hakemuksia ei ole")
    }

    @Test
    fun `jatkokausi alkaa voimassa olevan kauden jalkeen ja yhteystiedot ylikirjoitetaan`(
        @Autowired timeService: TestTimeService,
    ) {
        val edellinenAlku = LocalDate.of(2021, 9, 1)
        timeService.runWithFixedClock(hetki) {
            arvioijaService
                .luoArvioija(
                    TallennaArvioija(
                        arvioijaOid = oid(petronOid),
                        sukunimi = "Kivinen-Testi",
                        etunimet = "Petro Testi",
                        sahkopostiosoite = null,
                        katuosoite = "Vanha katu 1",
                        postinumero = "00100",
                        postitoimipaikka = "HELSINKI",
                        kaudenAlkupaiva = edellinenAlku,
                        ashaNumero = null,
                        arviointioikeudet =
                            listOf(TallennaArvioija.Arviointioikeus(Tutkintokieli.FIN, setOf(Tutkintotaso.PT))),
                    ),
                    tekija = null,
                ).getOrThrow()
        }
        ataru.hakemukset =
            listOf(
                hakemus(
                    "h1",
                    petronOid,
                    """{"kielet_0":"0","tasot_0":"0","address":"Uusi katu 2"}""",
                ),
            )

        assertEquals(Tuontiyhteenveto(kasitelty = 1), tuo(timeService))

        val arvioija = assertNotNull(arvioijaRepository.findByArvioijaOid(oid(petronOid)))
        assertEquals("Uusi katu 2", arvioija.katuosoite)
        assertEquals(
            listOf(edellinenAlku, LocalDate.of(2026, 9, 1)),
            kausiRepository.findKaudet(arvioija.id!!.toInt()).map { it.alkupaiva }.sorted(),
        )
    }

    private fun luoOlemassaoleva(
        timeService: TestTimeService,
        alkupaiva: LocalDate,
        automaattinenJatkokausi: Boolean = false,
    ) = timeService.runWithFixedClock(hetki) {
        arvioijaService.luoArvioija(
            TallennaArvioija(
                arvioijaOid = oid(petronOid),
                sukunimi = "Kivinen-Testi",
                etunimet = "Petro Testi",
                sahkopostiosoite = null,
                katuosoite = "Vanha katu 1",
                postinumero = "00100",
                postitoimipaikka = "HELSINKI",
                kaudenAlkupaiva = alkupaiva,
                ashaNumero = null,
                arviointioikeudet = listOf(TallennaArvioija.Arviointioikeus(Tutkintokieli.FIN, setOf(Tutkintotaso.PT))),
                automaattinenJatkokausi = automaattinenJatkokausi,
            ),
            tekija = null,
        )
    }

    @Test
    fun `automaattinen jatkokausi saa alkaa yli vuoden paasta`(
        @Autowired timeService: TestTimeService,
    ) {
        val edellinenAlku = LocalDate.of(2025, 1, 1)
        luoOlemassaoleva(timeService, edellinenAlku).getOrThrow()
        ataru.hakemukset = listOf(hakemus("h1", petronOid))

        assertEquals(Tuontiyhteenveto(kasitelty = 1), tuo(timeService))

        val arvioija = assertNotNull(arvioijaRepository.findByArvioijaOid(oid(petronOid)))
        assertEquals(
            listOf(edellinenAlku, LocalDate.of(2030, 1, 1)),
            kausiRepository.findKaudet(arvioija.id!!.toInt()).map { it.alkupaiva }.sorted(),
        )
    }

    @Test
    fun `jatkokausilippu ei ohita vuoden rajaa ilman edellista kautta`(
        @Autowired timeService: TestTimeService,
    ) {
        assertNotNull(
            luoOlemassaoleva(timeService, tanaan.plusYears(3), automaattinenJatkokausi = true).leftOrNull(),
            "uudella arvioijalla ei ole edellista kautta",
        )

        luoOlemassaoleva(timeService, LocalDate.of(2025, 1, 1)).getOrThrow()
        assertNotNull(
            luoOlemassaoleva(timeService, LocalDate.of(2030, 6, 1), automaattinenJatkokausi = true).leftOrNull(),
            "alkupaivan on oltava heti edellisen kauden jalkeen",
        )
    }

    @Test
    fun `yksiloimaton hakemus odottaa ja haetaan uudelleen jokaisella ajolla`(
        @Autowired timeService: TestTimeService,
    ) {
        ataru.hakemukset = listOf(hakemus("h1", yksiloimatonOid))

        assertEquals(Tuontiyhteenveto(odottaaYksilointia = 1), tuo(timeService))
        val rivi = repository.haeKasittelemattomat().single()
        assertEquals(ArvioijahakemuksenTila.ODOTTAA_YKSILOINTIA, rivi.tila)
        assertContains(rivi.syy.orEmpty(), "yksilöity")

        ataru.hakemukset = listOf(hakemus("h1", yksiloimatonOid), hakemus("h2", petronOid))
        assertEquals(Tuontiyhteenveto(kasitelty = 1, odottaaYksilointia = 1), tuo(timeService))

        assertEquals(listOf("h1", "h2"), ataru.siirtokutsut.last(), "odottavat haetaan uudelleen")
        assertEquals(ArvioijahakemuksenTila.KASITELTY, repository.haeKaikki().single { it.hakemusOid == "h2" }.tila)
        assertNotNull(arvioijaRepository.findByArvioijaOid(oid(petronOid)))
    }

    @Test
    fun `kartoitusvirhe kirjataan virheeksi ja hakemus yritetaan joka ajolla uudelleen`(
        @Autowired timeService: TestTimeService,
    ) {
        ataru.hakemukset =
            listOf(hakemus("h1", petronOid, """{"kielet_0":"0","tasot_2":"2","email":["a@oph.fi","b@oph.fi"]}"""))

        assertEquals(Tuontiyhteenveto(virhe = 1), tuo(timeService))

        assertEquals(emptyList(), repository.haeKaikki())
        val virhe = virheet().getValue("h1")
        assertEquals(petronOid, virhe.henkiloOid)
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

        assertEquals(Tuontiyhteenveto(virhe = 1), tuo(timeService, kello = myohemmin))

        assertEquals(listOf("h1"), ataru.siirtokutsut.last(), "virheellinen hakemus haetaan uudelleen")
        val toinen = virheet().getValue("h1")
        assertEquals(2, toinen.yrityksia)
        assertEquals(hetki, toinen.ensimmainenYritys.toInstant())
        assertEquals(myohemmin, toinen.viimeisinYritys.toInstant())
    }

    @Test
    fun `korjatun kartoituksen jalkeen hakemus kasitellaan ja virhe poistuu`(
        @Autowired timeService: TestTimeService,
    ) {
        ataru.hakemukset = listOf(hakemus("h1", petronOid, """{"kielet_0":"0","tasot_2":"2"}"""))
        tuo(timeService)

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

        assertEquals(Tuontiyhteenveto(kasitelty = 1), tuo(timeService, asetukset = korjattu))

        assertEquals(emptyMap(), virheet())
        assertEquals(ArvioijahakemuksenTila.KASITELTY, repository.haeKaikki().single().tila)
    }

    @Test
    fun `henkilo jota ONR ei tunne kirjataan virheeksi kunnes henkilo loytyy`(
        @Autowired timeService: TestTimeService,
    ) {
        ataru.hakemukset = listOf(hakemus("h1", tuntematonOid))

        assertEquals(Tuontiyhteenveto(virhe = 1), tuo(timeService))
        assertEquals(listOf(ArvioijahakemuksenVirhe(HENKILOA_EI_LOYDY)), virheet().getValue("h1").virheet)

        ataru.hakemukset = listOf(hakemus("h1", petronOid))

        assertEquals(Tuontiyhteenveto(kasitelty = 1), tuo(timeService))
        assertEquals(emptyMap(), virheet())
    }

    @Test
    fun `osoite joka puuttuu seka hakemukselta etta oppijanumerorekisterista kirjataan virheeksi`(
        @Autowired timeService: TestTimeService,
    ) {
        ataru.hakemukset =
            listOf(hakemus("h1", osoitteetonOid, """{"kielet_0":"0","tasot_0":"0","address":"Katu 1"}"""))

        assertEquals(Tuontiyhteenveto(virhe = 1), tuo(timeService))

        assertEquals(
            listOf(ArvioijahakemuksenVirhe(OSOITE_PUUTTUU, "postinumero, postitoimipaikka")),
            virheet().getValue("h1").virheet,
        )
        assertEquals(emptyList(), repository.haeKaikki())
    }

    @Test
    fun `tallennuksen validointivirhe poistaa keskeneraisen kirjauksen ja kirjataan virheeksi`(
        @Autowired timeService: TestTimeService,
    ) {
        ataru.hakemukset = listOf(hakemus("h1", petronOid, """{"kielet_0":"0","tasot_0":"0","postal-code":"abc"}"""))

        assertEquals(Tuontiyhteenveto(virhe = 1), tuo(timeService))

        assertEquals(emptyList(), repository.haeKaikki(), "KASITTELYSSA-rivi poistetaan")
        assertEquals(null, arvioijaRepository.findByArvioijaOid(oid(petronOid)))
        assertEquals(
            listOf(
                ArvioijahakemuksenVirhe(
                    VALIDOINTIVIRHE,
                    "postinumero",
                    viesti = "Postinumeron on oltava viisi numeroa",
                ),
            ),
            virheet().getValue("h1").virheet,
        )

        tuo(timeService, kello = myohemmin)

        assertEquals(2, virheet().getValue("h1").yrityksia)
    }

    @Test
    fun `ohimeneva ONR-virhe ei muuta kirjattua virhetta`(
        @Autowired timeService: TestTimeService,
        @Autowired tallennaValidation: TallennaArvioijaValidation,
        @Autowired paivitaValidation: PaivitaArvioijanTiedotValidation,
        @Autowired oppijanumeroService: OppijanumeroService,
        @Autowired solki: SolkiArvioijaService,
    ) {
        ataru.hakemukset = listOf(hakemus("h1", tuntematonOid))
        tuo(timeService)

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
            Tuontiyhteenveto(yritetaanUudelleen = 1),
            tuo(timeService, arvioijat = arvioijat, kello = myohemmin),
        )

        val virhe = virheet().getValue("h1")
        assertEquals(1, virhe.yrityksia)
        assertEquals(listOf(ArvioijahakemuksenVirhe(HENKILOA_EI_LOYDY)), virhe.virheet)
    }

    @Test
    fun `listaukselta poistuneen hakemuksen virhe poistetaan mutta tyhja listaus ei poista mitaan`(
        @Autowired timeService: TestTimeService,
    ) {
        ataru.hakemukset = listOf(hakemus("h1", tuntematonOid))
        tuo(timeService)

        ataru.hakemukset = emptyList()
        tuo(timeService)
        assertEquals(setOf("h1"), virheet().keys, "tyhja listaus voi johtua puuttuvasta oikeudesta")

        ataru.hakemukset = listOf(hakemus("h2", petronOid))
        tuo(timeService)
        assertEquals(emptyMap(), virheet())
    }

    @Test
    fun `kaatumisesta jaanyt virherivi poistetaan kun hakemuksella on kirjanpitorivi`(
        @Autowired timeService: TestTimeService,
    ) {
        repository.tallenna(
            ArvioijahakemusEntity(
                hakemusOid = "h1",
                henkiloOid = petronOid,
                tila = ArvioijahakemuksenTila.KASITTELYSSA,
                syy = null,
                arvioijaId = null,
                kaudenAlkupaiva = tanaan,
                kasitelty = hetki.atOffset(ZoneOffset.UTC),
            ),
        )
        virheRepository.tallenna(
            "h1",
            petronOid,
            nonEmptyListOf(ArvioijahakemuksenVirhe(VALIDOINTIVIRHE, "postinumero")),
            hetki.atOffset(ZoneOffset.UTC),
        )
        ataru.hakemukset = listOf(hakemus("h1", petronOid))

        tuo(timeService)

        assertEquals(emptyMap(), virheet())
        assertEquals(ArvioijahakemuksenTila.KASITTELYSSA, repository.haeKaikki().single().tila)
    }

    @Test
    fun `siirron rakennevirhe kirjataan hakemukselle ja tunnistamaton kaataa ajon muita estamatta`(
        @Autowired timeService: TestTimeService,
    ) {
        ataru.hakemukset = listOf(hakemus("h1", petronOid))
        ataru.jasentymattomat =
            listOf(
                SiirtoJasennysvirhe("h2", "1.2.246.562.24.2", "keyValues"),
                SiirtoJasennysvirhe("h3", null, "personOid"),
                SiirtoJasennysvirhe(null, null, ""),
            )

        assertFailsWith<IllegalStateException> { tuo(timeService) }

        assertEquals(ArvioijahakemuksenTila.KASITELTY, repository.haeKaikki().single().tila)
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
        ataru.hakemukset = listOf(hakemus("h1", yksiloimatonOid))
        tuo(timeService)
        assertEquals(ArvioijahakemuksenTila.ODOTTAA_YKSILOINTIA, repository.haeKaikki().single().tila)

        ataru.hakemukset = listOf(hakemus("h1", yksiloimatonOid, """{"kielet_0":"0","tasot_2":"2"}"""))
        assertEquals(Tuontiyhteenveto(virhe = 1), tuo(timeService))

        assertEquals(emptyList(), repository.haeKaikki())
        assertEquals(setOf("h1"), virheet().keys)

        ataru.hakemukset = listOf(hakemus("h2", petronOid))
        tuo(timeService)
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
            listOf(hakemus("h1", petronOid, """{"kielet_0":"0","tasot_0":"0","kielet_7":"$hetu","ssn":["$hetu"]}"""))

        assertEquals(Tuontiyhteenveto(virhe = 1), tuo(timeService, asetukset = asetukset))

        val tallennettu =
            jdbcTemplate.queryForObject("SELECT virheet::text FROM yki_arvioijahakemus_virhe", String::class.java)!!
        assertFalse(tallennettu.contains(hetu), tallennettu)
        assertEquals(
            listOf(TUNTEMATON_VAIHTOEHTO, VAARA_TIETOTYYPPI),
            virheet().getValue("h1").virheet.map { it.tyyppi },
        )
    }

    @Test
    fun `kaatuminen arvioijan tallennuksen jalkeen ei esta muita eika luo toista kautta`(
        @Autowired timeService: TestTimeService,
        @Autowired jdbc: NamedParameterJdbcTemplate,
    ) {
        val kaatuvaKirjanpito =
            object : ArvioijahakemusRepository(jdbc) {
                override fun tallenna(hakemus: ArvioijahakemusEntity) {
                    if (hakemus.hakemusOid == "h1" && hakemus.tila == ArvioijahakemuksenTila.KASITELTY) {
                        throw IllegalStateException("kaatui kesken")
                    }
                    super.tallenna(hakemus)
                }
            }
        ataru.hakemukset =
            listOf(
                hakemus("h1", petronOid),
                hakemus(
                    "h2",
                    osoitteetonOid,
                    """{"kielet_0":"0","tasot_0":"0","address":"Katu 1",
                        "postal-code":"00100","postal-office":"HELSINKI"}""",
                ),
            )

        assertFailsWith<IllegalStateException> { tuo(timeService, kaatuvaKirjanpito) }

        val rivit = repository.haeKaikki().associateBy { it.hakemusOid }
        assertEquals(ArvioijahakemuksenTila.KASITTELYSSA, rivit["h1"]?.tila, "keskeytynyt jaa nakyviin")
        assertEquals(ArvioijahakemuksenTila.KASITELTY, rivit["h2"]?.tila, "kaatuminen ei esta seuraavaa hakemusta")

        tuo(timeService)

        val arvioija = assertNotNull(arvioijaRepository.findByArvioijaOid(oid(petronOid)))
        assertEquals(1, kausiRepository.findKaudet(arvioija.id!!.toInt()).size, "keskeytynytta ei kasitella uudelleen")
        assertEquals(ArvioijahakemuksenTila.KASITTELYSSA, repository.haeKaikki().single { it.hakemusOid == "h1" }.tila)
    }

    private fun oid(s: String): Oid = Oid.parse(s).getOrThrow()

    private class FakeAtaruClient : AtaruClient {
        var hakemukset: List<SiirtoHakemus> = emptyList()
        var jasentymattomat: List<SiirtoJasennysvirhe> = emptyList()
        val siirtokutsut = mutableListOf<List<String>>()
        val listauskutsut = mutableListOf<String>()

        override fun haeHakemusavaimet(
            formKey: String,
            ehdot: List<OptionAnswer>,
            tilat: StatesAndFilters,
        ): Either<AtaruException, List<HakemusOtsake>> {
            listauskutsut += formKey
            return (hakemukset.map { it.hakemusOid } + jasentymattomat.mapNotNull { it.hakemusOid })
                .map { HakemusOtsake(it) }
                .right()
        }

        override fun haeHakemukset(
            avaimet: List<String>,
        ): Either<AtaruException, List<Either<SiirtoJasennysvirhe, SiirtoHakemus>>> {
            siirtokutsut += avaimet
            return (
                hakemukset.filter { it.hakemusOid in avaimet }.map { it.right() } +
                    jasentymattomat.filter { it.hakemusOid == null || it.hakemusOid in avaimet }.map { it.left() }
            ).right()
        }
    }
}
