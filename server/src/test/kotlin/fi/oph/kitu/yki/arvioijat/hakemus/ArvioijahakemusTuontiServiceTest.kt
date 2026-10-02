package fi.oph.kitu.yki.arvioijat.hakemus

import arrow.core.Either
import arrow.core.right
import fi.oph.kitu.DBContainerConfiguration
import fi.oph.kitu.TestTimeService
import fi.oph.kitu.ataru.AtaruClient
import fi.oph.kitu.ataru.AtaruException
import fi.oph.kitu.ataru.HakemusOtsake
import fi.oph.kitu.ataru.OptionAnswer
import fi.oph.kitu.ataru.SiirtoHakemus
import fi.oph.kitu.auditlogs.AuditLogger
import fi.oph.kitu.oid.Oid
import fi.oph.kitu.util.defaultObjectMapper
import fi.oph.kitu.util.result.getOrThrow
import fi.oph.kitu.yki.Tutkintokieli
import fi.oph.kitu.yki.Tutkintotaso
import fi.oph.kitu.yki.arvioijat.TallennaArvioija
import fi.oph.kitu.yki.arvioijat.YkiArvioijaKausiRepository
import fi.oph.kitu.yki.arvioijat.YkiArvioijaRepository
import fi.oph.kitu.yki.arvioijat.YkiArvioijaService
import fi.oph.kitu.yki.arvioijat.hakemus.ArvioijahakemusLomake.Kentat
import org.junit.jupiter.api.BeforeEach
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import java.time.Instant
import java.time.LocalDate
import kotlin.test.Test
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

@SpringBootTest
@Import(DBContainerConfiguration::class)
class ArvioijahakemusTuontiServiceTest(
    @param:Autowired private val repository: ArvioijahakemusRepository,
    @param:Autowired private val arvioijaRepository: YkiArvioijaRepository,
    @param:Autowired private val kausiRepository: YkiArvioijaKausiRepository,
    @param:Autowired private val arvioijaService: YkiArvioijaService,
    @param:Autowired private val auditLogger: AuditLogger,
) {
    /** Mock-ONR:n henkilo, jolla on osoite ja sahkoposti. */
    private val petronOid = "1.2.246.562.24.59267607404"

    /** Mock-ONR tuntee taman henkilon, mutta hanella ei ole oppijanumeroa. */
    private val yksiloimatonOid = "1.2.246.562.24.10691606777"

    private val hetki = Instant.parse("2026-06-01T09:00:00Z")
    private val tanaan = LocalDate.of(2026, 6, 1)

    private val ataru = FakeAtaruClient()

    @BeforeEach
    fun setup() {
        repository.deleteAll()
        arvioijaRepository.deleteAll()
        ataru.hakemukset = emptyList()
        ataru.siirtokutsut.clear()
    }

    private fun tuo(timeService: TestTimeService): Tuontiyhteenveto =
        timeService.runWithFixedClock(hetki) {
            ArvioijahakemusTuontiService(
                ataru,
                repository,
                arvioijaService,
                kausiRepository,
                timeService,
                auditLogger,
            ).tuo()
        }

    private fun hakemus(
        oid: String,
        personOid: String?,
        keyValues: String = """{"${Kentat.KIELET}":["fin"],"${Kentat.TASOT}":["PT","KT"]}""",
        state: String = "active",
    ) = SiirtoHakemus(
        hakemusOid = oid,
        personOid = personOid,
        state = state,
        keyValues =
            defaultObjectMapper
                .readTree(keyValues)
                .properties()
                .associate { it.key to it.value },
    )

    @Test
    fun `uudesta hakemuksesta syntyy arvioija ja kausi joka alkaa tanaan`(
        @Autowired timeService: TestTimeService,
    ) {
        ataru.hakemukset =
            listOf(
                hakemus(
                    "h1",
                    petronOid,
                    """{"${Kentat.KIELET}":["fin"],"${Kentat.TASOT}":["PT"],"${Kentat.SAHKOPOSTI}":"hakemus@oph.fi"}""",
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
                    """{"${Kentat.KIELET}":["fin"],"${Kentat.TASOT}":["PT"],"${Kentat.KATUOSOITE}":"Uusi katu 2"}""",
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
    fun `yksiloimaton hakija ja ehtoja tayttamaton hakemus kirjataan tarkistettaviksi`(
        @Autowired timeService: TestTimeService,
    ) {
        ataru.hakemukset =
            listOf(
                hakemus("h1", yksiloimatonOid),
                hakemus("h2", petronOid, state = "inactivated"),
            )

        assertEquals(Tuontiyhteenveto(hylatty = 1, eiTaytaEhtoja = 1), tuo(timeService))

        val rivit = repository.haeKasittelemattomat().associateBy { it.hakemusOid }
        assertEquals(ArvioijahakemuksenTila.HYLATTY, rivit["h1"]?.tila)
        assertContains(rivit["h1"]?.syy.orEmpty(), "yksilöity")
        assertEquals(ArvioijahakemuksenTila.EI_TAYTA_EHTOJA, rivit["h2"]?.tila)
        assertEquals(null, arvioijaRepository.findByArvioijaOid(oid(petronOid)))
    }

    private fun oid(s: String): Oid = Oid.parse(s).getOrThrow()

    private class FakeAtaruClient : AtaruClient {
        var hakemukset: List<SiirtoHakemus> = emptyList()
        val siirtokutsut = mutableListOf<List<String>>()

        override fun haeHakemusavaimet(
            formKey: String,
            ehdot: List<OptionAnswer>,
        ): Either<AtaruException, List<HakemusOtsake>> = hakemukset.map { HakemusOtsake(it.hakemusOid) }.right()

        override fun haeHakemukset(avaimet: List<String>): Either<AtaruException, List<SiirtoHakemus>> {
            siirtokutsut += avaimet
            return hakemukset.filter { it.hakemusOid in avaimet }.right()
        }
    }
}
