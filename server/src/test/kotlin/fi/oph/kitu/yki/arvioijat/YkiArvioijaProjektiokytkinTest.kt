package fi.oph.kitu.yki.arvioijat

import fi.oph.kitu.DBContainerConfiguration
import fi.oph.kitu.TestTimeService
import fi.oph.kitu.oid.Oid
import fi.oph.kitu.util.result.getOrThrow
import fi.oph.kitu.yki.Tutkintokieli
import fi.oph.kitu.yki.Tutkintotaso
import fi.oph.kitu.yki.arvioijat.Kausioikeus
import fi.oph.kitu.yki.arvioijat.YkiArvioijaEntity
import fi.oph.kitu.yki.arvioijat.YkiArvioijaKausiRepository
import fi.oph.kitu.yki.arvioijat.YkiArvioijaKausiService
import fi.oph.kitu.yki.arvioijat.YkiArvioijaRepository
import fi.oph.kitu.yki.arvioijat.YkiArviointioikeusEntity
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import java.time.Instant
import java.time.LocalDate
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

private val HETKI: Instant = Instant.parse("2026-06-01T09:00:00Z")

/** Projektiossa oleva kausi on paattynyt; masteriin on lisatty uudempi, joka on viela voimassa. */
private fun seedaaVanhentunutProjektio(
    repository: YkiArvioijaRepository,
    kausiRepository: YkiArvioijaKausiRepository,
): Int {
    repository.deleteAll()
    val oid = Oid.parse("1.2.246.562.24.59267607404").getOrThrow()
    val arvioijaId =
        repository.tallenna(
            YkiArvioijaEntity(
                id = null,
                arvioijaOid = oid,
                henkilotunnus = null,
                sukunimi = "Kivinen-Testi",
                etunimet = "Petro Testi",
                sahkopostiosoite = null,
                katuosoite = "Testikuja 5",
                postinumero = "40100",
                postitoimipaikka = "Testilä",
                arviointioikeudet =
                    listOf(
                        YkiArviointioikeusEntity(
                            id = null,
                            arvioijaId = null,
                            kieli = Tutkintokieli.FIN,
                            tasot = setOf(Tutkintotaso.PT),
                            tila = null,
                            kaudenAlkupaiva = LocalDate.of(2021, 1, 1),
                            kaudenPaattymispaiva = LocalDate.of(2026, 1, 1),
                            jatkorekisterointi = false,
                            ensimmainenRekisterointipaiva = LocalDate.of(2021, 1, 1),
                            rekisteriintuontiaika = null,
                        ),
                    ),
            ),
        )

    // Suoraan masteriin: repository ei kirjoita projektiota, joten se jaa vanhentuneeksi.
    kausiRepository.lisaaKausi(
        arvioijaId = arvioijaId,
        alkupaiva = LocalDate.of(2026, 2, 1),
        paattymispaiva = LocalDate.of(2031, 2, 1),
        oikeudet = listOf(Kausioikeus(Tutkintokieli.FIN, setOf(Tutkintotaso.PT))),
        ashaNumero = null,
        tekija = null,
    )
    return arvioijaId
}

/**
 * Portin merkitys: integraation ollessa pois Solki kirjoittaa yha koko payloadin
 * yki_arviointioikeuteen koskematta kausimasteriin, joten taulut ovat taatusti eri linjoilla.
 * Yliajo kirjoittaisi koko rekisterin vanhentuneesta masterista ja tyontaisi sen Solki-jonoon.
 */
@SpringBootTest(properties = ["kitu.yki.arvioijarekisteri.integraatio.enabled=false"])
@Import(DBContainerConfiguration::class)
class YkiArvioijaProjektiokytkinPoisTest(
    @param:Autowired private val repository: YkiArvioijaRepository,
    @param:Autowired private val kausiRepository: YkiArvioijaKausiRepository,
    @param:Autowired private val kausiService: YkiArvioijaKausiService,
    @param:Autowired private val timeService: TestTimeService,
) {
    @Test
    fun `kytkin pois jattaa projektion koskematta`() {
        val arvioijaId = seedaaVanhentunutProjektio(repository, kausiRepository)

        timeService.runWithFixedClock(HETKI) {
            assertEquals(0, kausiService.paivitaProjektiot())
        }

        val oikeus = kausiRepository.findArviointioikeudet(arvioijaId).single()
        assertEquals(
            LocalDate.of(2021, 1, 1),
            oikeus.kaudenAlkupaiva,
            "vanhentunut projektio on jaava tila: Solki on yha master",
        )
        assertEquals(
            LocalDate.of(2026, 1, 1),
            oikeus.kaudenPaattymispaiva,
            "yksikaan projektion kentta ei saa muuttua",
        )
    }
}

@SpringBootTest(properties = ["kitu.yki.arvioijarekisteri.integraatio.enabled=true"])
@Import(DBContainerConfiguration::class)
class YkiArvioijaProjektiokytkinPaallaTest(
    @param:Autowired private val repository: YkiArvioijaRepository,
    @param:Autowired private val kausiRepository: YkiArvioijaKausiRepository,
    @param:Autowired private val kausiService: YkiArvioijaKausiService,
    @param:Autowired private val timeService: TestTimeService,
) {
    @Test
    fun `kytkin paalla paivittaa projektion ja palauttaa rivin lahetysjonoon`() {
        val arvioijaId = seedaaVanhentunutProjektio(repository, kausiRepository)

        timeService.runWithFixedClock(HETKI) {
            assertEquals(1, kausiService.paivitaProjektiot())
        }

        val oikeus = kausiRepository.findArviointioikeudet(arvioijaId).single()
        assertEquals(
            LocalDate.of(2026, 2, 1),
            oikeus.kaudenAlkupaiva,
            "projektio seuraa voimassa olevaa kautta",
        )
        assertTrue(
            repository.findLahetettavat().any { it.id?.toInt() == arvioijaId },
            "muuttunut projektio on lahetettava Solkiin",
        )
    }
}
