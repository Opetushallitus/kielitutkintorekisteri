package fi.oph.kitu.yki

import fi.oph.kitu.DBContainerConfiguration
import fi.oph.kitu.TestTimeService
import fi.oph.kitu.oid.Oid
import fi.oph.kitu.util.result.getOrThrow
import fi.oph.kitu.yki.arvioijat.Kausioikeus
import fi.oph.kitu.yki.arvioijat.TallennaKausi
import fi.oph.kitu.yki.arvioijat.YkiArvioijaEntity
import fi.oph.kitu.yki.arvioijat.YkiArvioijaKausiService
import fi.oph.kitu.yki.arvioijat.YkiArvioijaRepository
import fi.oph.kitu.yki.arvioijat.YkiArviointioikeusEntity
import fi.oph.kitu.yki.arvioijat.solki.Lahetystulos
import fi.oph.kitu.yki.arvioijat.solki.SolkiArvioijaService
import org.junit.jupiter.api.BeforeEach
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Import
import org.springframework.context.annotation.Primary
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.transaction.PlatformTransactionManager
import org.springframework.transaction.TransactionDefinition
import org.springframework.transaction.support.TransactionTemplate
import java.time.Instant
import java.time.LocalDate
import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * Kirjaa jokaisen lahetyksen kohdalta, nakyyko kausimuutos jo omasta transaktiostaan: se on tosi
 * vain jos alkuperainen transaktio on ehtinyt commitoida.
 *
 * `isActualTransactionActive` ei kelpaa mittariksi — Spring tyhjentaa lipun vasta afterCommitin
 * jalkeen, joten se on tosi myos oikein toimivassa toteutuksessa.
 *
 * Toteuttaa rajapinnan suoraan: @WithSpan-annotoidusta kantaluokasta perittaessa CGLIB-proxy
 * peittaisi konstruktorin kentat.
 */
class TransaktiotaValvovaSolki(
    private val omaTransaktio: TransactionTemplate,
    private val jdbcTemplate: JdbcTemplate,
) : SolkiArvioijaService {
    override fun lahetaArvioija(arvioija: YkiArvioijaEntity): Lahetystulos {
        kausiNakyvissaLahetettaessa +=
            omaTransaktio.execute {
                jdbcTemplate.queryForObject(
                    "SELECT count(*) FROM yki_arvioija_arviointikausi WHERE alkupaiva = ?",
                    Int::class.java,
                    LocalDate.of(2026, 2, 1),
                )!! > 0
            } == true
        return Lahetystulos.EI_KAYTOSSA
    }

    override fun lahetaArvioijaKasin(arvioija: YkiArvioijaEntity): Lahetystulos = lahetaArvioija(arvioija)

    override fun lahetaLahettamattomat(maxYritykset: Int?): Int = 0

    companion object {
        val kausiNakyvissaLahetettaessa = mutableListOf<Boolean>()
    }
}

@TestConfiguration(proxyBeanMethods = false)
class ValvovaSolkiConfiguration {
    @Bean
    @Primary
    fun transaktiotaValvovaSolki(
        transactionManager: PlatformTransactionManager,
        jdbcTemplate: JdbcTemplate,
    ): TransaktiotaValvovaSolki =
        TransaktiotaValvovaSolki(
            TransactionTemplate(transactionManager).apply {
                propagationBehavior = TransactionDefinition.PROPAGATION_REQUIRES_NEW
            },
            jdbcTemplate,
        )
}

/**
 * Lahetys on HTTP-kutsu: transaktion sisalla se pitaisi yki_arvioija-rivin lukkoa ja poolatun
 * yhteyden auki koko etakutsun ajan, jolloin Solkin hidastuminen sarjallistaisi virkailijat.
 */
@SpringBootTest(properties = ["kitu.yki.arvioijarekisteri.integraatio.enabled=true"])
@Import(DBContainerConfiguration::class, ValvovaSolkiConfiguration::class)
class YkiArvioijaKausiLahetysTest(
    @param:Autowired private val repository: YkiArvioijaRepository,
    @param:Autowired private val kausiService: YkiArvioijaKausiService,
    @param:Autowired private val timeService: TestTimeService,
) {
    private val petro = "1.2.246.562.24.59267607404"

    @BeforeEach
    fun setup() {
        repository.deleteAll()
        TransaktiotaValvovaSolki.kausiNakyvissaLahetettaessa.clear()
        repository.tallenna(arvioija())
    }

    @Test
    fun `lahetys tapahtuu vasta commitin jalkeen`() {
        val id = repository.findByArvioijaOid(Oid.parse(petro).getOrThrow())!!.id!!.toInt()

        timeService.runWithFixedClock(Instant.parse("2026-06-01T09:00:00Z")) {
            kausiService.lisaaKausi(
                id,
                TallennaKausi(
                    arvioijaId = id,
                    kausiId = null,
                    alkupaiva = LocalDate.of(2026, 2, 1),
                    arviointioikeudet = listOf(Kausioikeus(Tutkintokieli.FIN, setOf(Tutkintotaso.PT))),
                    ashaNumero = null,
                ),
                null,
            )
        }

        assertEquals(
            listOf(true),
            TransaktiotaValvovaSolki.kausiNakyvissaLahetettaessa,
            "lahetyksen on tapahduttava tasan kerran ja vasta kun kausimuutos on commitoitu",
        )
    }

    private fun arvioija() =
        YkiArvioijaEntity(
            id = null,
            arvioijaOid = Oid.parse(petro).getOrThrow(),
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
        )
}
