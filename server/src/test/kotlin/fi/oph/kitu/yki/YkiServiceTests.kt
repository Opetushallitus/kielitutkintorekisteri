package fi.oph.kitu.yki

import fi.oph.kitu.DBContainerConfiguration
import fi.oph.kitu.auditlogs.OpenTelemetryTestConfig
import fi.oph.kitu.oid.Oid
import fi.oph.kitu.tiedontuontischema.Henkilo
import fi.oph.kitu.tiedontuontischema.Henkilosuoritus
import fi.oph.kitu.tiedontuontischema.Lahdejarjestelma
import fi.oph.kitu.tiedontuontischema.LahdejarjestelmanTunniste
import fi.oph.kitu.tiedontuontischema.YkiJarjestaja
import fi.oph.kitu.tiedontuontischema.YkiOsa
import fi.oph.kitu.tiedontuontischema.YkiSuoritus
import fi.oph.kitu.tiedontuontischema.YkiTarkastusarviointi
import fi.oph.kitu.tyhjennaYkiSuoritukset
import fi.oph.kitu.util.result.getOrThrow
import fi.oph.kitu.yki.arvioijat.YkiArvioijaRepository
import fi.oph.kitu.yki.suoritukset.Todistuskieli
import fi.oph.kitu.yki.suoritukset.YkiSuoritusEntity
import fi.oph.kitu.yki.suoritukset.YkiSuoritusFilter
import fi.oph.kitu.yki.suoritukset.YkiSuoritusRepository
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.jdbc.core.JdbcTemplate
import java.time.Instant
import java.time.LocalDate
import kotlin.test.assertEquals

@SpringBootTest
@Import(OpenTelemetryTestConfig::class, DBContainerConfiguration::class)
class YkiServiceTests(
    @param:Autowired private val ykiSuoritusRepository: YkiSuoritusRepository,
    @param:Autowired private val jdbcTemplate: JdbcTemplate,
    @param:Autowired private val ykiArvioijaRepository: YkiArvioijaRepository,
    @param:Autowired private val inMemorySpanExporter: InMemorySpanExporter,
) {
    @BeforeEach
    fun nukeDb() {
        ykiArvioijaRepository.deleteAll()
        jdbcTemplate.tyhjennaYkiSuoritukset()
        inMemorySpanExporter.reset()
    }

    @Test
    fun `Saving same suoritus multiple times with only different last modified will save only one history version`() {
        val oppijanumero = "1.2.246.562.24.11113355246"
        val suoritus =
            Henkilosuoritus(
                henkilo =
                    Henkilo(
                        oid = Oid.parse(oppijanumero).getOrThrow(),
                        etunimet = "Ranja Testi",
                        sukunimi = "Öhman-Testi",
                        hetu = "010180-9026",
                        sukupuoli = Sukupuoli.N,
                        kansalaisuus = "EST",
                        katuosoite = "Testikuja 5",
                        postinumero = "40100",
                        postitoimipaikka = "Testilä",
                        email = "testi@testi.fi",
                    ),
                suoritus =
                    YkiSuoritus(
                        tutkintotaso = Tutkintotaso.YT,
                        kieli = Tutkintokieli.FIN,
                        todistuskieli = Todistuskieli.FIN,
                        jarjestaja =
                            YkiJarjestaja(
                                oid = Oid.parse("1.2.246.562.10.14893989377").getOrThrow(),
                                nimi = "Jyväskylän yliopisto, Soveltavan kielentutkimuksen keskus",
                            ),
                        tutkintopaiva = LocalDate.of(2024, 9, 1),
                        arviointipaiva = LocalDate.of(2024, 12, 13),
                        arviointitila = Arviointitila.ARVIOITU,
                        osat =
                            listOf(
                                YkiOsa(
                                    tyyppi = TutkinnonOsa.puhuminen,
                                    arvosana = 5,
                                ),
                                YkiOsa(
                                    tyyppi = TutkinnonOsa.puheenYmmartaminen,
                                    arvosana = 5,
                                ),
                                YkiOsa(
                                    tyyppi = TutkinnonOsa.kirjoittaminen,
                                    arvosana = 5,
                                ),
                                YkiOsa(
                                    tyyppi = TutkinnonOsa.tekstinYmmartaminen,
                                    arvosana = 5,
                                ),
                                YkiOsa(
                                    tyyppi = TutkinnonOsa.rakenteetJaSanasto,
                                    arvosana = 5,
                                ),
                                YkiOsa(
                                    tyyppi = TutkinnonOsa.yleisarvosana,
                                    arvosana = 5,
                                ),
                            ),
                        tarkistusarviointi =
                            YkiTarkastusarviointi(
                                saapumispaiva = LocalDate.of(2024, 12, 14),
                                kasittelypaiva = LocalDate.of(2024, 12, 14),
                                asiatunnus = "OPH-5000-1234",
                                tarkistusarvioidutOsakokeet = listOf(TutkinnonOsa.puhuminen),
                                arvosanaMuuttui = listOf(TutkinnonOsa.puhuminen),
                                perustelu =
                                    "Suorituksesta jäänyt viimeinen tehtävä arvioimatta. Arvioinnin jälkeen puhumisen taitotasoa 6.",
                            ),
                        lahdejarjestelmanId =
                            LahdejarjestelmanTunniste(
                                id = "13032026",
                                lahde = Lahdejarjestelma.Solki,
                            ),
                    ),
            )

        val entity = YkiSuoritusEntity.from(suoritus)

        ykiSuoritusRepository.save(entity.copy(lastModified = Instant.now().minusSeconds(100000)), false)
        ykiSuoritusRepository.save(entity.copy(lastModified = Instant.now().minusSeconds(1000)), false)
        ykiSuoritusRepository.save(entity.copy(lastModified = Instant.now()), false)

        val suoritushistory =
            ykiSuoritusRepository.find(
                YkiSuoritusFilter.from(search = oppijanumero),
                distinct = false,
            )
        assertEquals(1, suoritushistory.count())
    }
}
