package fi.oph.kitu.vkt

import fi.oph.kitu.DBContainerConfiguration
import fi.oph.kitu.dev.mockdata.VktSuoritusMockGenerator
import fi.oph.kitu.jdbc.SortDirection
import fi.oph.kitu.koodisto.Koodisto
import fi.oph.kitu.oid.Oid
import fi.oph.kitu.tiedontuontischema.VktValidation
import fi.oph.kitu.util.result.getOrThrow
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.assertAll
import org.junit.jupiter.api.assertNull
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.testcontainers.postgresql.PostgreSQLContainer
import java.time.LocalDate
import kotlin.jvm.optionals.getOrNull
import kotlin.test.Test
import kotlin.test.assertEquals

@SpringBootTest
@Import(DBContainerConfiguration::class)
class VktSuoritusRepositoryTest(
    @param:Autowired private var repository: VktSuoritusRepository,
    @param:Autowired private var customRepository: CustomVktSuoritusRepository,
    @param:Autowired private var postgres: PostgreSQLContainer,
    @param:Autowired private val vktValidation: VktValidation,
) {
    @BeforeEach
    fun nukeDb() {
        repository.deleteAll()
    }

    @Test
    fun `save VKT suoritus`() {
        val suoritus =
            VktSuoritusEntity(
                ilmoittautumisenId = "1",
                suorittajanOid = Oid.parse("1.2.246.562.24.12345678910").getOrThrow(),
                etunimet = "Testi",
                sukunimi = "Testinen",
                tutkintokieli = Koodisto.Tutkintokieli.FIN,
                suorituspaikkakunta = "Helsinki",
                taitotaso = Koodisto.VktTaitotaso.Erinomainen,
                suorituksenVastaanottaja = null,
                osakokeet =
                    setOf(
                        VktSuoritusEntity.VktOsakoe(
                            tyyppi = Koodisto.VktOsakoe.Puhuminen,
                            tutkintopaiva = LocalDate.of(2025, 1, 1),
                            arviointipaiva = null,
                            arvosana = null,
                            merkittyPoistettavaksi = null,
                        ),
                        VktSuoritusEntity.VktOsakoe(
                            tyyppi = Koodisto.VktOsakoe.PuheenYmmärtäminen,
                            tutkintopaiva = LocalDate.of(2025, 1, 1),
                            arviointipaiva = null,
                            arvosana = null,
                            merkittyPoistettavaksi = null,
                        ),
                    ),
                tutkinnot =
                    setOf(
                        VktSuoritusEntity.VktTutkinto(
                            tyyppi = Koodisto.VktKielitaito.Suullinen,
                            arviointipaiva = null,
                            arvosana = null,
                        ),
                    ),
            )
        val suoritusId = repository.save(suoritus).id!!
        val savedSuoritus = repository.findById(suoritusId).getOrNull()!!
        assertEquals(
            suoritus,
            savedSuoritus.copy(
                id = null,
                osakokeet = savedSuoritus.osakokeet.map { it.copy(id = null) }.toSet(),
                tutkinnot = savedSuoritus.tutkinnot.map { it.copy(id = null) }.toSet(),
                createdAt = null,
            ),
        )
    }

    @Test
    fun `number of rows returned for list view equals number returned by count function`() {
        val generator = VktSuoritusMockGenerator()
        repository.saveAll(List(1000) { generator.generateRandomVktSuoritusEntity(vktValidation) })

        val taitotasot =
            listOf(
                listOf(Koodisto.VktTaitotaso.Erinomainen),
                listOf(Koodisto.VktTaitotaso.HyväJaTyydyttävä),
                listOf(Koodisto.VktTaitotaso.Erinomainen, Koodisto.VktTaitotaso.HyväJaTyydyttävä),
            )
        val arvioidut =
            listOf(
                listOf(VktArvioinninTila.ArvioituOsittainTaiKokonaan),
                listOf(VktArvioinninTila.ArviointejaPuuttuu),
                listOf(VktArvioinninTila.ArvioituOsittainTaiKokonaan, VktArvioinninTila.ArviointejaPuuttuu),
                emptyList(),
            )
        val searchQuerys =
            listOf(
                null,
                "aarne",
                "1.4.2020",
            )

        taitotasot.forEach { taitotaso ->
            arvioidut.forEach { arvioidut ->
                searchQuerys.forEach { searchQuery ->
                    val filter = VktSuoritusFilter(search = searchQuery, taitotaso = taitotaso, arvioitu = arvioidut)
                    val order = VktSuoritusOrder(VktSuoritusColumn.Sukunimi, SortDirection.ASC, pageSize = 100000)

                    val suoritukset = customRepository.find(filter, order)
                    val count = customRepository.numberOfRowsForListView(filter)

                    assertEquals(
                        suoritukset.toList().size,
                        count,
                        "taitotaso=$taitotaso, arvioidut=$arvioidut, searchQuery=$searchQuery --> find().size [expected] vs. numberOfRowsForListView() [actual]",
                    )
                }
            }
        }
    }

    @Test
    fun `find suoritukset for csv`() {
        val suoritukset =
            listOf(
                VktSuoritusEntity(
                    ilmoittautumisenId = "1",
                    suorittajanOid = Oid.parse("1.2.246.562.24.12345678910").getOrThrow(),
                    etunimet = "Testi",
                    sukunimi = "Testinen",
                    tutkintokieli = Koodisto.Tutkintokieli.FIN,
                    suorituspaikkakunta = "Helsinki",
                    taitotaso = Koodisto.VktTaitotaso.Erinomainen,
                    suorituksenVastaanottaja = Oid.parse("1.2.246.562.24.91757873900").getOrNull(),
                    osakokeet =
                        setOf(
                            VktSuoritusEntity.VktOsakoe(
                                tyyppi = Koodisto.VktOsakoe.Puhuminen,
                                tutkintopaiva = LocalDate.of(2025, 1, 1),
                                arviointipaiva = LocalDate.of(2025, 1, 2),
                                arvosana = Koodisto.VktArvosana.Erinomainen,
                                merkittyPoistettavaksi = null,
                            ),
                            VktSuoritusEntity.VktOsakoe(
                                tyyppi = Koodisto.VktOsakoe.PuheenYmmärtäminen,
                                tutkintopaiva = LocalDate.of(2025, 1, 1),
                                arviointipaiva = null,
                                arvosana = null,
                                merkittyPoistettavaksi = null,
                            ),
                        ),
                    tutkinnot = setOf(),
                ),
                VktSuoritusEntity(
                    ilmoittautumisenId = "2",
                    suorittajanOid = Oid.parse("1.2.246.562.24.12345678912").getOrThrow(),
                    etunimet = "Maija",
                    sukunimi = "Mehiläinen",
                    tutkintokieli = Koodisto.Tutkintokieli.FIN,
                    suorituspaikkakunta = "Helsinki",
                    taitotaso = Koodisto.VktTaitotaso.Erinomainen,
                    suorituksenVastaanottaja = Oid.parse("1.2.246.562.24.91757873900").getOrNull(),
                    osakokeet =
                        setOf(
                            VktSuoritusEntity.VktOsakoe(
                                tyyppi = Koodisto.VktOsakoe.Puhuminen,
                                tutkintopaiva = LocalDate.of(2025, 1, 1),
                                arviointipaiva = LocalDate.of(2025, 1, 2),
                                arvosana = Koodisto.VktArvosana.Erinomainen,
                                merkittyPoistettavaksi = null,
                            ),
                            VktSuoritusEntity.VktOsakoe(
                                tyyppi = Koodisto.VktOsakoe.PuheenYmmärtäminen,
                                tutkintopaiva = LocalDate.of(2025, 1, 1),
                                arviointipaiva = LocalDate.of(2025, 1, 2),
                                arvosana = Koodisto.VktArvosana.Erinomainen,
                                merkittyPoistettavaksi = null,
                            ),
                            VktSuoritusEntity.VktOsakoe(
                                tyyppi = Koodisto.VktOsakoe.Kirjoittaminen,
                                tutkintopaiva = LocalDate.of(2025, 1, 1),
                                arviointipaiva = LocalDate.of(2025, 1, 2),
                                arvosana = Koodisto.VktArvosana.Hylätty,
                                merkittyPoistettavaksi = null,
                            ),
                            VktSuoritusEntity.VktOsakoe(
                                tyyppi = Koodisto.VktOsakoe.TekstinYmmärtäminen,
                                tutkintopaiva = LocalDate.of(2025, 1, 1),
                                arviointipaiva = LocalDate.of(2025, 1, 2),
                                arvosana = Koodisto.VktArvosana.Erinomainen,
                                merkittyPoistettavaksi = null,
                            ),
                        ),
                    tutkinnot = setOf(),
                ),
            )

        repository.saveAll(suoritukset)
        val suorituksetCsv = getSuoritukset()
        val suoritus1Csv = suorituksetCsv.first { it.ilmoittautumisenId == "1" }
        val suoritus2Csv = suorituksetCsv.first { it.ilmoittautumisenId == "2" }
        assertAll(
            fun() = assertEquals(2, suorituksetCsv.count()),
            fun() = assertEquals("1.2.246.562.24.12345678910", suoritus1Csv.suorittajanOid),
            fun() = assertEquals("Erinomainen", suoritus1Csv.puhuminen),
            fun() = assertNull(suoritus1Csv.puheenYmmartaminen),
            fun() = assertNull(suoritus1Csv.kirjoittaminen),
            fun() = assertNull(suoritus1Csv.tekstinYmmartaminen),
            fun() = assertEquals("1.2.246.562.24.12345678912", suoritus2Csv.suorittajanOid),
            fun() = assertEquals("Erinomainen", suoritus2Csv.puhuminen),
            fun() = assertEquals("Erinomainen", suoritus2Csv.puheenYmmartaminen),
            fun() = assertEquals("Hylätty", suoritus2Csv.kirjoittaminen),
            fun() = assertEquals("Erinomainen", suoritus2Csv.tekstinYmmartaminen),
        )
    }

    @Test
    fun `find suoritukset for csv doesn't return suoritus with no arvosana`() {
        val suoritus =
            VktSuoritusEntity(
                ilmoittautumisenId = "1",
                suorittajanOid = Oid.parse("1.2.246.562.24.12345678910").getOrThrow(),
                etunimet = "Testi",
                sukunimi = "Testinen",
                tutkintokieli = Koodisto.Tutkintokieli.FIN,
                suorituspaikkakunta = "Helsinki",
                taitotaso = Koodisto.VktTaitotaso.Erinomainen,
                suorituksenVastaanottaja = Oid.parse("1.2.246.562.24.91757873900").getOrNull(),
                osakokeet =
                    setOf(
                        VktSuoritusEntity.VktOsakoe(
                            tyyppi = Koodisto.VktOsakoe.Puhuminen,
                            tutkintopaiva = LocalDate.of(2025, 1, 1),
                            arviointipaiva = null,
                            arvosana = null,
                            merkittyPoistettavaksi = null,
                        ),
                        VktSuoritusEntity.VktOsakoe(
                            tyyppi = Koodisto.VktOsakoe.PuheenYmmärtäminen,
                            tutkintopaiva = LocalDate.of(2025, 1, 1),
                            arviointipaiva = null,
                            arvosana = null,
                            merkittyPoistettavaksi = null,
                        ),
                    ),
                tutkinnot =
                    setOf(
                        VktSuoritusEntity.VktTutkinto(
                            tyyppi = Koodisto.VktKielitaito.Suullinen,
                            arviointipaiva = null,
                            arvosana = null,
                        ),
                    ),
            )
        repository.save(suoritus)
        val suoritukset = getSuoritukset()
        assertEquals(0, suoritukset.count())
    }

    @Test
    fun `find suoritukset for csv doesn't return duplicates`() {
        val suoritus =
            VktSuoritusEntity(
                ilmoittautumisenId = "1",
                suorittajanOid = Oid.parse("1.2.246.562.24.12345678910").getOrThrow(),
                etunimet = "Testi",
                sukunimi = "Testinen",
                tutkintokieli = Koodisto.Tutkintokieli.FIN,
                suorituspaikkakunta = "Helsinki",
                taitotaso = Koodisto.VktTaitotaso.Erinomainen,
                suorituksenVastaanottaja = Oid.parse("1.2.246.562.24.91757873900").getOrNull(),
                osakokeet =
                    setOf(
                        VktSuoritusEntity.VktOsakoe(
                            tyyppi = Koodisto.VktOsakoe.Puhuminen,
                            tutkintopaiva = LocalDate.of(2025, 1, 1),
                            arviointipaiva = LocalDate.of(2025, 1, 2),
                            arvosana = Koodisto.VktArvosana.Erinomainen,
                            merkittyPoistettavaksi = null,
                        ),
                        VktSuoritusEntity.VktOsakoe(
                            tyyppi = Koodisto.VktOsakoe.PuheenYmmärtäminen,
                            tutkintopaiva = LocalDate.of(2025, 1, 1),
                            arviointipaiva = LocalDate.of(2025, 1, 2),
                            arvosana = Koodisto.VktArvosana.Erinomainen,
                            merkittyPoistettavaksi = null,
                        ),
                    ),
                tutkinnot = setOf(),
            )
        repository.save(suoritus)
        repository.save(suoritus)
        val suoritukset = getSuoritukset()
        assertEquals(1, suoritukset.count())
    }

    @Test
    fun `find suoritukset for csv returns the newest version of an updated suoritus`() {
        val suoritus =
            VktSuoritusEntity(
                ilmoittautumisenId = "1",
                suorittajanOid = Oid.parse("1.2.246.562.24.12345678910").getOrThrow(),
                etunimet = "Testi",
                sukunimi = "Testinen",
                tutkintokieli = Koodisto.Tutkintokieli.FIN,
                suorituspaikkakunta = "Helsinki",
                taitotaso = Koodisto.VktTaitotaso.Erinomainen,
                suorituksenVastaanottaja = Oid.parse("1.2.246.562.24.91757873900").getOrNull(),
                osakokeet =
                    setOf(
                        VktSuoritusEntity.VktOsakoe(
                            tyyppi = Koodisto.VktOsakoe.Puhuminen,
                            tutkintopaiva = LocalDate.of(2025, 1, 1),
                            arviointipaiva = LocalDate.of(2025, 1, 2),
                            arvosana = Koodisto.VktArvosana.Hylätty,
                            merkittyPoistettavaksi = null,
                        ),
                        VktSuoritusEntity.VktOsakoe(
                            tyyppi = Koodisto.VktOsakoe.PuheenYmmärtäminen,
                            tutkintopaiva = LocalDate.of(2025, 1, 1),
                            arviointipaiva = null,
                            arvosana = null,
                            merkittyPoistettavaksi = null,
                        ),
                    ),
                tutkinnot = setOf(),
            )
        val updated =
            suoritus.copy(
                osakokeet =
                    setOf(
                        VktSuoritusEntity.VktOsakoe(
                            tyyppi = Koodisto.VktOsakoe.Puhuminen,
                            tutkintopaiva = LocalDate.of(2025, 1, 1),
                            arviointipaiva = LocalDate.of(2025, 1, 2),
                            arvosana = Koodisto.VktArvosana.Erinomainen,
                            merkittyPoistettavaksi = null,
                        ),
                        VktSuoritusEntity.VktOsakoe(
                            tyyppi = Koodisto.VktOsakoe.PuheenYmmärtäminen,
                            tutkintopaiva = LocalDate.of(2025, 1, 1),
                            arviointipaiva = LocalDate.of(2025, 1, 2),
                            arvosana = Koodisto.VktArvosana.Erinomainen,
                            merkittyPoistettavaksi = null,
                        ),
                    ),
            )
        repository.save(suoritus)
        repository.save(updated)
        val suoritukset = getSuoritukset()
        assertEquals(1, suoritukset.count())
        assertEquals(Koodisto.VktArvosana.Erinomainen.name, suoritukset.first().puhuminen)
        assertEquals(Koodisto.VktArvosana.Erinomainen.name, suoritukset.first().puheenYmmartaminen)
    }

    @Test
    fun `ArviointejaPuuttuu-suodatin ei palauta suoritusta jos uusin versio on arvioitu`() {
        val suoritus =
            VktSuoritusEntity(
                ilmoittautumisenId = "1",
                suorittajanOid = Oid.parse("1.2.246.562.24.12345678910").getOrThrow(),
                etunimet = "Testi",
                sukunimi = "Testinen",
                tutkintokieli = Koodisto.Tutkintokieli.FIN,
                suorituspaikkakunta = "Helsinki",
                taitotaso = Koodisto.VktTaitotaso.Erinomainen,
                suorituksenVastaanottaja = Oid.parse("1.2.246.562.24.91757873900").getOrNull(),
                osakokeet =
                    setOf(
                        VktSuoritusEntity.VktOsakoe(
                            tyyppi = Koodisto.VktOsakoe.Puhuminen,
                            tutkintopaiva = LocalDate.of(2025, 1, 1),
                            arviointipaiva = null,
                            arvosana = null,
                            merkittyPoistettavaksi = null,
                        ),
                        VktSuoritusEntity.VktOsakoe(
                            tyyppi = Koodisto.VktOsakoe.PuheenYmmärtäminen,
                            tutkintopaiva = LocalDate.of(2025, 1, 1),
                            arviointipaiva = null,
                            arvosana = null,
                            merkittyPoistettavaksi = null,
                        ),
                    ),
                tutkinnot = setOf(),
            )
        val updated =
            suoritus.copy(
                osakokeet =
                    setOf(
                        VktSuoritusEntity.VktOsakoe(
                            tyyppi = Koodisto.VktOsakoe.Puhuminen,
                            tutkintopaiva = LocalDate.of(2025, 1, 1),
                            arviointipaiva = LocalDate.of(2025, 1, 2),
                            arvosana = Koodisto.VktArvosana.Erinomainen,
                            merkittyPoistettavaksi = null,
                        ),
                        VktSuoritusEntity.VktOsakoe(
                            tyyppi = Koodisto.VktOsakoe.PuheenYmmärtäminen,
                            tutkintopaiva = LocalDate.of(2025, 1, 1),
                            arviointipaiva = LocalDate.of(2025, 1, 2),
                            arvosana = Koodisto.VktArvosana.Erinomainen,
                            merkittyPoistettavaksi = null,
                        ),
                    ),
            )
        repository.save(suoritus)
        repository.save(updated)

        val suoritukset =
            customRepository.find(
                VktSuoritusFilter(arvioitu = listOf(VktArvioinninTila.ArviointejaPuuttuu)),
                VktSuoritusOrder(),
            )
        assertEquals(0, suoritukset.count())
    }

    private fun getSuoritukset() =
        customRepository.find(
            VktSuoritusFilter(
                merkittyPoistettavaksi = false,
                arvioitu = listOf(VktArvioinninTila.ArvioituOsittainTaiKokonaan),
            ),
            VktSuoritusOrder(),
        )

    @Test
    fun `varhaisimmat tutkintopäivät lasketaan taitotasoittain ilmoittautumisten uusimmista versioista`() {
        val vanhaVersio =
            vktSuoritus("1", Koodisto.VktTaitotaso.Erinomainen, LocalDate.of(2019, 3, 1), LocalDate.of(2019, 4, 1))
        repository.save(vanhaVersio)
        repository.save(vanhaVersio.copy(osakokeet = osakokeet(LocalDate.of(2020, 6, 6), LocalDate.of(2020, 5, 5))))
        repository.save(vktSuoritus("2", Koodisto.VktTaitotaso.Erinomainen, LocalDate.of(2021, 1, 1)))
        repository.save(
            vktSuoritus(
                "3",
                Koodisto.VktTaitotaso.HyväJaTyydyttävä,
                LocalDate.of(2023, 2, 2),
                LocalDate.of(2022, 12, 12),
            ),
        )

        assertEquals(
            mapOf(
                Koodisto.VktTaitotaso.Erinomainen to LocalDate.of(2020, 5, 5),
                Koodisto.VktTaitotaso.HyväJaTyydyttävä to LocalDate.of(2022, 12, 12),
            ),
            customRepository.findVarhaisimmatTutkintopaivat(),
        )
    }

    @Test
    fun `varhaisimmat tutkintopäivät puuttuvat tyhjästä rekisteristä`() {
        assertEquals(emptyMap(), customRepository.findVarhaisimmatTutkintopaivat())
    }

    private fun vktSuoritus(
        ilmoittautumisenId: String,
        taitotaso: Koodisto.VktTaitotaso,
        vararg tutkintopaivat: LocalDate,
    ) = VktSuoritusEntity(
        ilmoittautumisenId = ilmoittautumisenId,
        suorittajanOid = Oid.parse("1.2.246.562.24.12345678910").getOrThrow(),
        etunimet = "Testi",
        sukunimi = "Testinen",
        tutkintokieli = Koodisto.Tutkintokieli.FIN,
        suorituspaikkakunta = "Helsinki",
        taitotaso = taitotaso,
        suorituksenVastaanottaja = null,
        osakokeet = osakokeet(*tutkintopaivat),
        tutkinnot = setOf(),
    )

    private fun osakokeet(vararg tutkintopaivat: LocalDate) =
        tutkintopaivat
            .zip(Koodisto.VktOsakoe.entries)
            .map { (tutkintopaiva, tyyppi) ->
                VktSuoritusEntity.VktOsakoe(
                    tyyppi = tyyppi,
                    tutkintopaiva = tutkintopaiva,
                    arviointipaiva = null,
                    arvosana = null,
                    merkittyPoistettavaksi = null,
                )
            }.toSet()
}
