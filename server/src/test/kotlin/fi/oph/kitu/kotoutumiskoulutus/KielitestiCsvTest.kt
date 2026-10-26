package fi.oph.kitu.kotoutumiskoulutus

import fi.oph.kitu.DBContainerConfiguration
import fi.oph.kitu.jdbc.SortDirection
import fi.oph.kitu.kotoutumiskoulutus.suoritukset.KielitestiSuoritusRepository
import fi.oph.kitu.kotoutumiskoulutus.suoritukset.error.KielitestiErrorService
import fi.oph.kitu.kotoutumiskoulutus.suoritukset.error.KielitestiSuoritusError
import fi.oph.kitu.kotoutumiskoulutus.suoritukset.error.KielitestiSuoritusErrorColumn
import fi.oph.kitu.kotoutumiskoulutus.suoritukset.error.KielitestiSuoritusErrorRepository
import fi.oph.kitu.oid.Oid
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.assertAll
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.testcontainers.postgresql.PostgreSQLContainer
import java.io.ByteArrayOutputStream
import java.time.Instant
import kotlin.test.Test
import kotlin.test.assertEquals

@SpringBootTest
@Import(DBContainerConfiguration::class)
class KielitestiCsvTest(
    @param:Autowired val kielitestiSuoritusRepository: KielitestiSuoritusRepository,
    @param:Autowired val kielitestiSuoritusErrorRepository: KielitestiSuoritusErrorRepository,
    @param:Autowired val kielitestiErrorService: KielitestiErrorService,
    @param:Autowired val kielitestiApiController: KielitestiApiController,
    @param:Autowired val postgres: PostgreSQLContainer,
) {
    @BeforeEach
    fun setup() {
        kielitestiSuoritusRepository.deleteAll()
        kielitestiSuoritusErrorRepository.deleteAll()
    }

    @Test
    fun `Kielitestin virheet kaantyvat csv-tiedostoksi oikein`() {
        val suoritukset =
            listOf(
                KielitestiSuoritusError(
                    id = null,
                    suorittajanOid = null,
                    hetu = "010180-9026",
                    nimi = "Ranja Testi Öhman-Testi",
                    etunimet = "Ranja",
                    sukunimi = "Testi Öhman-Testi",
                    kutsumanimi = "Ranja",
                    schoolOid = Oid.parse("1.2.246.562.10.14893989377").getOrNull(),
                    teacherEmail = "testi@example.com",
                    virheenLuontiaika = Instant.parse("2024-11-22T10:49:49Z"),
                    viesti = "Kirjoitusvirhe nimessä tai henkilötunnuksessa",
                    virheellinenKentta = null,
                    virheellinenArvo = null,
                    lisatietoja =
                        """
                        {"request": {"etunimet": "Ranja", "hetu": "010180-9026", "kutsumanimi": "Ranja", "sukunimi": "Testi Öhman-Testi"}}
                        """.trimIndent(),
                    onrLisatietoja = "Korjaa nimet lähdejärjestelmään näin:",
                    onrEtunimet = "Ranja Testi",
                    onrKutsumanimi = "Ranja",
                    onrSukunimi = "Öhman-Testi",
                ),
                KielitestiSuoritusError(
                    id = null,
                    suorittajanOid = "1.2.246.562.24.67409348034",
                    hetu = "010180-9026",
                    nimi = "Eino Testi Välimaa-Testi",
                    etunimet = "Eino Test",
                    sukunimi = "Välimaa-Testi",
                    kutsumanimi = "Eino",
                    schoolOid = Oid.parse("1.2.246.562.10.14893989377").getOrNull(),
                    teacherEmail = "testi@example.com",
                    virheenLuontiaika = Instant.parse("2024-11-22T10:49:49Z"),
                    viesti = "Puuttuva arvosana \"puhuminen\" kurssilla \"Testaus\" käyttäjälle \"1\".",
                    virheellinenKentta = "puhuminen",
                    virheellinenArvo = "virheellinen arvosana",
                    lisatietoja = null,
                    onrLisatietoja = null,
                    kurssiId = 32,
                    kurssi = "Testaus",
                    suoritusaika = Instant.parse("2024-11-20T08:00:00Z"),
                ),
            )

        kielitestiSuoritusErrorRepository.saveAll(suoritukset)

        val actualCsv =
            ByteArrayOutputStream().also { kielitestiApiController.getErrorsAsCsv().body!!.writeTo(it) }
        val expectedCsv =
            """
            Henkilötunnus;Nimi;Organisaatio;Opettajan sähköpostiosoite;Virheen luontiaika;Valmis;Virheviesti;Ratkaisuehdotus;Kurssin ID;Testin nimi;Suoritusaika
            010180-9026;"Etunimet: Ranja
            Kutsumanimi: Ranja
            Sukunimi: Testi Öhman-Testi";"Jyväskylän yliopisto, Soveltavan kielentutkimuksen keskus
            1.2.246.562.10.14893989377";testi@example.com;22.11.2024 12:49:49+02;Kyllä;Kirjoitusvirhe nimessä tai henkilötunnuksessa;"Korjaa nimet lähdejärjestelmään näin:
            Etunimet: Ranja Testi
            Kutsumanimi: Ranja
            Sukunimi: Öhman-Testi";;;
            010180-9026;"Etunimet: Eino Test
            Kutsumanimi: Eino
            Sukunimi: Välimaa-Testi";"Jyväskylän yliopisto, Soveltavan kielentutkimuksen keskus
            1.2.246.562.10.14893989377";testi@example.com;22.11.2024 12:49:49+02;Kyllä;"Puuttuva arvosana ""puhuminen"" kurssilla ""Testaus"" käyttäjälle ""1"".";;32;Testaus;20.11.2024 10:00:00+02

            """.trimIndent()

        assertEquals("\uFEFF" + expectedCsv, actualCsv.toString(Charsets.UTF_8))
    }

    @Test
    fun `virheet voi järjestää jokaisen sarakkeen mukaan`() {
        kielitestiSuoritusErrorRepository.saveAll(
            listOf(virhe(completed = true), virhe(completed = false)),
        )

        assertAll(
            KielitestiSuoritusErrorColumn.entries.flatMap { column ->
                SortDirection.entries.map { direction ->
                    fun() {
                        assertEquals(
                            2,
                            kielitestiErrorService.getErrors(column, direction).count(),
                            "Järjestäminen sarakkeen ${column.name} mukaan ($direction) ei saa aiheuttaa virhettä",
                        )
                    }
                }
            },
        )
    }

    private fun virhe(completed: Boolean) =
        KielitestiSuoritusError(
            id = null,
            suorittajanOid = null,
            hetu = "010180-9026",
            nimi = "Testi Henkilö",
            etunimet = "Testi",
            sukunimi = "Henkilö",
            kutsumanimi = "Testi",
            schoolOid = Oid.parse("1.2.246.562.10.14893989377").getOrNull(),
            teacherEmail = "testi@example.com",
            virheenLuontiaika = Instant.parse("2024-11-22T10:49:49Z"),
            viesti = "Virhe",
            virheellinenKentta = null,
            virheellinenArvo = null,
            lisatietoja = null,
            onrLisatietoja = null,
            completed = completed,
        )
}
