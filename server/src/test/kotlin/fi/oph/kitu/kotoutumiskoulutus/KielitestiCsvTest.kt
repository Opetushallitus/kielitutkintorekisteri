package fi.oph.kitu.kotoutumiskoulutus

import fi.oph.kitu.DBContainerConfiguration
import fi.oph.kitu.jdbc.SortDirection
import fi.oph.kitu.kotoutumiskoulutus.suoritukset.Arvosana
import fi.oph.kitu.kotoutumiskoulutus.suoritukset.KielitestiSuoritus
import fi.oph.kitu.kotoutumiskoulutus.suoritukset.KielitestiSuoritusRepository
import fi.oph.kitu.kotoutumiskoulutus.suoritukset.KielitestiSuoritusService
import fi.oph.kitu.kotoutumiskoulutus.suoritukset.Testikieli
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
import java.time.Instant
import kotlin.test.Test
import kotlin.test.assertEquals

@SpringBootTest
@Import(DBContainerConfiguration::class)
class KielitestiCsvTest(
    @param:Autowired val kielitestiSuoritusRepository: KielitestiSuoritusRepository,
    @param:Autowired val kielitestiSuoritusErrorRepository: KielitestiSuoritusErrorRepository,
    @param:Autowired val kielitestiErrorService: KielitestiErrorService,
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
                ),
            )

        kielitestiSuoritusErrorRepository.saveAll(suoritukset)

        val actualCsv = kielitestiErrorService.generateErrorsCsvStream()
        val expectedCsv =
            """
            virheenLuontiaika;suorittajanOid;hetu;nimi;etunimet;sukunimi;kutsumanimi;schoolOid;teacherEmail;viesti;lisatietoja;onrLisatietoja;onrEtunimet;onrKutsumanimi;onrSukunimi;virheellinenKentta;virheellinenArvo
            "2024-11-22T10:49:49Z";;"010180-9026";"Ranja Testi Öhman-Testi";Ranja;"Testi Öhman-Testi";Ranja;"1.2.246.562.10.14893989377";"testi@example.com";"Kirjoitusvirhe nimessä tai henkilötunnuksessa";"{""request"": {""etunimet"": ""Ranja"", ""hetu"": ""010180-9026"", ""kutsumanimi"": ""Ranja"", ""sukunimi"": ""Testi Öhman-Testi""}}";"Korjaa nimet lähdejärjestelmään näin:";"Ranja Testi";Ranja;"Öhman-Testi";;
            "2024-11-22T10:49:49Z";"1.2.246.562.24.67409348034";"010180-9026";"Eino Testi Välimaa-Testi";"Eino Test";"Välimaa-Testi";Eino;"1.2.246.562.10.14893989377";"testi@example.com";"Puuttuva arvosana ""puhuminen"" kurssilla ""Testaus"" käyttäjälle ""1"".";;;;;;puhuminen;"virheellinen arvosana"

            """.trimIndent()

        assertEquals(expectedCsv, actualCsv.toString(Charsets.UTF_8))
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
