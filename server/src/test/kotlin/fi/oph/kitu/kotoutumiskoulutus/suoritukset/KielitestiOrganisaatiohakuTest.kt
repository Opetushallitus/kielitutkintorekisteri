package fi.oph.kitu.kotoutumiskoulutus.suoritukset

import fi.oph.kitu.DBContainerConfiguration
import fi.oph.kitu.i18n.LocalizedString
import fi.oph.kitu.oid.Oid
import fi.oph.kitu.organisaatiot.OrganisaatioService
import fi.oph.kitu.organisaatiot.Organisaatiot
import fi.oph.kitu.util.result.getOrThrow
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import java.time.Instant
import kotlin.test.assertEquals

@SpringBootTest
@Import(DBContainerConfiguration::class)
class KielitestiOrganisaatiohakuTest(
    @param:Autowired private val kielitestiSuoritusRepository: KielitestiSuoritusRepository,
    @param:Autowired private val jdbc: NamedParameterJdbcTemplate,
) {
    private val ressu = Oid.parse("1.2.246.562.10.65693669254").getOrThrow()
    private val jyu = Oid.parse("1.2.246.562.10.14893989377").getOrThrow()

    private class HautKirjaavaOrganisaatioService(
        private val osumat: Organisaatiot = Organisaatiot.EMPTY,
    ) : OrganisaatioService {
        val haut = mutableListOf<String>()

        override fun getOrganisaatioCache(): Nothing = error("Ei kuulu tähän testiin")

        override fun getOrganisaatiohierarkia(): Nothing = error("Ei kuulu tähän testiin")

        override fun searchOrganisaatiot(query: String): Organisaatiot {
            haut += query
            return osumat
        }
    }

    private fun suoritus(
        sukunimi: String,
        oppijanumero: String,
        oppilaitos: Oid,
    ) = KielitestiSuoritus(
        etunimet = "Testi",
        sukunimi = sukunimi,
        kutsumanimi = "Testi",
        oppijanumero = Oid.parse(oppijanumero).getOrThrow(),
        email = "testi@example.com",
        suoritusaika = Instant.parse("2024-11-22T10:49:49Z"),
        oppilaitosOid = oppilaitos,
        opettajanEmail = "ope@example.com",
        kurssiId = 32,
        kurssi = "Kurssi",
        luetunYmmartaminen = Arvosana.A1,
        kuullunYmmartaminen = Arvosana.B1,
        puhe = Arvosana.B1,
        kirjoittaminen = Arvosana.A2,
        testikieli = Testikieli.FIN,
        tehtavapaketti = null,
    )

    @BeforeEach
    fun valmistele() {
        kielitestiSuoritusRepository.deleteAll()
        kielitestiSuoritusRepository.saveAll(
            listOf(
                suoritus("Ressulainen", "1.2.246.562.24.10000000001", ressu),
                suoritus("Öhman", "1.2.246.562.24.10000000002", jyu),
            ),
        )
    }

    @AfterEach
    fun siivoa() {
        kielitestiSuoritusRepository.deleteAll()
    }

    private fun sukunimet(suoritukset: List<KielitestiSuoritus>) = suoritukset.map { it.sukunimi }.toSet()

    @Test
    fun `tyhjä haku ei hae organisaatioita`() {
        listOf(null, "", "   ").forEach { hakusana ->
            val organisaatiot = HautKirjaavaOrganisaatioService()
            val repository = CustomKielitestiSuoritusRepository(organisaatiot, jdbc)
            val suodatin = KielitestiSuoritusFilter(search = hakusana)

            assertEquals(2, repository.findSuoritukset(suodatin).size, "Hakusana: '$hakusana'")
            assertEquals(2, repository.countSuoritukset(suodatin), "Hakusana: '$hakusana'")
            assertEquals(emptyList(), organisaatiot.haut, "Hakusana: '$hakusana'")
        }
    }

    @Test
    fun `hakusanalla haetaan organisaatiot sekä listalle että laskentaan`() {
        val organisaatiot =
            HautKirjaavaOrganisaatioService(Organisaatiot(mapOf(ressu to LocalizedString(fi = "Ressun peruskoulu"))))
        val repository = CustomKielitestiSuoritusRepository(organisaatiot, jdbc)
        val suodatin = KielitestiSuoritusFilter(search = "peruskoulu")

        assertEquals(setOf("Ressulainen"), sukunimet(repository.findSuoritukset(suodatin)))
        assertEquals(1, repository.countSuoritukset(suodatin))
        assertEquals(listOf("peruskoulu", "peruskoulu"), organisaatiot.haut)
    }

    @Test
    fun `organisaation ja suorituksen sarakkeiden osumat yhdistyvät`() {
        val organisaatiot =
            HautKirjaavaOrganisaatioService(Organisaatiot(mapOf(ressu to LocalizedString(fi = "Öhmanin koulu"))))
        val repository = CustomKielitestiSuoritusRepository(organisaatiot, jdbc)
        val suodatin = KielitestiSuoritusFilter(search = "öhman")

        assertEquals(setOf("Ressulainen", "Öhman"), sukunimet(repository.findSuoritukset(suodatin)))
        assertEquals(2, repository.countSuoritukset(suodatin))
    }
}
