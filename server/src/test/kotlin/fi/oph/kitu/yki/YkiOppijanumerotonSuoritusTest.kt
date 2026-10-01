package fi.oph.kitu.yki

import arrow.core.Either
import fi.oph.kitu.DBContainerConfiguration
import fi.oph.kitu.dev.mockdata.generateRandomYkiSuoritusEntity
import fi.oph.kitu.ilmoittautumisjarjestelma.YkiArvioinninTila
import fi.oph.kitu.koski.KoskiYkiMappingError
import fi.oph.kitu.koski.KoskiYkiRequestMapper
import fi.oph.kitu.oid.Oid
import fi.oph.kitu.util.result.getOrThrow
import fi.oph.kitu.yki.suoritukset.YkiSuoritusEntity
import fi.oph.kitu.yki.suoritukset.YkiSuoritusRepository
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import java.time.LocalDate
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * Oppijanumeroton suoritus on vain 2011-2016 historiadatan erikoistapaus: se saa olla kannassa,
 * mutta se ei saa lahtea KOSKEEN eika ilmoittautumisjarjestelmaan. Esto on kahdessa kerroksessa,
 * ja molemmat testataan: kyselysuodatin jottei yoajo edes lue rivia, ja tarkistus
 * lahetyspolulla joka kattaa myos suorat kutsut.
 */
@SpringBootTest
@Import(DBContainerConfiguration::class)
class YkiOppijanumerotonSuoritusTest(
    @param:Autowired private val suoritusRepository: YkiSuoritusRepository,
    @param:Autowired private val koskiYkiRequestMapper: KoskiYkiRequestMapper,
) {
    private val oppijanumero = Oid.parse("1.2.246.562.24.12345678910").getOrThrow()

    @BeforeEach
    fun tyhjennaTaulut() {
        suoritusRepository.deleteAll()
    }

    private fun suoritus(
        solkiId: Int,
        suorittajanOID: Oid?,
    ): YkiSuoritusEntity =
        generateRandomYkiSuoritusEntity().copy(
            suorittajanOID = suorittajanOID,
            solkiId = solkiId,
            arviointitila = Arviointitila.ARVIOITU,
            koskiSiirtoKasitelty = false,
            tutkintopaiva = LocalDate.of(2016, 5, 14),
            arviointipaiva = LocalDate.of(2016, 6, 1),
            tutkintotaso = Tutkintotaso.PT,
            tekstinYmmartaminen = 2,
            kirjoittaminen = 2,
            puheenYmmartaminen = 2,
            puhuminen = 2,
            rakenteetJaSanasto = null,
            yleisarvosana = null,
            lahdejarjestelmanTunnus = "yki.$solkiId",
        )

    @Test
    fun `KOSKI-siirto estetaan ja syy kerrotaan`() {
        val tulos = koskiYkiRequestMapper.ykiSuoritusToKoskiRequest(suoritus(1, null))

        val virhe = assertNotNull((tulos as? Either.Left)?.value as? KoskiYkiMappingError.EstoSyyt)
        assertTrue(
            virhe.syyt.contains("Suorituksella ei ole oppijanumeroa"),
            "estosyyt: ${virhe.syyt}",
        )
    }

    @Test
    fun `oppijanumerollinen suoritus menee KOSKEen normaalisti`() {
        val tulos = koskiYkiRequestMapper.ykiSuoritusToKoskiRequest(suoritus(2, oppijanumero))

        assertTrue(tulos.isRight(), "siirto estyi: ${(tulos as? Either.Left)?.value}")
    }

    @Test
    fun `oppijanumeroton ei paady KOSKI-siirron jonoon mutta oppijanumerollinen paatyy`() {
        suoritusRepository.save(suoritus(3, null), false)
        suoritusRepository.save(suoritus(4, oppijanumero), false)

        val jonossa = suoritusRepository.findKoskeenLahettamattomatSuoritukset().map { it.solkiId }

        assertEquals(listOf(4), jonossa)
    }

    @Test
    fun `arvioinnin tilaa ei muodosteta ilman oppijanumeroa`() {
        assertNull(YkiArvioinninTila.of(suoritus(5, null)))
        assertNotNull(YkiArvioinninTila.of(suoritus(6, oppijanumero)))
    }

    @Test
    fun `oppijanumeroton ei paady ilmoittautumisjarjestelman jonoon mutta oppijanumerollinen paatyy`() {
        suoritusRepository.save(suoritus(7, null), false)
        suoritusRepository.save(suoritus(8, oppijanumero), false)

        val jonossa = suoritusRepository.findSuorituksetWithUnsentArvioinninTila().map { it.solkiId }

        assertEquals(listOf(8), jonossa)
    }

    @Test
    fun `oppijanumeroton suoritus tallentuu ja luetaan takaisin nullina`() {
        suoritusRepository.save(suoritus(9, null), false)

        val tallennettu = suoritusRepository.findLatestBySolkiIds(listOf(9)).single()

        assertNull(tallennettu.suorittajanOID)
        assertEquals(9, tallennettu.solkiId)
    }
}
