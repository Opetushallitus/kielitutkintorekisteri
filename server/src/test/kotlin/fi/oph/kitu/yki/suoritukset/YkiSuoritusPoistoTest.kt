package fi.oph.kitu.yki.suoritukset

import arrow.core.Either
import fi.oph.kitu.DBContainerConfiguration
import fi.oph.kitu.dev.mockdata.generateRandomYkiSuoritusEntity
import fi.oph.kitu.koski.KoskiYkiMappingError
import fi.oph.kitu.koski.KoskiYkiRequestMapper
import fi.oph.kitu.oid.Oid
import fi.oph.kitu.tiedontuontischema.Lahdejarjestelma
import fi.oph.kitu.tiedontuontischema.LahdejarjestelmanTunniste
import fi.oph.kitu.tyhjennaYkiSuoritukset
import fi.oph.kitu.util.result.getOrThrow
import fi.oph.kitu.yki.Arviointitila
import fi.oph.kitu.yki.TutkinnonOsa
import fi.oph.kitu.yki.YkiService
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.jdbc.core.JdbcTemplate
import java.time.Instant
import java.time.LocalDate
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

@SpringBootTest
@Import(DBContainerConfiguration::class)
class YkiSuoritusPoistoTest(
    @param:Autowired private val ykiService: YkiService,
    @param:Autowired private val suoritusRepository: YkiSuoritusRepository,
    @param:Autowired private val koskiYkiRequestMapper: KoskiYkiRequestMapper,
    @param:Autowired private val jdbcTemplate: JdbcTemplate,
) {
    private val solkiId = 1001
    private val tunniste = tunniste(solkiId)
    private val eiSuoritusta = 12

    private val ensimmainenVersio = Instant.parse("2025-01-01T10:00:00Z")
    private val toinenVersio = Instant.parse("2025-02-01T10:00:00Z")

    @BeforeEach
    @AfterEach
    fun tyhjenna() {
        jdbcTemplate.tyhjennaYkiSuoritukset()
    }

    private fun tunniste(id: Int) = LahdejarjestelmanTunniste(id.toString(), Lahdejarjestelma.Solki)

    private fun suoritus(
        arviointitila: Arviointitila,
        id: Int = solkiId,
        lastModified: Instant = ensimmainenVersio,
    ) = generateRandomYkiSuoritusEntity().copy(
        solkiId = id,
        lahdejarjestelmanTunnus = "yki.$id",
        arviointitila = arviointitila,
        tutkintopaiva = LocalDate.of(2020, 1, 1),
        lastModified = lastModified,
        receivedAt = lastModified,
    )

    private fun ilmoittautuminen(lastModified: Instant = ensimmainenVersio) =
        suoritus(Arviointitila.ILMOITTAUTUNUT, lastModified = lastModified)
            .copy(
                arviointipaiva = null,
                tekstinYmmartaminen = null,
                kirjoittaminen = null,
                rakenteetJaSanasto = null,
                puheenYmmartaminen = null,
                puhuminen = null,
                yleisarvosana = null,
                tarkistusarvioinninSaapumisPvm = null,
                tarkistusarvioinninAsiatunnus = null,
                tarkistusarvioidutOsakokeet = null,
                arvosanaMuuttui = null,
                perustelu = null,
                tarkistusarvioinninKasittelyPvm = null,
            ).also {
                it.ilmoitetutOsakokeet = setOf(TutkinnonOsa.PU, TutkinnonOsa.KI, TutkinnonOsa.TY, TutkinnonOsa.PY)
            }

    private fun viimeisin(id: Int = solkiId) = suoritusRepository.findLatestBySolkiIds(listOf(id)).single()

    private fun versiomaara(id: Int = solkiId): Int =
        jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM yki_suoritus WHERE solki_id = ?",
            Int::class.javaObjectType,
            id,
        )!!

    @Test
    fun `poisto kirjoittaa uuden version, jossa jokaisen osakokeen arvosana on 12`() {
        suoritusRepository.save(ilmoittautuminen(), false)

        val tulos = ykiService.poistaSuoritus(tunniste)

        assertEquals(Either.Right(Unit), tulos)
        val poistettu = viimeisin()
        assertEquals(Arviointitila.EI_SUORITUSTA, poistettu.arviointitila)
        assertEquals(eiSuoritusta, poistettu.puhuminen)
        assertEquals(eiSuoritusta, poistettu.kirjoittaminen)
        assertEquals(eiSuoritusta, poistettu.tekstinYmmartaminen)
        assertEquals(eiSuoritusta, poistettu.puheenYmmartaminen)
        assertNull(poistettu.rakenteetJaSanasto, "osakoetta ei ollut ilmoittautumisessa")
        assertNull(poistettu.yleisarvosana, "osakoetta ei ollut ilmoittautumisessa")
        assertEquals(
            setOf(TutkinnonOsa.PU, TutkinnonOsa.KI, TutkinnonOsa.TY, TutkinnonOsa.PY),
            poistettu.ilmoitetutOsakokeet,
        )
    }

    @Test
    fun `poisto säilyttää suorituksen ja aiemman version`() {
        suoritusRepository.save(ilmoittautuminen(), false)

        ykiService.poistaSuoritus(tunniste).getOrThrow()

        assertEquals(1L, suoritusRepository.countSuoritukset())
        val versiot = suoritusRepository.find(distinct = false).toList().sortedBy { it.lastModified }
        assertEquals(2, versiot.size)
        assertEquals(Arviointitila.ILMOITTAUTUNUT, versiot.first().arviointitila)
        assertNull(versiot.first().puhuminen, "aiempi versio ei saa muuttua")
        assertEquals(Arviointitila.EI_SUORITUSTA, versiot.last().arviointitila)
    }

    @Test
    fun `poisto säilyttää saapumisajan, koska se ei ole uusi saapunut suoritus`() {
        suoritusRepository.save(ilmoittautuminen(), false)

        ykiService.poistaSuoritus(tunniste).getOrThrow()

        assertEquals(ensimmainenVersio, viimeisin().receivedAt)
        assertTrue(viimeisin().lastModified.isAfter(ensimmainenVersio))
    }

    @Test
    fun `poisto on sallittu vain tilassa ILMOITTAUTUNUT`() {
        Arviointitila.entries.forEachIndexed { indeksi, tila ->
            val id = 2000 + indeksi
            suoritusRepository.save(suoritus(tila, id = id), false)

            val tulos = ykiService.poistaSuoritus(tunniste(id))

            if (tila == Arviointitila.ILMOITTAUTUNUT) {
                assertEquals(Either.Right(Unit), tulos, "tila $tila")
                assertEquals(Arviointitila.EI_SUORITUSTA, viimeisin(id).arviointitila, "tila $tila")
                assertEquals(2, versiomaara(id), "tila $tila")
            } else {
                assertEquals(
                    Either.Left(PoistaSuoritusError.TilaEstaaPoiston(tunniste(id), tila)),
                    tulos,
                    "tila $tila",
                )
                assertEquals(tila, viimeisin(id).arviointitila, "tila $tila")
                assertEquals(1, versiomaara(id), "tila $tila")
            }
        }
    }

    @Test
    fun `viimeisin versio ratkaisee - ilmoittautumisen jälkeen arvioitua ei voi poistaa`() {
        suoritusRepository.save(ilmoittautuminen(), false)
        suoritusRepository.save(suoritus(Arviointitila.ARVIOITU, lastModified = toinenVersio), false)

        val tulos = ykiService.poistaSuoritus(tunniste)

        assertEquals(Either.Left(PoistaSuoritusError.TilaEstaaPoiston(tunniste, Arviointitila.ARVIOITU)), tulos)
    }

    @Test
    fun `viimeisin versio ratkaisee - arvioinnin jälkeen ilmoittautunut voidaan poistaa`() {
        suoritusRepository.save(suoritus(Arviointitila.ARVIOITU), false)
        suoritusRepository.save(ilmoittautuminen(lastModified = toinenVersio), false)

        assertEquals(Either.Right(Unit), ykiService.poistaSuoritus(tunniste))
        assertEquals(Arviointitila.EI_SUORITUSTA, viimeisin().arviointitila)
    }

    @Test
    fun `jo poistetun suorituksen uusi poisto palauttaa virheen tekemättä uutta versiota`() {
        suoritusRepository.save(ilmoittautuminen(), false)
        ykiService.poistaSuoritus(tunniste).getOrThrow()
        val poistettu = viimeisin()

        assertEquals(
            Either.Left(PoistaSuoritusError.TilaEstaaPoiston(tunniste, Arviointitila.EI_SUORITUSTA)),
            ykiService.poistaSuoritus(tunniste),
        )

        assertEquals(2, versiomaara())
        assertEquals(poistettu.lastModified, viimeisin().lastModified)
    }

    @Test
    fun `tuntematon tunniste palauttaa virheen`() {
        suoritusRepository.save(ilmoittautuminen(), false)
        val tuntematon = tunniste(999999)

        assertEquals(
            Either.Left(PoistaSuoritusError.SuoritustaEiLoydy(tuntematon)),
            ykiService.poistaSuoritus(tuntematon),
        )
    }

    @Test
    fun `saman numeron tunniste toisesta lähdejärjestelmästä ei poista suoritusta`() {
        suoritusRepository.save(ilmoittautuminen(), false)
        val toinenLahde = LahdejarjestelmanTunniste(solkiId.toString(), Lahdejarjestelma.OPH)

        assertEquals(
            Either.Left(PoistaSuoritusError.SuoritustaEiLoydy(toinenLahde)),
            ykiService.poistaSuoritus(toinenLahde),
        )
        assertEquals(Arviointitila.ILMOITTAUTUNUT, viimeisin().arviointitila)
    }

    @Test
    fun `poistettua suoritusta ei siirretä KOSKI-järjestelmään`() {
        suoritusRepository.save(ilmoittautuminen(), false)

        ykiService.poistaSuoritus(tunniste).getOrThrow()

        val tulos = koskiYkiRequestMapper.ykiSuoritusToKoskiRequest(viimeisin())
        val esto = assertNotNull((tulos as? Either.Left)?.value as? KoskiYkiMappingError.EstoSyyt)
        assertContains(esto.syyt, "Suoritus ei ole arvioitu")
    }

    @Test
    fun `poisto säilyttää KOSKI-tiedot, joten käsiteltyä suoritusta ei jonoteta uudelleen`() {
        val opiskeluoikeus = Oid.parse("1.2.246.562.15.00000000001").getOrThrow()
        suoritusRepository.save(
            ilmoittautuminen().copy(
                koskiOpiskeluoikeus = opiskeluoikeus,
                koskiSiirtoKasitelty = true,
            ),
            false,
        )

        ykiService.poistaSuoritus(tunniste).getOrThrow()

        assertEquals(opiskeluoikeus, viimeisin().koskiOpiskeluoikeus)
        assertEquals(emptyList(), suoritusRepository.findKoskeenLahettamattomatSuoritukset().toList())
    }

    @Test
    fun `poisto päätyy ilmoittautumisjärjestelmän lähetysjonoon`() {
        suoritusRepository.save(ilmoittautuminen(), false)
        assertEquals(emptyList(), suoritusRepository.findSuorituksetWithUnsentArvioinninTila())

        ykiService.poistaSuoritus(tunniste).getOrThrow()

        assertEquals(
            listOf(Arviointitila.EI_SUORITUSTA),
            suoritusRepository.findSuorituksetWithUnsentArvioinninTila().map { it.arviointitila },
        )
    }

    @Test
    fun `epäonnistuva auditointi peruu poiston`() {
        val pitkaId = "1".repeat(1000)
        suoritusRepository.save(ilmoittautuminen().copy(lahdejarjestelmanTunnus = "yki.$pitkaId"), false)

        val virhe =
            assertFailsWith<IllegalStateException> {
                ykiService.poistaSuoritus(LahdejarjestelmanTunniste(pitkaId, Lahdejarjestelma.Solki))
            }

        assertContains(virhe.message.orEmpty(), "Audit log value")
        assertEquals(1, versiomaara())
        assertEquals(Arviointitila.ILMOITTAUTUNUT, viimeisin().arviointitila)
    }
}
