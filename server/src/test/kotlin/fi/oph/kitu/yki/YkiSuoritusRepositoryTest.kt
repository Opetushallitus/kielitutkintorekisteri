package fi.oph.kitu.yki

import arrow.core.Either
import fi.oph.kitu.DBContainerConfiguration
import fi.oph.kitu.dev.mockdata.createOid
import fi.oph.kitu.dev.mockdata.generateRandomYkiSuoritusEntity
import fi.oph.kitu.oid.OidClass
import fi.oph.kitu.yki.suoritukset.HyvaksyTarkistusarviointiError
import fi.oph.kitu.yki.suoritukset.YkiSuoritusEntity
import fi.oph.kitu.yki.suoritukset.YkiSuoritusFilter
import fi.oph.kitu.yki.suoritukset.YkiSuoritusRepository
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertAll
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.jdbc.core.JdbcTemplate
import org.testcontainers.postgresql.PostgreSQLContainer
import java.time.Instant
import java.time.LocalDate
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

@SpringBootTest
@Import(DBContainerConfiguration::class)
class YkiSuoritusRepositoryTest(
    @param:Autowired private val ykiSuoritusRepository: YkiSuoritusRepository,
    @param:Autowired private val jdbcTemplate: JdbcTemplate,
    @param:Autowired private val postgres: PostgreSQLContainer,
) {
    @BeforeEach
    fun nukeDb() {
        ykiSuoritusRepository.deleteAll()
    }

    @Test
    fun `suoritus is saved correctly`() {
        val suoritus = generateRandomYkiSuoritusEntity()
        val savedSuoritukset = ykiSuoritusRepository.saveAllNewEntities(listOf(suoritus)).toList()
        assertEquals(suoritus, savedSuoritukset[0].copy(id = null))
    }

    @Test
    fun `saveAll returns only the saved suoritus`() {
        val initialSuoritus =
            generateRandomYkiSuoritusEntity()
                .copy(lastModified = Instant.parse("2025-01-01T10:00:00Z"))
        ykiSuoritusRepository.saveAllNewEntities(listOf(initialSuoritus)).toList()
        val updatedSuoritus =
            initialSuoritus.copy(
                tutkintopaiva = initialSuoritus.tutkintopaiva.plusDays(1),
                lastModified = Instant.parse("2025-01-02T13:53:56Z"),
            )
        val savedSuoritukset = ykiSuoritusRepository.saveAllNewEntities(listOf(initialSuoritus, updatedSuoritus))
        assertEquals(1, savedSuoritukset.count())
        assertEquals(
            updatedSuoritus,
            savedSuoritukset.elementAt(0).copy(id = null),
        )
    }

    @Test
    fun `suoritus with null values is saved correctly`() {
        val suoritus =
            generateRandomYkiSuoritusEntity().copy(
                email = null,
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
                koskiOpiskeluoikeus = null,
                arviointipaiva = null,
                maa = null,
            )
        val savedSuoritukset = ykiSuoritusRepository.saveAllNewEntities(listOf(suoritus)).toList()
        assertEquals(suoritus, savedSuoritukset[0].copy(id = null))
    }

    @Test
    fun `ilmoittautunut suoritus tallentaa ja lukee arvostelemattomat osakokeet`() {
        val ilmoittautunut =
            generateRandomYkiSuoritusEntity()
                .copy(
                    arviointitila = Arviointitila.ILMOITTAUTUNUT,
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
                    it.ilmoitetutOsakokeet = setOf(TutkinnonOsa.TY, TutkinnonOsa.KI, TutkinnonOsa.PU)
                }
        ykiSuoritusRepository.saveAllNewEntities(listOf(ilmoittautunut))

        val stored = ykiSuoritusRepository.findLatestBySolkiIds(listOf(ilmoittautunut.solkiId)).first()

        assertAll(
            fun() =
                assertEquals(
                    setOf(TutkinnonOsa.PU, TutkinnonOsa.KI, TutkinnonOsa.TY),
                    stored.ilmoitetutOsakokeet,
                ),
            fun() =
                assertEquals(
                    listOf(TutkinnonOsa.PU, TutkinnonOsa.KI, TutkinnonOsa.TY),
                    stored.osakokeet().map { it.tyyppi },
                ),
            fun() = assertTrue(stored.osakokeet().all { it.arvosana == null }, "osakokeilla ei saa olla arvosanaa"),
            fun() = assertNull(stored.yleisarvosana),
            fun() = assertNull(stored.puhuminen),
            fun() = assertEquals(Arviointitila.ILMOITTAUTUNUT, stored.arviointitila),
        )
    }

    @Test
    fun `finding distinct suoritukset returns the latest suoritus of same suoritusId`() {
        val suoritus = generateRandomYkiSuoritusEntity(maxDate = LocalDate.of(2024, 9, 1))
        val suoritus2 = generateRandomYkiSuoritusEntity(maxDate = LocalDate.of(2024, 9, 1))

        val updatedSuoritus =
            suoritus.copy(
                lastModified = Instant.parse("2024-11-01T13:53:56Z"),
                tekstinYmmartaminen = 5,
                kirjoittaminen = 4,
                rakenteetJaSanasto = 3,
                puheenYmmartaminen = 1,
                puhuminen = 2,
                yleisarvosana = 3,
                tarkistusarvioinninSaapumisPvm = LocalDate.of(2024, 10, 1),
                tarkistusarvioinninAsiatunnus = "123123",
                tarkistusarvioidutOsakokeet = setOf(TutkinnonOsa.puhuminen, TutkinnonOsa.puheenYmmartaminen),
                arvosanaMuuttui = setOf(TutkinnonOsa.puhuminen),
                perustelu = "Tarkistusarvioinnin testi",
                tarkistusarvioinninKasittelyPvm = LocalDate.of(2024, 10, 15),
            )
        ykiSuoritusRepository.saveAllNewEntities(listOf(suoritus, suoritus2, updatedSuoritus))

        val suoritukset =
            ykiSuoritusRepository
                .find(distinct = true)
                .map { it.copy(id = null) }
                .toList()

        assertAll(
            fun() = assertContains(suoritukset, updatedSuoritus),
            fun() = assertContains(suoritukset, suoritus2),
            fun() = assertFalse(suoritukset.contains(suoritus)),
            fun() = assertEquals(2, suoritukset.count()),
        )
    }

    @Test
    fun `suoritukset with legacy language codes are saved correctly`() {
        val suoritusSWE10 = generateRandomYkiSuoritusEntity().copy(tutkintokieli = Tutkintokieli.SWE10)
        val suoritusENG11 = generateRandomYkiSuoritusEntity().copy(tutkintokieli = Tutkintokieli.ENG11)
        val suoritusENG12 = generateRandomYkiSuoritusEntity().copy(tutkintokieli = Tutkintokieli.ENG12)
        val suoritukset = listOf(suoritusSWE10, suoritusENG11, suoritusENG12)
        val savedSuoritukset = ykiSuoritusRepository.saveAllNewEntities(suoritukset).toList()
        assertTrue(savedSuoritukset.map { it.copy(id = null) }.containsAll(suoritukset))
    }

    @Test
    fun `find suoritus with search term`() {
        val suoritus = generateRandomYkiSuoritusEntity().copy(etunimet = "Ranja Testi", email = null)
        val suoritus2 =
            generateRandomYkiSuoritusEntity().copy(
                etunimet = "Testi",
                sukunimi = "Testilä",
                email = null,
            )
        ykiSuoritusRepository.saveAllNewEntities(listOf(suoritus, suoritus2))

        val searchStr = "ranja"
        val suoritukset = ykiSuoritusRepository.find(YkiSuoritusFilter.from(search = searchStr))
        assertEquals(1, suoritukset.count())
        assertEquals(suoritus, suoritukset.first().copy(id = null))

        val anotherSuoritukset = ykiSuoritusRepository.find(YkiSuoritusFilter.from(search = "testi"))
        assertEquals(2, anotherSuoritukset.count())
    }

    @Test
    fun `find suoritus with first name and surname search term`() {
        val target =
            generateRandomYkiSuoritusEntity().copy(
                etunimet = "Matti",
                sukunimi = "Virtanen",
                email = null,
            )
        val decoy =
            generateRandomYkiSuoritusEntity().copy(
                etunimet = "Matti",
                sukunimi = "Korhonen",
                email = null,
            )
        ykiSuoritusRepository.saveAllNewEntities(listOf(target, decoy))

        val byFullName = ykiSuoritusRepository.find(YkiSuoritusFilter.from(search = "Matti Virtanen"))
        assertEquals(1, byFullName.count())
        assertEquals(target, byFullName.first().copy(id = null))

        val byReversedName = ykiSuoritusRepository.find(YkiSuoritusFilter.from(search = "Virtanen Matti"))
        assertEquals(1, byReversedName.count())
        assertEquals(target, byReversedName.first().copy(id = null))

        val byWrongCombo = ykiSuoritusRepository.find(YkiSuoritusFilter.from(search = "Matti Lahtinen"))
        assertEquals(0, byWrongCombo.count())
    }

    @Test
    fun `find suoritus with solki id search term`() {
        val target = generateRandomYkiSuoritusEntity().copy(solkiId = 314159)
        val other =
            generateRandomYkiSuoritusEntity().copy(
                solkiId = 271828,
                suorittajanOID = createOid(OidClass.USER, 22222222222),
                jarjestajanTunnusOid = createOid(OidClass.ORG, 22222222222),
                hetu = "010100A002H",
            )
        ykiSuoritusRepository.saveAllNewEntities(listOf(target, other))

        val exact = ykiSuoritusRepository.find(YkiSuoritusFilter.from(search = "314159"))
        assertEquals(1, exact.count())
        assertEquals(target.solkiId, exact.first().solkiId)

        val partial = ykiSuoritusRepository.find(YkiSuoritusFilter.from(search = "3141"))
        assertEquals(0, partial.count())
    }

    @Test
    fun `find suoritus henkilö-oidilla palauttaa vain kyseisen suorittajan`() {
        val oid1 = createOid(OidClass.OPPIJA, 11111111111)
        val oid2 = createOid(OidClass.OPPIJA, 22222222222)
        val target = generateRandomYkiSuoritusEntity().copy(solkiId = 100001, suorittajanOID = oid1)
        val muu = generateRandomYkiSuoritusEntity().copy(solkiId = 100002, suorittajanOID = oid2)
        ykiSuoritusRepository.saveAllNewEntities(listOf(target, muu))

        val tulos = ykiSuoritusRepository.find(YkiSuoritusFilter.from(search = oid1.toString()))

        assertEquals(1, tulos.count())
        assertEquals(oid1, tulos.first().suorittajanOID)
    }

    @Test
    fun `find suoritukset usealla henkilö-oidilla palauttaa kaikki osumat`() {
        val oid1 = createOid(OidClass.OPPIJA, 11111111111)
        val oid2 = createOid(OidClass.OPPIJA, 22222222222)
        val oid3 = createOid(OidClass.OPPIJA, 33333333333)
        val a = generateRandomYkiSuoritusEntity().copy(solkiId = 100001, suorittajanOID = oid1)
        val b = generateRandomYkiSuoritusEntity().copy(solkiId = 100002, suorittajanOID = oid2)
        val c = generateRandomYkiSuoritusEntity().copy(solkiId = 100003, suorittajanOID = oid3)
        ykiSuoritusRepository.saveAllNewEntities(listOf(a, b, c))

        val tulos = ykiSuoritusRepository.find(YkiSuoritusFilter.from(search = "$oid1 $oid2"))

        assertEquals(setOf(oid1, oid2), tulos.map { it.suorittajanOID }.toSet())
    }

    @Test
    fun `find suoritus organisaatio-oidilla palauttaa vain kyseisen järjestäjän`() {
        val org1 = createOid(OidClass.ORG, 11111111111)
        val org2 = createOid(OidClass.ORG, 22222222222)
        val target = generateRandomYkiSuoritusEntity().copy(solkiId = 100001, jarjestajanTunnusOid = org1)
        val muu = generateRandomYkiSuoritusEntity().copy(solkiId = 100002, jarjestajanTunnusOid = org2)
        ykiSuoritusRepository.saveAllNewEntities(listOf(target, muu))

        val tulos = ykiSuoritusRepository.find(YkiSuoritusFilter.from(search = org1.toString()))

        assertEquals(1, tulos.count())
        assertEquals(org1, tulos.first().jarjestajanTunnusOid)
    }

    @Test
    fun `find henkilö- ja organisaatio-oidilla vaatii molempien täsmäävän samalla rivillä`() {
        val oppija = createOid(OidClass.OPPIJA, 11111111111)
        val org = createOid(OidClass.ORG, 11111111111)
        val target =
            generateRandomYkiSuoritusEntity().copy(
                solkiId = 100001,
                suorittajanOID = oppija,
                jarjestajanTunnusOid = org,
            )
        val eriJarjestaja =
            generateRandomYkiSuoritusEntity().copy(
                solkiId = 100002,
                suorittajanOID = oppija,
                jarjestajanTunnusOid = createOid(OidClass.ORG, 99999999999),
            )
        ykiSuoritusRepository.saveAllNewEntities(listOf(target, eriJarjestaja))

        val molemmat = ykiSuoritusRepository.find(YkiSuoritusFilter.from(search = "$oppija $org"))

        assertEquals(1, molemmat.count())
        assertEquals(target.solkiId, molemmat.first().solkiId)
    }

    @Test
    fun `countSuoritukset henkilö-oidilla tuottaa kelvollisen SQL_n ja saman määrän kuin find`() {
        val oid1 = createOid(OidClass.OPPIJA, 11111111111)
        val oid2 = createOid(OidClass.OPPIJA, 22222222222)
        ykiSuoritusRepository.saveAllNewEntities(
            listOf(
                generateRandomYkiSuoritusEntity().copy(solkiId = 100001, suorittajanOID = oid1),
                generateRandomYkiSuoritusEntity().copy(solkiId = 100002, suorittajanOID = oid2),
            ),
        )

        val filter = YkiSuoritusFilter.from(search = oid1.toString())

        assertEquals(1L, ykiSuoritusRepository.countSuoritukset(filter))
        assertEquals(
            ykiSuoritusRepository.find(filter).count().toLong(),
            ykiSuoritusRepository.countSuoritukset(filter),
        )
    }

    @Test
    fun `countSuoritukset numerohaulla tuottaa kelvollisen SQL_n`() {
        ykiSuoritusRepository.saveAllNewEntities(
            listOf(
                generateRandomYkiSuoritusEntity().copy(solkiId = 424242),
                generateRandomYkiSuoritusEntity().copy(solkiId = 515151),
            ),
        )

        val filter = YkiSuoritusFilter.from(search = "424242")

        assertEquals(1L, ykiSuoritusRepository.countSuoritukset(filter))
        assertEquals(
            ykiSuoritusRepository.find(filter).count().toLong(),
            ykiSuoritusRepository.countSuoritukset(filter),
        )
    }

    @Test
    fun `hyvaksyTarkistusarvioinnit returns Left for a non-tarkistusarvioitu suoritus`() {
        val suoritus =
            generateRandomYkiSuoritusEntity()
                .copy(arviointitila = Arviointitila.ARVIOITU)
        ykiSuoritusRepository.saveAllNewEntities(listOf(suoritus))

        val result =
            ykiSuoritusRepository.hyvaksyTarkistusarvioinnit(
                suoritusIds = listOf(suoritus.solkiId),
                pvm = LocalDate.now(),
            )

        assertIs<Either.Left<*>>(result)
        assertIs<HyvaksyTarkistusarviointiError.EiTarkistusarvioitu>(result.value)
        // Ei muutosta arviointitilassa
        val storedAfter = ykiSuoritusRepository.findLatestBySolkiIds(listOf(suoritus.solkiId)).first()
        assertEquals(Arviointitila.ARVIOITU, storedAfter.arviointitila)
    }

    @Test
    fun `hyvaksyTarkistusarvioinnit returns Left when kasittelyPvm is missing`() {
        val suoritus =
            generateRandomYkiSuoritusEntity()
                .copy(
                    arviointitila = Arviointitila.TARKISTUSARVIOITU,
                    tarkistusarvioinninKasittelyPvm = null,
                )
        ykiSuoritusRepository.saveAllNewEntities(listOf(suoritus))

        val result =
            ykiSuoritusRepository.hyvaksyTarkistusarvioinnit(
                suoritusIds = listOf(suoritus.solkiId),
                pvm = LocalDate.now(),
            )

        assertIs<Either.Left<*>>(result)
        assertIs<HyvaksyTarkistusarviointiError.KasittelyPvmPuuttuu>(result.value)
    }

    @Test
    fun `hyvaksyTarkistusarvioinnit returns Left when hyvaksyttyPvm is before kasittelyPvm`() {
        val kasittelyPvm = LocalDate.of(2025, 6, 15)
        val suoritus =
            generateRandomYkiSuoritusEntity().copy(
                arviointitila = Arviointitila.TARKISTUSARVIOITU,
                tarkistusarvioinninKasittelyPvm = kasittelyPvm,
            )
        ykiSuoritusRepository.saveAllNewEntities(listOf(suoritus))

        val result =
            ykiSuoritusRepository.hyvaksyTarkistusarvioinnit(
                suoritusIds = listOf(suoritus.solkiId),
                pvm = kasittelyPvm.minusDays(1),
            )

        assertIs<Either.Left<*>>(result)
        assertIs<HyvaksyTarkistusarviointiError.PaivamaaraEnnenKasittelya>(result.value)
        val storedAfter = ykiSuoritusRepository.findLatestBySolkiIds(listOf(suoritus.solkiId)).first()
        assertEquals(Arviointitila.TARKISTUSARVIOITU, storedAfter.arviointitila)
    }

    @Test
    fun `hyvaksyTarkistusarvioinnit returns Right and updates state when conditions are met`() {
        val kasittelyPvm = LocalDate.of(2025, 6, 15)
        val suoritus =
            generateRandomYkiSuoritusEntity().copy(
                arviointitila = Arviointitila.TARKISTUSARVIOITU,
                tarkistusarvioinninKasittelyPvm = kasittelyPvm,
            )
        ykiSuoritusRepository.saveAllNewEntities(listOf(suoritus))

        val result =
            ykiSuoritusRepository.hyvaksyTarkistusarvioinnit(
                suoritusIds = listOf(suoritus.solkiId),
                pvm = kasittelyPvm,
            )

        assertIs<Either.Right<Int>>(result)
        assertEquals(1, result.value)
        val storedAfter = ykiSuoritusRepository.findLatestBySolkiIds(listOf(suoritus.solkiId)).first()
        assertEquals(Arviointitila.TARKISTUSARVIOINTI_HYVAKSYTTY, storedAfter.arviointitila)
    }

    @Test
    fun `findSuorituksetWithUnsentArvioinninTila palauttaa lähettämättömän arvioidun suorituksen`() {
        val arvioitu = generateRandomYkiSuoritusEntity().copy(arviointitila = Arviointitila.ARVIOITU)
        ykiSuoritusRepository.saveAllNewEntities(listOf(arvioitu))

        val unsent = ykiSuoritusRepository.findSuorituksetWithUnsentArvioinninTila()

        assertEquals(1, unsent.size)
        assertEquals(arvioitu.solkiId, unsent.first().solkiId)
    }

    @Test
    fun `findSuorituksetWithUnsentArvioinninTila ei palauta pelkkää ilmoittautumista tai peruutusta`() {
        val ilmoittautunut = generateRandomYkiSuoritusEntity().copy(arviointitila = Arviointitila.ILMOITTAUTUNUT)
        val peruttu = generateRandomYkiSuoritusEntity().copy(arviointitila = Arviointitila.PERUTTU)
        val arvioitu = generateRandomYkiSuoritusEntity().copy(arviointitila = Arviointitila.ARVIOITU)
        ykiSuoritusRepository.saveAllNewEntities(listOf(ilmoittautunut, peruttu, arvioitu))

        val unsentSolkiIds = ykiSuoritusRepository.findSuorituksetWithUnsentArvioinninTila().map { it.solkiId }

        assertEquals(listOf(arvioitu.solkiId), unsentSolkiIds)
    }

    @Test
    fun `findSuorituksetWithUnsentArvioinninTila ei palauta jo lähetettyä arvioinnin tilaa`() {
        val arvioitu = generateRandomYkiSuoritusEntity().copy(arviointitila = Arviointitila.ARVIOITU)
        ykiSuoritusRepository.saveAllNewEntities(listOf(arvioitu))
        ykiSuoritusRepository.setArvioinninTilaSent(arvioitu.solkiId)

        assertTrue(ykiSuoritusRepository.findSuorituksetWithUnsentArvioinninTila().isEmpty())
    }

    @Test
    fun `findOpiskeluoikeusOidsBySolkiIds palauttaa tyhjän mapin tyhjällä id-listalla`() {
        assertEquals(emptyMap(), ykiSuoritusRepository.findOpiskeluoikeusOidsBySolkiIds(emptyList()))
    }

    @Test
    fun `findOpiskeluoikeusOidsBySolkiIds palauttaa opiskeluoikeudet vain niille id_ille joilla se on`() {
        val opiskeluoikeus1 = createOid(OidClass.OPPIJA, 99999999991)
        val opiskeluoikeus2 = createOid(OidClass.OPPIJA, 99999999992)
        val withOpiskeluoikeus1 = generateRandomYkiSuoritusEntity().copy(koskiOpiskeluoikeus = opiskeluoikeus1)
        val withOpiskeluoikeus2 = generateRandomYkiSuoritusEntity().copy(koskiOpiskeluoikeus = opiskeluoikeus2)
        val withoutOpiskeluoikeus = generateRandomYkiSuoritusEntity().copy(koskiOpiskeluoikeus = null)
        ykiSuoritusRepository.saveAllNewEntities(
            listOf(withOpiskeluoikeus1, withOpiskeluoikeus2, withoutOpiskeluoikeus),
        )

        val tuntematonSolkiId = 1
        val result =
            ykiSuoritusRepository.findOpiskeluoikeusOidsBySolkiIds(
                listOf(
                    withOpiskeluoikeus1.solkiId,
                    withOpiskeluoikeus2.solkiId,
                    withoutOpiskeluoikeus.solkiId,
                    tuntematonSolkiId,
                ),
            )

        assertEquals(
            mapOf(
                withOpiskeluoikeus1.solkiId to opiskeluoikeus1,
                withOpiskeluoikeus2.solkiId to opiskeluoikeus2,
            ),
            result,
        )
    }

    @Test
    fun `findOpiskeluoikeusOidsBySolkiIds palauttaa viimeisimpänä saapuneen version opiskeluoikeuden`() {
        val vanhaOpiskeluoikeus = createOid(OidClass.OPPIJA, 99999999991)
        val uusiOpiskeluoikeus = createOid(OidClass.OPPIJA, 99999999992)
        val vanha =
            generateRandomYkiSuoritusEntity().copy(
                koskiOpiskeluoikeus = vanhaOpiskeluoikeus,
                lastModified = Instant.parse("2025-01-01T10:00:00Z"),
                receivedAt = Instant.parse("2025-01-01T10:00:00Z"),
            )
        val uusi =
            vanha.copy(
                koskiOpiskeluoikeus = uusiOpiskeluoikeus,
                katuosoite = "Uusi katu 1",
                lastModified = Instant.parse("2025-02-01T10:00:00Z"),
                receivedAt = Instant.parse("2025-02-01T10:00:00Z"),
            )
        ykiSuoritusRepository.saveAllNewEntities(listOf(vanha, uusi))

        assertEquals(
            mapOf(vanha.solkiId to uusiOpiskeluoikeus),
            ykiSuoritusRepository.findOpiskeluoikeusOidsBySolkiIds(listOf(vanha.solkiId)),
        )
    }

    @Test
    fun `count suoritukset`() {
        val suoritus = generateRandomYkiSuoritusEntity().copy(etunimet = "Ranja Testi")
        val suoritus2 = generateRandomYkiSuoritusEntity()
        val updatedSuoritus =
            suoritus.copy(
                lastModified = Instant.parse("2024-11-01T13:53:56Z"),
                tekstinYmmartaminen = 5,
                kirjoittaminen = 4,
                rakenteetJaSanasto = 3,
                puheenYmmartaminen = 1,
                puhuminen = 2,
                yleisarvosana = 3,
                tarkistusarvioinninSaapumisPvm = LocalDate.of(2024, 10, 1),
                tarkistusarvioinninAsiatunnus = "123123",
                tarkistusarvioidutOsakokeet = setOf(TutkinnonOsa.puhuminen, TutkinnonOsa.puheenYmmartaminen),
                arvosanaMuuttui = setOf(TutkinnonOsa.puhuminen),
                perustelu = "Tarkistusarvioinnin testi",
                tarkistusarvioinninKasittelyPvm = LocalDate.of(2024, 10, 15),
            )
        ykiSuoritusRepository.saveAllNewEntities(listOf(suoritus, suoritus2, updatedSuoritus))
        val countDistinct = ykiSuoritusRepository.countSuoritukset()
        assertEquals(2L, countDistinct, "Assert failed for count distinct suoritukset")
        val countAll = ykiSuoritusRepository.countSuoritukset(distinct = false)
        assertEquals(3L, countAll, "Assert failed for count all suoritukset")
        val countRanjaDistinct = ykiSuoritusRepository.countSuoritukset(YkiSuoritusFilter.from(search = "ranja"))
        assertEquals(1L, countRanjaDistinct, "Assert failed for count distinct suoritukset with a search term")
        val countRanjaAll =
            ykiSuoritusRepository.countSuoritukset(
                YkiSuoritusFilter.from(search = "ranja"),
                distinct = false,
            )
        assertEquals(2L, countRanjaAll, "Assert failed for count all suoritukset with a search term")
    }

    @Test
    fun `count suoritukset with tutkintokieli filter matches find result size`() {
        val fin = generateRandomYkiSuoritusEntity().copy(tutkintokieli = Tutkintokieli.FIN)
        val swe = generateRandomYkiSuoritusEntity().copy(tutkintokieli = Tutkintokieli.SWE)
        val fin2 = generateRandomYkiSuoritusEntity().copy(tutkintokieli = Tutkintokieli.FIN)
        ykiSuoritusRepository.saveAllNewEntities(listOf(fin, swe, fin2))

        val filter = YkiSuoritusFilter(tutkintokieli = Tutkintokieli.FIN)
        val count = ykiSuoritusRepository.countSuoritukset(filter)
        val found = ykiSuoritusRepository.find(filter).count().toLong()

        assertEquals(found, count, "count ja find tuloksen koko poikkesivat alitauluhaarassa")
        assertEquals(2L, count)
    }

    @Test
    fun `count suoritukset arviointitila-filtterillä laskee vain viimeisimmän version`() {
        val kasittelyPvm = LocalDate.of(2025, 6, 15)
        val suoritus =
            generateRandomYkiSuoritusEntity().copy(
                arviointitila = Arviointitila.TARKISTUSARVIOITU,
                tarkistusarvioinninKasittelyPvm = kasittelyPvm,
            )
        ykiSuoritusRepository.saveAllNewEntities(listOf(suoritus))
        ykiSuoritusRepository.hyvaksyTarkistusarvioinnit(
            suoritusIds = listOf(suoritus.solkiId),
            pvm = kasittelyPvm,
        )

        val tarkistusarvioituFilter = YkiSuoritusFilter(arviointitila = Arviointitila.TARKISTUSARVIOITU)
        val hyvaksyttyFilter = YkiSuoritusFilter(arviointitila = Arviointitila.TARKISTUSARVIOINTI_HYVAKSYTTY)

        assertAll(
            {
                assertEquals(
                    0L,
                    ykiSuoritusRepository.countSuoritukset(tarkistusarvioituFilter),
                    "vanhentunutta TARKISTUSARVIOITU-versiota ei pidä laskea mukaan",
                )
            },
            {
                assertEquals(
                    ykiSuoritusRepository.find(tarkistusarvioituFilter).count().toLong(),
                    ykiSuoritusRepository.countSuoritukset(tarkistusarvioituFilter),
                    "count ja find poikkesivat TARKISTUSARVIOITU-suodatuksella",
                )
            },
            {
                assertEquals(
                    1L,
                    ykiSuoritusRepository.countSuoritukset(hyvaksyttyFilter),
                    "viimeisimmän version TARKISTUSARVIOINTI_HYVAKSYTTY pitää tulla lasketuksi",
                )
            },
            {
                assertEquals(
                    ykiSuoritusRepository.find(hyvaksyttyFilter).count().toLong(),
                    ykiSuoritusRepository.countSuoritukset(hyvaksyttyFilter),
                    "count ja find poikkesivat TARKISTUSARVIOINTI_HYVAKSYTTY-suodatuksella",
                )
            },
        )
    }

    @Test
    fun `hetua ei tallenneta suoritukselle, jonka tutkintopaiva on rajapaivana tai sen jalkeen`() {
        val suoritus =
            generateRandomYkiSuoritusEntity()
                .copy(
                    tutkintopaiva = LocalDate.of(2026, 1, 1),
                    hetu = "010106A911C",
                )
        ykiSuoritusRepository.saveAllNewEntities(listOf(suoritus))

        val stored = ykiSuoritusRepository.findLatestBySolkiIds(listOf(suoritus.solkiId)).first()
        assertNull(stored.hetu)
    }

    @Test
    fun `hetu tallennetaan suoritukselle, jonka tutkintopaiva on ennen rajapaivaa`() {
        val suoritus =
            generateRandomYkiSuoritusEntity(maxDate = LocalDate.of(2025, 12, 31))
                .copy(hetu = "010180-9026")
        ykiSuoritusRepository.saveAllNewEntities(listOf(suoritus))

        val stored = ykiSuoritusRepository.findLatestBySolkiIds(listOf(suoritus.solkiId)).first()
        assertEquals("010180-9026", stored.hetu)
    }

    @Test
    fun `hetullisen 2026-suorituksen uudelleensiirto ei luo uutta versiota`() {
        val suoritus =
            generateRandomYkiSuoritusEntity()
                .copy(
                    tutkintopaiva = LocalDate.of(2026, 3, 15),
                    hetu = "010106A911C",
                )
        ykiSuoritusRepository.saveAllNewEntities(listOf(suoritus))

        val uudelleensiirretty = ykiSuoritusRepository.saveAllNewEntities(listOf(suoritus))
        assertEquals(0, uudelleensiirretty.count())
    }

    private fun versioidenMaara(solkiId: Int): Int =
        jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM yki_suoritus WHERE solki_id = ?",
            Int::class.java,
            solkiId,
        )!!

    private fun uudelleensiirto(suoritus: YkiSuoritusEntity): YkiSuoritusEntity =
        suoritus
            .copy(
                id = null,
                lastModified = Instant.now(),
                receivedAt = Instant.now(),
            ).also { it.ilmoitetutOsakokeet = suoritus.ilmoitetutOsakokeet }

    @Test
    fun `muuttumaton uudelleensiirto ei luo uutta versiota Koski-siirron jalkeen`() {
        val suoritus = generateRandomYkiSuoritusEntity()
        ykiSuoritusRepository.save(suoritus, false)
        ykiSuoritusRepository.save(
            suoritus.copy(
                koskiOpiskeluoikeus = createOid(OidClass.OPPIJA, 99999999991),
                koskiSiirtoKasitelty = true,
            ),
            updateOnConflict = true,
            forceWrite = true,
        )

        assertNull(ykiSuoritusRepository.save(uudelleensiirto(suoritus), false))
        assertEquals(1, versioidenMaara(suoritus.solkiId))
    }

    @Test
    fun `Koski-siirron tulos tallentuu vaikka vain Kosken omat kentat muuttuvat`() {
        val suoritus = generateRandomYkiSuoritusEntity()
        ykiSuoritusRepository.save(suoritus, false)
        val opiskeluoikeus = createOid(OidClass.OPPIJA, 99999999991)

        ykiSuoritusRepository.save(
            suoritus.copy(koskiOpiskeluoikeus = opiskeluoikeus, koskiSiirtoKasitelty = true),
            updateOnConflict = true,
            forceWrite = true,
        )

        val tallennettu = ykiSuoritusRepository.findLatestBySolkiIds(listOf(suoritus.solkiId)).first()
        assertEquals(opiskeluoikeus, tallennettu.koskiOpiskeluoikeus)
        assertTrue(tallennettu.koskiSiirtoKasitelty == true)
        assertEquals(1, versioidenMaara(suoritus.solkiId))
    }

    @Test
    fun `muuttumaton uudelleensiirto ei luo uutta versiota arviointitilan lahetyksen jalkeen`() {
        val suoritus = generateRandomYkiSuoritusEntity()
        ykiSuoritusRepository.save(suoritus, false)
        ykiSuoritusRepository.setArvioinninTilaSent(suoritus.solkiId)

        assertNull(ykiSuoritusRepository.save(uudelleensiirto(suoritus), false))
        assertEquals(1, versioidenMaara(suoritus.solkiId))
    }

    @Test
    fun `muuttumaton uudelleensiirto ei luo uutta versiota arviointitilan lahetysvirheen jalkeen`() {
        val suoritus = generateRandomYkiSuoritusEntity()
        ykiSuoritusRepository.save(suoritus, false)
        ykiSuoritusRepository.setArvioinninTilanLahetysvirhe(suoritus.solkiId, "SUORITUSTA_EI_LOYDY")

        assertNull(ykiSuoritusRepository.save(uudelleensiirto(suoritus), false))
        assertEquals(1, versioidenMaara(suoritus.solkiId))
    }

    @Test
    fun `muuttumaton uudelleensiirto ei luo uutta versiota ilman tarkistusarvioituja osakokeita`() {
        val suoritus =
            generateRandomYkiSuoritusEntity().copy(
                tarkistusarvioidutOsakokeet = null,
                arvosanaMuuttui = null,
            )
        ykiSuoritusRepository.save(suoritus, false)

        assertNull(ykiSuoritusRepository.save(uudelleensiirto(suoritus), false))
        assertEquals(1, versioidenMaara(suoritus.solkiId))
    }

    @Test
    fun `muuttumaton uudelleensiirto ei luo uutta versiota tyhjalla arvosanaMuuttui-joukolla`() {
        val suoritus = generateRandomYkiSuoritusEntity().copy(arvosanaMuuttui = emptySet())
        ykiSuoritusRepository.save(suoritus, false)

        assertNull(ykiSuoritusRepository.save(uudelleensiirto(suoritus), false))
        assertEquals(1, versioidenMaara(suoritus.solkiId))
    }

    @Test
    fun `muuttumaton uudelleensiirto ei luo uutta versiota kun tarkistusarvioitua osakoetta ei ole suorituksella`() {
        val suoritus =
            generateRandomYkiSuoritusEntity().copy(
                tarkistusarvioidutOsakokeet = setOf(TutkinnonOsa.puhuminen, TutkinnonOsa.yleisarvosana),
                arvosanaMuuttui = setOf(TutkinnonOsa.puhuminen),
                yleisarvosana = null,
            )
        ykiSuoritusRepository.save(suoritus, false)

        assertNull(ykiSuoritusRepository.save(uudelleensiirto(suoritus), false))
        assertEquals(1, versioidenMaara(suoritus.solkiId))
    }

    @Test
    fun `hyvaksytty tarkistusarviointi sailyy kun Solki lahettaa saman suorituksen uudelleen`() {
        val kasittelyPvm = LocalDate.of(2025, 6, 15)
        val suoritus =
            generateRandomYkiSuoritusEntity().copy(
                arviointitila = Arviointitila.TARKISTUSARVIOITU,
                tarkistusarvioinninKasittelyPvm = kasittelyPvm,
            )
        ykiSuoritusRepository.save(suoritus, false)
        ykiSuoritusRepository.hyvaksyTarkistusarvioinnit(listOf(suoritus.solkiId), kasittelyPvm)

        assertNull(ykiSuoritusRepository.save(uudelleensiirto(suoritus), false))
        assertEquals(2, versioidenMaara(suoritus.solkiId), "vain hyvaksynta luo uuden version")
        assertEquals(
            Arviointitila.TARKISTUSARVIOINTI_HYVAKSYTTY,
            ykiSuoritusRepository.findLatestBySolkiIds(listOf(suoritus.solkiId)).first().arviointitila,
        )
    }

    @Test
    fun `hyvaksytty tarkistusarviointi sailyy kun Solki lahettaa muuttuneen suorituksen`() {
        val kasittelyPvm = LocalDate.of(2025, 6, 15)
        val suoritus =
            generateRandomYkiSuoritusEntity().copy(
                arviointitila = Arviointitila.TARKISTUSARVIOITU,
                tarkistusarvioinninKasittelyPvm = kasittelyPvm,
            )
        ykiSuoritusRepository.save(suoritus, false)
        ykiSuoritusRepository.hyvaksyTarkistusarvioinnit(listOf(suoritus.solkiId), kasittelyPvm)

        val muuttunut = uudelleensiirto(suoritus).copy(katuosoite = "Uusi katu 1")
        assertNotNull(ykiSuoritusRepository.save(muuttunut, false))

        val viimeisin = ykiSuoritusRepository.findLatestBySolkiIds(listOf(suoritus.solkiId)).first()
        assertEquals(Arviointitila.TARKISTUSARVIOINTI_HYVAKSYTTY, viimeisin.arviointitila)
        assertEquals("Uusi katu 1", viimeisin.katuosoite)
    }

    @Test
    fun `Solkin oma arviointitilan muutos tallentuu hyvaksynnan jalkeen`() {
        val kasittelyPvm = LocalDate.of(2025, 6, 15)
        val suoritus =
            generateRandomYkiSuoritusEntity().copy(
                arviointitila = Arviointitila.TARKISTUSARVIOITU,
                tarkistusarvioinninKasittelyPvm = kasittelyPvm,
            )
        ykiSuoritusRepository.save(suoritus, false)
        ykiSuoritusRepository.hyvaksyTarkistusarvioinnit(listOf(suoritus.solkiId), kasittelyPvm)

        val uusiTarkistuskierros =
            uudelleensiirto(suoritus).copy(
                arviointitila = Arviointitila.TARKISTUSARVIOITAVA,
                tarkistusarvioinninKasittelyPvm = null,
            )
        assertNotNull(ykiSuoritusRepository.save(uusiTarkistuskierros, false))

        assertEquals(
            Arviointitila.TARKISTUSARVIOITAVA,
            ykiSuoritusRepository.findLatestBySolkiIds(listOf(suoritus.solkiId)).first().arviointitila,
        )
    }

    @Test
    fun `muuttunut suoritus luo edelleen uuden version`() {
        val suoritus = generateRandomYkiSuoritusEntity()
        ykiSuoritusRepository.save(suoritus, false)
        ykiSuoritusRepository.setArvioinninTilaSent(suoritus.solkiId)

        val muuttunut = uudelleensiirto(suoritus).copy(sukunimi = "Muuttunut")
        assertNotNull(ykiSuoritusRepository.save(muuttunut, false))
        assertEquals(2, versioidenMaara(suoritus.solkiId))
    }
}
