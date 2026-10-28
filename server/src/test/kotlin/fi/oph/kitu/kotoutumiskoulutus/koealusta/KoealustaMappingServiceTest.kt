package fi.oph.kitu.kotoutumiskoulutus.koealusta

import arrow.core.Either
import arrow.core.left
import arrow.core.right
import fi.oph.kitu.kotoutumiskoulutus.koealusta.KoealustaSuorituksetResponse.User
import fi.oph.kitu.kotoutumiskoulutus.koealusta.KoealustaSuorituksetResponse.User.Completion
import fi.oph.kitu.kotoutumiskoulutus.suoritukset.error.KielitestiSuoritusError
import fi.oph.kitu.oid.Oid
import fi.oph.kitu.oppijanumero.EmptyRequest
import fi.oph.kitu.oppijanumero.Oppija
import fi.oph.kitu.oppijanumero.OppijanumeroException
import fi.oph.kitu.oppijanumero.OppijanumeroService
import fi.oph.kitu.oppijanumero.OppijanumeroTroubleshootingService
import fi.oph.kitu.oppijanumero.OppijanumerorekisteriHenkilo
import fi.oph.kitu.oppijanumero.YleistunnisteHaeRequest
import fi.oph.kitu.util.TimeService
import fi.oph.kitu.util.defaultObjectMapper
import fi.oph.kitu.util.result.getOrThrow
import org.junit.jupiter.api.assertAll
import org.springframework.http.ResponseEntity
import java.time.Instant
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class KoealustaMappingServiceTest {
    private val ensimmainenAjo = Instant.parse("2026-10-01T03:00:00Z")
    private val toinenAjo = Instant.parse("2026-10-02T03:00:00Z")

    private val antero =
        User(
            userid = 2,
            firstnames = "Antero",
            lastname = "Testi-Moikka",
            preferredname = "Antero",
            oppijanumero = null,
            ssn = "010180-9026",
            email = "antero.testi@oph.fi",
            completions =
                listOf(
                    Completion(
                        courseid = 32,
                        coursename = "Integraatio testaus",
                        lang = "fi",
                        schoolOID = "1.2.246.562.10.42923230015",
                        results =
                            listOf("luetun ymmärtäminen", "kuullun ymmärtäminen", "puhuminen", "kirjoittaminen")
                                .map { Completion.Result(it, "B1") },
                        timecompleted = 1728969131,
                        teacheremail = "opettaja@testi.oph.fi",
                        questionbank = null,
                    ),
                ),
        )

    @Test
    fun `toistuvalle henkilötietovirheelle tehdään vain oppijanumeron haku ilman vianetsintää`() {
        val onr = LaskevaOnr()
        val aiemmat = tuoVirheet(onr, ensimmainenAjo)
        val haututEnsimmaisellaAjolla = onr.haut

        val uudet = tuoVirheet(onr, toinenAjo, aiemmat)

        val aiempi = aiemmat.single()
        val uusi = uudet.single()
        assertAll(
            fun() = assertTrue(haututEnsimmaisellaAjolla > 1),
            fun() =
                assertEquals(haututEnsimmaisellaAjolla + 1, onr.haut, "Toinen ajo saa tehdä vain oppijanumeron haun"),
            fun() = assertEquals(400, aiempi.onrStatus),
            fun() = assertEquals(aiempi.viesti, uusi.viesti),
            fun() = assertEquals(aiempi.lisatietoja, uusi.lisatietoja),
            fun() = assertEquals(aiempi.onrLisatietoja, uusi.onrLisatietoja),
            fun() = assertEquals(aiempi.onrStatus, uusi.onrStatus),
        )
    }

    @Test
    fun `aiemmin kirjattu henkilötietovirhe saa kurssin tiedot uudesta tuonnista`() {
        val onr = LaskevaOnr()
        val aiempi = tuoVirheet(onr, ensimmainenAjo).single()
        val uusiSuoritus =
            antero.completions.single().copy(courseid = 33, coursename = "Uusintatesti", timecompleted = 1730000000)

        val uusi = tuoVirheet(onr, toinenAjo, listOf(aiempi), antero.copy(completions = listOf(uusiSuoritus))).single()

        assertAll(
            fun() = assertEquals(32, aiempi.kurssiId),
            fun() = assertEquals("Integraatio testaus", aiempi.kurssi),
            fun() = assertEquals(Instant.ofEpochSecond(1728969131), aiempi.suoritusaika),
            fun() = assertEquals(33, uusi.kurssiId),
            fun() = assertEquals("Uusintatesti", uusi.kurssi),
            fun() = assertEquals(Instant.ofEpochSecond(1730000000), uusi.suoritusaika),
        )
    }

    @Test
    fun `oppijanumerorekisterissä korjattu henkilötietovirhe poistuu, vaikka nimet koealustalla eivät muutu`() {
        val onr = LaskevaOnr()
        val aiemmat = tuoVirheet(onr, ensimmainenAjo)
        onr.oppijanumero = Oid.parse("1.2.246.562.24.33342764709").getOrThrow()

        val uudet = tuoVirheet(onr, toinenAjo, aiemmat)

        assertAll(
            fun() = assertEquals(1, aiemmat.size),
            fun() = assertEquals(emptyList(), uudet),
        )
    }

    @Test
    fun `hylätystä tietueesta ei kirjata virhettä, kun sen suoritukset on jo tallennettu hetun henkilölle`() {
        val onr = LaskevaOnr().apply { henkiloHetulla = henkilo(anteronOppijanumero) }

        val virheet = tuoVirheet(onr, ensimmainenAjo, tallennetut = tallennettu(32, anteronOppijanumero))

        assertAll(
            fun() = assertEquals(emptyList(), virheet),
            fun() = assertEquals(2, onr.haut, "Vianetsintää ei saa ajaa"),
        )
    }

    @Test
    fun `aiemmin kirjattu virhe poistuu, kun suoritus on jo tallennettu hetun henkilölle`() {
        val onr = LaskevaOnr()
        val aiemmat = tuoVirheet(onr, ensimmainenAjo)
        onr.henkiloHetulla = henkilo(anteronOppijanumero)

        val uudet = tuoVirheet(onr, toinenAjo, aiemmat, tallennetut = tallennettu(32, anteronOppijanumero))

        assertAll(
            fun() = assertEquals(1, aiemmat.size),
            fun() = assertEquals(emptyList(), uudet),
        )
    }

    @Test
    fun `virhe kirjataan, kun suoritus on tallennettu toiselle henkilölle`() {
        val onr = LaskevaOnr().apply { henkiloHetulla = henkilo(anteronOppijanumero) }
        val toinenHenkilo = Oid.parse("1.2.246.562.24.10000000008").getOrThrow()

        val virheet = tuoVirheet(onr, ensimmainenAjo, tallennetut = tallennettu(32, toinenHenkilo))

        assertEquals(1, virheet.size)
    }

    @Test
    fun `virhe kirjataan, kun kaikkia tietueen suorituksia ei ole tallennettu`() {
        val onr = LaskevaOnr().apply { henkiloHetulla = henkilo(anteronOppijanumero) }
        val toinenSuoritus = antero.completions.single().copy(courseid = 33, timecompleted = 1730000000)

        val virheet =
            tuoVirheet(
                onr,
                ensimmainenAjo,
                user = antero.copy(completions = antero.completions + toinenSuoritus),
                tallennetut = tallennettu(32, anteronOppijanumero),
            )

        assertEquals(1, virheet.size)
    }

    @Test
    fun `vianetsintä ajetaan uudelleen, kun nimet muuttuvat koealustalla`() {
        val onr = LaskevaOnr()
        val aiemmat = tuoVirheet(onr, ensimmainenAjo)
        val haututEnsimmaisellaAjolla = onr.haut

        tuoVirheet(onr, toinenAjo, aiemmat, antero.copy(lastname = "Testi-Moikkanen"))

        assertTrue(onr.haut > haututEnsimmaisellaAjolla + 1)
    }

    @Test
    fun `vianetsintä ajetaan uudelleen, kun oppijanumerorekisterin virheviesti muuttuu`() {
        val onr = LaskevaOnr()
        val aiempi = tuoVirheet(onr, ensimmainenAjo).single()
        val haututEnsimmaisellaAjolla = onr.haut

        val uusi =
            tuoVirheet(onr, toinenAjo, listOf(aiempi.copy(viesti = "Oppijanumeron haku epäonnistui: muu syy"))).single()

        assertAll(
            fun() = assertTrue(onr.haut > haututEnsimmaisellaAjolla + 1),
            fun() = assertEquals(aiempi.viesti, uusi.viesti),
        )
    }

    @Test
    fun `vianetsintä ajetaan uudelleen, kun oppijanumerorekisteri vastaa eri tilalla kuin aiemmin`() {
        val onr = LaskevaOnr()
        val aiempi = tuoVirheet(onr, ensimmainenAjo).single()

        assertAll(
            listOf(null, 401, 404, 500).map { status ->
                fun() {
                    val haututEnnen = onr.haut
                    tuoVirheet(onr, toinenAjo, listOf(aiempi.copy(onrStatus = status)))
                    assertTrue(onr.haut > haututEnnen + 1, "Virheelle tilalla $status pitää ajaa vianetsintä")
                }
            },
        )
    }

    private fun tuoVirheet(
        onr: OppijanumeroService,
        nyt: Instant,
        aiemmatVirheet: List<KielitestiSuoritusError> = emptyList(),
        user: User = antero,
        tallennetut: (Int, Instant) -> Set<Oid> = { _, _ -> emptySet() },
    ): List<KielitestiSuoritusError> {
        val palvelu =
            KoealustaMappingService(
                KoealustaResponseParser(defaultObjectMapper),
                KoealustaSuoritusValidator(),
                onr,
                OppijanumeroTroubleshootingService(onr),
                object : TimeService {
                    override fun now(): Instant = nyt
                },
            )
        val (_, virheet) =
            palvelu.convertToKielitestiSuoritus(
                KoealustaSuorituksetResponse(listOf(user)),
                aiemmatVirheet,
                tallennetut,
            )
        return palvelu.convertErrors(virheet?.oppijanumeroExceptions.orEmpty()).toList()
    }

    private val anteronOppijanumero = Oid.parse("1.2.246.562.24.33342764709").getOrThrow()

    private fun tallennettu(
        kurssiId: Int,
        oppijanumero: Oid,
    ): (Int, Instant) -> Set<Oid> =
        { kurssi, suoritusaika ->
            if (kurssi == kurssiId &&
                suoritusaika == Instant.ofEpochSecond(1728969131)
            ) {
                setOf(oppijanumero)
            } else {
                emptySet()
            }
        }

    private fun henkilo(oppijanumero: Oid): OppijanumerorekisteriHenkilo =
        defaultObjectMapper.readValue(
            """{ "oidHenkilo": "$oppijanumero", "oppijanumero": "$oppijanumero" }""",
            OppijanumerorekisteriHenkilo::class.java,
        )

    private class LaskevaOnr : OppijanumeroService {
        var haut = 0
        var oppijanumero: Oid? = null
        var henkiloHetulla: OppijanumerorekisteriHenkilo? = null

        override fun getMasterOid(oppija: Oppija): Either<OppijanumeroException, Oid> {
            haut++
            oppijanumero?.let { return it.right() }
            return OppijanumeroException
                .BadRequest(
                    YleistunnisteHaeRequest(oppija.etunimet, oppija.hetu, oppija.kutsumanimi, oppija.sukunimi),
                    ResponseEntity.badRequest().build(),
                ).left()
        }

        override fun getHenkiloByHetu(hetu: String): Either<OppijanumeroException, OppijanumerorekisteriHenkilo> {
            haut++
            henkiloHetulla?.let { return it.right() }
            return OppijanumeroException
                .OppijaNotFoundException(
                    EmptyRequest(),
                    ResponseEntity.notFound().build(),
                ).left()
        }

        override fun getMasterOid(henkiloOid: Oid): Either<OppijanumeroException, Oid> = error("ei käytössä")

        override fun getOppijanumero(henkiloOid: Oid): Either<OppijanumeroException, Oid> = error("ei käytössä")

        override fun getHenkiloByMasterOid(
            masterOid: Oid,
        ): Either<OppijanumeroException, OppijanumerorekisteriHenkilo> = error("ei käytössä")

        override fun getLinkedOids(henkiloOid: Oid): Either<OppijanumeroException, Set<Oid>> = error("ei käytössä")
    }
}
