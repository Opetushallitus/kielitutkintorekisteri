package fi.oph.kitu.kotoutumiskoulutus.koealusta

import arrow.core.Either
import arrow.core.left
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
    fun `aiemmin kirjattua henkilötietovirhettä ei tarkisteta oppijanumerorekisteristä uudelleen`() {
        val onr = LaskevaOnr()
        val aiemmat = tuoVirheet(onr, ensimmainenAjo)
        val haututEnsimmaisellaAjolla = onr.haut

        val uudet = tuoVirheet(onr, toinenAjo, aiemmat)

        val aiempi = aiemmat.single()
        val uusi = uudet.single()
        assertAll(
            fun() = assertTrue(haututEnsimmaisellaAjolla > 0),
            fun() =
                assertEquals(haututEnsimmaisellaAjolla, onr.haut, "Toinen ajo ei saa kysyä oppijanumerorekisteriltä"),
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
    fun `henkilötietovirhe tarkistetaan uudelleen, kun nimet muuttuvat koealustalla`() {
        val onr = LaskevaOnr()
        val aiemmat = tuoVirheet(onr, ensimmainenAjo)
        val haututEnsimmaisellaAjolla = onr.haut

        tuoVirheet(onr, toinenAjo, aiemmat, antero.copy(lastname = "Testi-Moikkanen"))

        assertTrue(onr.haut > haututEnsimmaisellaAjolla)
    }

    @Test
    fun `muu kuin henkilötietovirhe tarkistetaan uudelleen`() {
        val onr = LaskevaOnr()
        val aiempi = tuoVirheet(onr, ensimmainenAjo).single()

        assertAll(
            listOf(null, 401, 500).map { status ->
                fun() {
                    val haututEnnen = onr.haut
                    tuoVirheet(onr, toinenAjo, listOf(aiempi.copy(onrStatus = status)))
                    assertTrue(onr.haut > haututEnnen, "Virhe tilalla $status pitää tarkistaa uudelleen")
                }
            },
        )
    }

    private fun tuoVirheet(
        onr: OppijanumeroService,
        nyt: Instant,
        aiemmatVirheet: List<KielitestiSuoritusError> = emptyList(),
        user: User = antero,
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
            )
        return palvelu.convertErrors(virheet?.oppijanumeroExceptions.orEmpty()).toList()
    }

    private class LaskevaOnr : OppijanumeroService {
        var haut = 0

        override fun getMasterOid(oppija: Oppija): Either<OppijanumeroException, Oid> {
            haut++
            return OppijanumeroException
                .BadRequest(
                    YleistunnisteHaeRequest(oppija.etunimet, oppija.hetu, oppija.kutsumanimi, oppija.sukunimi),
                    ResponseEntity.badRequest().build(),
                ).left()
        }

        override fun getHenkiloByHetu(hetu: String): Either<OppijanumeroException, OppijanumerorekisteriHenkilo> {
            haut++
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
