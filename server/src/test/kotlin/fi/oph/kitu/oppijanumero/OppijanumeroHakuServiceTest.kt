package fi.oph.kitu.oppijanumero

import arrow.core.Either
import arrow.core.left
import arrow.core.right
import fi.oph.kitu.oid.Oid
import fi.oph.kitu.util.result.getOrThrow
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class OppijanumeroHakuServiceTest {
    private val oppija =
        Oppija(
            etunimet = "Matti Ilmari",
            hetu = "010180-9026",
            kutsumanimi = "Matti",
            sukunimi = "Virtanen",
        )
    private val loydettyOid = Oid.parse("1.2.246.562.24.33342764709").getOrThrow()

    private fun palvelu(vastaus: (Oppija) -> Either<OppijanumeroException, Oid>) =
        object : OppijanumeroService {
            val kutsutut = mutableListOf<Oppija>()

            override fun getMasterOid(oppija: Oppija): Either<OppijanumeroException, Oid> {
                kutsutut.add(oppija)
                return vastaus(oppija)
            }

            override fun getMasterOid(henkiloOid: Oid): Either<OppijanumeroException, Oid> = throw NotImplementedError()

            override fun getOppijanumero(henkiloOid: Oid): Either<OppijanumeroException, Oid> =
                throw NotImplementedError()

            override fun getHenkiloByMasterOid(
                masterOid: Oid,
            ): Either<OppijanumeroException, OppijanumerorekisteriHenkilo> = throw NotImplementedError()

            override fun getLinkedOids(henkiloOid: Oid): Either<OppijanumeroException, Set<Oid>> =
                throw NotImplementedError()
        }

    private fun badRequest(status: HttpStatus) =
        OppijanumeroException
            .BadRequest(
                YleistunnisteHaeRequest(
                    etunimet = oppija.etunimet,
                    hetu = oppija.hetu,
                    kutsumanimi = oppija.kutsumanimi,
                    sukunimi = oppija.sukunimi,
                ),
                response = ResponseEntity.status(status).body("onr sanoo ei"),
            ).left()

    @Test
    fun `ONR-n 400 yritetaan uudelleen nimivaihtoehdoilla`() {
        val service =
            palvelu { yritetty ->
                if (yritetty.kutsumanimi == "Ilmari") loydettyOid.right() else badRequest(HttpStatus.BAD_REQUEST)
            }

        val tulos = OppijanumeroHakuService(service, OppijanumeroTroubleshootingService(service)).haeMasterOid(oppija)

        assertEquals(loydettyOid, tulos.getOrNull())
        assertTrue(service.kutsutut.any { it.kutsumanimi == "Ilmari" }, "nimivaihtoehtoja kokeiltiin")
    }

    @Test
    fun `kuormanrajoitusta ei yriteta uudelleen nimivaihtoehdoilla`() {
        val service = palvelu { badRequest(HttpStatus.TOO_MANY_REQUESTS) }

        val tulos = OppijanumeroHakuService(service, OppijanumeroTroubleshootingService(service)).haeMasterOid(oppija)

        assertTrue(tulos.isLeft())
        assertEquals(
            1,
            service.kutsutut.size,
            "429 ei ole nimiongelma, eikä sitä pidä monistaa nimikombinaatioilla",
        )
    }

    @Test
    fun `tunnistamaton oppija yritetaan yha uudelleen nimivaihtoehdoilla`() {
        val service =
            palvelu { yritetty ->
                if (yritetty.sukunimi == "Matti Ilmari") {
                    loydettyOid.right()
                } else {
                    OppijanumeroException.OppijaNotIdentifiedException(EmptyRequest()).left()
                }
            }

        val tulos = OppijanumeroHakuService(service, OppijanumeroTroubleshootingService(service)).haeMasterOid(oppija)

        assertEquals(loydettyOid, tulos.getOrNull())
    }
}
