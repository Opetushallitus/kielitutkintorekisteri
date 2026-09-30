package fi.oph.kitu.oppijanumero

import arrow.core.Either
import arrow.core.left
import arrow.core.right
import fi.oph.kitu.oid.Oid
import fi.oph.kitu.util.defaultObjectMapper
import fi.oph.kitu.util.validation.Validation.ValidationError
import org.springframework.http.ResponseEntity
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs

class OppijanumeroValidationTest {
    private val oid = Oid("1.2.246.562.24.20281155246")
    private val path = listOf("henkilo", "oid")

    private fun validation(
        masterOid: () -> Either<OppijanumeroException, Oid> = { throw NotImplementedError() },
        henkiloByMasterOid: () -> Either<OppijanumeroException, OppijanumerorekisteriHenkilo> = {
            throw NotImplementedError()
        },
    ): OppijanumeroValidation =
        OppijanumeroValidation(
            object : OppijanumeroService {
                override fun getMasterOid(oppija: Oppija): Either<OppijanumeroException, Oid> =
                    throw NotImplementedError()

                override fun getMasterOid(henkiloOid: Oid): Either<OppijanumeroException, Oid> = masterOid()

                override fun getOppijanumero(henkiloOid: Oid): Either<OppijanumeroException, Oid> =
                    throw NotImplementedError()

                override fun getHenkiloByMasterOid(
                    masterOid: Oid,
                ): Either<OppijanumeroException, OppijanumerorekisteriHenkilo> = henkiloByMasterOid()

                override fun getLinkedOids(henkiloOid: Oid): Either<OppijanumeroException, Set<Oid>> =
                    throw NotImplementedError()
            },
        )

    private fun henkiloWith(
        oppijanumero: String?,
        oidHenkilo: String?,
    ): OppijanumerorekisteriHenkilo =
        defaultObjectMapper.convertValue(
            mapOf("oppijanumero" to oppijanumero, "oidHenkilo" to oidHenkilo),
            OppijanumerorekisteriHenkilo::class.java,
        )

    private fun oppijaNotFound(): OppijanumeroException.OppijaNotFoundException =
        OppijanumeroException.OppijaNotFoundException(EmptyRequest(), ResponseEntity.notFound().build())

    // mapHenkiloOidToMasterOid

    @Test
    fun `mapHenkiloOidToMasterOid palauttaa oppijanumerorekisterin master-oidin`() {
        val master = Oid("1.2.246.562.24.33342764709")

        val result = validation(masterOid = { master.right() }).mapHenkiloOidToMasterOid(oid, path)

        assertEquals(master.right(), result)
    }

    @Test
    fun `mapHenkiloOidToMasterOid tuottaa selkeän suomenkielisen virheen kun oppijaa ei löydy`() {
        val result = validation(masterOid = { oppijaNotFound().left() }).mapHenkiloOidToMasterOid(oid, path)

        val error = assertIs<ValidationError.EnrichmentError>((result as Either.Left).value)
        assertEquals("Oppijanumeroa $oid ei löydy Oppijanumerorekisteristä", error.message)
    }

    @Test
    fun `mapHenkiloOidToMasterOid kääntää oppijanumeropalvelun poikkeuksen EnrichmentErroriksi`() {
        val result = validation(masterOid = { throw RuntimeException("boom") }).mapHenkiloOidToMasterOid(oid, path)

        assertIs<ValidationError.EnrichmentError>((result as Either.Left).value)
    }

    // validateOppijanumeroInOnr

    @Test
    fun `validateOppijanumeroInOnr palauttaa löytyvän henkilön`() {
        val henkilo = henkiloWith(oppijanumero = oid.toString(), oidHenkilo = oid.toString())

        val result =
            validation(
                masterOid = { oid.right() },
                henkiloByMasterOid = { henkilo.right() },
            ).validateOppijanumeroInOnr(oid, path)

        assertEquals(henkilo.right(), result)
    }

    @Test
    fun `validateOppijanumeroInOnr pitää puuttuvan oppijan tavallisena kenttävirheenä`() {
        val result = validation(masterOid = { oppijaNotFound().left() }).validateOppijanumeroInOnr(oid, path)

        assertIs<ValidationError.FieldError>((result as Either.Left).value)
    }

    @Test
    fun `validateOppijanumeroInOnr kääntää odottamattoman oppijanumerovirheen EnrichmentErroriksi`() {
        val result =
            validation(
                masterOid = { oid.right() },
                henkiloByMasterOid = { OppijanumeroException.NullResponse(EmptyRequest()).left() },
            ).validateOppijanumeroInOnr(oid, path)

        assertIs<ValidationError.EnrichmentError>((result as Either.Left).value)
    }

    @Test
    fun `validateOppijanumeroInOnr kääntää oppijanumeropalvelun poikkeuksen EnrichmentErroriksi`() {
        val result = validation(masterOid = { throw RuntimeException("boom") }).validateOppijanumeroInOnr(oid, path)

        assertIs<ValidationError.EnrichmentError>((result as Either.Left).value)
    }
}
