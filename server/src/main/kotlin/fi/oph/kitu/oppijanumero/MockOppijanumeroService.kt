package fi.oph.kitu.oppijanumero

import arrow.core.Either
import arrow.core.flatMap
import arrow.core.left
import arrow.core.right
import fi.oph.kitu.oid.Oid
import fi.oph.kitu.util.defaultObjectMapper
import fi.oph.kitu.util.result.getOrThrow
import org.springframework.beans.factory.ObjectProvider
import org.springframework.context.annotation.Profile
import org.springframework.core.io.ClassPathResource
import org.springframework.core.io.support.PathMatchingResourcePatternResolver
import org.springframework.http.ResponseEntity
import org.springframework.stereotype.Service
import java.io.FileNotFoundException

@Service
@Profile("test | e2e | local-opintopolku")
class MockOppijanumeroService(
    private val henkilolahteet: ObjectProvider<MockHenkilolahde>,
) : OppijanumeroService {
    override fun getMasterOid(oppija: Oppija): Either<OppijanumeroException, Oid> {
        require(oppija.etunimet.isNotEmpty()) { "etunimet cannot be empty" }
        require(oppija.hetu.isNotEmpty()) { "hetu cannot be empty" }
        require(oppija.sukunimi.isNotEmpty()) { "sukunimi cannot be empty" }
        require(oppija.kutsumanimi.isNotEmpty()) { "kutsumanimi cannot be empty" }

        val request =
            YleistunnisteHaeRequest(
                etunimet = oppija.etunimet,
                hetu = oppija.hetu,
                kutsumanimi = oppija.kutsumanimi,
                sukunimi = oppija.sukunimi,
            )

        return when (oppija.hetu) {
            "INVALID_HETU" -> {
                OppijanumeroException.BadRequest(request, response = ResponseEntity.badRequest().build()).left()
            }

            "WRONG_HETU" -> {
                OppijanumeroException.OppijaNotIdentifiedException(request).left()
            }

            else -> {
                val oid =
                    oppijaToOid[oppija]
                        ?: henkiloHetulla(oppija.hetu)?.let { it.oppijanumero ?: it.oidHenkilo }
                oid?.let {
                    Oid
                        .parse(it)
                        .getOrThrow()
                        .right()
                } ?: OppijanumeroException.OppijaNotFoundException(request, ResponseEntity.notFound().build()).left()
            }
        }
    }

    override fun getMasterOid(henkiloOid: Oid): Either<OppijanumeroException, Oid> =
        getHenkiloByMasterOid(henkiloOid).flatMap { henkilo ->
            parseOid(henkilo.oppijanumero ?: henkilo.oidHenkilo)
        }

    override fun getOppijanumero(henkiloOid: Oid): Either<OppijanumeroException, Oid> =
        getHenkiloByMasterOid(henkiloOid).flatMap { henkilo ->
            henkilo.oppijanumero?.let { parseOid(it) }
                ?: OppijanumeroException.OppijaNotIdentifiedException(EmptyRequest()).left()
        }

    override fun getHenkiloByMasterOid(masterOid: Oid): Either<OppijanumeroException, OppijanumerorekisteriHenkilo> =
        (fixtureHenkilo(masterOid) ?: henkiloLahteista(masterOid))
            ?.right()
            ?: OppijanumeroException.OppijaNotFoundException(EmptyRequest(), ResponseEntity.notFound().build()).left()

    override fun getLinkedOids(henkiloOid: Oid): Either<OppijanumeroException, Set<Oid>> =
        linkedOids
            .find { it.contains(henkiloOid) }
            .orEmpty()
            .right()

    private fun fixtureHenkilo(oid: Oid): OppijanumerorekisteriHenkilo? =
        try {
            val source =
                ClassPathResource(
                    "./opintopolku-mocks/oppijanumerorekisteri-service/henkilo/$oid.json",
                ).file
            defaultObjectMapper.readValue(source, OppijanumerorekisteriHenkilo::class.java)
        } catch (_: FileNotFoundException) {
            null
        }

    private fun henkiloLahteista(oid: Oid): OppijanumerorekisteriHenkilo? =
        henkilolahteet.firstNotNullOfOrNull { it.henkilot()[oid] }

    private fun henkiloHetulla(hetu: String): OppijanumerorekisteriHenkilo? =
        henkilolahteet.firstNotNullOfOrNull { lahde ->
            lahde.henkilot().values.firstOrNull { hetu in it.hetut() }
        }

    companion object {
        private const val HENKILO_FIXTURES =
            "classpath*:opintopolku-mocks/oppijanumerorekisteri-service/henkilo/*.json"

        val linkedOids: List<Set<Oid>> =
            PathMatchingResourcePatternResolver()
                .getResources(HENKILO_FIXTURES)
                .map { resource ->
                    resource.inputStream.use {
                        defaultObjectMapper.readValue(it, OppijanumerorekisteriHenkilo::class.java)
                    }
                }.mapNotNull { henkilo ->
                    val oid = Oid.parse(henkilo.oidHenkilo).getOrNull() ?: return@mapNotNull null
                    val master =
                        Oid.parse(henkilo.oppijanumero ?: henkilo.oidHenkilo).getOrNull()
                            ?: return@mapNotNull null
                    master to oid
                }.groupBy({ it.first }, { it.second })
                .map { (master, oids) -> (oids + master).toSet() }
                .filter { it.size > 1 }

        val oppijaToOid =
            mapOf(
                Oppija(
                    "Ranja Testi",
                    "010180-9026",
                    "Ranja",
                    "Öhman-Testi",
                ) to "1.2.246.562.24.33342764709",
                Oppija(
                    "Minerva Alli Aniitta",
                    "040265-9985",
                    "Aniitta",
                    "Marttila",
                ) to "1.2.246.562.24.92472049678",
            )
    }
}
