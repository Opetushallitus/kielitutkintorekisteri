package fi.oph.kitu.organisaatiot

import arrow.core.Either
import fi.oph.kitu.i18n.LocalizedString
import fi.oph.kitu.oid.Oid
import fi.oph.kitu.util.cache.PersistentCache
import fi.oph.kitu.util.retry.RetryOutboundIntegration
import io.opentelemetry.instrumentation.annotations.WithSpan
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import org.springframework.boot.ApplicationArguments
import org.springframework.boot.ApplicationRunner
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Component
import org.springframework.stereotype.Service

data class Organisaatiot(
    val nimet: Map<Oid, LocalizedString>,
) {
    companion object {
        val EMPTY = Organisaatiot(emptyMap())
    }
}

interface OrganisaatioService {
    fun getOrganisaatioCache(): PersistentCache

    fun getOrganisaatiohierarkia(): Either<OrganisaatiopalveluException, GetOrganisaatiohierarkiaResponse>

    fun getOrganisaatiot(): Organisaatiot =
        getOrganisaatioCache().getAsap(Organisaatiot::class.java, "OrganisaatioService.organisaatiot") {
            getOrganisaatiohierarkia().map { it.getOrganisaatiot() }.getOrNull()
        } ?: Organisaatiot.EMPTY

    fun searchOrganisaatiot(query: String): Organisaatiot =
        Organisaatiot(getOrganisaatiot().nimet.filter { (_, nimi) -> nimi.contains(query, ignoreCase = true) }.toMap())
}

@Service
@Profile("!test && !e2e && !local-opintopolku")
class OrganisaatioServiceImpl(
    val client: OrganisaatiopalveluClient,
    val cache: PersistentCache,
) : OrganisaatioService {
    override fun getOrganisaatioCache(): PersistentCache = cache

    @WithSpan
    @RetryOutboundIntegration
    override fun getOrganisaatiohierarkia(): Either<OrganisaatiopalveluException, GetOrganisaatiohierarkiaResponse> =
        client.get(
            endpoint = "api/hierarkia/hae",
            query =
                mapOf(
                    "aktiiviset" to true,
                    "suunnitellut" to false,
                    "lakkautetut" to false,
                ),
            responseType = GetOrganisaatiohierarkiaResponse::class.java,
        )
}

@Component
@Profile("!local") // Ei ladata koko organisaatiohierarkiaa joka kerta kun lokaali instanssi käynnistyy
class OrganisaatioPalveluStartupTasks(
    val organisaatioService: OrganisaatioService,
) : ApplicationRunner {
    override fun run(args: ApplicationArguments) {
        CoroutineScope(Dispatchers.Default).launch {
            organisaatioService.getOrganisaatiot() // Lataa tuoreet nimet välimuistiin
        }
    }
}

data class GetOrganisaatiohierarkiaResponse(
    val numHits: Int,
    val organisaatiot: List<Organisaatiohierarkia>,
) {
    fun getOrganisaatiot(): Organisaatiot = Organisaatiot(organisaatiot.flatMap { it.getNames() }.toMap())
}

data class Organisaatiohierarkia(
    val aliOrganisaatioMaara: Int,
    val alkuPvm: Long,
    val children: List<Organisaatiohierarkia>,
    val kieletUris: List<String>,
    val kotipaikkaUri: String,
    val lyhytNimi: LocalizedString,
    val match: Boolean,
    val nimi: LocalizedString,
    val oid: Oid,
    val organisaatiotyypit: List<String>,
    val parentOid: Oid?,
    val parentOidPath: String?,
    val status: String,
    val toimipistekoodi: String?,
    val tyypit: List<String>,
    val yTunnus: String?,
) {
    fun getNames(): List<Pair<Oid, LocalizedString>> = listOf(oid to nimi) + children.flatMap { it.getNames() }
}
