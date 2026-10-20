package fi.oph.kitu.koodisto

import fi.oph.kitu.util.defaultObjectMapper
import org.springframework.context.annotation.Profile
import org.springframework.core.io.support.PathMatchingResourcePatternResolver
import org.springframework.stereotype.Service

@Service
@Profile("local-opintopolku")
class OfflineKoodistoService : KoodistoService {
    private val koodistot: Map<String, List<KoodistopalveluKoodiviite>> by lazy {
        PathMatchingResourcePatternResolver()
            .getResources("classpath*:opintopolku-mocks/koodisto-service/codeelement/codes/*.json")
            .associate { resource ->
                resource.filename!!.removeSuffix(".json") to
                    resource.inputStream.use {
                        defaultObjectMapper.readValue(it, Array<KoodistopalveluKoodiviite>::class.java).toList()
                    }
            }
    }

    override fun getKoodiviitteet(koodistoUri: String): List<KoodistopalveluKoodiviite>? = koodistot[koodistoUri]
}
