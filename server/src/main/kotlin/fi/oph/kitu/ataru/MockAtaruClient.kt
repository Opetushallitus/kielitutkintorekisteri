package fi.oph.kitu.ataru

import arrow.core.Either
import arrow.core.right
import fi.oph.kitu.util.defaultObjectMapper
import org.springframework.context.annotation.Profile
import org.springframework.core.io.ClassPathResource
import org.springframework.stereotype.Component
import tools.jackson.databind.JsonNode

@Component
@Profile("e2e | local-opintopolku")
class MockAtaruClient : AtaruClient {
    private val hakemukset: List<JsonNode> by lazy {
        ClassPathResource("opintopolku-mocks/ataru/arvioijahakemukset.json").inputStream.use {
            defaultObjectMapper.readValue(it, Array<JsonNode>::class.java).toList()
        }
    }

    override fun haeHakemusavaimet(
        formKey: String,
        ehdot: List<OptionAnswer>,
        tilat: StatesAndFilters,
    ): Either<AtaruException, List<HakemusOtsake>> =
        hakemukset
            .mapNotNull { hakemus ->
                hakemus.path("hakemusOid").stringValueOpt().orElse(null)?.let {
                    HakemusOtsake(it, hakemus.path("state").stringValueOpt().orElse(null))
                }
            }.right()

    private val maksut: Map<String, List<AtaruLasku>> by lazy {
        ClassPathResource("opintopolku-mocks/ataru/maksut.json").inputStream.use {
            defaultObjectMapper
                .readValue(it, Array<JsonNode>::class.java)
                .groupBy({ lasku -> lasku.path("hakemusOid").stringValue() }) { lasku ->
                    defaultObjectMapper.treeToValue(lasku, AtaruLasku::class.java)
                }
        }
    }

    override fun haeMaksut(hakemusOid: String): Either<AtaruException, List<AtaruLasku>> =
        maksut[hakemusOid].orEmpty().right()

    override fun haeHakemukset(
        avaimet: List<String>,
    ): Either<AtaruException, List<Either<SiirtoJasennysvirhe, SiirtoHakemus>>> =
        hakemukset
            .filter { it.path("hakemusOid").stringValueOpt().orElse(null) in avaimet }
            .map(SiirtoHakemus::jasenna)
            .right()
}
