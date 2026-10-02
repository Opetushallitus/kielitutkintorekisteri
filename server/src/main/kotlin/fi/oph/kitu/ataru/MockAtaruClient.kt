package fi.oph.kitu.ataru

import arrow.core.Either
import arrow.core.right
import fi.oph.kitu.util.defaultObjectMapper
import org.springframework.context.annotation.Profile
import org.springframework.core.io.ClassPathResource
import org.springframework.stereotype.Component

@Component
@Profile("e2e | local-opintopolku")
class MockAtaruClient : AtaruClient {
    private val hakemukset: List<SiirtoHakemus> by lazy {
        ClassPathResource("opintopolku-mocks/ataru/arvioijahakemukset.json").inputStream.use {
            defaultObjectMapper.readValue(it, Array<SiirtoHakemus>::class.java).toList()
        }
    }

    override fun haeHakemusavaimet(
        formKey: String,
        ehdot: List<OptionAnswer>,
    ): Either<AtaruException, List<HakemusOtsake>> = hakemukset.map { HakemusOtsake(it.hakemusOid, it.state) }.right()

    override fun haeHakemukset(avaimet: List<String>): Either<AtaruException, List<SiirtoHakemus>> =
        hakemukset.filter { it.hakemusOid in avaimet }.right()
}
