package fi.oph.kitu.koodisto

import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Service

@Service
@Profile("test | e2e")
class MockKoodistoService : KoodistoService {
    override fun getKoodiviitteet(koodistoUri: String): List<KoodistopalveluKoodiviite>? =
        when (koodistoUri) {
            "maatjavaltiot1" -> {
                listOf(
                    KoodistopalveluKoodiviite(
                        koodiUri = "maatjavaltiot1",
                        koodiArvo = "FIN",
                        versio = 2,
                        metadata =
                            listOf(
                                KoodistopalveluKoodiviiteMetadata(
                                    nimi = "Suomi",
                                    kieli = KoodistopalveluLanguage.FI,
                                ),
                            ),
                    ),
                    KoodistopalveluKoodiviite(
                        koodiUri = "maatjavaltiot1",
                        koodiArvo = "EST",
                        versio = 2,
                        metadata =
                            listOf(
                                KoodistopalveluKoodiviiteMetadata(
                                    nimi = "Viro",
                                    kieli = KoodistopalveluLanguage.FI,
                                ),
                            ),
                    ),
                )
            }

            "kunta" -> {
                listOf(
                    KoodistopalveluKoodiviite(
                        koodiUri = "kunta",
                        koodiArvo = "091",
                        versio = 2,
                        metadata =
                            listOf(
                                KoodistopalveluKoodiviiteMetadata(
                                    nimi = "Helsinki",
                                    kieli = KoodistopalveluLanguage.FI,
                                ),
                            ),
                    ),
                )
            }

            else -> {
                null
            }
        }
}
