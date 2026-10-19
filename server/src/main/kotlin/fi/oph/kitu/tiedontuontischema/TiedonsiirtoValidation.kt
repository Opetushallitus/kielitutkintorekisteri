package fi.oph.kitu.tiedontuontischema

import arrow.core.raise.either
import fi.oph.kitu.util.validation.ValidationResult
import org.springframework.stereotype.Service

@Service
class TiedonsiirtoValidation(
    private val commonValidation: HenkilosuoritusValidation,
    private val vkt: VktValidation,
    private val ykiSuoritus: YkiSuoritusValidation,
) {
    fun <T : KielitutkinnonSuoritus> validateAndEnrich(hs: Henkilosuoritus<T>): ValidationResult<Henkilosuoritus<T>> =
        either {
            val common = with(commonValidation) { validateAndEnrich(hs) }
            val enriched =
                when (val s = common.suoritus) {
                    is VktSuoritus -> {
                        with(vkt) {
                            validateAndEnrich(Henkilosuoritus(common.henkilo, s, common.lisatty))
                        }
                    }

                    is YkiSuoritus -> {
                        with(ykiSuoritus) {
                            validateAndEnrich(Henkilosuoritus(common.henkilo, s, common.lisatty))
                        }
                    }
                }
            @Suppress("UNCHECKED_CAST")
            enriched as Henkilosuoritus<T>
        }
}
