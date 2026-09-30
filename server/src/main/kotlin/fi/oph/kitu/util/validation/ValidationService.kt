package fi.oph.kitu.util.validation

import arrow.core.NonEmptyList
import arrow.core.raise.either
import fi.oph.kitu.tiedontuontischema.Henkilosuoritus
import fi.oph.kitu.tiedontuontischema.HenkilosuoritusValidation
import fi.oph.kitu.tiedontuontischema.KielitutkinnonSuoritus
import fi.oph.kitu.tiedontuontischema.VktSuoritus
import fi.oph.kitu.tiedontuontischema.VktValidation
import fi.oph.kitu.tiedontuontischema.YkiSuoritus
import fi.oph.kitu.tiedontuontischema.YkiSuoritusValidation
import fi.oph.kitu.yki.arvioijat.PaivitaArvioijanTiedot
import fi.oph.kitu.yki.arvioijat.PaivitaArvioijanTiedotValidation
import fi.oph.kitu.yki.arvioijat.TallennaArvioija
import fi.oph.kitu.yki.arvioijat.TallennaArvioijaValidation
import fi.oph.kitu.yki.arvioijat.TallennaKausi
import fi.oph.kitu.yki.arvioijat.TallennaKausiValidation
import fi.oph.kitu.yki.arvioijat.YkiArvioija
import fi.oph.kitu.yki.arvioijat.YkiArvioijaValidation
import org.springframework.stereotype.Service

@Service
final class ValidationService(
    val commonValidation: HenkilosuoritusValidation,
    val vkt: VktValidation,
    val ykiSuoritus: YkiSuoritusValidation,
    val ykiArvioija: YkiArvioijaValidation,
    val tallennaArvioija: TallennaArvioijaValidation,
    val tallennaKausi: TallennaKausiValidation,
    val paivitaArvioijanTiedot: PaivitaArvioijanTiedotValidation,
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

    fun validateAndEnrich(arvioija: YkiArvioija): ValidationResult<YkiArvioija> =
        either { with(ykiArvioija) { validateAndEnrich(arvioija) } }

    fun validateAndEnrich(komento: TallennaArvioija): ValidationResult<TallennaArvioija> =
        either { with(tallennaArvioija) { validateAndEnrich(komento) } }

    fun validateAndEnrich(komento: TallennaKausi): ValidationResult<TallennaKausi> =
        either { with(tallennaKausi) { validateAndEnrich(komento) } }

    fun validateAndEnrich(komento: PaivitaArvioijanTiedot): ValidationResult<PaivitaArvioijanTiedot> =
        either { with(paivitaArvioijanTiedot) { validateAndEnrich(komento) } }
}

/**
 * Heittaa ValidationExceptionin, jonka GlobalControllerExceptionHandler kaantaa 400:ksi.
 * Nimi erottaa taman util.result.getOrThrow'sta, joka heittaa Leftin sellaisenaan tai
 * IllegalStateExceptionin eli paatyy 500:aan — vaara import vaihtaisi statuskoodin hiljaa.
 */
fun <T> ValidationResult<T>.orThrowBadRequest(): T =
    fold(
        ifLeft = { errors: NonEmptyList<Validation.ValidationError> ->
            throw Validation.ValidationException(errors)
        },
        ifRight = { it },
    )
