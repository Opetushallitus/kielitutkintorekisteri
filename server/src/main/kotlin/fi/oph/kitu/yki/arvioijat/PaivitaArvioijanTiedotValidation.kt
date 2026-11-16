package fi.oph.kitu.yki.arvioijat

import arrow.core.raise.accumulate
import fi.oph.kitu.oppijanumero.OppijanumeroValidation
import fi.oph.kitu.util.validation.Validation
import fi.oph.kitu.util.validation.ValidationRaise
import org.springframework.stereotype.Service

@Service
class PaivitaArvioijanTiedotValidation(
    val onr: OppijanumeroValidation,
) : Validation<PaivitaArvioijanTiedot> {
    override fun ValidationRaise.validateBeforeEnrichment(value: PaivitaArvioijanTiedot) {
        accumulate {
            accumulating { onr.validateOppijanumeroInOnr(value.arvioijaOid, listOf("arvioijaOid")).bind() }
            validatePakollisetYhteystiedot(
                value.sukunimi,
                value.etunimet,
                value.katuosoite,
                value.postinumero,
                value.postitoimipaikka,
            )
            accumulating { validatePostinumero(value.postinumero) }
            accumulating { validateSahkopostiosoite(value.sahkopostiosoite) }
            accumulating { validatePuhelinnumero(value.puhelinnumero) }
        }
    }
}
