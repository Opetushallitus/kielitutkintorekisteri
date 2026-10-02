package fi.oph.kitu.yki.arvioijat

import arrow.core.nonEmptyListOf
import arrow.core.raise.accumulate
import arrow.core.raise.ensure
import fi.oph.kitu.oppijanumero.OppijanumeroValidation
import fi.oph.kitu.util.TimeService
import fi.oph.kitu.util.validation.Validation
import fi.oph.kitu.util.validation.Validation.ValidationError
import fi.oph.kitu.util.validation.ValidationRaise
import org.springframework.stereotype.Service

@Service
class TallennaArvioijaValidation(
    val onr: OppijanumeroValidation,
    val timeService: TimeService,
    val arvioijaRepository: YkiArvioijaRepository,
    val kausiRepository: YkiArvioijaKausiRepository,
) : Validation<TallennaArvioija> {
    override fun ValidationRaise.validateBeforeEnrichment(value: TallennaArvioija) {
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
            if (!value.automaattinenJatkokausi) {
                accumulating { validateKaudenAlkupaiva(value.kaudenAlkupaiva, timeService.today(), "kaudenAlkupaiva") }
            }
            validateArviointioikeudet(value.arviointioikeudet.map { it.kieli to it.tasot })
        }
    }

    /** Lisayslomake toimii myos jatkokauden kirjaamisena, joten paallekkaisyys on estettava tassakin. */
    override fun ValidationRaise.validateAfterEnrichment(value: TallennaArvioija) {
        val arvioijaId = arvioijaRepository.findByArvioijaOid(value.arvioijaOid)?.id?.toInt()
        if (arvioijaId == null) {
            ensure(!value.automaattinenJatkokausi) { nonEmptyListOf(jatkokausiIlmanEdellista()) }
            return
        }
        val kaudet = kausiRepository.findKaudet(arvioijaId)

        accumulate {
            if (value.automaattinenJatkokausi) {
                accumulating {
                    ensure(kaudet.any { it.paattymispaiva == value.kaudenAlkupaiva.minusDays(1) }) {
                        jatkokausiIlmanEdellista()
                    }
                }
            }
            accumulating {
                validateEiPaallekkaisiaKausia(
                    kaudet = kaudet,
                    alkupaiva = value.kaudenAlkupaiva,
                    paattymispaiva = value.kaudenPaattymispaiva,
                    kentta = "kaudenAlkupaiva",
                )
            }
        }
    }
}

private fun jatkokausiIlmanEdellista() =
    ValidationError(
        listOf("kaudenAlkupaiva"),
        "Jatkokaudelle ei löydy edellistä kautta, joka päättyy alkupäivää edeltävänä päivänä",
    )
