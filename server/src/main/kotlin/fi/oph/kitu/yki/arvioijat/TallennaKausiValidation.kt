package fi.oph.kitu.yki.arvioijat

import arrow.core.raise.Raise
import arrow.core.raise.accumulate
import arrow.core.raise.ensure
import fi.oph.kitu.util.TimeService
import fi.oph.kitu.util.validation.EnrichmentRaise
import fi.oph.kitu.util.validation.Validation
import fi.oph.kitu.util.validation.Validation.ValidationError
import fi.oph.kitu.util.validation.ValidationRaise
import org.springframework.stereotype.Service

@Service
class TallennaKausiValidation(
    private val kausiRepository: YkiArvioijaKausiRepository,
    private val timeService: TimeService,
) : Validation<TallennaKausi> {
    override fun ValidationRaise.validateBeforeEnrichment(value: TallennaKausi) {
        accumulate {
            accumulating { validateKaudenAlkupaiva(value.alkupaiva, timeService.today(), "alkupaiva") }
            validateArviointioikeudet(value.arviointioikeudet.map { it.kieli to it.tasot })
            accumulating { validateEiVanhentuneitaKielia(value) }
        }
    }

    /**
     * Passivoitu kausi on katkaistu tarkoituksella, joten alkupaivan korjaus ei saa laskea
     * paattymispaivaa uudelleen eika siten elvyttaa merkintaa.
     */
    override fun EnrichmentRaise.enrich(value: TallennaKausi): TallennaKausi {
        val nykyinen = value.kausiId?.let { kausiRepository.findKausi(it) }
        return value.copy(
            paattymispaiva =
                if (nykyinen?.passivoitu != null) {
                    nykyinen.paattymispaiva
                } else {
                    Arviointikausi.paattymispaiva(value.alkupaiva)
                },
        )
    }

    override fun ValidationRaise.validateAfterEnrichment(value: TallennaKausi) {
        accumulate {
            accumulating { validatePaivienJarjestys(value) }
            accumulating { validateEiPaallekkaisyytta(value) }
        }
    }

    /** Vanhentunutta tutkintokielta ei voi myontaa eika perua, joten sita ei oteta vastaan. */
    private fun Raise<ValidationError>.validateEiVanhentuneitaKielia(value: TallennaKausi) {
        val vanhentuneet = value.arviointioikeudet.filter { it.kieli.isLegacy() }
        ensure(vanhentuneet.isEmpty()) {
            ValidationError(
                listOf("arviointioikeus"),
                "Vanhentunutta tutkintokieltä ei voi lisätä arviointikaudelle",
            )
        }
    }

    /**
     * Katkaistun kauden alkupaivan muokkaus voi siirtaa sen paattymispaivan ohi: paattymispaiva
     * sailyy passivoituna, alkupaiva ei. Kannassa ei ole jarjestysehtoa, koska tuodussa datassa on
     * rivaeja jotka rikkoisivat sen.
     */
    private fun Raise<ValidationError>.validatePaivienJarjestys(value: TallennaKausi) {
        val paattymispaiva = value.paattymispaiva ?: return
        ensure(!paattymispaiva.isBefore(value.alkupaiva)) {
            ValidationError(
                listOf("alkupaiva"),
                "Kauden alkupäivä ei voi olla päättymispäivän jälkeen",
            )
        }
    }

    private fun Raise<ValidationError>.validateEiPaallekkaisyytta(value: TallennaKausi) {
        validateEiPaallekkaisiaKausia(
            kaudet = kausiRepository.findKaudet(value.arvioijaId),
            alkupaiva = value.alkupaiva,
            paattymispaiva = value.paattymispaiva,
            kielet = value.arviointioikeudet.map { it.kieli }.toSet(),
            ohitaKausiId = value.kausiId,
            kentta = "alkupaiva",
        )
    }
}
