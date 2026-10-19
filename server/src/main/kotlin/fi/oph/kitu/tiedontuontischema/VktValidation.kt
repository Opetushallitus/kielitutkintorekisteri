package fi.oph.kitu.tiedontuontischema

import arrow.core.raise.accumulate
import arrow.core.raise.ensure
import arrow.core.raise.ensureNotNull
import fi.oph.kitu.koodisto.Koodisto
import fi.oph.kitu.oid.Oid
import fi.oph.kitu.util.validation.EnrichmentRaise
import fi.oph.kitu.util.validation.Validation
import fi.oph.kitu.util.validation.ValidationRaise
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service

@Service
class VktValidation : Validation<VktHenkilosuoritus> {
    @Value($$"${kitu.oids.palvelukayttaja}")
    lateinit var palvelukayttajaOid: String

    override fun EnrichmentRaise.enrich(value: VktHenkilosuoritus): VktHenkilosuoritus =
        if (value.suoritus.taitotaso == Koodisto.VktTaitotaso.Erinomainen) {
            value.modifySuoritus {
                it.copy(
                    suorituspaikkakunta =
                        it.suorituspaikkakunta ?: "091",
                    suorituksenVastaanottaja =
                        it.suorituksenVastaanottaja ?: Oid.parse(palvelukayttajaOid).getOrNull(),
                )
            }
        } else {
            value
        }

    override fun ValidationRaise.validateAfterEnrichment(value: VktHenkilosuoritus) {
        accumulate {
            accumulating {
                ensureNotNull(value.suoritus.suorituspaikkakunta) {
                    Validation.ValidationError(listOf("suoritus", "suorituspaikkakunta"), "Suorituspaikkakunta puuttuu")
                }
            }
            accumulating {
                ensureNotNull(value.suoritus.suorituksenVastaanottaja) {
                    Validation.ValidationError(
                        listOf("suoritus", "suorituksenVastaanottaja"),
                        "Suorituksen vastaanottaja puuttuu",
                    )
                }
            }
            accumulating {
                ensure(
                    value.suoritus.taitotaso != Koodisto.VktTaitotaso.HyväJaTyydyttävä ||
                        value.suoritus.osat.all { it.arviointi != null },
                ) {
                    Validation.ValidationError(
                        listOf("suoritus", "osakokeet", "arviointi"),
                        "Suorituksella on arvioimattomia osakokeita",
                    )
                }
            }
        }
    }
}
