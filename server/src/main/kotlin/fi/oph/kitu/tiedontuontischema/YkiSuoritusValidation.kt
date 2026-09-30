package fi.oph.kitu.tiedontuontischema

import arrow.core.raise.Raise
import arrow.core.raise.RaiseAccumulate
import arrow.core.raise.accumulate
import arrow.core.raise.ensure
import fi.oph.kitu.i18n.finnishDate
import fi.oph.kitu.koodisto.Koodisto
import fi.oph.kitu.organisaatiot.OrganisaatioService
import fi.oph.kitu.util.validation.EnrichmentRaise
import fi.oph.kitu.util.validation.Validation
import fi.oph.kitu.util.validation.ValidationRaise
import fi.oph.kitu.yki.Arviointitila
import fi.oph.kitu.yki.Tutkintokieli
import fi.oph.kitu.yki.laskeArviointitila
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service
import java.time.LocalDate

@Service
class YkiSuoritusValidation(
    val organisaatiot: OrganisaatioService,
    @param:Value($$"${kitu.validaatiot.yki.hetunSiirronRajapaiva}")
    val hetunSiirronRajapaiva: LocalDate,
    @param:Value($$"${kitu.validaatiot.yki.todistuskielenSiirronRajapaiva}")
    val todistuskielenSiirronRajapaiva: LocalDate,
) : Validation<YkiHenkilosuoritus> {
    @Value($$"${kitu.yki.convertLegacyArviointitila.enabled}")
    val convertLegacyArviointitila: Boolean = false

    override fun ValidationRaise.validateBeforeEnrichment(value: YkiHenkilosuoritus) {
        accumulate {
            accumulating { validateHetu(value) }
            accumulating { validateArvointitila(value) }
            accumulating { validateTarkistusarviointi(value) }
            accumulating { validateKielikoodi(value) }
            accumulating { validateTodistuskieli(value) }
        }
    }

    override fun EnrichmentRaise.enrich(value: YkiHenkilosuoritus): YkiHenkilosuoritus =
        if (convertLegacyArviointitila) {
            val arvosanat = value.suoritus.osat.map { it.arvosana }
            value.copy(
                suoritus =
                    value.suoritus.copy(
                        arviointitila =
                            laskeArviointitila(
                                nykyinen = value.suoritus.arviointitila,
                                osakoeCount = arvosanat.size,
                                arvosanaPuuttuu = arvosanat.count { it == null },
                                oikeitaArvosanoja = arvosanat.count { it != null && it < 9 },
                                onTarkistusarviointi = value.suoritus.tarkistusarviointi != null,
                                tarkistuksenKasittelypaiva = value.suoritus.tarkistusarviointi?.kasittelypaiva,
                            ),
                    ),
            )
        } else {
            value
        }

    private fun Raise<Validation.ValidationError>.validateArviointitilaVastaaArvosanoja(s: YkiHenkilosuoritus) {
        val arvosanat = s.suoritus.osat.map { it.arvosana }
        val tarkistusarviointi = s.suoritus.tarkistusarviointi
        val yksikaanArvosanaEiAnnettu = arvosanat.all { it == null }
        val jokinArvosanaPuuttuu = arvosanat.any { it == null }
        val kaikkiArvosanatEiSuoritettuja = arvosanat.all { it != null && it >= 9 }
        val jokinOikeaArvosana = arvosanat.any { it != null && it < 9 }

        fun virhe(viesti: String) = Validation.ValidationError(listOf("suoritus", "arviointitila"), viesti)

        when (s.suoritus.arviointitila) {
            Arviointitila.ILMOITTAUTUNUT,
            Arviointitila.PERUTTU,
            -> {
                ensure(yksikaanArvosanaEiAnnettu) {
                    virhe(
                        "Arviointitila '${s.suoritus.arviointitila}' edellyttää, " +
                            "ettei millään osakokeella ole arvosanaa",
                    )
                }
            }

            Arviointitila.ARVIOITAVA -> {
                ensure(jokinArvosanaPuuttuu) {
                    virhe(
                        "Arviointitila 'ARVIOITAVA' edellyttää, " +
                            "että vähintään yhdeltä osakokeelta puuttuu arvosana",
                    )
                }
            }

            Arviointitila.EI_SUORITUSTA -> {
                ensure(kaikkiArvosanatEiSuoritettuja) {
                    virhe(
                        "Arviointitila 'EI_SUORITUSTA' edellyttää, " +
                            "että kaikilla osakokeilla on arvosana eikä yksikään ole oikea arvosana",
                    )
                }
            }

            Arviointitila.ARVIOITU -> {
                ensure(jokinOikeaArvosana) {
                    virhe(
                        "Arviointitila 'ARVIOITU' edellyttää, " +
                            "että vähintään yhdellä osakokeella on oikea arvosana",
                    )
                }
            }

            Arviointitila.TARKISTUSARVIOITAVA -> {
                ensure(tarkistusarviointi != null && tarkistusarviointi.kasittelypaiva == null) {
                    virhe(
                        "Arviointitila 'TARKISTUSARVIOITAVA' edellyttää tarkistusarviointia, " +
                            "jolla ei ole käsittelypäivää",
                    )
                }
            }

            Arviointitila.TARKISTUSARVIOITU -> {
                ensure(tarkistusarviointi != null && tarkistusarviointi.kasittelypaiva != null) {
                    virhe(
                        "Arviointitila 'TARKISTUSARVIOITU' edellyttää tarkistusarviointia, " +
                            "jolla on käsittelypäivä",
                    )
                }
            }

            Arviointitila.TARKISTUSARVIOINTI_HYVAKSYTTY -> {
                raise(virhe("Arviointitilaa '${s.suoritus.arviointitila}' ei voi tuoda"))
            }
        }
    }

    private fun Raise<Validation.ValidationError>.validateTarkistusarviointiSallittu(s: YkiHenkilosuoritus) {
        val onTarkistusarviointi = s.suoritus.tarkistusarviointi != null
        when (s.suoritus.arviointitila) {
            Arviointitila.ILMOITTAUTUNUT,
            Arviointitila.PERUTTU,
            Arviointitila.EI_SUORITUSTA,
            Arviointitila.ARVIOITAVA,
            Arviointitila.ARVIOITU,
            -> {
                ensure(!onTarkistusarviointi) {
                    Validation.ValidationError(
                        listOf("suoritus", "tarkistusarviointi"),
                        "Arviointitila '${s.suoritus.arviointitila}' ei salli tarkistusarviointia",
                    )
                }
            }

            Arviointitila.TARKISTUSARVIOITAVA,
            Arviointitila.TARKISTUSARVIOITU,
            Arviointitila.TARKISTUSARVIOINTI_HYVAKSYTTY,
            -> {}
        }
    }

    override fun ValidationRaise.validateAfterEnrichment(value: YkiHenkilosuoritus) {
        accumulate {
            accumulating { validateOsakokeitaOnAtLeastOne(value) }
            accumulating { validateArvosanat(value) }
            accumulating { validateArviointitilaVastaaArvosanoja(value) }
            accumulating { validateTarkistusarviointiSallittu(value) }
        }
    }

    private fun Raise<Validation.ValidationError>.validateTodistuskieli(s: YkiHenkilosuoritus) {
        ensure(s.suoritus.tutkintopaiva.isBefore(todistuskielenSiirronRajapaiva) || s.suoritus.todistuskieli != null) {
            Validation.ValidationError(
                listOf("suoritus", "todistuskieli"),
                "Todistuskieli on pakollinen ${todistuskielenSiirronRajapaiva.finnishDate()} alkaen",
            )
        }
    }

    private fun Raise<Validation.ValidationError>.validateHetu(s: YkiHenkilosuoritus) {
        ensure(s.suoritus.tutkintopaiva.isBefore(hetunSiirronRajapaiva) || s.henkilo.hetu == null) {
            Validation.ValidationError(
                listOf("henkilo", "hetu"),
                "Henkilötunnusta ei voi siirtää suoritukselle, jonka tutkintopäivä on " +
                    "${hetunSiirronRajapaiva.finnishDate()} tai myöhemmin",
            )
        }
    }

    private fun RaiseAccumulate<Validation.ValidationError>.validateArvointitila(s: YkiHenkilosuoritus) {
        if (s.suoritus.arviointitila.arvioitu()) {
            accumulating {
                ensure(s.suoritus.arviointipaiva != null) {
                    Validation.ValidationError(
                        listOf("suoritus", "arviointipaiva"),
                        "Arviointitilan '${s.suoritus.arviointitila}' mukaan suoritus on arvioitu, " +
                            "mutta arviointipäivä puuttuu",
                    )
                }
            }
            s.suoritus.osat.forEachIndexed { i, osakoe ->
                accumulating {
                    ensure(osakoe.arvosana != null) {
                        Validation.ValidationError(
                            listOf("suoritus", "osat", i.toString(), "arvosana"),
                            "Arviointitilan '${s.suoritus.arviointitila}' mukaan suoritus on arvioitu, " +
                                "mutta arviointi puuttuu osakokeelta '${osakoe.tyyppi.name}'",
                        )
                    }
                }
            }
        } else {
            ensure(s.suoritus.arviointipaiva == null) {
                Validation.ValidationError(
                    listOf("suoritus", "arviointipaiva"),
                    "Arviointitilan '${s.suoritus.arviointitila}' mukaan suoritusta ei ole vielä arvioitu, " +
                        "mutta arviointipäivä on määritelty",
                )
            }
        }
    }

    private fun RaiseAccumulate<Validation.ValidationError>.validateTarkistusarviointi(s: YkiHenkilosuoritus) {
        accumulating {
            val tarkastettavatOsakokeet =
                s.suoritus.tarkistusarviointi
                    ?.tarkistusarvioidutOsakokeet
                    .orEmpty()
                    .toSet()
            val muuttuneetOsakokeet =
                s.suoritus.tarkistusarviointi
                    ?.arvosanaMuuttui
                    .orEmpty()
                    .toSet()
            ensure(muuttuneetOsakokeet.minus(tarkastettavatOsakokeet).isEmpty()) {
                Validation.ValidationError(
                    listOf("suoritus", "tarkistusarviointi", "arvosanaMuuttui"),
                    "Muuttuneet arvosanat sisälsivät osakokeita, jotka eivät olleet osa tarkistettavia osakokeita",
                )
            }
        }
        accumulating {
            ensure(
                (s.suoritus.tarkistusarviointi?.saapumispaiva ?: LocalDate.MIN) <=
                    (s.suoritus.tarkistusarviointi?.kasittelypaiva ?: LocalDate.MAX),
            ) {
                Validation.ValidationError(
                    listOf("suoritus", "tarkistusarviointi", "kasittelypaiva"),
                    "Käsittelypäivä on ennen saapumispäivää",
                )
            }
        }
    }

    private fun Raise<Validation.ValidationError>.validateKielikoodi(s: YkiHenkilosuoritus) {
        ensure(!s.suoritus.kieli.isLegacy() || s.suoritus.tutkintopaiva.isBefore(LocalDate.of(2017, 1, 1))) {
            Validation.ValidationError(
                listOf("suoritus", "kieli"),
                "Käytöstä poistuneita kielikoodeja (${Tutkintokieli.legacyEntries.joinToString(
                    ", ",
                )}) ei voi käyttää",
            )
        }
    }

    private fun Raise<Validation.ValidationError>.validateOsakokeitaOnAtLeastOne(s: YkiHenkilosuoritus) {
        ensure(s.suoritus.osat.isNotEmpty()) {
            Validation.ValidationError(
                listOf("suoritus", "osat"),
                "Suorituksella täytyy olla vähintään yksi osakoe, johon on ilmottauduttu",
            )
        }
    }

    private fun Raise<Validation.ValidationError>.validateArvosanat(s: YkiHenkilosuoritus) {
        val tutkintotaso = s.suoritus.tutkintotaso
        val validArvosanat = Koodisto.YkiArvosana.validIntegersFor(tutkintotaso)
        val invalidArvosanat = s.suoritus.osat.mapNotNull { if (it.arvosana in validArvosanat) null else it.arvosana }
        ensure(invalidArvosanat.isEmpty()) {
            Validation.ValidationError(
                listOf("suoritus", "osat", "arvosana"),
                "Suoritus sisältää tutkintotasolle $tutkintotaso virheellisiä arvosanoja: " +
                    invalidArvosanat.joinToString(", "),
            )
        }
    }
}
