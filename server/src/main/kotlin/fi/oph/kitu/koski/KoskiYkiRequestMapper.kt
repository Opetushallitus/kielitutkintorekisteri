package fi.oph.kitu.koski

import arrow.core.Either
import arrow.core.raise.either
import arrow.core.raise.ensure
import arrow.core.raise.ensureNotNull
import fi.oph.kitu.koodisto.Koodisto
import fi.oph.kitu.koodisto.Koodisto.YkiArvosana
import fi.oph.kitu.koski.KoskiRequest.Henkilo
import fi.oph.kitu.koski.KoskiRequest.Opiskeluoikeus
import fi.oph.kitu.koski.KoskiRequest.Opiskeluoikeus.KielitutkintoSuoritus
import fi.oph.kitu.koski.KoskiRequest.Opiskeluoikeus.KielitutkintoSuoritus.KoulutusModuuli
import fi.oph.kitu.koski.KoskiRequest.Opiskeluoikeus.KielitutkintoSuoritus.Organisaatio
import fi.oph.kitu.koski.KoskiRequest.Opiskeluoikeus.LahdeJarjestelmanId
import fi.oph.kitu.koski.KoskiRequest.Opiskeluoikeus.Tila
import fi.oph.kitu.koski.KoskiRequest.Opiskeluoikeus.Tila.OpiskeluoikeusJakso
import fi.oph.kitu.oid.Oid
import fi.oph.kitu.util.result.getOrThrow
import fi.oph.kitu.yki.Tutkintotaso
import fi.oph.kitu.yki.suoritukset.YkiSuoritusEntity
import io.opentelemetry.instrumentation.annotations.WithSpan
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service
import java.time.LocalDate

@Service
class KoskiYkiRequestMapper {
    @Value($$"${kitu.oids.yleisetkielitutkinnot}")
    lateinit var ykiOrganisaatioOid: String

    private val keskeytetty = YkiArvosana.Keskeytetty.koodiarvo.toInt()
    private val vilppi = YkiArvosana.Vilppi.koodiarvo.toInt()
    private val eiSuoritusta = YkiArvosana.EiSuoritusta.koodiarvo.toInt()
    private val eiOppijanumeroa = "Suorituksella ei ole oppijanumeroa"

    @WithSpan
    fun ykiSuoritusToKoskiRequest(ykiSuoritus: YkiSuoritusEntity): Either<KoskiYkiMappingError, KoskiRequest> =
        either {
            val estonSyyt = koskiSiirronEstonSyyt(ykiSuoritus)
            ensure(estonSyyt.isEmpty()) { KoskiYkiMappingError.EstoSyyt(estonSyyt) }
            val oppijanumero =
                ensureNotNull(ykiSuoritus.suorittajanOID) {
                    KoskiYkiMappingError.EstoSyyt(listOf(eiOppijanumeroa))
                }

            val osasuoritukset = convertYkiSuoritusToKoskiOsasuoritukset(ykiSuoritus).bind()
            ensure(osasuoritukset.isNotEmpty()) {
                KoskiYkiMappingError.EstoSyyt(listOf("Suorituksella ei ole siirrettäviä osakokeita"))
            }
            val yleisarvosana =
                ykiSuoritus.yleisarvosana?.let {
                    YkiArvosana
                        .of(it, ykiSuoritus.tutkintotaso)
                        .mapLeft { e -> KoskiYkiMappingError.InvalidArvosana(e) }
                        .bind()
                        .toKoski()
                }
            val tutkintotasoKoodi =
                Koodisto.YkiTutkintotaso
                    .fromName(ykiSuoritus.tutkintotaso.name)
                    .mapLeft { e -> KoskiYkiMappingError.InvalidKoodistoValue(e) }
                    .bind()
            val tutkintokieliKoodi =
                Koodisto.Tutkintokieli
                    .fromName(ykiSuoritus.tutkintokieli.name)
                    .mapLeft { e -> KoskiYkiMappingError.InvalidKoodistoValue(e) }
                    .bind()

            KoskiRequest(
                henkilö = Henkilo(oid = oppijanumero),
                opiskeluoikeudet =
                    listOf(
                        Opiskeluoikeus(
                            oid = ykiSuoritus.koskiOpiskeluoikeus,
                            lähdejärjestelmänId =
                                LahdeJarjestelmanId(
                                    id = ykiSuoritus.lahdejarjestelmanTunnus,
                                ),
                            tila =
                                Tila(
                                    opiskeluoikeusjaksot =
                                        listOfNotNull(
                                            OpiskeluoikeusJakso(
                                                alku = ykiSuoritus.tutkintopaiva,
                                                tila = Koodisto.OpiskeluoikeudenTila.Lasna,
                                            ),
                                            ykiSuoritus.arviointipaiva?.let {
                                                OpiskeluoikeusJakso(
                                                    alku = ykiSuoritus.arviointipaiva,
                                                    tila = Koodisto.OpiskeluoikeudenTila.Paattynyt,
                                                )
                                            },
                                        ),
                                ),
                            suoritukset =
                                listOf(
                                    KielitutkintoSuoritus(
                                        tyyppi = Koodisto.SuorituksenTyyppi.YleinenKielitutkinto,
                                        koulutusmoduuli =
                                            KoulutusModuuli(
                                                tunniste = tutkintotasoKoodi.toKoski(),
                                                kieli = tutkintokieliKoodi,
                                            ),
                                        toimipiste = Organisaatio(oid = Oid.parse(ykiOrganisaatioOid).getOrThrow()),
                                        järjestäjä = Organisaatio(oid = ykiSuoritus.jarjestajanTunnusOid),
                                        vahvistus =
                                            ykiSuoritus.arviointipaiva?.let {
                                                KielitutkintoSuoritus.VahvistusImpl(
                                                    päivä = ykiSuoritus.arviointipaiva,
                                                    myöntäjäOrganisaatio =
                                                        Organisaatio(ykiSuoritus.jarjestajanTunnusOid),
                                                )
                                            },
                                        osasuoritukset = osasuoritukset,
                                        yleisarvosana = yleisarvosana,
                                    ),
                                ),
                        ),
                    ),
            )
        }

    private fun convertYkiSuoritusToKoskiOsasuoritukset(
        suoritusEntity: YkiSuoritusEntity,
    ): Either<KoskiYkiMappingError, List<Osasuoritus>> =
        either {
            mapOf(
                Koodisto.YkiSuorituksenOsa.TekstinYmmartaminen to suoritusEntity.tekstinYmmartaminen,
                Koodisto.YkiSuorituksenOsa.Kirjoittaminen to suoritusEntity.kirjoittaminen,
                Koodisto.YkiSuorituksenOsa.PuheenYmmartaminen to suoritusEntity.puheenYmmartaminen,
                Koodisto.YkiSuorituksenOsa.Puhuminen to suoritusEntity.puhuminen,
                Koodisto.YkiSuorituksenOsa.RakenteetJaSanasto to suoritusEntity.rakenteetJaSanasto,
            ).mapNotNull { (suorituksenNimi, arvosana) ->
                arvosana
                    ?.takeUnless { it == keskeytetty || it == eiSuoritusta }
                    ?.let {
                        suoritusEntity.arviointipaiva?.let {
                            yleisenKielitutkinnonOsa(
                                suorituksenNimi,
                                arvosana,
                                suoritusEntity.tutkintotaso,
                                suoritusEntity.arviointipaiva,
                            ).bind()
                        }
                    }
            }
        }

    private fun koskiSiirronEstonSyyt(suoritusEntity: YkiSuoritusEntity): List<String> =
        listOfNotNull(
            if (suoritusEntity.suorittajanOID == null) {
                eiOppijanumeroa
            } else {
                null
            },
            if (!suoritusEntity.arviointitila.arvioitu()) {
                "Suoritus ei ole arvioitu"
            } else {
                null
            },
            if (
                listOf(
                    suoritusEntity.tekstinYmmartaminen,
                    suoritusEntity.kirjoittaminen,
                    suoritusEntity.puheenYmmartaminen,
                    suoritusEntity.puhuminen,
                    suoritusEntity.rakenteetJaSanasto,
                ).any { it == vilppi }
            ) {
                "Suoritus sisältää arvosanan vilppi"
            } else {
                null
            },
        )

    private fun yleisenKielitutkinnonOsa(
        suorituksenNimi: Koodisto.YkiSuorituksenOsa,
        arvosana: Int,
        tutkintotaso: Tutkintotaso,
        arviointipaiva: LocalDate,
    ): Either<KoskiYkiMappingError, YkiOsasuoritus> =
        YkiArvosana
            .of(arvosana, tutkintotaso)
            .mapLeft { e -> KoskiYkiMappingError.InvalidArvosana(e) }
            .map { arvosanaKoodi ->
                YkiOsasuoritus(
                    koulutusmoduuli =
                        OsasuorituksenKoulutusmoduuli(
                            tunniste = suorituksenNimi.toKoski(),
                        ),
                    arviointi =
                        listOf(
                            Arvosana(
                                arvosana = arvosanaKoodi.toKoski(),
                                päivä = arviointipaiva,
                            ),
                        ),
                    alkamispäivä = null,
                )
            }
}
