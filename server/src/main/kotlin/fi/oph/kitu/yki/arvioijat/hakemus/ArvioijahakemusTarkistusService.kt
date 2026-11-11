package fi.oph.kitu.yki.arvioijat.hakemus

import arrow.core.Either
import arrow.core.getOrElse
import arrow.core.left
import arrow.core.raise.either
import arrow.core.raise.ensure
import arrow.core.right
import arrow.core.toNonEmptyListOrNull
import fi.oph.kitu.auditlogs.AuditLogger
import fi.oph.kitu.i18n.UiText
import fi.oph.kitu.oid.Oid
import fi.oph.kitu.util.TimeService
import fi.oph.kitu.util.validation.Validation.ValidationError
import fi.oph.kitu.util.validation.validate
import fi.oph.kitu.yki.arvioijat.ArvioijaFormData
import fi.oph.kitu.yki.arvioijat.ArvioijanEsitaytto
import fi.oph.kitu.yki.arvioijat.TallennaArvioija
import fi.oph.kitu.yki.arvioijat.TallennaArvioijaValidation
import fi.oph.kitu.yki.arvioijat.YkiArvioijaEntity
import fi.oph.kitu.yki.arvioijat.YkiArvioijaError
import fi.oph.kitu.yki.arvioijat.YkiArvioijaKausiRepository
import fi.oph.kitu.yki.arvioijat.YkiArvioijaService
import io.opentelemetry.instrumentation.annotations.WithSpan
import org.springframework.stereotype.Service
import java.time.LocalDate
import java.time.ZoneOffset

sealed interface Tarkistusvirhe {
    data object EiTarkistettavana : Tarkistusvirhe

    data object JoKasittelyssa : Tarkistusvirhe

    data class Arvioija(
        val virhe: YkiArvioijaError,
    ) : Tarkistusvirhe
}

/**
 * Virkailijan tarkistus ja vahvistus. Hakemus varataan kirjanpitoon tilaan KASITTELYSSA ennen
 * [YkiArvioijaService.luoArvioija]a, joten samaa hakemusta ei voi vahvistaa kahdesti.
 */
@Service
class ArvioijahakemusTarkistusService(
    private val tarkistettavaRepository: ArvioijahakemusTarkistettavaRepository,
    private val repository: ArvioijahakemusRepository,
    private val arvioijaService: YkiArvioijaService,
    private val kausiRepository: YkiArvioijaKausiRepository,
    private val tallennaArvioijaValidation: TallennaArvioijaValidation,
    private val timeService: TimeService,
    private val auditLogger: AuditLogger,
) {
    @WithSpan
    fun lomake(hakemusOid: String): Either<Tarkistusvirhe, ArvioijaFormData> =
        either {
            val tarkistettava = hae(hakemusOid).bind()
            val esitaytto = esitaytto(tarkistettava).bind()
            auditLogger.logAllInternalOnly("Yki arvioijahakemus tarkistus viewed", listOf(tarkistettava)) {
                arrayOf("hakemus.oid" to it.hakemusOid, "henkilo.oid" to it.henkiloOid)
            }
            val hakemus = tarkistettava.hakemus
            val pohja = ArvioijaFormData.of(esitaytto)
            pohja.copy(
                sahkopostiosoite = hakemus.sahkopostiosoite ?: pohja.sahkopostiosoite,
                katuosoite = hakemus.katuosoite ?: pohja.katuosoite,
                postinumero = hakemus.postinumero ?: pohja.postinumero,
                postitoimipaikka = hakemus.postitoimipaikka ?: pohja.postitoimipaikka,
                kaudenAlkupaiva = ehdotettuAlkupaiva(esitaytto),
                ashaNumero = null,
                arviointioikeus = TarkistettavaHakemus.valinnat(hakemus.arviointioikeudet),
            )
        }

    @WithSpan
    fun vahvista(
        hakemusOid: String,
        form: ArvioijaFormData,
        tekija: Oid?,
    ): Either<Tarkistusvirhe, YkiArvioijaEntity> =
        either {
            val tarkistettava = hae(hakemusOid).bind()
            val esitaytto = esitaytto(tarkistettava).bind()
            val komento = komento(form, esitaytto).mapLeft(Tarkistusvirhe::Arvioija).bind()
            val henkiloOid = esitaytto.arvioijaOid.toString()

            ensure(repository.varaa(hakemusOid, henkiloOid, komento.kaudenAlkupaiva, nyt())) {
                Tarkistusvirhe.JoKasittelyssa
            }
            val arvioija =
                arvioijaService.luoArvioija(komento, tekija, form.muokattu).getOrElse { virhe ->
                    when (virhe) {
                        is YkiArvioijaError.Validointivirheet, YkiArvioijaError.MuokattuSamanaikaisesti -> {
                            repository.poista(hakemusOid, ArvioijahakemuksenTila.KASITTELYSSA)
                            raise(Tarkistusvirhe.Arvioija(virhe))
                        }

                        else -> {
                            error(
                                "Arvioijan tallennus palautti virheen $virhe, hakemus $hakemusOid jää tilaan KASITTELYSSA",
                            )
                        }
                    }
                }

            repository.merkitseKasitellyksi(
                hakemusOid,
                henkiloOid,
                arvioija.id?.toInt(),
                komento.kaudenAlkupaiva,
                nyt(),
            )
            tarkistettavaRepository.poista(hakemusOid)
            auditLogger.logAllInternalOnly("Yki arvioija tallennettu ataru-hakemuksesta", listOf(arvioija)) {
                arrayOf("arvioija.oid" to it.arvioijaOid.toString(), "hakemus.oid" to hakemusOid)
            }
            arvioija
        }

    /** Jatkokausi alkaa voimassa olevan tai tulevan kauden jalkeen, muuten kausi alkaa tanaan. */
    fun ehdotettuAlkupaiva(esitaytto: ArvioijanEsitaytto): LocalDate {
        val tanaan = timeService.today()
        val arvioijaId = esitaytto.olemassaolevaMerkinta?.id?.toInt() ?: return tanaan
        return kausiRepository
            .findKaudet(arvioijaId)
            .mapNotNull { it.paattymispaiva }
            .filterNot { it.isBefore(tanaan) }
            .maxOrNull()
            ?.plusDays(1)
            ?: tanaan
    }

    private fun hae(hakemusOid: String): Either<Tarkistusvirhe, TarkistettavaHakemus> =
        tarkistettavaRepository.hae(hakemusOid)?.right()
            ?: when (repository.hae(hakemusOid)?.tila) {
                ArvioijahakemuksenTila.KASITTELYSSA, ArvioijahakemuksenTila.KASITELTY -> Tarkistusvirhe.JoKasittelyssa
                ArvioijahakemuksenTila.ODOTTAA_YKSILOINTIA, null -> Tarkistusvirhe.EiTarkistettavana
            }.left()

    private fun esitaytto(tarkistettava: TarkistettavaHakemus): Either<Tarkistusvirhe, ArvioijanEsitaytto> =
        arvioijaService.haeHenkilotiedot(tarkistettava.henkiloOid).mapLeft(Tarkistusvirhe::Arvioija)

    /**
     * Lomakkeen piilokentta `arvioijaOid` ohitetaan: henkilo ratkaistaan hakemukselta. Vuoden raja
     * ohitetaan vain, kun alkupaiva jatkaa olemassa olevaa kautta, samalla ehdolla jonka
     * [TallennaArvioijaValidation] tarkistaa.
     */
    private fun komento(
        form: ArvioijaFormData,
        esitaytto: ArvioijanEsitaytto,
    ): Either<YkiArvioijaError, TallennaArvioija> {
        val alkupaiva = form.kaudenAlkupaiva
        val pakolliset =
            listOfNotNull(
                ValidationError(
                    listOf("kaudenAlkupaiva"),
                    UiText.Yki.Arvioijahakemus.Tarkistus.alkupaivaPakollinen
                        .toString(),
                ).takeIf { alkupaiva == null },
                ValidationError(
                    listOf("ashaNumero"),
                    UiText.Yki.Arvioijahakemus.Tarkistus.ashaPakollinen
                        .toString(),
                ).takeIf { form.ashaNumero.isNullOrBlank() },
            )
        if (alkupaiva == null) return YkiArvioijaError.Validointivirheet(pakolliset.toNonEmptyListOrNull()!!).left()

        val komento =
            form
                .toCommand(esitaytto.arvioijaOid, alkupaiva)
                .copy(automaattinenJatkokausi = jatkaaKautta(esitaytto, alkupaiva))
        val muut =
            if (pakolliset.isEmpty()) {
                emptyList()
            } else {
                tallennaArvioijaValidation
                    .validate(
                        komento,
                    ).leftOrNull()
                    .orEmpty()
            }
        return (pakolliset + muut).toNonEmptyListOrNull()?.let { YkiArvioijaError.Validointivirheet(it).left() }
            ?: komento.right()
    }

    private fun jatkaaKautta(
        esitaytto: ArvioijanEsitaytto,
        alkupaiva: LocalDate,
    ): Boolean {
        val arvioijaId = esitaytto.olemassaolevaMerkinta?.id?.toInt() ?: return false
        return kausiRepository.findKaudet(arvioijaId).any { it.paattymispaiva == alkupaiva.minusDays(1) }
    }

    private fun nyt() = timeService.now().atOffset(ZoneOffset.UTC)
}
