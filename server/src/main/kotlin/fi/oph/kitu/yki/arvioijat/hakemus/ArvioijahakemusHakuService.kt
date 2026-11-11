package fi.oph.kitu.yki.arvioijat.hakemus

import arrow.core.NonEmptyList
import arrow.core.getOrElse
import arrow.core.nonEmptyListOf
import fi.oph.kitu.ataru.AtaruClient
import fi.oph.kitu.ataru.SiirtoHakemus
import fi.oph.kitu.ataru.SiirtoJasennysvirhe
import fi.oph.kitu.auditlogs.AuditLogger
import fi.oph.kitu.config.ConditionalOnNonEmptyProperty
import fi.oph.kitu.oid.Oid
import fi.oph.kitu.oppijanumero.OppijanumeroException
import fi.oph.kitu.util.TimeService
import fi.oph.kitu.util.validation.Validation
import fi.oph.kitu.yki.arvioijat.YkiArvioijaError
import fi.oph.kitu.yki.arvioijat.YkiArvioijaKausiRepository
import fi.oph.kitu.yki.arvioijat.YkiArvioijaRepository
import fi.oph.kitu.yki.arvioijat.YkiArvioijaService
import io.opentelemetry.api.trace.Span
import io.opentelemetry.instrumentation.annotations.WithSpan
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.time.Duration
import java.time.OffsetDateTime
import java.time.ZoneOffset

data class Hakuyhteenveto(
    val tarkistettavia: Int = 0,
    val virheellisia: Int = 0,
    val odottaaYksilointia: Int = 0,
    val yritetaanUudelleen: Int = 0,
    val epaonnistui: Int = 0,
)

/**
 * Hakee atarun arvioijahakemukset virkailijan tarkistettaviksi (`yki_arvioijahakemus_tarkistettava`).
 * Haku ei koskaan luo arvioijaa: rekisteriin hakemus tulee vasta [ArvioijahakemusTarkistusService.vahvista]lla.
 * Hakemus on aina vain yhdessa tilassa: tarkistettavana, virheissa tai kirjanpidossa
 * ([ArvioijahakemuksenTila]). Haku kirjoittaa kirjanpitoon tilan ODOTTAA_YKSILOINTIA ja selvittaa
 * kesken jaaneet vahvistukset ([selvitaKeskenJaaneetVahvistukset]).
 */
@Service
@ConditionalOnNonEmptyProperty("kitu.ataru.service.url")
class ArvioijahakemusHakuService(
    private val ataru: AtaruClient,
    private val lomake: ArvioijahakemusLomake,
    private val repository: ArvioijahakemusRepository,
    private val tarkistettavaRepository: ArvioijahakemusTarkistettavaRepository,
    private val virheRepository: ArvioijahakemusVirheRepository,
    private val arvioijaService: YkiArvioijaService,
    private val arvioijaRepository: YkiArvioijaRepository,
    private val kausiRepository: YkiArvioijaKausiRepository,
    private val timeService: TimeService,
    private val auditLogger: AuditLogger,
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    @WithSpan("ataru.arvioijahakemus.haku")
    fun hae(): Hakuyhteenveto {
        val puutteet = lomake.puutteet()
        check(puutteet.isEmpty()) {
            "Arvioijahakemuslomakkeen asetukset (kitu.ataru.arvioijahakemus) ovat puutteelliset: ${puutteet.joinToString()}"
        }
        selvitaKeskenJaaneetVahvistukset()
        val avaimet =
            ataru
                .haeHakemusavaimet(
                    lomake.lomakkeenAvain,
                    ArvioijahakemusKriteeri.atarunSuodatus,
                    ArvioijahakemusKriteeri.atarunTilat,
                ).getOrElse { throw it }
                .map { it.key }
        virheRepository.poistaKirjatut()
        tarkistettavaRepository.poistaKirjatut()
        virheRepository.poistaVanhentuneet(avaimet)
        tarkistettavaRepository.poistaVanhentuneet(avaimet)
        repository.poistaVanhentuneetYksiloimattomat(avaimet)
        val kasitellyt = repository.kasitellyt(avaimet)
        val uudet = avaimet.filterNot(kasitellyt::contains).toSet()
        if (uudet.isEmpty()) return Hakuyhteenveto()

        val hetki = timeService.now().atOffset(ZoneOffset.UTC)
        val tulokset =
            ataru
                .haeHakemukset(uudet.toList())
                .getOrElse { throw it }
                .mapIndexed { indeksi, alkio ->
                    try {
                        alkio.fold(
                            ifLeft = { kirjaaJasennysvirhe(indeksi, it, uudet, hetki) },
                            ifRight = { kasittele(it, hetki) },
                        )
                    } catch (e: Exception) {
                        val hakemusOid = alkio.fold(ifLeft = { it.hakemusOid }, ifRight = { it.hakemusOid })
                        logger.error("Arvioijahakemuksen $hakemusOid käsittely epäonnistui", e)
                        Kasittelytulos.Epaonnistui
                    }
                }

        val tarkistettavat = tulokset.filterIsInstance<Kasittelytulos.Tarkistettava>().map { it.rivi }
        auditLogger.logAllInternalOnly("Yki arvioijahakemus haettu tarkistettavaksi", tarkistettavat) {
            arrayOf("arvioija.oid" to it.arvioijaOid, "hakemus.oid" to it.hakemusOid)
        }

        val yhteenveto =
            Hakuyhteenveto(
                tarkistettavia = tarkistettavat.size,
                virheellisia = tulokset.count { it == Kasittelytulos.Virhe },
                odottaaYksilointia = tulokset.count { it == Kasittelytulos.OdottaaYksilointia },
                yritetaanUudelleen = tulokset.count { it == Kasittelytulos.YritetaanUudelleen },
                epaonnistui = tulokset.count { it == Kasittelytulos.Epaonnistui },
            )
        Span.current().apply {
            setAttribute("arvioijahakemus.tarkistettavia", yhteenveto.tarkistettavia.toLong())
            setAttribute("arvioijahakemus.virhe", yhteenveto.virheellisia.toLong())
            setAttribute("arvioijahakemus.odottaaYksilointia", yhteenveto.odottaaYksilointia.toLong())
            setAttribute("arvioijahakemus.yritetaanUudelleen", yhteenveto.yritetaanUudelleen.toLong())
            setAttribute("arvioijahakemus.epaonnistui", yhteenveto.epaonnistui.toLong())
        }
        check(yhteenveto.epaonnistui == 0) {
            "${yhteenveto.epaonnistui} arvioijahakemuksen käsittely epäonnistui, ks. lokit"
        }
        return yhteenveto
    }

    /**
     * Kaatunut vahvistus jattaa KASITTELYSSA-varauksen voimaan. Kausi, joka alkaa varauksen
     * alkupaivana tai on luotu varauksen jalkeen, tarkoittaa etta arvioija ehti tallentua.
     */
    private fun selvitaKeskenJaaneetVahvistukset() {
        val nyt = timeService.now()
        var kirjattu = 0
        var vapautettu = 0
        val varattuEnnen = nyt.minus(KESKEN_JAANEEN_VAHVISTUKSEN_ODOTUS).atOffset(ZoneOffset.UTC)
        repository.haeKeskeytyneet(varattuEnnen).forEach { rivi ->
            val henkiloOid = rivi.henkiloOid?.let { Oid.parse(it).getOrNull() }
            val alkupaiva = rivi.kaudenAlkupaiva
            if (henkiloOid == null || alkupaiva == null) {
                logger.warn("Kesken jäänyttä arvioijahakemuksen ${rivi.hakemusOid} vahvistusta ei voi selvittää")
                return@forEach
            }
            val arvioija = arvioijaRepository.findByArvioijaOid(henkiloOid)
            val kausi =
                arvioija?.id?.toInt()?.let(kausiRepository::findKaudet)?.firstOrNull { kausi ->
                    kausi.alkupaiva == alkupaiva || kausi.luotu?.let { !it.isBefore(rivi.kasitelty) } == true
                }
            if (arvioija != null && kausi != null) {
                repository.merkitseKasitellyksi(
                    rivi.hakemusOid,
                    henkiloOid.toString(),
                    arvioija.id?.toInt(),
                    alkupaiva,
                    nyt.atOffset(ZoneOffset.UTC),
                    syy = "Kesken jäänyt vahvistus: kausi löytyi rekisteristä",
                )
                if (kausi.ashaNumero == null) {
                    logger.warn("Arvioijahakemuksen ${rivi.hakemusOid} kesken jääneeltä kaudelta puuttuu ASHA-numero")
                }
                kirjattu++
            } else {
                repository.poista(rivi.hakemusOid, ArvioijahakemuksenTila.KASITTELYSSA)
                vapautettu++
            }
            logger.info(
                "Arvioijahakemuksen ${rivi.hakemusOid} kesken jäänyt vahvistus selvitetty: " +
                    if (kausi != null) "kirjattu käsitellyksi" else "palautettu tarkistettavaksi",
            )
        }
        Span.current().apply {
            setAttribute("arvioijahakemus.keskenJaaneitaKirjattu", kirjattu.toLong())
            setAttribute("arvioijahakemus.keskenJaaneitaVapautettu", vapautettu.toLong())
        }
    }

    private fun kirjaaJasennysvirhe(
        indeksi: Int,
        virhe: SiirtoJasennysvirhe,
        uudet: Set<String>,
        hetki: OffsetDateTime,
    ): Kasittelytulos {
        val hakemusOid = virhe.hakemusOid?.takeIf { it in uudet }
        if (hakemusOid == null) {
            logger.error("Siirron alkiota $indeksi ei voitu jäsentää eikä liittää hakemukseen (polku: ${virhe.polku})")
            return Kasittelytulos.Epaonnistui
        }
        return kirjaaVirheet(
            hakemusOid,
            virhe.personOid,
            nonEmptyListOf(
                ArvioijahakemuksenVirhe(
                    ArvioijahakemuksenVirhetyyppi.VIRHEELLINEN_RAKENNE,
                    kentta = virhe.polku.ifEmpty { null },
                ),
            ),
            hetki,
        )
    }

    private fun kasittele(
        hakemus: SiirtoHakemus,
        hetki: OffsetDateTime,
    ): Kasittelytulos {
        val kartoitettu =
            lomake.kartoita(hakemus).getOrElse {
                return kirjaaVirheet(hakemus.hakemusOid, hakemus.personOid, it, hetki)
            }
        val esitaytto =
            arvioijaService.haeHenkilotiedot(hakemus.personOid).getOrElse {
                return henkilotietovirhe(hakemus, it, hetki)
            }
        val rivi =
            TarkistettavaHakemus(
                hakemusOid = hakemus.hakemusOid,
                henkiloOid = hakemus.personOid,
                arvioijaOid = esitaytto.arvioijaOid.toString(),
                sukunimi = esitaytto.sukunimi,
                etunimet = esitaytto.etunimet,
                hakemus = kartoitettu,
                ensimmainenHaku = hetki,
                viimeisinHaku = hetki,
            )
        virheRepository.poista(hakemus.hakemusOid)
        repository.poista(hakemus.hakemusOid, ArvioijahakemuksenTila.ODOTTAA_YKSILOINTIA)
        tarkistettavaRepository.tallenna(rivi, hetki)
        return Kasittelytulos.Tarkistettava(rivi)
    }

    private fun henkilotietovirhe(
        hakemus: SiirtoHakemus,
        virhe: YkiArvioijaError,
        hetki: OffsetDateTime,
    ): Kasittelytulos =
        when (virhe) {
            is YkiArvioijaError.Validointivirheet -> {
                kirjaaVirheet(hakemus.hakemusOid, hakemus.personOid, virhe.virheet.map(::validointivirhe), hetki)
            }

            is YkiArvioijaError.OppijaaEiYksiloity -> {
                tarkistettavaRepository.poista(hakemus.hakemusOid)
                virheRepository.poista(hakemus.hakemusOid)
                repository.odottaaYksilointia(
                    hakemus.hakemusOid,
                    hakemus.personOid,
                    "Henkilöä ei ole vielä yksilöity oppijanumerorekisterissä",
                    hetki,
                )
                Kasittelytulos.OdottaaYksilointia
            }

            is YkiArvioijaError.OppijanumeroaEiSaatu -> {
                if (virhe.syy is OppijanumeroException.OppijaNotFoundException) {
                    kirjaaVirheet(
                        hakemus.hakemusOid,
                        hakemus.personOid,
                        nonEmptyListOf(ArvioijahakemuksenVirhe(ArvioijahakemuksenVirhetyyppi.HENKILOA_EI_LOYDY)),
                        hetki,
                    )
                } else {
                    logger.warn(
                        "Arvioijahakemus ${hakemus.hakemusOid} yritetään uudelleen: " +
                            "Oppijanumerorekisterin kysely epäonnistui: ${virhe.syy.message}",
                    )
                    Kasittelytulos.YritetaanUudelleen
                }
            }

            YkiArvioijaError.ArvioijaaEiLoydy,
            YkiArvioijaError.KauttaEiLoydy,
            YkiArvioijaError.KausiEiOleAktiivinen,
            YkiArvioijaError.MuokattuSamanaikaisesti,
            YkiArvioijaError.ViimeistaKauttaEiVoiPoistaa,
            -> {
                error("Odottamaton virhe henkilötietojen haussa: $virhe")
            }
        }

    private fun validointivirhe(virhe: Validation.ValidationError) =
        ArvioijahakemuksenVirhe(
            ArvioijahakemuksenVirhetyyppi.VALIDOINTIVIRHE,
            kentta = virhe.path.joinToString(".").ifEmpty { null },
            viesti = virhe.message,
        )

    private fun kirjaaVirheet(
        hakemusOid: String,
        henkiloOid: String?,
        virheet: NonEmptyList<ArvioijahakemuksenVirhe>,
        hetki: OffsetDateTime,
    ): Kasittelytulos {
        tarkistettavaRepository.poista(hakemusOid)
        repository.poista(hakemusOid, ArvioijahakemuksenTila.ODOTTAA_YKSILOINTIA)
        virheRepository.tallenna(hakemusOid, henkiloOid, virheet, hetki)
        logger.warn(
            "Arvioijahakemuksen $hakemusOid käsittely epäonnistui: ${virheet.map {
                it.tyyppi
            }.distinct().joinToString()}",
        )
        return Kasittelytulos.Virhe
    }

    companion object {
        val KESKEN_JAANEEN_VAHVISTUKSEN_ODOTUS: Duration = Duration.ofMinutes(10)
    }

    private sealed interface Kasittelytulos {
        data class Tarkistettava(
            val rivi: TarkistettavaHakemus,
        ) : Kasittelytulos

        data object OdottaaYksilointia : Kasittelytulos

        data object Virhe : Kasittelytulos

        data object YritetaanUudelleen : Kasittelytulos

        data object Epaonnistui : Kasittelytulos
    }
}
