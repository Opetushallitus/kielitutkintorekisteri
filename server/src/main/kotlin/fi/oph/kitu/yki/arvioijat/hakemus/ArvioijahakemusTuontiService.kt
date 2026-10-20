package fi.oph.kitu.yki.arvioijat.hakemus

import arrow.core.Either
import arrow.core.NonEmptyList
import arrow.core.getOrElse
import arrow.core.nonEmptyListOf
import arrow.core.raise.either
import arrow.core.raise.ensure
import fi.oph.kitu.ataru.AtaruClient
import fi.oph.kitu.ataru.SiirtoHakemus
import fi.oph.kitu.ataru.SiirtoJasennysvirhe
import fi.oph.kitu.auditlogs.AuditLogger
import fi.oph.kitu.config.ConditionalOnNonEmptyProperty
import fi.oph.kitu.oppijanumero.OppijanumeroException
import fi.oph.kitu.util.TimeService
import fi.oph.kitu.util.validation.Validation
import fi.oph.kitu.yki.arvioijat.ArvioijanEsitaytto
import fi.oph.kitu.yki.arvioijat.TallennaArvioija
import fi.oph.kitu.yki.arvioijat.YkiArvioijaError
import fi.oph.kitu.yki.arvioijat.YkiArvioijaKausiRepository
import fi.oph.kitu.yki.arvioijat.YkiArvioijaService
import io.opentelemetry.api.trace.Span
import io.opentelemetry.instrumentation.annotations.WithSpan
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.time.LocalDate
import java.time.ZoneOffset

data class Tuontiyhteenveto(
    val kasitelty: Int = 0,
    val virhe: Int = 0,
    val odottaaYksilointia: Int = 0,
    val yritetaanUudelleen: Int = 0,
    val epaonnistui: Int = 0,
)

/**
 * Atarun arvioijahakemuksista YKI-arvioijamerkinnat. Kasittelytila elaa taulussa
 * `yki_arvioijahakemus`: jo kirjattua hakemusta ei haeta uudelleen, paitsi tilassa
 * [ArvioijahakemuksenTila.ODOTTAA_YKSILOINTIA]. Epaonnistunut kasittely kirjataan tauluun
 * `yki_arvioijahakemus_virhe` ilman kirjanpitorivia, joten hakemus haetaan joka ajolla uudelleen.
 * Ohimenevat virheet (ONR ei vastaa, samanaikainen muokkaus) jatetaan kirjaamatta.
 *
 * Rivi kirjataan tilaan [ArvioijahakemuksenTila.KASITTELYSSA] ennen arvioijan tallennusta: jos ajo
 * kaatuu tallennuksen ja kirjanpidon valissa, rivi jaa nakyviin tarkistettavaksi eika seuraava ajo
 * luo samasta hakemuksesta toista kautta.
 */
@Service
@ConditionalOnNonEmptyProperty("kitu.ataru.service.url")
class ArvioijahakemusTuontiService(
    private val ataru: AtaruClient,
    private val lomake: ArvioijahakemusLomake,
    private val repository: ArvioijahakemusRepository,
    private val virheRepository: ArvioijahakemusVirheRepository,
    private val arvioijaService: YkiArvioijaService,
    private val kausiRepository: YkiArvioijaKausiRepository,
    private val timeService: TimeService,
    private val auditLogger: AuditLogger,
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    @WithSpan("ataru.arvioijahakemus.tuonti")
    fun tuo(): Tuontiyhteenveto {
        val puutteet = lomake.puutteet()
        check(puutteet.isEmpty()) {
            "Arvioijahakemuslomakkeen asetukset (kitu.ataru.arvioijahakemus) ovat puutteelliset: ${puutteet.joinToString()}"
        }
        val avaimet =
            ataru
                .haeHakemusavaimet(
                    lomake.lomakkeenAvain,
                    ArvioijahakemusKriteeri.atarunSuodatus,
                    ArvioijahakemusKriteeri.atarunTilat,
                ).getOrElse { throw it }
                .map { it.key }
        virheRepository.poistaKirjatut()
        virheRepository.poistaVanhentuneet(avaimet)
        val kasitellyt = repository.kasitellyt(avaimet)
        val uudet = avaimet.filterNot(kasitellyt::contains).toSet()
        if (uudet.isEmpty()) return Tuontiyhteenveto()

        val tulokset =
            ataru
                .haeHakemukset(uudet.toList())
                .getOrElse { throw it }
                .mapIndexed { indeksi, alkio ->
                    try {
                        alkio.fold(
                            ifLeft = { kirjaaJasennysvirhe(indeksi, it, uudet) },
                            ifRight = { kasittele(it) },
                        )
                    } catch (e: Exception) {
                        val hakemusOid = alkio.fold(ifLeft = { it.hakemusOid }, ifRight = { it.hakemusOid })
                        logger.error("Arvioijahakemuksen $hakemusOid käsittely epäonnistui", e)
                        Kasittelytulos.Epaonnistui
                    }
                }

        val rivit = tulokset.filterIsInstance<Kasittelytulos.Kirjattu>().map { it.rivi }
        auditLogger.logAllInternalOnly(
            "Yki arvioija tallennettu ataru-hakemuksesta",
            rivit.filter { it.tila == ArvioijahakemuksenTila.KASITELTY },
        ) { arrayOf("arvioija.oid" to it.henkiloOid, "hakemus.oid" to it.hakemusOid) }

        val tilat = rivit.groupingBy { it.tila }.eachCount()
        val virhe = tulokset.count { it == Kasittelytulos.Virhe }
        val epaonnistui = tulokset.count { it == Kasittelytulos.Epaonnistui }
        val yhteenveto =
            Tuontiyhteenveto(
                kasitelty = tilat[ArvioijahakemuksenTila.KASITELTY] ?: 0,
                virhe = virhe,
                odottaaYksilointia = tilat[ArvioijahakemuksenTila.ODOTTAA_YKSILOINTIA] ?: 0,
                yritetaanUudelleen = uudet.size - rivit.size - virhe - epaonnistui,
                epaonnistui = epaonnistui,
            )
        Span.current().apply {
            setAttribute("arvioijahakemus.kasitelty", yhteenveto.kasitelty.toLong())
            setAttribute("arvioijahakemus.virhe", yhteenveto.virhe.toLong())
            setAttribute("arvioijahakemus.odottaaYksilointia", yhteenveto.odottaaYksilointia.toLong())
            setAttribute("arvioijahakemus.yritetaanUudelleen", yhteenveto.yritetaanUudelleen.toLong())
            setAttribute("arvioijahakemus.epaonnistui", yhteenveto.epaonnistui.toLong())
        }
        check(epaonnistui == 0) { "$epaonnistui arvioijahakemuksen käsittely epäonnistui, ks. lokit" }
        return yhteenveto
    }

    private fun kirjaaJasennysvirhe(
        indeksi: Int,
        virhe: SiirtoJasennysvirhe,
        uudet: Set<String>,
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
        )
    }

    private fun kasittele(hakemus: SiirtoHakemus): Kasittelytulos {
        val kartoitettu =
            lomake.kartoita(hakemus).getOrElse { return kirjaaVirheet(hakemus.hakemusOid, hakemus.personOid, it) }
        val esitaytto =
            arvioijaService.haeHenkilotiedot(hakemus.personOid).getOrElse { return henkilotietovirhe(hakemus, it) }
        val alku = kaudenAlkupaiva(esitaytto)
        val komento =
            komento(kartoitettu, esitaytto, alku).getOrElse {
                return kirjaaVirheet(hakemus.hakemusOid, hakemus.personOid, it)
            }

        repository.tallenna(
            rivi(
                hakemus,
                ArvioijahakemuksenTila.KASITTELYSSA,
                henkiloOid = esitaytto.arvioijaOid.toString(),
                kaudenAlkupaiva = alku.paiva,
            ),
        )
        val arvioija =
            arvioijaService.luoArvioija(komento, tekija = null).getOrElse { return tallennusvirhe(hakemus, it) }
        return kirjaa(
            rivi(
                hakemus,
                ArvioijahakemuksenTila.KASITELTY,
                henkiloOid = arvioija.arvioijaOid.toString(),
                arvioijaId = arvioija.id?.toInt(),
                kaudenAlkupaiva = alku.paiva,
            ),
        )
    }

    private fun henkilotietovirhe(
        hakemus: SiirtoHakemus,
        virhe: YkiArvioijaError,
    ): Kasittelytulos =
        when (virhe) {
            is YkiArvioijaError.Validointivirheet -> {
                kirjaaVirheet(hakemus.hakemusOid, hakemus.personOid, virhe.virheet.map(::validointivirhe))
            }

            is YkiArvioijaError.OppijaaEiYksiloity -> {
                kirjaa(
                    rivi(
                        hakemus,
                        ArvioijahakemuksenTila.ODOTTAA_YKSILOINTIA,
                        syy = "Henkilöä ei ole vielä yksilöity oppijanumerorekisterissä",
                    ),
                )
            }

            is YkiArvioijaError.OppijanumeroaEiSaatu -> {
                if (virhe.syy is OppijanumeroException.OppijaNotFoundException) {
                    kirjaaVirheet(
                        hakemus.hakemusOid,
                        hakemus.personOid,
                        nonEmptyListOf(ArvioijahakemuksenVirhe(ArvioijahakemuksenVirhetyyppi.HENKILOA_EI_LOYDY)),
                    )
                } else {
                    yritetaanUudelleen(hakemus, "Oppijanumerorekisterin kysely epäonnistui: ${virhe.syy.message}")
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

    /**
     * Vain validointi ja samanaikainen muokkaus palaavat ennen tallennusta; muut jattavat rivin
     * tilaan KASITTELYSSA.
     */
    private fun tallennusvirhe(
        hakemus: SiirtoHakemus,
        virhe: YkiArvioijaError,
    ): Kasittelytulos =
        when (virhe) {
            is YkiArvioijaError.Validointivirheet -> {
                kirjaaVirheet(
                    hakemus.hakemusOid,
                    hakemus.personOid,
                    virhe.virheet.map(::validointivirhe),
                    kirjanpidonTila = ArvioijahakemuksenTila.KASITTELYSSA,
                )
            }

            YkiArvioijaError.MuokattuSamanaikaisesti -> {
                repository.poista(hakemus.hakemusOid, ArvioijahakemuksenTila.KASITTELYSSA)
                yritetaanUudelleen(hakemus, "Arvioijaa muokattiin samanaikaisesti")
            }

            is YkiArvioijaError.OppijaaEiYksiloity,
            is YkiArvioijaError.OppijanumeroaEiSaatu,
            YkiArvioijaError.ArvioijaaEiLoydy,
            YkiArvioijaError.KauttaEiLoydy,
            YkiArvioijaError.KausiEiOleAktiivinen,
            YkiArvioijaError.ViimeistaKauttaEiVoiPoistaa,
            -> {
                error("Arvioijan tallennus palautti virheen $virhe, hakemus jää tilaan KASITTELYSSA")
            }
        }

    private fun validointivirhe(virhe: Validation.ValidationError) =
        ArvioijahakemuksenVirhe(
            ArvioijahakemuksenVirhetyyppi.VALIDOINTIVIRHE,
            kentta = virhe.path.joinToString(".").ifEmpty { null },
            viesti = virhe.message,
        )

    private fun kirjaa(rivi: ArvioijahakemusEntity): Kasittelytulos {
        virheRepository.poista(rivi.hakemusOid)
        repository.tallenna(rivi)
        return Kasittelytulos.Kirjattu(rivi)
    }

    private fun kirjaaVirheet(
        hakemusOid: String,
        henkiloOid: String?,
        virheet: NonEmptyList<ArvioijahakemuksenVirhe>,
        kirjanpidonTila: ArvioijahakemuksenTila = ArvioijahakemuksenTila.ODOTTAA_YKSILOINTIA,
    ): Kasittelytulos {
        repository.poista(hakemusOid, kirjanpidonTila)
        virheRepository.tallenna(hakemusOid, henkiloOid, virheet, timeService.now().atOffset(ZoneOffset.UTC))
        logger.warn(
            "Arvioijahakemuksen $hakemusOid käsittely epäonnistui: ${virheet.map {
                it.tyyppi
            }.distinct().joinToString()}",
        )
        return Kasittelytulos.Virhe
    }

    private fun yritetaanUudelleen(
        hakemus: SiirtoHakemus,
        syy: String,
    ): Kasittelytulos {
        logger.warn("Arvioijahakemus ${hakemus.hakemusOid} yritetään uudelleen: $syy")
        return Kasittelytulos.YritetaanUudelleen
    }

    /** Jatkokausi alkaa voimassa olevan tai tulevan kauden jalkeen, muuten kausi alkaa tanaan. */
    private fun kaudenAlkupaiva(esitaytto: ArvioijanEsitaytto): KaudenAlku {
        val tanaan = timeService.today()
        val arvioijaId = esitaytto.olemassaolevaMerkinta?.id?.toInt() ?: return KaudenAlku(tanaan, jatkokausi = false)
        return kausiRepository
            .findKaudet(arvioijaId)
            .mapNotNull { it.paattymispaiva }
            .filterNot { it.isBefore(tanaan) }
            .maxOrNull()
            ?.let { KaudenAlku(it.plusDays(1), jatkokausi = true) }
            ?: KaudenAlku(tanaan, jatkokausi = false)
    }

    private data class KaudenAlku(
        val paiva: LocalDate,
        val jatkokausi: Boolean,
    )

    private fun komento(
        hakemus: Arvioijahakemus,
        esitaytto: ArvioijanEsitaytto,
        alku: KaudenAlku,
    ): Either<NonEmptyList<ArvioijahakemuksenVirhe>, TallennaArvioija> =
        either {
            val katuosoite = hakemus.katuosoite ?: esitaytto.katuosoite
            val postinumero = hakemus.postinumero ?: esitaytto.postinumero
            val postitoimipaikka = hakemus.postitoimipaikka ?: esitaytto.postitoimipaikka
            ensure(katuosoite != null && postinumero != null && postitoimipaikka != null) {
                nonEmptyListOf(
                    ArvioijahakemuksenVirhe(
                        ArvioijahakemuksenVirhetyyppi.OSOITE_PUUTTUU,
                        kentta =
                            listOfNotNull(
                                "katuosoite".takeIf { katuosoite == null },
                                "postinumero".takeIf { postinumero == null },
                                "postitoimipaikka".takeIf { postitoimipaikka == null },
                            ).joinToString(", "),
                    ),
                )
            }
            TallennaArvioija(
                arvioijaOid = esitaytto.arvioijaOid,
                sukunimi = esitaytto.sukunimi,
                etunimet = esitaytto.etunimet,
                sahkopostiosoite = hakemus.sahkopostiosoite ?: esitaytto.sahkopostiosoite,
                katuosoite = katuosoite,
                postinumero = postinumero,
                postitoimipaikka = postitoimipaikka,
                kaudenAlkupaiva = alku.paiva,
                ashaNumero = null,
                arviointioikeudet = hakemus.arviointioikeudet,
                automaattinenJatkokausi = alku.jatkokausi,
            )
        }

    private fun rivi(
        hakemus: SiirtoHakemus,
        tila: ArvioijahakemuksenTila,
        henkiloOid: String? = hakemus.personOid,
        syy: String? = null,
        arvioijaId: Int? = null,
        kaudenAlkupaiva: LocalDate? = null,
    ) = ArvioijahakemusEntity(
        hakemusOid = hakemus.hakemusOid,
        henkiloOid = henkiloOid,
        tila = tila,
        syy = syy,
        arvioijaId = arvioijaId,
        kaudenAlkupaiva = kaudenAlkupaiva,
        kasitelty = timeService.now().atOffset(ZoneOffset.UTC),
    )

    private sealed interface Kasittelytulos {
        data class Kirjattu(
            val rivi: ArvioijahakemusEntity,
        ) : Kasittelytulos

        data object Virhe : Kasittelytulos

        data object YritetaanUudelleen : Kasittelytulos

        data object Epaonnistui : Kasittelytulos
    }
}
