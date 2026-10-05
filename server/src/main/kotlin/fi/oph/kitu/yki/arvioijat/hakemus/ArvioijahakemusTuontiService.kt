package fi.oph.kitu.yki.arvioijat.hakemus

import arrow.core.Either
import arrow.core.getOrElse
import arrow.core.raise.either
import arrow.core.raise.ensure
import fi.oph.kitu.ataru.AtaruClient
import fi.oph.kitu.ataru.SiirtoHakemus
import fi.oph.kitu.auditlogs.AuditLogger
import fi.oph.kitu.config.ConditionalOnNonEmptyProperty
import fi.oph.kitu.oppijanumero.OppijanumeroException
import fi.oph.kitu.util.TimeService
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
    val hylatty: Int = 0,
    val eiTaytaEhtoja: Int = 0,
    val odottaaYksilointia: Int = 0,
    val yritetaanUudelleen: Int = 0,
    val epaonnistui: Int = 0,
)

/**
 * Atarun arvioijahakemuksista YKI-arvioijamerkinnat. Kasittelytila elaa taulussa
 * `yki_arvioijahakemus`: jo kirjattua hakemusta ei haeta uudelleen, paitsi tilassa
 * [ArvioijahakemuksenTila.ODOTTAA_YKSILOINTIA]. Ohimenevat virheet (ONR ei vastaa, samanaikainen
 * muokkaus) jatetaan kirjaamatta, jolloin hakemus yritetaan seuraavalla ajolla.
 *
 * Rivi kirjataan tilaan [ArvioijahakemuksenTila.KASITTELYSSA] ennen arvioijan tallennusta: jos ajo
 * kaatuu tallennuksen ja kirjanpidon valissa, rivi jaa nakyviin tarkistettavaksi eika seuraava ajo
 * luo samasta hakemuksesta toista kautta.
 */
@Service
@ConditionalOnNonEmptyProperty("kitu.ataru.service.url")
class ArvioijahakemusTuontiService(
    private val ataru: AtaruClient,
    private val repository: ArvioijahakemusRepository,
    private val arvioijaService: YkiArvioijaService,
    private val kausiRepository: YkiArvioijaKausiRepository,
    private val timeService: TimeService,
    private val auditLogger: AuditLogger,
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    @WithSpan("ataru.arvioijahakemus.tuonti")
    fun tuo(): Tuontiyhteenveto {
        val avaimet =
            ataru
                .haeHakemusavaimet(ArvioijahakemusLomake.LOMAKKEEN_AVAIN, ArvioijahakemusKriteeri.atarunSuodatus)
                .getOrElse { throw it }
                .map { it.key }
        val kasitellyt = repository.kasitellyt(avaimet)
        val uudet = avaimet.filterNot(kasitellyt::contains)
        if (uudet.isEmpty()) return Tuontiyhteenveto()

        var epaonnistui = 0
        val rivit =
            ataru
                .haeHakemukset(uudet)
                .getOrElse { throw it }
                .mapNotNull { hakemus ->
                    try {
                        kasittele(hakemus)
                    } catch (e: Exception) {
                        epaonnistui++
                        logger.error("Arvioijahakemuksen ${hakemus.hakemusOid} käsittely epäonnistui", e)
                        null
                    }
                }

        auditLogger.logAllInternalOnly(
            "Yki arvioija tallennettu ataru-hakemuksesta",
            rivit.filter { it.tila == ArvioijahakemuksenTila.KASITELTY },
        ) { arrayOf("arvioija.oid" to it.henkiloOid, "hakemus.oid" to it.hakemusOid) }

        val tilat = rivit.groupingBy { it.tila }.eachCount()
        val yhteenveto =
            Tuontiyhteenveto(
                kasitelty = tilat[ArvioijahakemuksenTila.KASITELTY] ?: 0,
                hylatty = tilat[ArvioijahakemuksenTila.HYLATTY] ?: 0,
                eiTaytaEhtoja = tilat[ArvioijahakemuksenTila.EI_TAYTA_EHTOJA] ?: 0,
                odottaaYksilointia = tilat[ArvioijahakemuksenTila.ODOTTAA_YKSILOINTIA] ?: 0,
                yritetaanUudelleen = uudet.size - rivit.size - epaonnistui,
                epaonnistui = epaonnistui,
            )
        Span.current().apply {
            setAttribute("arvioijahakemus.kasitelty", yhteenveto.kasitelty.toLong())
            setAttribute("arvioijahakemus.hylatty", yhteenveto.hylatty.toLong())
            setAttribute("arvioijahakemus.eiTaytaEhtoja", yhteenveto.eiTaytaEhtoja.toLong())
            setAttribute("arvioijahakemus.odottaaYksilointia", yhteenveto.odottaaYksilointia.toLong())
            setAttribute("arvioijahakemus.yritetaanUudelleen", yhteenveto.yritetaanUudelleen.toLong())
            setAttribute("arvioijahakemus.epaonnistui", yhteenveto.epaonnistui.toLong())
        }
        check(epaonnistui == 0) { "$epaonnistui arvioijahakemuksen käsittely epäonnistui, ks. lokit" }
        return yhteenveto
    }

    private fun kasittele(hakemus: SiirtoHakemus): ArvioijahakemusEntity? {
        if (!ArvioijahakemusKriteeri.tayttyy(hakemus)) {
            return kirjaa(rivi(hakemus, ArvioijahakemuksenTila.EI_TAYTA_EHTOJA))
        }
        if (hakemus.personOid.isNullOrBlank()) {
            return kirjaa(
                rivi(
                    hakemus,
                    ArvioijahakemuksenTila.ODOTTAA_YKSILOINTIA,
                    syy = "Hakemukselle ei ole vielä luotu henkilöä",
                ),
            )
        }

        return either {
            val kartoitettu = ArvioijahakemusLomake.kartoita(hakemus).mapLeft { Hylkays(it) }.bind()
            val esitaytto = arvioijaService.haeHenkilotiedot(kartoitettu.henkiloOid).mapLeft(::hylkays).bind()
            val alku = kaudenAlkupaiva(esitaytto)
            val komento = komento(kartoitettu, esitaytto, alku).bind()

            kirjaa(
                rivi(
                    hakemus,
                    ArvioijahakemuksenTila.KASITTELYSSA,
                    henkiloOid = esitaytto.arvioijaOid.toString(),
                    kaudenAlkupaiva = alku.paiva,
                ),
            )
            val arvioija =
                arvioijaService
                    .luoArvioija(komento, tekija = null)
                    .mapLeft(::hylkays)
                    .onLeft { if (it.tila == null) repository.poista(hakemus.hakemusOid) }
                    .bind()
            rivi(
                hakemus,
                ArvioijahakemuksenTila.KASITELTY,
                henkiloOid = arvioija.arvioijaOid.toString(),
                arvioijaId = arvioija.id?.toInt(),
                kaudenAlkupaiva = alku.paiva,
            )
        }.fold(
            ifLeft = { hylkays ->
                hylkays.tila?.let { rivi(hakemus, it, syy = hylkays.syy) } ?: run {
                    logger.warn("Arvioijahakemus ${hakemus.hakemusOid} yritetään uudelleen: ${hylkays.syy}")
                    null
                }
            },
            ifRight = { it },
        )?.let(::kirjaa)
    }

    private fun kirjaa(rivi: ArvioijahakemusEntity): ArvioijahakemusEntity = rivi.also(repository::tallenna)

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
    ): Either<Hylkays, TallennaArvioija> =
        either {
            val katuosoite = hakemus.katuosoite ?: esitaytto.katuosoite
            val postinumero = hakemus.postinumero ?: esitaytto.postinumero
            val postitoimipaikka = hakemus.postitoimipaikka ?: esitaytto.postitoimipaikka
            ensure(katuosoite != null && postinumero != null && postitoimipaikka != null) {
                Hylkays("Osoite puuttuu sekä hakemukselta että oppijanumerorekisteristä")
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

    /** [tila] null = ohimenevä virhe: rivia ei kirjata, jolloin hakemus yritetaan seuraavalla ajolla. */
    private data class Hylkays(
        val syy: String,
        val tila: ArvioijahakemuksenTila? = ArvioijahakemuksenTila.HYLATTY,
    )

    private fun hylkays(virhe: YkiArvioijaError): Hylkays =
        when (virhe) {
            is YkiArvioijaError.Validointivirheet -> {
                Hylkays(virhe.virheet.joinToString("; ") { it.message })
            }

            is YkiArvioijaError.OppijaaEiYksiloity -> {
                Hylkays(
                    "Henkilöä ei ole vielä yksilöity oppijanumerorekisterissä",
                    tila = ArvioijahakemuksenTila.ODOTTAA_YKSILOINTIA,
                )
            }

            is YkiArvioijaError.OppijanumeroaEiSaatu -> {
                Hylkays(
                    "Oppijanumerorekisterin kysely epäonnistui: ${virhe.syy.message}",
                    tila =
                        ArvioijahakemuksenTila.HYLATTY.takeIf {
                            virhe.syy is OppijanumeroException.OppijaNotFoundException
                        },
                )
            }

            YkiArvioijaError.MuokattuSamanaikaisesti -> {
                Hylkays("Arvioijaa muokattiin samanaikaisesti", tila = null)
            }

            else -> {
                Hylkays(virhe.toString())
            }
        }
}
