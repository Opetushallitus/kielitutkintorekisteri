package fi.oph.kitu.yki.arvioijat

import arrow.core.Either
import arrow.core.flatMap
import arrow.core.left
import arrow.core.nonEmptyListOf
import arrow.core.right
import fi.oph.kitu.auditlogs.AuditLogOperation
import fi.oph.kitu.auditlogs.AuditLogger
import fi.oph.kitu.oid.Oid
import fi.oph.kitu.oppijanumero.OppijanumeroException
import fi.oph.kitu.oppijanumero.OppijanumeroService
import fi.oph.kitu.util.TimeService
import fi.oph.kitu.util.validation.Validation.ValidationError
import fi.oph.kitu.util.validation.ValidationService
import fi.oph.kitu.yki.arvioijat.solki.Lahetystulos
import fi.oph.kitu.yki.arvioijat.solki.SolkiArvioijaService
import io.opentelemetry.instrumentation.annotations.WithSpan
import org.springframework.dao.OptimisticLockingFailureException
import org.springframework.stereotype.Service
import java.time.LocalDate
import java.time.OffsetDateTime
import java.time.ZoneOffset

@Service
class YkiArvioijaService(
    private val repository: YkiArvioijaRepository,
    private val kausiRepository: YkiArvioijaKausiRepository,
    private val validationService: ValidationService,
    private val oppijanumeroService: OppijanumeroService,
    private val auditLogger: AuditLogger,
    private val timeService: TimeService,
    private val solki: SolkiArvioijaService,
) {
    @WithSpan
    fun haeSivullinen(params: YkiArvioijaParams): List<YkiArvioijaListRow> =
        repository.findForListView(params, timeService.today()).also { rows ->
            auditLogger.logAllInternalOnly("Yki arvioija viewed", rows) {
                arrayOf("arvioija.oid" to it.arvioijaOid.toString())
            }
        }

    @WithSpan
    fun laske(params: YkiArvioijaParams): Int = repository.countForListView(params, timeService.today())

    /** CSV-vientiin: koko suodatettu joukko ilman sivutusta. */
    @WithSpan
    fun haeKaikki(params: YkiArvioijaParams): List<YkiArvioijaListRow> =
        haeSivullinen(params.copy(page = 1, limit = Int.MAX_VALUE))

    @WithSpan
    fun haeArvioija(id: Int): YkiArvioijaEntity? =
        repository.findArvioijaById(id)?.also {
            auditLogger.log(AuditLogOperation.YkiArvioijaViewed, it.arvioijaOid)
        }

    /** ONR-kyselyn epaonnistuminen on eri asia kuin "ei turvakieltoa", ks. [Turvakieltotieto]. */
    @WithSpan
    fun haeTurvakielto(oid: Oid): Turvakieltotieto =
        oppijanumeroService.getHenkiloByMasterOid(oid).fold(
            ifLeft = { Turvakieltotieto.EI_TIEDOSSA },
            ifRight = { if (it.turvakielto == true) Turvakieltotieto.ON else Turvakieltotieto.EI },
        )

    @WithSpan
    fun haeHenkilotiedot(oppijanumero: String?): Either<YkiArvioijaError, ArvioijanEsitaytto> =
        haeOid(oppijanumero).flatMap { oid -> haeEsitaytto(oid) }

    @WithSpan
    fun luoArvioija(
        komento: TallennaArvioija,
        tekija: Oid?,
        odotettuMuokkaushetki: OffsetDateTime? = null,
    ): Either<YkiArvioijaError, YkiArvioijaEntity> =
        validationService
            .validateAndEnrich(komento)
            .mapLeft { YkiArvioijaError.Validointivirheet(it) }
            .flatMap { validoitu ->
                // Lisayslomakkeelle voi paatya myos jo rekisterissa oleva arvioija (jatkokausi),
                // jolloin tallennus paivittaa merkintaa eika aloita sita alusta.
                val olemassaoleva = repository.findByArvioijaOid(validoitu.arvioijaOid)
                tallennaTaiKonflikti(
                    entiteetti(validoitu, olemassaoleva),
                    tekija,
                    odotettuMuokkaushetki,
                ).flatMap { id ->
                    kausiRepository.viimeisteleKausi(
                        id,
                        validoitu.kaudenAlkupaiva,
                        validoitu.ashaNumero,
                        timeService.today(),
                    )
                    auditLogger.log(
                        if (olemassaoleva == null) {
                            AuditLogOperation.YkiArvioijaCreated
                        } else {
                            AuditLogOperation.YkiArvioijaUpdated
                        },
                        validoitu.arvioijaOid,
                    )
                    lahetaSolkiin(id)
                }
            }

    @WithSpan
    fun haeKausihistoria(id: Int): List<YkiArvioijaKausiEntity> = repository.findKausihistoria(id)

    /**
     * Passivointi ei kulje lomakkeen kautta, joten se rakentaa entiteetin suoraan olemassa olevasta
     * rivista. Tila lasketaan kaudesta, joten passivointi paattaa kauden tahan paivaan — mutta vain
     * kiristaen: nappi tarjotaan myos luonnollisesti paattyneelle merkinnalle, ja ilman kiristysta
     * klikkaus pidentaisi vanhentuneen arviointioikeuden tahan paivaan asti ja kirjaisi
     * kausihistoriaan hallintopaatoksen jota ei tehty. Sailytysajan laskenta alkaa
     * passivointihetkesta, joten aiemmin passivoidun leimaa ei siirreta eteenpain.
     */
    @WithSpan
    fun passivoiArvioija(
        id: Int,
        tekija: Oid?,
    ): Either<YkiArvioijaError, YkiArvioijaEntity> {
        val olemassaoleva = repository.findArvioijaById(id) ?: return YkiArvioijaError.ArvioijaaEiLoydy.left()
        val tanaan = timeService.today()

        val passivoitu =
            olemassaoleva.copy(
                passivoitu = olemassaoleva.passivoitu ?: timeService.now().atOffset(ZoneOffset.UTC),
                arviointioikeudet =
                    olemassaoleva.arviointioikeudet.map { oikeus ->
                        // Alkupaivaan ei kosketa: passivoiKaudet ei koske siihen masterissa
                        // kaan, ja jos ne eroavat, tallennan synkronoima kausi ei osu mihinkaan
                        // olemassa olevaan ja syntyy haamukausi.
                        oikeus.copy(
                            tila = null,
                            kaudenPaattymispaiva = minOf(oikeus.kaudenPaattymispaiva ?: tanaan, tanaan),
                        )
                    },
            )

        // Kaudet ovat master, joten ne katkaistaan ensin: ilman sita projektion uudelleenlaskenta
        // palauttaisi alkuperaiset paivat. Jarjestys ratkaisee myos siksi, etta tallenna
        // synkronoi kaudet oikeuksista — katkaistuun kauteen se osuu paivityksena, ei lisayksena.
        kausiRepository.passivoiKaudet(id, tanaan, tekija)
        repository.tallenna(passivoitu, tekija)
        auditLogger.log(AuditLogOperation.YkiArvioijaPassivated, olemassaoleva.arvioijaOid)

        return lahetaSolkiin(id)
    }

    /**
     * Ajastettu (§6.2). Poisto on peruuttamaton, joten tehtava on oletuksena kaytannossa pois
     * paalta ja otetaan kayttoon vasta kun poistuvien maara on tarkistettu untuvassa.
     */
    @WithSpan
    fun poistaSailytysajanYlittaneet(): Int {
        val tanaan = timeService.today()
        val poistetut = repository.poistaSailytysajanYlittaneet(tanaan, tanaan.minusYears(Sailytysaika.VUOSIA))

        // Ajastetussa tehtavassa ei ole AuditContextia, joten auditLogger.log ei toimi.
        auditLogger.logAllInternalOnly("Yki arvioija poistettu sailytysajan umpeuduttua", poistetut) { oid ->
            arrayOf("arvioija.oid" to oid.toString())
        }

        return poistetut.size
    }

    /**
     * Virkailijan kaynnistama uusintalahetys virhetilanteen jalkeen. Toimii myos silloin, kun
     * automaattinen lahetys on kytketty pois, jotta integraatiota voi kokeilla rivi kerrallaan.
     */
    @WithSpan
    fun lahetaUudelleen(id: Int): Either<YkiArvioijaError, Lahetystulos> {
        val arvioija = repository.findArvioijaById(id) ?: return YkiArvioijaError.ArvioijaaEiLoydy.left()

        // Lahetys vie henkilotietoja ulos jarjestelmasta virkailijan komennosta, joten se kuuluu
        // auditlokiin siina missa muutkin kirjoituspolut.
        auditLogger.log(AuditLogOperation.YkiArvioijaSolkiinLahetetty, arvioija.arvioijaOid)

        return solki.lahetaArvioijaKasin(arvioija).right()
    }

    /**
     * Yksi synkroninen lahetysyritys tallennuksen jalkeen (§5.3): virkailija nakee tuloksen heti.
     * Epaonnistuminen ei kaada tallennusta, vaan rivi jaa lahetysjonoon.
     */
    private fun lahetaSolkiin(id: Int): Either<YkiArvioijaError, YkiArvioijaEntity> {
        val tallennettu = repository.findArvioijaById(id) ?: return YkiArvioijaError.ArvioijaaEiLoydy.left()
        solki.lahetaArvioija(tallennettu)
        return repository.findArvioijaById(id)?.right() ?: YkiArvioijaError.ArvioijaaEiLoydy.left()
    }

    /**
     * Tarpeen ennen lomakkeen kenttatarkistuksia: ilman tata tuntemattomalle id:lle lahetetty
     * vajaa lomake saisi vastaukseksi 200:n ja lomakesivun 404:n sijaan.
     */
    @WithSpan
    fun onOlemassa(id: Int): Boolean = repository.findArvioijaById(id) != null

    /**
     * Vain yhteystiedot: kaudet muokataan omilla reiteillaan. Arviointioikeudet kopioidaan
     * tallennettavaan entiteettiin sellaisenaan, joten myos Solkin kirjaama tila sailyy eika
     * yhteystiedon korjaus elvyta passivoitua merkintaa.
     */
    @WithSpan
    fun paivitaArvioija(
        id: Int,
        komento: PaivitaArvioijanTiedot,
        tekija: Oid?,
        odotettuMuokkaushetki: OffsetDateTime? = null,
    ): Either<YkiArvioijaError, YkiArvioijaEntity> {
        val olemassaoleva = repository.findArvioijaById(id) ?: return YkiArvioijaError.ArvioijaaEiLoydy.left()

        // Polun id ratkaisee kenen tietoja muokataan, ei lomakkeen piilokentta.
        val kohdistettu = komento.copy(arvioijaOid = olemassaoleva.arvioijaOid)

        return validationService
            .validateAndEnrich(kohdistettu)
            .mapLeft { YkiArvioijaError.Validointivirheet(it) }
            .flatMap { validoitu ->
                tallennaTaiKonflikti(
                    olemassaoleva.copy(
                        sukunimi = validoitu.sukunimi,
                        etunimet = validoitu.etunimet,
                        sahkopostiosoite = validoitu.sahkopostiosoite,
                        katuosoite = validoitu.katuosoite,
                        postinumero = validoitu.postinumero,
                        postitoimipaikka = validoitu.postitoimipaikka,
                    ),
                    tekija,
                    odotettuMuokkaushetki,
                ).flatMap {
                    auditLogger.log(AuditLogOperation.YkiArvioijaUpdated, validoitu.arvioijaOid)
                    lahetaSolkiin(id)
                }
            }
    }

    /**
     * Lomake kantaa vain virkailijan syottamat kentat; tila lasketaan kaudesta. Sailytysajan
     * alkuhetki paatellaan tallennettavista kausista eika lomakkeen kaudesta, koska
     * [paataMuuttumatonPassivoitu] voi viela kiristaa kautta: muuten passiiviseksi jaava merkinta
     * menettaisi alkuhetkensa.
     */
    private fun entiteetti(
        validoitu: TallennaArvioija,
        olemassaoleva: YkiArvioijaEntity?,
    ): YkiArvioijaEntity {
        val tanaan = timeService.today()
        val tallennettava = validoitu.toEntity(ensimmainenRekisterointipaiva(olemassaoleva, validoitu))
        val arviointioikeudet = tallennettava.arviointioikeudet
        val merkintaJaaPassiiviseksi =
            arviointioikeudet.all { Rekisterointitila.laske(it, tanaan) == Rekisterointitila.PASSIVOITU }

        return tallennettava.copy(
            passivoitu = olemassaoleva?.passivoitu?.takeIf { merkintaJaaPassiiviseksi },
            arviointioikeudet = arviointioikeudet,
        )
    }

    private fun tallennaTaiKonflikti(
        arvioija: YkiArvioijaEntity,
        tekija: Oid?,
        odotettuMuokkaushetki: OffsetDateTime?,
    ): Either<YkiArvioijaError, Int> =
        try {
            repository.tallenna(arvioija, tekija, odotettuMuokkaushetki = odotettuMuokkaushetki).right()
        } catch (_: OptimisticLockingFailureException) {
            YkiArvioijaError.MuokattuSamanaikaisesti.left()
        }

    private fun ensimmainenRekisterointipaiva(
        olemassaoleva: YkiArvioijaEntity?,
        komento: TallennaArvioija,
    ): LocalDate =
        olemassaoleva
            ?.arviointioikeudet
            ?.minOfOrNull { it.ensimmainenRekisterointipaiva }
            ?: komento.kaudenAlkupaiva

    private fun haeOid(oppijanumero: String?): Either<YkiArvioijaError, Oid> {
        val syote =
            oppijanumero?.trim()?.takeIf { it.isNotEmpty() }
                ?: return virhe("oppijanumero", "Oppijanumero on pakollinen tieto").left()

        val syotetty =
            Oid.parse(syote).getOrNull()
                ?: return virhe("oppijanumero", "Oppijanumero on virheellinen").left()

        // Virkailija voi syottaa duplikaatin OIDin. Rekisteri avaimennetaan master-OIDilla, joten
        // ilman ratkaisua sama henkilo saisi toisen merkinnan eika olemassa olevaa loydettaisi.
        // getOppijanumero ei putoa takaisin henkilo-OIDiin, joten yksiloimaton henkilo hylataan.
        return oppijanumeroService.getOppijanumero(syotetty).mapLeft { onrVirhe ->
            when (onrVirhe) {
                is OppijanumeroException.OppijaNotIdentifiedException -> YkiArvioijaError.OppijaaEiYksiloity(syotetty)
                else -> YkiArvioijaError.OppijanumeroaEiSaatu(onrVirhe)
            }
        }
    }

    private fun haeEsitaytto(oid: Oid): Either<YkiArvioijaError, ArvioijanEsitaytto> =
        oppijanumeroService
            .getHenkiloByMasterOid(oid)
            .mapLeft { YkiArvioijaError.OppijanumeroaEiSaatu(it) }
            .map { henkilo ->
                val yhteystiedot =
                    henkilo.yhteystiedotRyhma
                        .orEmpty()
                        .flatMap { it.yhteystieto.orEmpty() }

                fun arvo(tyyppi: String): String? =
                    yhteystiedot
                        .firstOrNull { it.yhteystietoTyyppi == tyyppi && !it.yhteystietoArvo.isNullOrBlank() }
                        ?.yhteystietoArvo

                ArvioijanEsitaytto(
                    arvioijaOid = oid,
                    sukunimi = henkilo.sukunimi.orEmpty(),
                    etunimet = henkilo.etunimet.orEmpty(),
                    sahkopostiosoite = arvo("YHTEYSTIETO_SAHKOPOSTI"),
                    katuosoite = arvo("YHTEYSTIETO_KATUOSOITE"),
                    postinumero = arvo("YHTEYSTIETO_POSTINUMERO"),
                    postitoimipaikka = arvo("YHTEYSTIETO_KAUPUNKI"),
                    turvakielto = if (henkilo.turvakielto == true) Turvakieltotieto.ON else Turvakieltotieto.EI,
                    olemassaolevaMerkinta = repository.findByArvioijaOid(oid),
                )
            }

    private fun virhe(
        kentta: String,
        viesti: String,
    ) = YkiArvioijaError.Validointivirheet(nonEmptyListOf(ValidationError(listOf(kentta), viesti)))
}
