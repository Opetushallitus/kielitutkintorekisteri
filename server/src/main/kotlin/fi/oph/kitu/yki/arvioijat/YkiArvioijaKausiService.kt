package fi.oph.kitu.yki.arvioijat

import arrow.core.Either
import arrow.core.left
import arrow.core.right
import fi.oph.kitu.auditlogs.AuditLogOperation
import fi.oph.kitu.auditlogs.AuditLogger
import fi.oph.kitu.oid.Oid
import fi.oph.kitu.util.TimeService
import fi.oph.kitu.util.validation.validate
import fi.oph.kitu.yki.arvioijat.solki.SolkiArvioijaService
import io.opentelemetry.instrumentation.annotations.WithSpan
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import org.springframework.transaction.support.TransactionSynchronization
import org.springframework.transaction.support.TransactionSynchronizationManager
import java.time.LocalDate

@Service
class YkiArvioijaKausiService(
    private val arvioijaRepository: YkiArvioijaRepository,
    private val kausiRepository: YkiArvioijaKausiRepository,
    private val tallennaKausiValidation: TallennaKausiValidation,
    private val auditLogger: AuditLogger,
    private val timeService: TimeService,
    private val solki: SolkiArvioijaService,
    private val asetukset: ArvioijarekisteriAsetukset,
) {
    /**
     * Projisoitava kausi riippuu kuluvasta paivasta, joten projektio vanhenee kun tuleva kausi
     * alkaa. Ajo kirjoittaa vain tosiasiassa muuttuneet rivit, jottei koko rekisteri paady
     * turhaan Solki-lahetysjonoon.
     *
     * Integraatiokytkimen ollessa pois paalta kitu ei ole master, joten projektiota ei kosketa:
     * muuten migraation aikainen tilannekuva yliajaisi Solkin tuoreemman datan.
     */
    @WithSpan
    fun paivitaProjektiot(): Int {
        if (!asetukset.integraatioKaytossa) return 0
        val tanaan = timeService.today()

        return kausiRepository.findArvioijaIdt().count { arvioijaId ->
            val muuttui = kausiRepository.paivitaProjektio(arvioijaId, tanaan)
            if (muuttui) kausiRepository.merkitseMuuttuneeksi(arvioijaId, null)
            muuttui
        }
    }

    /**
     * Rakentaa kaudet uudelleen arviointioikeuksista. Ajetaan kasin ennen kuin integraatiokytkin
     * avataan ymparistossa: V122:n siirto on siihen mennessa vanhentunut, koska taysi sisaantulo
     * on kirjoittanut pelkkaa projektiota koskematta masteriin.
     */
    @WithSpan
    fun synkronoiKaudetArviointioikeuksista(): Int {
        val arvioijat = arvioijaRepository.findAll().mapNotNull { it.id?.toInt() }

        return arvioijat.count { arvioijaId ->
            val oikeudet = kausiRepository.findArviointioikeudet(arvioijaId)
            if (oikeudet.isEmpty()) {
                false
            } else {
                kausiRepository.synkronoiKaudet(arvioijaId, oikeudet, null)
                true
            }
        }
    }

    @WithSpan
    fun haeKaudet(arvioijaId: Int): List<YkiArviointikausiEntity> = kausiRepository.findKaudet(arvioijaId)

    @WithSpan
    fun haeMuutosloki(arvioijaId: Int): List<YkiArvioijaKausiEntity> = kausiRepository.findMuutosloki(arvioijaId)

    /** Polun arvioija ratkaisee: toisen arvioijan kautta ei paase muokkaamaan tunnisteella. */
    @WithSpan
    fun haeKausi(
        arvioijaId: Int,
        kausiId: Int,
    ): YkiArviointikausiEntity? = kausiRepository.findKausi(kausiId)?.takeIf { it.arvioijaId?.toInt() == arvioijaId }

    @WithSpan
    @Transactional
    fun lisaaKausi(
        arvioijaId: Int,
        komento: TallennaKausi,
        tekija: Oid?,
    ): Either<YkiArvioijaError, Unit> =
        tallenna(arvioijaId, komento, AuditLogOperation.YkiArvioijaKausiCreated, tekija) { validoitu ->
            val kausiId =
                kausiRepository.lisaaKausi(
                    arvioijaId,
                    validoitu.alkupaiva,
                    validoitu.paattymispaiva,
                    validoitu.arviointioikeudet,
                    validoitu.ashaNumero,
                    tekija,
                )
            kirjaa(arvioijaId, kausiId, Kausitoimenpide.LISAYS, tekija)
        }

    @WithSpan
    @Transactional
    fun paivitaKausi(
        arvioijaId: Int,
        kausiId: Int,
        komento: TallennaKausi,
        tekija: Oid?,
    ): Either<YkiArvioijaError, Unit> {
        haeKausi(arvioijaId, kausiId) ?: return YkiArvioijaError.KauttaEiLoydy.left()

        return tallenna(arvioijaId, komento, AuditLogOperation.YkiArvioijaKausiUpdated, tekija) { validoitu ->
            kausiRepository.paivitaKausi(
                kausiId,
                validoitu.alkupaiva,
                validoitu.paattymispaiva,
                validoitu.arviointioikeudet,
                validoitu.ashaNumero,
                tekija,
            )
            kirjaa(arvioijaId, kausiId, Kausitoimenpide.MUOKKAUS, tekija)
        }
    }

    /**
     * Passivointi paattaa kauden tahan paivaan. Vain aktiivinen kausi voidaan passivoida: jo
     * paattyneella klikkaus siirtaisi sailytysajan alkua eteenpain, ja tulevalla se kirjaisi
     * hallintopaatoksen jota ei tehty.
     */
    @WithSpan
    @Transactional
    fun passivoiKausi(
        arvioijaId: Int,
        kausiId: Int,
        tekija: Oid?,
    ): Either<YkiArvioijaError, Unit> {
        val arvioija =
            arvioijaRepository.findArvioijaById(arvioijaId) ?: return YkiArvioijaError.ArvioijaaEiLoydy.left()
        val kausi = haeKausi(arvioijaId, kausiId) ?: return YkiArvioijaError.KauttaEiLoydy.left()
        val tanaan = timeService.today()

        if (Rekisterointitila.laske(kausi, tanaan) != Rekisterointitila.AKTIIVINEN) {
            return YkiArvioijaError.KausiEiOleAktiivinen.left()
        }

        kausiRepository.passivoiKausi(kausiId, tanaan, tekija)
        kirjaa(arvioijaId, kausiId, Kausitoimenpide.PASSIVOINTI, tekija)
        auditLogger.log(AuditLogOperation.YkiArvioijaKausiPassivated, arvioija.arvioijaOid)
        paivitaJohdetutTiedot(arvioijaId, tekija, tanaan)

        return Unit.right()
    }

    /**
     * Kovapoisto: vaaralle henkilolle kirjattu kausi ei saa jaada rekisteriin eika lahtea Solkiin.
     * Auditloki kirjataan ennen poistoa, koska se on poiston jalkeen ainoa jaljelle jaava merkki.
     */
    @WithSpan
    @Transactional
    fun poistaKausi(
        arvioijaId: Int,
        kausiId: Int,
        tekija: Oid?,
    ): Either<YkiArvioijaError, Unit> {
        val arvioija =
            arvioijaRepository.findArvioijaById(arvioijaId) ?: return YkiArvioijaError.ArvioijaaEiLoydy.left()
        haeKausi(arvioijaId, kausiId) ?: return YkiArvioijaError.KauttaEiLoydy.left()

        if (kausiRepository.findKaudet(arvioijaId).size <= 1) {
            return YkiArvioijaError.ViimeistaKauttaEiVoiPoistaa.left()
        }

        kirjaa(arvioijaId, kausiId, Kausitoimenpide.POISTO, tekija)
        auditLogger.log(AuditLogOperation.YkiArvioijaKausiDeleted, arvioija.arvioijaOid)
        kausiRepository.poistaKausi(kausiId)
        paivitaJohdetutTiedot(arvioijaId, tekija, timeService.today())

        return Unit.right()
    }

    private fun tallenna(
        arvioijaId: Int,
        komento: TallennaKausi,
        operaatio: AuditLogOperation,
        tekija: Oid?,
        kirjoita: (TallennaKausi) -> Unit,
    ): Either<YkiArvioijaError, Unit> {
        val arvioija =
            arvioijaRepository.findArvioijaById(arvioijaId) ?: return YkiArvioijaError.ArvioijaaEiLoydy.left()

        return tallennaKausiValidation
            .validate(komento)
            .mapLeft { YkiArvioijaError.Validointivirheet(it) }
            .map { validoitu ->
                kirjoita(validoitu)
                auditLogger.log(operaatio, arvioija.arvioijaOid)
                paivitaJohdetutTiedot(arvioijaId, tekija, timeService.today())
            }
    }

    /**
     * Projektio kirjoitetaan uusiksi vasta kausimuutoksen jalkeen. Lahetysjonoon rivi palautetaan
     * vain jos projektio tosiasiassa muuttui: vanhan, ei-projisoitavan kauden korjaus ei muuta
     * Solkille lahtevaa sisaltoa.
     */
    private fun paivitaJohdetutTiedot(
        arvioijaId: Int,
        tekija: Oid?,
        tanaan: LocalDate,
    ) {
        kausiRepository.paivitaEnsimmainenRekisterointipaiva(arvioijaId)
        val muuttui = kausiRepository.paivitaProjektio(arvioijaId, tanaan)
        kausiRepository.poistaPassivointileimaJosAktiivinen(arvioijaId, tanaan)
        if (muuttui) {
            kausiRepository.merkitseMuuttuneeksi(arvioijaId, tekija)
            lahetaSolkiinCommitinJalkeen(arvioijaId)
        }
    }

    /**
     * Kausimuutokset ovat transaktionaalisia, joten lahetys ei saa tapahtua niiden sisalla:
     * HTTP-kutsu pitaisi yki_arvioija-rivin lukkoa ja poolatun yhteyden auki koko etakutsun ajan,
     * ja Solkin hidastuminen sarjallistaisi virkailijat sen taakse. Rivi on jo merkitty
     * lahetysjonoon, joten commitin jalkeen epaonnistuva lahetys jaa jonoon kuten muutenkin.
     */
    private fun lahetaSolkiinCommitinJalkeen(arvioijaId: Int) {
        if (!TransactionSynchronizationManager.isSynchronizationActive()) {
            laheta(arvioijaId)
            return
        }

        TransactionSynchronizationManager.registerSynchronization(
            object : TransactionSynchronization {
                override fun afterCommit() {
                    laheta(arvioijaId)
                }
            },
        )
    }

    private fun laheta(arvioijaId: Int) {
        arvioijaRepository.findArvioijaById(arvioijaId)?.let { solki.lahetaArvioija(it) }
    }

    private fun kirjaa(
        arvioijaId: Int,
        kausiId: Int,
        toimenpide: Kausitoimenpide,
        tekija: Oid?,
    ) {
        val kausi = kausiRepository.findKausi(kausiId) ?: return
        val ensimmainen = kausiRepository.findKaudet(arvioijaId).minOfOrNull { it.alkupaiva }
        kausiRepository.kirjaaMuutos(
            arvioijaId = arvioijaId,
            kausiId = kausiId,
            toimenpide = toimenpide,
            kausi = kausi,
            jatkorekisterointi = ensimmainen != null && kausi.alkupaiva > ensimmainen,
            tekija = tekija,
        )
    }
}
