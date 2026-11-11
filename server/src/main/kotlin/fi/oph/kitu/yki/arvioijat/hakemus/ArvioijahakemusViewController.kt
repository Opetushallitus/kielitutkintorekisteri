package fi.oph.kitu.yki.arvioijat.hakemus

import fi.oph.kitu.auditlogs.AuditLogger
import fi.oph.kitu.html.FormErrors
import fi.oph.kitu.html.ViewMessage
import fi.oph.kitu.html.copyable
import fi.oph.kitu.html.warningMessage
import fi.oph.kitu.i18n.UiText
import fi.oph.kitu.i18n.unaryPlus
import fi.oph.kitu.security.CurrentUser
import fi.oph.kitu.webmvc.Links
import fi.oph.kitu.webmvc.ResourceNotFoundException
import fi.oph.kitu.yki.arvioijat.ArvioijaFormData
import fi.oph.kitu.yki.arvioijat.ArvioijarekisteriAsetukset
import fi.oph.kitu.yki.arvioijat.YkiArvioijaLomakePage
import fi.oph.kitu.yki.arvioijat.lomakevirheet
import kotlinx.html.p
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.ObjectProvider
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.stereotype.Controller
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.ModelAttribute
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestMapping
import java.net.URI

@Controller
@RequestMapping("/yki/arvioijat/hakemukset")
class ArvioijahakemusViewController(
    private val repository: ArvioijahakemusRepository,
    private val tarkistettavaRepository: ArvioijahakemusTarkistettavaRepository,
    private val virheRepository: ArvioijahakemusVirheRepository,
    private val tarkistusService: ArvioijahakemusTarkistusService,
    private val hakuService: ObjectProvider<ArvioijahakemusHakuService>,
    private val auditLogger: AuditLogger,
    private val asetukset: ArvioijarekisteriAsetukset,
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    @GetMapping("", produces = ["text/html"])
    fun hakemuksetView(viewMessage: ViewMessage? = null): ResponseEntity<String> {
        tarkistaKaytossa()
        val tarkistettavat = tarkistettavaRepository.haeKaikki()
        val keskeneraiset = repository.haeKasittelemattomat()
        auditLogger.logAllInternalOnly(
            "Yki arvioijahakemus viewed",
            tarkistettavat.map {
                it.hakemusOid to it.henkiloOid
            } + keskeneraiset.map { it.hakemusOid to it.henkiloOid },
        ) { (hakemusOid, henkiloOid) -> arrayOf("hakemus.oid" to hakemusOid, "henkilo.oid" to henkiloOid) }
        return ResponseEntity.ok(
            ArvioijahakemusPage.render(
                tarkistettavat = tarkistettavat,
                keskeneraiset = keskeneraiset,
                virheita = virheRepository.count(),
                muokkausKaytossa = asetukset.muokkausKaytossa,
                flash = viewMessage?.consume(),
            ),
        )
    }

    @PostMapping("/hae", produces = ["text/html"])
    fun haeUudet(viewMessage: ViewMessage? = null): ResponseEntity<String> {
        tarkistaKaytossa()
        val haku = hakuService.ifAvailable ?: throw ArvioijahakemusNotFoundError()
        runCatching { haku.hae() }
            .onSuccess {
                viewMessage?.showSuccess(
                    UiText.Yki.Arvioijahakemus
                        .hakuValmis(it.tarkistettavia.toLong(), it.virheellisia.toLong())
                        .toString(),
                )
            }.onFailure {
                logger.error("Arvioijahakemusten haku epäonnistui", it)
                viewMessage?.showError(
                    UiText.Yki.Arvioijahakemus.hakuEpaonnistui
                        .toString(),
                )
            }
        return listaan()
    }

    @GetMapping("/{hakemusOid}/tarkista", produces = ["text/html"])
    fun tarkistaView(
        @PathVariable hakemusOid: String,
        viewMessage: ViewMessage? = null,
    ): ResponseEntity<String> {
        tarkistaKaytossa()
        return tarkistusService.lomake(hakemusOid).fold(
            ifLeft = { virhe -> virheenJalkeen(virhe, viewMessage) },
            ifRight = { form -> ResponseEntity.ok(lomake(hakemusOid, form, FormErrors.EMPTY)) },
        )
    }

    @PostMapping("/{hakemusOid}/tarkista", produces = ["text/html"])
    fun vahvista(
        @PathVariable hakemusOid: String,
        @ModelAttribute form: ArvioijaFormData,
        viewMessage: ViewMessage? = null,
    ): ResponseEntity<String> {
        tarkistaKaytossa()
        return tarkistusService.vahvista(hakemusOid, form, CurrentUser.oid()).fold(
            ifLeft = { virhe ->
                when (virhe) {
                    is Tarkistusvirhe.Arvioija -> {
                        ResponseEntity.ok(
                            lomake(hakemusOid, form, lomakevirheet(virhe.virhe)),
                        )
                    }

                    else -> {
                        virheenJalkeen(virhe, viewMessage)
                    }
                }
            },
            ifRight = { arvioija ->
                viewMessage?.showSuccess(
                    UiText.Yki.Arvioija.tallennettu
                        .toString(),
                )
                ResponseEntity
                    .status(HttpStatus.SEE_OTHER)
                    .location(URI.create(Links.Yki.arvioija(arvioija.id!!.toInt())))
                    .build()
            },
        )
    }

    @GetMapping("/virheet", produces = ["text/html"])
    fun virheetView(): ResponseEntity<String> {
        tarkistaKaytossa()
        val rivit = virheRepository.haeKaikki()
        auditLogger.logAllInternalOnly("Yki arvioijahakemuksen virhe viewed", rivit) {
            arrayOf("hakemus.oid" to it.hakemusOid, "henkilo.oid" to it.henkiloOid)
        }
        return ResponseEntity.ok(ArvioijahakemusVirhePage.render(rivit))
    }

    private fun tarkistaKaytossa() {
        if (!asetukset.hakemustenTuontiKaytossa) throw ArvioijahakemusNotFoundError()
    }

    private fun virheenJalkeen(
        virhe: Tarkistusvirhe,
        viewMessage: ViewMessage?,
    ): ResponseEntity<String> =
        when (virhe) {
            Tarkistusvirhe.EiTarkistettavana -> {
                throw TarkistettavaArvioijahakemusNotFoundError()
            }

            Tarkistusvirhe.JoKasittelyssa -> {
                viewMessage?.showInfo(
                    UiText.Yki.Arvioijahakemus.Tarkistus.joKasittelyssa
                        .toString(),
                )
                listaan()
            }

            is Tarkistusvirhe.Arvioija -> {
                viewMessage?.showError(
                    UiText.Yki.Arvioijahakemus.Tarkistus.henkilotiedotEpaonnistui
                        .toString(),
                )
                listaan()
            }
        }

    private fun listaan(): ResponseEntity<String> =
        ResponseEntity
            .status(HttpStatus.SEE_OTHER)
            .location(URI.create(Links.Yki.arvioijahakemukset()))
            .build()

    private fun lomake(
        hakemusOid: String,
        form: ArvioijaFormData,
        errors: FormErrors,
    ): String =
        YkiArvioijaLomakePage.renderLomake(
            form = form,
            errors = errors,
            otsikko = UiText.Yki.Arvioijahakemus.Tarkistus.otsikko,
            action = Links.Yki.tarkistaArvioijahakemus(hakemusOid),
            tallennaTeksti = UiText.Yki.Arvioijahakemus.Tarkistus.vahvista,
            peruutusLinkki = Links.Yki.arvioijahakemukset(),
            johdanto = {
                val maksu = tarkistettavaRepository.hae(hakemusOid)?.maksu ?: Maksutieto.EI_TIEDOSSA
                if (maksu.tila != Maksuntila.MAKSETTU) {
                    warningMessage(UiText.Yki.Arvioijahakemus.Maksu.eiMaksettuVaroitus)
                }
                p {
                    +UiText.Yki.Arvioijahakemus.hakemus
                    +": "
                    copyable(hakemusOid)
                }
                p {
                    +UiText.Yki.Arvioijahakemus.Maksu.otsikko
                    +": "
                    maksunTila(maksu)
                }
                p { +UiText.Yki.Arvioijahakemus.Tarkistus.ohje }
            },
        )
}

class ArvioijahakemusNotFoundError : ResourceNotFoundException("Arvioijahakemusten tuonti ei ole käytössä")

class TarkistettavaArvioijahakemusNotFoundError : ResourceNotFoundException("Arvioijahakemus ei odota tarkistusta")
