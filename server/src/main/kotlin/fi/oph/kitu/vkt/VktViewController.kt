package fi.oph.kitu.vkt

import fi.oph.kitu.html.ViewMessage
import fi.oph.kitu.html.ViewMessageData
import fi.oph.kitu.html.ViewMessageType
import fi.oph.kitu.html.table.httpParams
import fi.oph.kitu.i18n.CurrentLanguage
import fi.oph.kitu.i18n.LocalizationService
import fi.oph.kitu.i18n.UiText
import fi.oph.kitu.i18n.unaryPlus
import fi.oph.kitu.koodisto.Koodisto
import fi.oph.kitu.koski.KoskiErrorService
import fi.oph.kitu.koski.KoskiVktRequestMapper
import fi.oph.kitu.koski.VktMappingId
import fi.oph.kitu.oppijanumero.OppijanumeroService
import fi.oph.kitu.tiedontuontischema.VktHenkilosuoritus
import fi.oph.kitu.vkt.html.KoskiTransferState
import fi.oph.kitu.vkt.html.VktErinomaisenArviointiPage
import fi.oph.kitu.vkt.html.VktHyvaJaTyydyttavaTarkasteluPage
import fi.oph.kitu.vkt.html.VktKoskiErrors
import fi.oph.kitu.vkt.html.VktSuorituksetPage
import fi.oph.kitu.webmvc.Links
import fi.oph.kitu.webmvc.ResourceNotFoundException
import kotlinx.html.a
import kotlinx.html.br
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.stereotype.Controller
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.ModelAttribute
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.ResponseBody
import org.springframework.web.bind.annotation.ResponseStatus
import org.springframework.web.servlet.view.RedirectView
import tools.jackson.databind.json.JsonMapper

@Controller
@RequestMapping("/vkt")
class VktViewController(
    private val vktSuoritukset: VktSuoritusService,
    private val localizationService: LocalizationService,
    private val oppijanumeroService: OppijanumeroService,
    private val koskiErrorService: KoskiErrorService,
    private val koskiVktRequestMapper: KoskiVktRequestMapper,
    @param:Qualifier("koskiObjectMapper")
    private val koskiObjectMapper: JsonMapper,
) {
    @GetMapping("/", produces = ["text/html"])
    fun suorituksetView(
        @ModelAttribute order: VktSuoritusOrder = VktSuoritusOrder(),
        @ModelAttribute filter: VktSuoritusFilter = VktSuoritusFilter(),
    ): ResponseEntity<String> {
        val (suoritukset, pagination) = vktSuoritukset.getSuorituksetAndPagination(filter, order)

        val translations =
            localizationService
                .translationBuilder()
                .koodistot("kieli", "vkttutkintotaso")
                .build()

        return ResponseEntity.ok(
            VktSuorituksetPage.render(
                suoritukset,
                filter,
                order,
                pagination,
                translations,
                messages = getMessages(),
            ),
        )
    }

    @GetMapping("/erinomainen/ilmoittautuneet", produces = ["text/html"])
    fun erinomaisenTaitotasonIlmoittautuneetView(): RedirectView =
        redirectToSuorituksetView(VktSuoritusFilter.ERINOMAISEN_TASON_ILMOITTAUTUNEET)

    @GetMapping("/erinomainen/arvioidut", produces = ["text/html"])
    fun erinomaisenTaitotasonArvioidutSuorituksetView(): RedirectView =
        redirectToSuorituksetView(VktSuoritusFilter.ERINOMAISEN_TASON_SUORITUKSET)

    @GetMapping("/hyvajatyydyttava/suoritukset", produces = ["text/html"])
    fun hyvanJaTyydyttavanTaitotasonSuorituksetView(): RedirectView =
        redirectToSuorituksetView(VktSuoritusFilter.HYVAN_JA_TYYDYTTAVAN_TASON_SUORITUKSET)

    private fun redirectToSuorituksetView(filter: VktSuoritusFilter): RedirectView {
        val params = httpParams(filter.toMap())
        return RedirectView(Links.Vkt.suoritukset() + params)
    }

    @GetMapping("/suoritukset/{oppijanumero}/{kieli}/{taso}", produces = ["text/html"])
    @ResponseBody
    fun ilmoittautuneenArviointiView(
        @PathVariable oppijanumero: String,
        @PathVariable kieli: Koodisto.Tutkintokieli,
        @PathVariable taso: Koodisto.VktTaitotaso,
        viewMessage: ViewMessage? = null,
    ): ResponseEntity<String> {
        val id = CustomVktSuoritusRepository.Tutkintoryhma(oppijanumero, kieli, taso)
        val suoritus = vktSuoritukset.getOppijanSuoritukset(id) ?: throw VktSuoritusNotFoundError()

        val henkilo = oppijanumeroService.getHenkiloByHenkiloOid(suoritus.henkilo.oid)

        val translations =
            localizationService
                .translationBuilder()
                .koodistot("vkttutkintotaso", "kieli", "kunta", "vktosakoe", "vktarvosana", "vktkielitaito")
                .build()

        val messages =
            listOfNotNull(
                viewMessage?.consume(),
                koskiErrorService.findById(VktMappingId(id))?.let { ViewMessageData.from(it) },
            )

        return ResponseEntity.ok(
            if (suoritus.suoritus.taitotaso == Koodisto.VktTaitotaso.Erinomainen) {
                VktErinomaisenArviointiPage.render(
                    suoritus,
                    henkilo,
                    translations,
                    messages,
                    getKoskiTransferState(suoritus),
                )
            } else {
                VktHyvaJaTyydyttavaTarkasteluPage.render(
                    suoritus,
                    henkilo,
                    translations,
                    messages,
                    getKoskiTransferState(suoritus),
                )
            },
        )
    }

    @PostMapping(
        "/suoritukset/{oppijanumero}/{kieli}/{taso}",
        consumes = [MediaType.APPLICATION_FORM_URLENCODED_VALUE],
        produces = ["text/html"],
    )
    fun saveIlmoittautuneenArviointi(
        @PathVariable oppijanumero: String,
        @PathVariable kieli: Koodisto.Tutkintokieli,
        @PathVariable taso: Koodisto.VktTaitotaso,
        form: VktErinomaisenArviointiPage.ArvosanaFormData,
        viewMessage: ViewMessage,
    ): RedirectView {
        form.toEntries().forEach {
            if (it.merkittyPoistettavaksi) {
                vktSuoritukset.deleteOsakoe(osakoeId = it.id)
            } else {
                vktSuoritukset.setOsakoeArvosana(
                    osakoeId = it.id,
                    arvosana = it.arvosana,
                    arviointipaiva = it.arviointipaiva,
                )
            }
        }
        vktSuoritukset.requestTransferToKoski(CustomVktSuoritusRepository.Tutkintoryhma(oppijanumero, kieli, taso))
        viewMessage.showSuccess(UiText.Vkt.muutoksetTallennettu.toString())
        return RedirectView(Links.Vkt.ilmoittautuneenArviointi(oppijanumero, kieli, taso))
    }

    @GetMapping("/koski-virheet", produces = ["text/html"])
    fun showKoskiVirheet(hidden: Boolean = false): ResponseEntity<String> {
        val errors = koskiErrorService.findAllByEntity("vkt", hidden)
        val hiddenCount = if (hidden) null else koskiErrorService.countByEntity("vkt", true)
        val translations =
            localizationService
                .translationBuilder()
                .koodistot("vkttutkintotaso", "kieli")
                .build()
        return ResponseEntity.ok(VktKoskiErrors.render(errors, hiddenCount, translations))
    }

    @GetMapping("/koski-virheet/piilota/{oppijanumero}/{tutkintokieli}/{taitotaso}/{hidden}", produces = ["text/html"])
    fun hideKoskiVirheet(
        @PathVariable oppijanumero: String,
        @PathVariable tutkintokieli: Koodisto.Tutkintokieli,
        @PathVariable taitotaso: Koodisto.VktTaitotaso,
        @PathVariable hidden: Boolean,
    ): RedirectView {
        koskiErrorService.setHidden(
            VktMappingId(
                CustomVktSuoritusRepository.Tutkintoryhma(
                    oppijanumero = oppijanumero,
                    tutkintokieli = tutkintokieli,
                    taitotaso = taitotaso,
                ),
            ),
            hidden = hidden,
        )
        return RedirectView(Links.Vkt.koskiVirheet())
    }

    @GetMapping("/koski-request/{oppijanumero}/{kieli}/{taso}", produces = ["application/json"])
    fun koskiRequestJson(
        @PathVariable oppijanumero: String,
        @PathVariable kieli: Koodisto.Tutkintokieli,
        @PathVariable taso: Koodisto.VktTaitotaso,
    ): ResponseEntity<String> =
        vktSuoritukset
            .getOppijanSuoritukset(CustomVktSuoritusRepository.Tutkintoryhma(oppijanumero, kieli, taso))
            ?.let { koskiVktRequestMapper.vktSuoritusToKoskiRequest(it).getOrNull() }
            ?.let { ResponseEntity.ok(koskiObjectMapper.writeValueAsString(it)) }
            ?: ResponseEntity.notFound().build()

    private fun getMessages(): List<ViewMessageData> =
        listOfNotNull(
            koskiErrorService.countByEntity("vkt", false).let {
                if (it > 0) {
                    val text = "$it siirtoa KOSKI-tietovarantoon on epäonnistunut"
                    ViewMessageData.html(ViewMessageType.ERROR) {
                        +text
                        br()
                        a(href = Links.Vkt.koskiVirheet()) { +UiText.Vkt.naytaVirheet }
                    }
                } else {
                    null
                }
            },
        )

    private fun getKoskiTransferState(suoritus: VktHenkilosuoritus): Pair<KoskiTransferState, List<String>> =
        if (suoritus.suoritus.koskiSiirtoKasitelty) {
            if (suoritus.suoritus.koskiOpiskeluoikeusOid != null) {
                KoskiTransferState.SUCCESS to emptyList()
            } else {
                KoskiTransferState.INVALID to
                    listOf(UiText.Vkt.merkittyKasitellyksiEiOid.toString())
            }
        } else {
            koskiVktRequestMapper.vktSuoritusToKoskiRequest(suoritus).fold(
                ifRight = { KoskiTransferState.PENDING to emptyList() },
                ifLeft = { KoskiTransferState.NOT_READY to it },
            )
        }
}

class VktSuoritusNotFoundError : ResourceNotFoundException("VKT suoritusta ei löytynyt")
