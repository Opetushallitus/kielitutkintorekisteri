package fi.oph.kitu.yki.arvioijat.hakemus

import fi.oph.kitu.auditlogs.AuditLogger
import fi.oph.kitu.webmvc.ResourceNotFoundException
import fi.oph.kitu.yki.arvioijat.ArvioijarekisteriAsetukset
import org.springframework.http.ResponseEntity
import org.springframework.stereotype.Controller
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestMapping

@Controller
@RequestMapping("/yki/arvioijat/hakemukset")
class ArvioijahakemusViewController(
    private val repository: ArvioijahakemusRepository,
    private val auditLogger: AuditLogger,
    private val asetukset: ArvioijarekisteriAsetukset,
) {
    @GetMapping("", produces = ["text/html"])
    fun hakemuksetView(): ResponseEntity<String> {
        if (!asetukset.hakemustenTuontiKaytossa) throw ArvioijahakemusNotFoundError()
        val rivit = repository.haeKasittelemattomat()
        auditLogger.logAllInternalOnly("Yki arvioijahakemus viewed", rivit) {
            arrayOf("hakemus.oid" to it.hakemusOid, "henkilo.oid" to it.henkiloOid)
        }
        return ResponseEntity.ok(ArvioijahakemusPage.render(rivit))
    }
}

class ArvioijahakemusNotFoundError : ResourceNotFoundException("Arvioijahakemusten tuonti ei ole käytössä")
