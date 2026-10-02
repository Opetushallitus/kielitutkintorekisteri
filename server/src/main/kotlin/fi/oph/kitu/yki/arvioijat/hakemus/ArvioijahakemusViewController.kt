package fi.oph.kitu.yki.arvioijat.hakemus

import fi.oph.kitu.auditlogs.AuditLogger
import org.springframework.http.ResponseEntity
import org.springframework.stereotype.Controller
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestMapping

@Controller
@RequestMapping("/yki/arvioijat/hakemukset")
class ArvioijahakemusViewController(
    private val repository: ArvioijahakemusRepository,
    private val auditLogger: AuditLogger,
) {
    @GetMapping("", produces = ["text/html"])
    fun hakemuksetView(): ResponseEntity<String> {
        val rivit = repository.haeKasittelemattomat()
        auditLogger.logAllInternalOnly("Yki arvioijahakemus viewed", rivit) {
            arrayOf("hakemus.oid" to it.hakemusOid, "henkilo.oid" to it.henkiloOid)
        }
        return ResponseEntity.ok(ArvioijahakemusPage.render(rivit))
    }
}
