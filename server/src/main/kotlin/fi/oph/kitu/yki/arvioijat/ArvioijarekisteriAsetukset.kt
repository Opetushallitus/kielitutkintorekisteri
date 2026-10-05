package fi.oph.kitu.yki.arvioijat

import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component

/**
 * Kaksi erillista kytkinta, koska ne vastaavat eri kysymyksiin: saako virkailija muokata, ja onko
 * kitu rekisterin master. Kayttoonotossa kayttoliittyma avataan ensin ja Solkin kirjoitukset
 * katkaistaan vasta sen jalkeen, joten valiin jaa jakso jolla vain toinen on paalla.
 */
@Component
class ArvioijarekisteriAsetukset(
    @param:Value($$"${kitu.yki.arvioijarekisteri.muokkaus.enabled:false}")
    val muokkausKaytossa: Boolean,
    /** Kitu on rekisterin master: sisaantulo kavennetaan, lahetys on paalla, projektio paivitetaan. */
    @param:Value($$"${kitu.yki.arvioijarekisteri.integraatio.enabled:false}")
    val integraatioKaytossa: Boolean,
    @param:Value($$"${kitu.ataru.service.url:}")
    private val ataruServiceUrl: String = "",
) {
    val hakemustenTuontiKaytossa: Boolean get() = ataruServiceUrl.isNotBlank()
}
