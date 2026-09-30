package fi.oph.kitu.kotoutumiskoulutus.suoritukset
import fi.oph.kitu.html.Page
import fi.oph.kitu.html.card
import fi.oph.kitu.html.infoTable
import fi.oph.kitu.html.orDash
import fi.oph.kitu.i18n.UiText
import fi.oph.kitu.i18n.finnishDateTime
import fi.oph.kitu.i18n.unaryPlus
import fi.oph.kitu.organisaatiot.Organisaatiot
import kotlinx.html.FlowContent
import kotlinx.html.h1
import kotlinx.html.h2
import kotlinx.html.h3

object KielitestiSuoritusPage {
    fun render(
        suoritus: KielitestiSuoritus,
        orgs: Organisaatiot,
    ) = Page.renderHtml {
        h1 { +"${suoritus.sukunimi} ${suoritus.etunimet}" }
        h2 { +UiText.Nav.kotoutumiskoulutuksenPaattotesti }
        henkilonTiedot(suoritus)
        tutkintotiedot(suoritus, orgs)
        arviointi(suoritus)
        integraatiot(suoritus)
    }

    fun FlowContent.henkilonTiedot(suoritus: KielitestiSuoritus) {
        h3 { +UiText.Koto.henkilotiedot }
        card(compact = true) {
            infoTable(
                UiText.Koto.Sarake.oppijanumero to { +suoritus.oppijanumero?.toString().orDash() },
                UiText.Koto.Sarake.sukunimi to { +suoritus.sukunimi },
                UiText.Koto.Sarake.etunimet to { +suoritus.etunimet },
                UiText.Koto.Sarake.kutsumanimi to { +suoritus.kutsumanimi },
                UiText.Koto.Sarake.sahkoposti to { +suoritus.email },
            )
        }
    }

    fun FlowContent.tutkintotiedot(
        suoritus: KielitestiSuoritus,
        orgs: Organisaatiot,
    ) {
        h3 { +UiText.Koto.tutkinnonTiedot }
        card(compact = true) {
            infoTable(
                UiText.Koto.kurssi to { +"${suoritus.kurssi} (${suoritus.kurssiId})" },
                UiText.Koto.jarjestaja to
                    { +"${orgs.nimet[suoritus.oppilaitosOid]} (${suoritus.oppilaitosOid})" },
                UiText.Koto.Sarake.opettajanSahkopostiosoite to { +suoritus.opettajanEmail.orEmpty() },
                UiText.Koto.Sarake.suoritusaika to
                    { suoritus.suoritusaika?.let { finnishDateTime(it) } ?: +"-" },
                UiText.Koto.Sarake.testikieli to { +suoritus.testikieli?.toString().orDash() },
                UiText.Koto.tehtavapaketti to { +suoritus.tehtavapaketti.orEmpty() },
            )
        }
    }

    fun FlowContent.arviointi(suoritus: KielitestiSuoritus) {
        h3 { +UiText.Koto.arviointi }
        card(compact = true) {
            infoTable(
                UiText.Koto.Sarake.luetunYmmartaminen to
                    { +suoritus.luetunYmmartaminen?.toString().orDash() },
                UiText.Koto.Sarake.kuullunYmmartaminen to
                    { +suoritus.kuullunYmmartaminen?.toString().orDash() },
                UiText.Koto.Sarake.puhe to { +suoritus.puhe?.toString().orDash() },
                UiText.Koto.Sarake.kirjoittaminen to { +suoritus.kirjoittaminen?.toString().orDash() },
            )
        }
    }

    fun FlowContent.integraatiot(suoritus: KielitestiSuoritus) {
        h3 { +UiText.Koto.integraatiot }
        card(compact = true) {
            infoTable(
                UiText.Koto.viimeksiMuokattu to { finnishDateTime(suoritus.lastModified) },
            )
        }
    }
}
