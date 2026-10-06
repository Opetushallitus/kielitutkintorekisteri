package fi.oph.kitu.kotoutumiskoulutus.suoritukset.error
import fi.oph.kitu.html.json
import fi.oph.kitu.html.table.DisplayTableEnum
import fi.oph.kitu.html.testId
import fi.oph.kitu.i18n.LocalizedString
import fi.oph.kitu.i18n.UiText
import fi.oph.kitu.i18n.finnishDateTime
import fi.oph.kitu.i18n.unaryPlus
import fi.oph.kitu.organisaatiot.Organisaatiot
import fi.oph.kitu.util.toJsonNode
import kotlinx.html.FlowContent
import kotlinx.html.details
import kotlinx.html.div
import kotlinx.html.small
import kotlinx.html.summary

enum class KielitestiSuoritusErrorColumn(
    override val entityName: String,
    override val uiHeaderValue: LocalizedString,
    override val urlParam: String,
    val getValue: (Organisaatiot) -> (KielitestiSuoritusError) -> String,
    val renderHtml: ((Organisaatiot) -> FlowContent.(KielitestiSuoritusError) -> Unit)? = null,
) : DisplayTableEnum {
    Henkilötunnus(
        entityName = "hetu",
        uiHeaderValue = UiText.Koto.Sarake.henkilotunnus,
        urlParam = "henkilötunnus",
        getValue = { { it.hetu.orEmpty() } },
    ),
    Nimi(
        entityName = "nimi",
        uiHeaderValue = UiText.Koto.Sarake.nimi,
        urlParam = "nimi",
        getValue = { { it.nimi } },
        renderHtml = { { nimitiedot(it.etunimet, it.kutsumanimi, it.sukunimi) } },
    ),
    SchoolOid(
        entityName = "schoolOid",
        uiHeaderValue = UiText.Koto.Sarake.organisaatio,
        urlParam = "schooloid",
        getValue = { orgs ->
            { it.schoolOid?.let { oid -> orgs.nimet[oid]?.toString() }.orEmpty() }
        },
        renderHtml = { orgs ->
            {
                it.schoolOid?.let { oid ->
                    orgs.nimet[oid]?.let { name ->
                        div { +name.toString() }
                    }
                    small {
                        testId("schoolOid")
                        +it.schoolOid.toString()
                    }
                }
            }
        },
    ),
    TeacherEmail(
        entityName = "teacherEmail",
        uiHeaderValue = UiText.Koto.Sarake.opettajanSahkopostiosoite,
        urlParam = "teacheremail",
        getValue = { { it.teacherEmail.orEmpty() } },
    ),
    VirheenLuontiaika(
        entityName = "virheenLuontiaika",
        uiHeaderValue = UiText.Koto.Sarake.virheenLuontiaika,
        urlParam = "virheenluontiaika",
        getValue = { { it.virheenLuontiaika.finnishDateTime() } },
    ),
    ValmisSuoritus(
        entityName = "completed",
        uiHeaderValue = UiText.Koto.Sarake.valmis,
        urlParam = "valmisSuoritus",
        getValue = { { if (it.completed) UiText.Filter.kylla.toString() else UiText.Filter.ei.toString() } },
    ),
    Viesti(
        entityName = "viesti",
        uiHeaderValue = UiText.Koto.Sarake.virheviesti,
        urlParam = "viesti",
        getValue = { { it.viesti } },
        renderHtml = {
            {
                testId("viesti")
                if (it.lisatietoja != null) {
                    details {
                        summary { +it.viesti }
                        json(it.lisatietoja.toJsonNode())
                    }
                } else {
                    +it.viesti
                }
            }
        },
    ),
    Ratkaisuehdotus(
        entityName = "onrLisatietoja",
        uiHeaderValue = UiText.Koto.Sarake.ratkaisuehdotus,
        urlParam = "onrLisatietoja",
        getValue = { { it.onrLisatietoja.orEmpty() } },
        renderHtml = {
            {
                it.onrLisatietoja?.let { viesti -> div { +viesti } }
                if (it.onrEtunimet != null || it.onrKutsumanimi != null || it.onrSukunimi != null) {
                    div(classes = "ratkaisuehdotus-nimet") {
                        nimitiedot(it.onrEtunimet, it.onrKutsumanimi, it.onrSukunimi)
                    }
                }
            }
        },
    ),
    VirheellinenKentta(
        entityName = "virheellinenKentta",
        uiHeaderValue = UiText.Koto.Sarake.virheellinenKentta,
        urlParam = "virheellinenkentta",
        getValue = { { it.virheellinenKentta.orEmpty() } },
    ),
    VirheellinenArvo(
        entityName = "virheellinenArvo",
        uiHeaderValue = UiText.Koto.Sarake.virheellinenArvo,
        urlParam = "virheellinenarvo",
        getValue = { { it.virheellinenArvo.orEmpty() } },
    ),
}

private fun FlowContent.nimitiedot(
    etunimet: String?,
    kutsumanimi: String?,
    sukunimi: String?,
) {
    div {
        +UiText.Koto.Sarake.etunimet
        +": ${etunimet.orEmpty()}"
    }
    div {
        +UiText.Koto.Sarake.kutsumanimi
        +": ${kutsumanimi.orEmpty()}"
    }
    div {
        +UiText.Koto.Sarake.sukunimi
        +": ${sukunimi.orEmpty()}"
    }
}
