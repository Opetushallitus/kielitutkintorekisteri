package fi.oph.kitu.kotoutumiskoulutus.suoritukset.error
import fi.oph.kitu.html.ModalCommand
import fi.oph.kitu.html.json
import fi.oph.kitu.html.modal
import fi.oph.kitu.html.modalCommandButton
import fi.oph.kitu.html.table.ColumnTag
import fi.oph.kitu.html.table.ColumnTags
import fi.oph.kitu.html.table.RenderableDisplayTableEnum
import fi.oph.kitu.html.testId
import fi.oph.kitu.i18n.LocalizedString
import fi.oph.kitu.i18n.UiText
import fi.oph.kitu.i18n.finnishDateTime
import fi.oph.kitu.i18n.unaryPlus
import fi.oph.kitu.oppijanumero.nimetVastaavat
import fi.oph.kitu.util.toJsonNode
import kotlinx.html.FlowContent
import kotlinx.html.div
import kotlinx.html.footer
import kotlinx.html.small

enum class KielitestiSuoritusErrorColumn(
    override val entityName: String,
    override val uiHeaderValue: LocalizedString,
    override val urlParam: String,
    override val getValue: (KielitestiSuoritusError) -> String,
    override val renderHtml: (FlowContent.(KielitestiSuoritusError) -> Unit)? = null,
) : RenderableDisplayTableEnum<KielitestiSuoritusError> {
    @ColumnTags(ColumnTag.LIST_VIEW, ColumnTag.CSV_EXPORT)
    Henkilötunnus(
        entityName = "hetu",
        uiHeaderValue = UiText.Koto.Sarake.henkilotunnus,
        urlParam = "henkilötunnus",
        getValue = { it.hetu.orEmpty() },
    ),

    @ColumnTags(ColumnTag.LIST_VIEW, ColumnTag.CSV_EXPORT)
    Nimi(
        entityName = "nimi",
        uiHeaderValue = UiText.Koto.Sarake.nimi,
        urlParam = "nimi",
        getValue = { nimirivit(it.etunimet, it.kutsumanimi, it.sukunimi).joinToString("\n") },
        renderHtml = { riveina(nimirivit(it.etunimet, it.kutsumanimi, it.sukunimi)) },
    ),

    @ColumnTags(ColumnTag.LIST_VIEW, ColumnTag.CSV_EXPORT)
    SchoolOid(
        entityName = "schoolOid",
        uiHeaderValue = UiText.Koto.Sarake.organisaatio,
        urlParam = "schooloid",
        getValue = { listOfNotNull(it.organisaatio?.toString(), it.schoolOid?.toString()).joinToString("\n") },
        renderHtml = {
            it.schoolOid?.let { oid ->
                it.organisaatio?.let { nimi -> div { +nimi } }
                small {
                    testId("schoolOid")
                    +oid.toString()
                }
            }
        },
    ),

    @ColumnTags(ColumnTag.LIST_VIEW, ColumnTag.CSV_EXPORT)
    TeacherEmail(
        entityName = "teacherEmail",
        uiHeaderValue = UiText.Koto.Sarake.opettajanSahkopostiosoite,
        urlParam = "teacheremail",
        getValue = { it.teacherEmail.orEmpty() },
    ),

    @ColumnTags(ColumnTag.LIST_VIEW, ColumnTag.CSV_EXPORT)
    VirheenLuontiaika(
        entityName = "virheenLuontiaika",
        uiHeaderValue = UiText.Koto.Sarake.virheenLuontiaika,
        urlParam = "virheenluontiaika",
        getValue = { it.virheenLuontiaika.finnishDateTime() },
    ),

    @ColumnTags(ColumnTag.LIST_VIEW, ColumnTag.CSV_EXPORT)
    ValmisSuoritus(
        entityName = "completed",
        uiHeaderValue = UiText.Koto.Sarake.valmis,
        urlParam = "valmisSuoritus",
        getValue = { if (it.completed) UiText.Filter.kylla.toString() else UiText.Filter.ei.toString() },
    ),

    @ColumnTags(ColumnTag.LIST_VIEW, ColumnTag.CSV_EXPORT)
    Viesti(
        entityName = "viesti",
        uiHeaderValue = UiText.Koto.Sarake.virheviesti,
        urlParam = "viesti",
        getValue = { it.viesti },
        renderHtml = {
            +it.viesti
            it.lisatietoja?.let { lisatiedot -> lisatietoModaali("virhe-lisatiedot-${it.id}", lisatiedot) }
        },
    ),

    @ColumnTags(ColumnTag.LIST_VIEW, ColumnTag.CSV_EXPORT)
    Ratkaisuehdotus(
        entityName = "onrLisatietoja",
        uiHeaderValue = UiText.Koto.Sarake.ratkaisuehdotus,
        urlParam = "onrLisatietoja",
        getValue = { (listOfNotNull(it.onrLisatietoja) + it.onrNimirivit().orEmpty()).joinToString("\n") },
        renderHtml = {
            it.onrLisatietoja?.let { viesti -> div { +viesti } }
            it.onrNimirivit()?.let { rivit ->
                val vastaa = nimetVastaavat(it.etunimet, it.sukunimi, it.onrEtunimet, it.onrSukunimi)
                div(classes = if (vastaa) "ratkaisuehdotus-nimet" else "ratkaisuehdotus-nimet ei-vastaa") {
                    riveina(rivit)
                }
            }
        },
    ),

    @ColumnTags(ColumnTag.LIST_VIEW, ColumnTag.CSV_EXPORT)
    KurssinId(
        entityName = "kurssiId",
        uiHeaderValue = UiText.Koto.Sarake.kurssinId,
        urlParam = "kurssinid",
        getValue = { it.kurssiId?.toString().orEmpty() },
    ),

    @ColumnTags(ColumnTag.LIST_VIEW, ColumnTag.CSV_EXPORT)
    KurssinNimi(
        entityName = "kurssi",
        uiHeaderValue = UiText.Koto.Sarake.kurssinNimi,
        urlParam = "kurssinnimi",
        getValue = { it.kurssi.orEmpty() },
    ),

    @ColumnTags(ColumnTag.LIST_VIEW, ColumnTag.CSV_EXPORT)
    Suoritusaika(
        entityName = "suoritusaika",
        uiHeaderValue = UiText.Koto.Sarake.suoritusaika,
        urlParam = "suoritusaika",
        getValue = { it.suoritusaika?.finnishDateTime().orEmpty() },
    ),
}

private fun nimirivit(
    etunimet: String?,
    kutsumanimi: String?,
    sukunimi: String?,
): List<String> =
    listOf(
        "${UiText.Koto.Sarake.etunimet}: ${etunimet.orEmpty()}",
        "${UiText.Koto.Sarake.kutsumanimi}: ${kutsumanimi.orEmpty()}",
        "${UiText.Koto.Sarake.sukunimi}: ${sukunimi.orEmpty()}",
    )

private fun KielitestiSuoritusError.onrNimirivit(): List<String>? =
    if (onrEtunimet != null || onrKutsumanimi != null || onrSukunimi != null) {
        nimirivit(onrEtunimet, onrKutsumanimi, onrSukunimi)
    } else {
        null
    }

private fun FlowContent.riveina(rivit: List<String>) {
    rivit.forEach { div { +it } }
}

private fun FlowContent.lisatietoModaali(
    modalId: String,
    lisatiedot: String,
) {
    div {
        modalCommandButton(modalId, ModalCommand.OPEN, classes = "outline secondary tight") {
            +UiText.Koto.naytaLisatiedot
        }
    }
    modal(modalId, UiText.Koto.virheenLisatiedot.toString()) {
        json(lisatiedot.toJsonNode())
        footer {
            modalCommandButton(modalId, ModalCommand.CLOSE, classes = "secondary") {
                +UiText.Toiminto.sulje
            }
        }
    }
}
