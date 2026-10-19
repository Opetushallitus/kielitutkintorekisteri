package fi.oph.kitu.vkt.html

import fi.oph.kitu.html.listSearchForm
import fi.oph.kitu.i18n.UiText
import fi.oph.kitu.vkt.VktSuoritusFilter
import kotlinx.html.FlowContent

fun FlowContent.vktSearch(filter: VktSuoritusFilter) =
    listSearchForm(
        value = filter.search,
        placeholder = UiText.Vkt.oppijanumeroTaiNimi,
        buttonText = UiText.Vkt.suodata,
        params = filter.toMap(),
    )
