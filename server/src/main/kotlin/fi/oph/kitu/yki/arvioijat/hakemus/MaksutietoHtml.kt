package fi.oph.kitu.yki.arvioijat.hakemus

import fi.oph.kitu.html.testId
import fi.oph.kitu.i18n.UiText
import fi.oph.kitu.i18n.finnishDate
import fi.oph.kitu.i18n.unaryPlus
import kotlinx.html.FlowContent
import kotlinx.html.span

fun FlowContent.maksunTila(maksu: Maksutieto) {
    span {
        testId("maksunTila")
        +maksu.tila.nimi
        when (maksu.tila) {
            Maksuntila.MAKSETTU -> {
                maksu.maksettu?.let { +" ${it.finnishDate()}" }
            }

            Maksuntila.AVOINNA, Maksuntila.ERAANTYNYT -> {
                maksu.erapaiva?.let { +", ${UiText.Yki.Arvioijahakemus.Maksu.erapaiva} ${it.finnishDate()}" }
            }

            Maksuntila.EI_LASKUA, Maksuntila.EI_TIEDOSSA -> {}
        }
    }
}
