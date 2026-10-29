package fi.oph.kitu.vkt.html
import arrow.core.Either
import fi.oph.kitu.html.card
import fi.oph.kitu.html.cardContent
import fi.oph.kitu.html.copyable
import fi.oph.kitu.html.errorMessage
import fi.oph.kitu.html.infoTable
import fi.oph.kitu.i18n.UiText
import fi.oph.kitu.i18n.finnishDate
import fi.oph.kitu.i18n.unaryPlus
import fi.oph.kitu.oppijanumero.OppijanumeroException
import fi.oph.kitu.oppijanumero.OppijanumerorekisteriHenkilo
import fi.oph.kitu.tiedontuontischema.VktHenkilosuoritus
import kotlinx.html.FlowContent

fun FlowContent.vktHenkilonTiedot(
    data: VktHenkilosuoritus,
    henkilo: Either<OppijanumeroException, OppijanumerorekisteriHenkilo>,
) {
    henkilo
        .onRight { hlo ->
            card(compact = true) {
                infoTable(
                    UiText.Vkt.henkilotunnus to {
                        hlo.hetut().forEachIndexed { index, hetu ->
                            if (index > 0) +", "
                            copyable(hetu)
                        }
                    },
                    UiText.Vkt.henkiloOid to { copyable(data.henkilo.oid.toString()) },
                    UiText.Vkt.syntymaaika to { hlo.syntymaaika?.finnishDate()?.let { +it } },
                    UiText.Vkt.yksilointi to {
                        if (hlo.yksiloityVTJ == true || hlo.yksiloity == true) {
                            +UiText.Vkt.yksiloity
                        } else if (hlo.yksilointiYritetty == true) {
                            +UiText.Vkt.yksilointiaYritetty
                        } else {
                            +UiText.Vkt.eiYksiloity
                        }
                    },
                )
            }
        }.onLeft {
            card(compact = true) {
                infoTable(
                    UiText.Vkt.Sarake.oppijanumero to { copyable(data.henkilo.oid.toString()) },
                )
                cardContent {
                    errorMessage(
                        if (it is OppijanumeroException.OppijaNotFoundException) {
                            UiText.Error.oppijaEiLoydyOnr
                        } else {
                            UiText.Error.oppijanHakuOnrEpaonnistui
                        },
                    )
                }
            }
        }
}
