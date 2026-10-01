@file:Suppress("ktlint:standard:no-wildcard-imports")

package fi.oph.kitu.yki.suoritukset
import arrow.core.Either
import fi.oph.kitu.html.Comparison
import fi.oph.kitu.html.Page
import fi.oph.kitu.html.card
import fi.oph.kitu.html.comparisonTable
import fi.oph.kitu.html.errorMessage
import fi.oph.kitu.html.infoTable
import fi.oph.kitu.html.json
import fi.oph.kitu.html.orDash
import fi.oph.kitu.html.warningMessage
import fi.oph.kitu.i18n.LocalizedString
import fi.oph.kitu.i18n.Translations
import fi.oph.kitu.i18n.UiText
import fi.oph.kitu.i18n.finnishDate
import fi.oph.kitu.i18n.finnishDateTime
import fi.oph.kitu.i18n.unaryPlus
import fi.oph.kitu.koski.KoskiErrorEntity
import fi.oph.kitu.oid.Oid
import fi.oph.kitu.oppijanumero.OppijanumeroException
import fi.oph.kitu.oppijanumero.OppijanumerorekisteriHenkilo
import fi.oph.kitu.tiedontuontischema.YkiTarkastusarviointi
import fi.oph.kitu.webmvc.Links
import fi.oph.kitu.yki.TutkinnonOsa
import kotlinx.html.FlowContent
import kotlinx.html.a
import kotlinx.html.h1
import kotlinx.html.h2
import kotlinx.html.h3
import kotlinx.html.span
import kotlinx.html.strong

object YkiSuoritusPage {
    fun render(
        henkilo: Either<OppijanumeroException, OppijanumerorekisteriHenkilo>?,
        suoritus: YkiSuoritusEntity,
        viimeisinSuoritus: YkiSuoritusEntity,
        koskiError: KoskiErrorEntity?,
        koskiSiirronEstonSyyt: List<String>?,
        opiskeluoikeusOid: Oid?,
        t: Translations,
    ) = Page.renderHtml {
        h1 { +suoritus.kokoNimi() }
        h2 { +UiText.Nav.yki }

        if (suoritus.id != viimeisinSuoritus.id) {
            warningMessage(
                LocalizedString(
                    fi = "Tämä on suorituksen vanhempi versio (${suoritus.lastModified.finnishDateTime()}). ",
                ),
            ) {
                a(href = Links.Yki.suoritus(viimeisinSuoritus.id!!)) {
                    +UiText.Yki.naytaUusinVersio
                    +" ("
                    finnishDateTime(viimeisinSuoritus.lastModified)
                    +")"
                }
            }
        }

        henkilonTiedot(henkilo, suoritus, t)
        todistuksenPostitusosoite(suoritus)
        tutkintotiedot(suoritus)
        arviointi(suoritus)
        integraatiot(suoritus, koskiError, koskiSiirronEstonSyyt, opiskeluoikeusOid)
    }

    fun FlowContent.henkilonTiedot(
        henkilo: Either<OppijanumeroException, OppijanumerorekisteriHenkilo>?,
        suoritus: YkiSuoritusEntity,
        t: Translations,
    ) {
        h3 { +UiText.Yki.henkilotiedot }
        henkilo?.onLeft { onrException ->
            errorMessage(
                if (onrException is OppijanumeroException.OppijaNotFoundException) {
                    UiText.Error.oppijaEiLoydyOnr
                } else {
                    UiText.Error.oppijanHakuOnrEpaonnistui
                },
            )
        }
        card(compact = true) {
            val hlo = henkilo?.getOrNull()
            comparisonTable(
                UiText.Yki.ilmoittautumisenTiedot,
                UiText.Yki.oppijanumerorekisteri,
                oppijanumeroVertailu(suoritus.suorittajanOID, hlo),
                Comparison.of(
                    UiText.Yki.Sarake.sukunimi,
                    suoritus.sukunimi,
                    hlo?.sukunimi,
                ),
                Comparison.of(
                    UiText.Yki.Sarake.etunimet,
                    suoritus.etunimet,
                    hlo?.etunimet,
                ),
                Comparison.of(
                    UiText.Yki.Sarake.henkilotunnus,
                    suoritus.hetu,
                    hlo?.hetu,
                    ignoreDiff = true,
                ),
                Comparison.of(
                    UiText.Yki.Sarake.sukupuoli,
                    suoritus.sukupuoli.text.toString(),
                    hlo?.sukupuoli?.let {
                        when (it) {
                            "1" -> UiText.Sukupuoli.mies.toString()
                            "2" -> UiText.Sukupuoli.nainen.toString()
                            else -> it
                        }
                    },
                ),
                Comparison.of(
                    UiText.Yki.Sarake.kansalaisuus,
                    t.getByKoodiviite("maatjavaltiot1", suoritus.kansalaisuus),
                    hlo
                        ?.kansalaisuus
                        ?.mapNotNull { it.kansalaisuusKoodi }
                        ?.joinToString(", ") { t.getByKoodiviite("maatjavaltiot2", it) },
                ),
            )
        }
    }

    fun FlowContent.todistuksenPostitusosoite(suoritus: YkiSuoritusEntity) {
        h3 { +UiText.Yki.todistuksenPostitusosoite }
        card(compact = true) {
            infoTable(
                UiText.Yki.katuosoite to { +suoritus.katuosoite },
                UiText.Yki.postinumero to { +suoritus.postinumero },
                UiText.Yki.postitoimipaikka to { +suoritus.postitoimipaikka },
                UiText.Yki.maa to { +suoritus.maa.orDash() },
                UiText.Yki.Sarake.sahkoposti to { +suoritus.email.orDash() },
                UiText.Yki.todistuksenKieli to { +suoritus.todistuskieli?.toString().orDash() },
            )
        }
    }

    fun FlowContent.tutkintotiedot(suoritus: YkiSuoritusEntity) {
        h3 { +UiText.Yki.tutkinnonTiedot }
        card(compact = true) {
            infoTable(
                UiText.Yki.jarjestaja to
                    { +"${suoritus.jarjestajanNimi} (${suoritus.jarjestajanTunnusOid})" },
                UiText.Yki.Sarake.tutkintopaiva to { +suoritus.tutkintopaiva.finnishDate() },
                UiText.Yki.Sarake.tutkintokieli to { +suoritus.tutkintokieli.toString() },
                UiText.Yki.Sarake.tutkintotaso to { +suoritus.tutkintotaso.toString() },
            )
        }
    }

    fun FlowContent.arviointi(suoritus: YkiSuoritusEntity) {
        val tarkistusarviointi = YkiTarkastusarviointi.from(suoritus)

        h3 { +UiText.Yki.arviointi }
        card(compact = true) {
            infoTable(
                UiText.Yki.arvioinninTila to { +suoritus.arviointitila.viewText },
                UiText.Yki.Sarake.arviointipaiva to { +suoritus.arviointipaiva?.finnishDate().orDash() },
                tarkistusarviointi?.let {
                    UiText.Yki.tarkistusarvioinninSaapumispaiva to
                        { +it.saapumispaiva.finnishDate() }
                },
                tarkistusarviointi?.let {
                    UiText.Yki.tarkistusarvioinninKasittelypaiva to
                        { +it.kasittelypaiva?.finnishDate().orDash() }
                },
                tarkistusarviointi?.let { UiText.Yki.tarkistusarvioinninAsiatunnus to { +it.asiatunnus } },
                tarkistusarviointi?.let { arviointi ->
                    UiText.Yki.tarkistusarvioidutOsakokeet to
                        { +arviointi.tarkistusarvioidutOsakokeet?.joinToString(", ") { it.viewText }.orDash() }
                },
                tarkistusarviointi?.let { arviointi ->
                    UiText.Yki.arvosanaMuuttui to
                        { +arviointi.arvosanaMuuttui?.joinToString(", ") { it.viewText }.orDash() }
                },
                tarkistusarviointi?.let { UiText.Yki.perustelu to { +it.perustelu } },
            )
        }

        card(compact = true) {
            val osakokeet = suoritus.osakokeet().associateBy { it.tyyppi }

            fun osakoeRivi(tyyppi: TutkinnonOsa): Pair<String, FlowContent.() -> Unit>? =
                osakokeet[tyyppi]?.let { osakoe ->
                    tyyppi.viewText to {
                        val arvosana = osakoe.arvosana
                        if (arvosana != null) {
                            ykiArvosana(arvosana, suoritus.tutkintotaso)
                        } else {
                            +"–"
                        }
                    }
                }

            infoTable(
                osakoeRivi(TutkinnonOsa.TY),
                osakoeRivi(TutkinnonOsa.KI),
                osakoeRivi(TutkinnonOsa.PY),
                osakoeRivi(TutkinnonOsa.PU),
                osakoeRivi(TutkinnonOsa.RS),
                osakoeRivi(TutkinnonOsa.YL),
            )
        }
    }

    fun FlowContent.integraatiot(
        suoritus: YkiSuoritusEntity,
        koskiError: KoskiErrorEntity?,
        koskiSiirronEstonSyyt: List<String>?,
        opiskeluoikeusOid: Oid?,
    ) {
        h3 { +UiText.Yki.integraatiot }
        infoTable(
            UiText.Yki.Sarake.solkiTunniste to { +"${suoritus.solkiId}" },
            UiText.Yki.viimeksiMuokattu to {
                finnishDateTime(suoritus.lastModified)
            },
            UiText.Yki.Sarake.rekisteriintuontiaika to {
                finnishDateTime(suoritus.receivedAt)
            },
            UiText.Yki.koski to {
                if (koskiSiirronEstonSyyt?.isNotEmpty() == true) {
                    +("${UiText.Yki.siirtoaEiTehda}: ${koskiSiirronEstonSyyt.joinToString("; ")}")
                } else if (suoritus.koskiSiirtoKasitelty == true) {
                    +UiText.Yki.siirrettyKoski
                } else {
                    +UiText.Yki.odottaaSiirtoa
                }
            },
            koskiError?.errorJson()?.let {
                UiText.Yki.koskiVirheet to { json(it) }
            },
            opiskeluoikeusOid?.let { oid ->
                UiText.Yki.Sarake.opiskeluoikeusOid to {
                    +UiText.Yki.opiskeluoikeudenOid
                    +": "
                    a(href = "/koski/oppija/$oid?opiskeluoikeudenTyyppi=kielitutkinto") {
                        +oid.toString()
                    }
                }
            },
            UiText.Yki.kios to {
                (
                    suoritus.arviointitilaLahetetty?.let {
                        +UiText.Yki.arviointitilaLahetetty
                        +" "
                        finnishDateTime(it.toInstant())
                    } ?: +(
                        if (suoritus.arviointitila.pelkkäIlmoittautuminen()) {
                            UiText.Yki.suoritustaEdeltavaEiLaheteta.toString()
                        } else {
                            UiText.Yki.arviointitilaaEiLahetetty.toString()
                        }
                    )
                )
            },
            suoritus.arviointitilanLahetysvirhe?.let {
                UiText.Yki.kiosVirhe to { +it }
            },
        )
    }

    private fun oppijanumeroVertailu(
        suorittajanOID: Oid?,
        hlo: OppijanumerorekisteriHenkilo?,
    ): Comparison =
        when {
            suorittajanOID == null -> {
                Comparison(
                    UiText.Yki.Sarake.oppijanumero,
                    { +UiText.Yki.eiOppijanumeroa },
                    ignoreDiff = true,
                )
            }

            hlo?.oppijanumero != null -> {
                Comparison(
                    UiText.Yki.Sarake.oppijanumero,
                    { +hlo.oppijanumero },
                    { +hlo.oppijanumero },
                )
            }

            else -> {
                Comparison(UiText.Yki.henkiloOid, {
                    +suorittajanOID.toString()
                }, {
                    a(
                        href = Links.Opintopolku.onr(suorittajanOID),
                        classes = "tight secondary",
                    ) {
                        attributes["role"] = "button"
                        +UiText.Yki.teeYksilointi
                    }
                })
            }
        }
}
