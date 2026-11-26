@file:Suppress("ktlint:standard:no-wildcard-imports")

package fi.oph.kitu.yki.suoritukset
import arrow.core.Either
import fi.oph.kitu.html.Comparison
import fi.oph.kitu.html.Page
import fi.oph.kitu.html.SVG_ATTRIBUTES
import fi.oph.kitu.html.card
import fi.oph.kitu.html.comparisonTable
import fi.oph.kitu.html.copyable
import fi.oph.kitu.html.errorMessage
import fi.oph.kitu.html.infoTable
import fi.oph.kitu.html.json
import fi.oph.kitu.html.orDash
import fi.oph.kitu.html.testId
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
import kotlinx.html.DIV
import kotlinx.html.FlowContent
import kotlinx.html.a
import kotlinx.html.div
import kotlinx.html.figure
import kotlinx.html.h1
import kotlinx.html.h2
import kotlinx.html.h3
import kotlinx.html.p
import kotlinx.html.strong
import kotlinx.html.unsafe

object YkiSuoritusPage {
    fun render(
        henkilo: Either<OppijanumeroException, OppijanumerorekisteriHenkilo>?,
        suoritus: YkiSuoritusEntity,
        viimeisinSuoritus: YkiSuoritusEntity,
        koskiError: KoskiErrorEntity?,
        koskiSiirronEstonSyyt: List<String>?,
        opiskeluoikeusOid: Oid?,
        t: Translations,
    ) = Page.renderHtml(ohje = UiText.Ohje.Yki.suoritus) {
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
        integraatiot(
            suoritus,
            YkiIntegraatiotilat.laske(suoritus, viimeisinSuoritus, koskiError, koskiSiirronEstonSyyt),
            koskiError,
            opiskeluoikeusOid,
        )
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
                Comparison.ofCopyable(
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
                UiText.Yki.jarjestaja to {
                    +"${suoritus.jarjestajanNimi} "
                    copyable(suoritus.jarjestajanTunnusOid.toString()) { +"(${suoritus.jarjestajanTunnusOid})" }
                },
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
                tarkistusarviointi?.let {
                    UiText.Yki.tarkistusarvioinninHyvaksymispaiva to
                        { +suoritus.tarkistusarvioinninHyvaksymispaivaTekstina().ifEmpty { null }.orDash() }
                },
                tarkistusarviointi?.let { UiText.Yki.tarkistusarvioinninAsiatunnus to { copyable(it.asiatunnus) } },
                tarkistusarviointi?.let { arviointi ->
                    UiText.Yki.tarkistusarvioidutOsakokeet to
                        { +arviointi.tarkistusarvioidutOsakokeet?.joinToString(", ") { it.viewText }.orDash() }
                },
                tarkistusarviointi?.let { arviointi ->
                    UiText.Yki.arvosanaMuuttui to
                        { +arviointi.arvosanaMuuttui?.joinToString(", ") { it.viewText }.orDash() }
                },
                tarkistusarviointi?.let { UiText.Yki.perustelu to { div(classes = "pitka-teksti") { +it.perustelu } } },
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
        tilat: YkiIntegraatiotilat,
        koskiError: KoskiErrorEntity?,
        opiskeluoikeusOid: Oid?,
    ) {
        h3 { +UiText.Yki.integraatiot }
        figure(classes = "integraatiokaavio") {
            testId("integraatiokaavio")
            attributes["aria-label"] =
                UiText.Yki.Integraatio.kaavionKuvaus
                    .toString()
            jarjestelma("solki", "ulkoinen", UiText.Yki.Integraatio.solki) {
                copyable(suoritus.solkiId.toString())
            }
            integraatioNuoli("solki", tilat.solki)
            jarjestelma("kitu", "kitu", UiText.Yki.Integraatio.kielitutkintorekisteri) {
                div {
                    testId("viimeksi-muokattu")
                    +UiText.Yki.viimeksiMuokattu
                    +" "
                    finnishDateTime(suoritus.lastModified)
                }
            }
            integraatioNuoli("koski", tilat.koski)
            jarjestelma("koski", "oph", UiText.Yki.koski) {
                if (opiskeluoikeusOid != null) {
                    div {
                        testId("opiskeluoikeus-oid")
                        +UiText.Yki.opiskeluoikeudenOid
                        +": "
                        copyable(opiskeluoikeusOid.toString()) {
                            a(href = "/koski/oppija/$opiskeluoikeusOid?opiskeluoikeudenTyyppi=kielitutkinto") {
                                +opiskeluoikeusOid.toString()
                            }
                        }
                    }
                } else {
                    div {
                        testId("opiskeluoikeus-oid")
                        +UiText.Yki.Integraatio.eiOpiskeluoikeutta
                    }
                }
            }
            integraatioNuoli("kios", tilat.kios)
            jarjestelma("kios", "kios", UiText.Yki.kios) {}
            integraatiovirheet(suoritus, koskiError)
        }
    }

    private fun FlowContent.jarjestelma(
        id: String,
        tyyppi: String,
        nimi: LocalizedString,
        sisalto: DIV.() -> Unit,
    ) {
        div(classes = "jarjestelma $tyyppi alue-$id") {
            testId("jarjestelma-$id")
            strong { +nimi }
            sisalto()
        }
    }

    private fun FlowContent.integraatioNuoli(
        id: String,
        integraatio: Integraatio,
    ) {
        div(classes = "integraatio alue-nuoli-$id") {
            testId("integraatio-$id")
            attributes["data-tila"] = integraatio.tila.name
            div(classes = "integraation-teksti") {
                div(classes = "integraation-otsikko") {
                    unsafe { raw(tilanIkoni(integraatio.tila)) }
                    strong { +integraatio.otsikko }
                }
                integraatio.aikaleima?.let { aikaleima ->
                    div {
                        testId("aikaleima")
                        aikaleima.nimi?.let {
                            +it
                            +" "
                        }
                        aikaleima.aika?.let { finnishDateTime(it) } ?: +UiText.Yki.Integraatio.aikaEiTiedossa
                    }
                }
                integraatio.selitteet.forEach { selite ->
                    div(classes = "integraation-selite") {
                        testId("selite")
                        +selite
                    }
                }
            }
            div(classes = "integraation-viiva") { attributes["aria-hidden"] = "true" }
        }
    }

    private fun FlowContent.integraatiovirheet(
        suoritus: YkiSuoritusEntity,
        koskiError: KoskiErrorEntity?,
    ) {
        if (koskiError == null && suoritus.arviointitilanLahetysvirhe == null) return
        div(classes = "integraatiovirheet") {
            koskiError?.let { virhe ->
                div {
                    testId("koski-virheet")
                    strong { +UiText.Yki.koskiVirheet }
                    virhe.errorJson()?.let { json(it) } ?: p { +virhe.message }
                }
            }
            suoritus.arviointitilanLahetysvirhe?.let { virhe ->
                div {
                    testId("kios-virhe")
                    strong { +UiText.Yki.kiosVirhe }
                    p { +virhe }
                }
            }
        }
    }

    private fun tilanIkoni(tila: IntegraationTila): String {
        val polku =
            when (tila) {
                IntegraationTila.ONNISTUI -> """<path d="M5 12l5 5L20 7"/>"""
                IntegraationTila.EPAONNISTUI -> """<path d="M6 6l12 12M18 6L6 18"/>"""
                IntegraationTila.ODOTTAA -> """<circle cx="12" cy="12" r="9"/><path d="M12 7v5l3 2"/>"""
                IntegraationTila.EI_SIIRRETA -> """<path d="M6 12h12"/>"""
            }
        return """<svg class="tilan-ikoni" $SVG_ATTRIBUTES>$polku</svg>"""
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
                    { copyable(hlo.oppijanumero) },
                    { copyable(hlo.oppijanumero) },
                )
            }

            else -> {
                Comparison(UiText.Yki.henkiloOid, {
                    copyable(suorittajanOID.toString())
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
