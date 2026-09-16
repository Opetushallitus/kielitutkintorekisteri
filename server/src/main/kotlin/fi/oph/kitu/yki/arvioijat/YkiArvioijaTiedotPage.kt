package fi.oph.kitu.yki.arvioijat

import fi.oph.kitu.html.ModalCommand
import fi.oph.kitu.html.Page
import fi.oph.kitu.html.ViewMessageData
import fi.oph.kitu.html.buttonGroup
import fi.oph.kitu.html.buttonLink
import fi.oph.kitu.html.card
import fi.oph.kitu.html.cardContent
import fi.oph.kitu.html.errorMessage
import fi.oph.kitu.html.formPost
import fi.oph.kitu.html.infoTable
import fi.oph.kitu.html.modal
import fi.oph.kitu.html.modalCommandButton
import fi.oph.kitu.html.selitettyEsto
import fi.oph.kitu.html.table.DisplayTableColumn
import fi.oph.kitu.html.table.displayTable
import fi.oph.kitu.html.testId
import fi.oph.kitu.html.viewMessage
import fi.oph.kitu.html.warningMessage
import fi.oph.kitu.i18n.LocalizedString
import fi.oph.kitu.i18n.UiText
import fi.oph.kitu.i18n.finnishDate
import fi.oph.kitu.i18n.finnishDateTime
import fi.oph.kitu.i18n.unaryPlus
import fi.oph.kitu.security.Authority
import fi.oph.kitu.security.CurrentUser
import fi.oph.kitu.webmvc.Links
import kotlinx.html.ButtonType
import kotlinx.html.FlowContent
import kotlinx.html.a
import kotlinx.html.button
import kotlinx.html.details
import kotlinx.html.div
import kotlinx.html.footer
import kotlinx.html.h1
import kotlinx.html.h2
import kotlinx.html.p
import kotlinx.html.summary
import java.time.LocalDate

object YkiArvioijaTiedotPage {
    fun render(
        arvioija: YkiArvioijaEntity,
        kaudet: List<YkiArviointikausiEntity>,
        muutosloki: List<YkiArvioijaKausiEntity>,
        turvakielto: Turvakieltotieto,
        flash: ViewMessageData?,
        muokkausKaytossa: Boolean,
        integraatioKaytossa: Boolean,
        tanaan: LocalDate,
    ): String =
        Page.renderHtml {
            h1 { +"${arvioija.etunimet} ${arvioija.sukunimi}" }

            viewMessage(flash)

            turvakielto.varoitus?.let { warningMessage(it) }

            if (CurrentUser.hasAuthority(Authority.YKI_ARVIOIJAREKISTERI)) {
                buttonGroup {
                    buttonLink(
                        href = Links.Yki.muokkaaArvioijaa(arvioija.id!!.toInt()),
                        enabled = muokkausKaytossa,
                        testId = "muokkaaArvioijaa",
                        disabledTooltip = UiText.Yki.Arvioija.kirjoitusEiKaytossa,
                    ) {
                        +UiText.Yki.Arvioija.muokkaa
                    }

                    passivointiNappi(arvioija, muokkausKaytossa, tanaan)
                }

                passivointiDialogi(arvioija, muokkausKaytossa, tanaan)
            }

            card {
                cardContent {
                    h2 { +UiText.Yki.henkilotiedot }
                    infoTable(
                        UiText.Yki.Arvioija.oppijanumero to {
                            a(href = Links.Opintopolku.onr(arvioija.arvioijaOid)) {
                                +arvioija.arvioijaOid.toString()
                            }
                        },
                        UiText.Yki.Arvioija.sukunimi to { +arvioija.sukunimi },
                        UiText.Yki.Arvioija.etunimet to { +arvioija.etunimet },
                        UiText.Yki.Arvioija.sahkopostiosoite to { +arvioija.sahkopostiosoite.orEmpty() },
                        UiText.Yki.Arvioija.katuosoite to { +arvioija.katuosoite },
                        UiText.Yki.Arvioija.postinumero to { +arvioija.postinumero },
                        UiText.Yki.Arvioija.postitoimipaikka to { +arvioija.postitoimipaikka },
                    )
                }
            }

            card(overflowAuto = true) {
                cardContent {
                    h2 { +UiText.Yki.Arvioija.kausihistoria }

                    if (CurrentUser.hasAuthority(Authority.YKI_ARVIOIJAREKISTERI)) {
                        p {
                            buttonLink(
                                href = Links.Yki.uusiKausi(arvioija.id!!.toInt()),
                                enabled = muokkausKaytossa,
                                testId = "uusiKausi",
                                disabledTooltip = UiText.Yki.Arvioija.kirjoitusEiKaytossa,
                            ) {
                                +UiText.Yki.Arvioija.Kausi.uusi
                            }
                        }
                    }

                    if (kaudet.isEmpty()) {
                        p { +UiText.Yki.Arvioija.Kausi.eiKausia }
                    } else {
                        kaudetTaulukko(arvioija, kaudet, muokkausKaytossa, tanaan)
                    }

                    details {
                        summary {
                            testId("naytaMuutoshistoria")
                            +UiText.Yki.Arvioija.Kausi.naytaMuutoshistoria
                        }
                        if (muutosloki.isEmpty()) {
                            p { +UiText.Yki.Arvioija.eiMuutoshistoriaa }
                        } else {
                            kausihistoriaTaulukko(muutosloki, tanaan)
                        }
                    }
                }
            }

            // Natiivi dialog leikkautuu overflow-kontekstissa, joten ne renderoidaan kortin ulkopuolelle.
            if (CurrentUser.hasAuthority(Authority.YKI_ARVIOIJAREKISTERI) && muokkausKaytossa) {
                kaudet.forEach { kausi -> kausidialogit(arvioija, kausi, kaudet.size, tanaan) }
            }

            val vanhentuneet = arvioija.arviointioikeudet.filter { it.kieli.isLegacy() }
            if (vanhentuneet.isNotEmpty()) {
                card(overflowAuto = true) {
                    cardContent {
                        h2 { +UiText.Yki.Arvioija.Kausi.vanhentuneet }
                        p { +UiText.Yki.Arvioija.Kausi.vanhentuneetOhje }
                        vanhentuneetOikeudet(vanhentuneet)
                    }
                }
            }

            card {
                cardContent {
                    h2 { +UiText.Yki.integraatiot }
                    infoTable(
                        UiText.Yki.Arvioija.solkiinLahetetty to {
                            arvioija.solkiinLahetetty
                                ?.toInstant()
                                ?.let { finnishDateTime(it) }
                                ?: +UiText.Yki.Arvioija.lahetysjonossa
                        },
                        UiText.Yki.Arvioija.solkiTunnus to {
                            arvioija.solkiTunnus
                                ?.let { tunnus -> +tunnus }
                                ?: +UiText.Yki.Arvioija.solkiTunnusEiTiedossa
                        },
                        UiText.Yki.Arvioija.solkiLahetysyritykset to {
                            +arvioija.solkiLahetysyritykset.toString()
                        },
                    )
                    arvioija.solkiLahetysvirhe?.let { errorMessage(LocalizedString(fi = it)) }

                    if (!integraatioKaytossa) {
                        p { +UiText.Yki.Arvioija.automaattilahetysEiKaytossa }
                    }

                    if (CurrentUser.hasAuthority(Authority.YKI_ARVIOIJAREKISTERI)) {
                        // Vain muokkauskytkin: reitti on sen takana, joten ilman tata nappi
                        // nayttaisi aktiiviselta ja vastaisi 403. Integraatiokytkin ei esta
                        // kasin lahetysta, ks. SolkiArvioijaService.lahetaArvioijaKasin.
                        val estonSyy =
                            when {
                                !muokkausKaytossa -> UiText.Yki.Arvioija.kirjoitusEiKaytossa
                                else -> null
                            }

                        formPost(Links.Yki.lahetaArvioijaSolkiin(arvioija.id!!.toInt())) {
                            selitettyEsto(estonSyy) {
                                button(type = ButtonType.submit, classes = "secondary") {
                                    testId("lahetaArvioijaSolkiin")
                                    disabled = estonSyy != null
                                    +UiText.Yki.Arvioija.lahetaUudelleen
                                }
                            }
                        }
                    }
                }
            }

            p {
                a(href = Links.Yki.arvioijat()) { +UiText.Yki.Arvioija.takaisinListaan }
            }
        }
}

private fun FlowContent.passivointiNappi(
    arvioija: YkiArvioijaEntity,
    muokkausKaytossa: Boolean,
    tanaan: LocalDate,
) {
    val estonSyy = passivoinninEstonSyy(arvioija, muokkausKaytossa, tanaan)

    if (estonSyy != null) {
        buttonLink(
            href = Links.Yki.passivoiArvioija(arvioija.id!!.toInt()),
            enabled = false,
            testId = "passivoiArvioija",
            disabledTooltip = estonSyy,
        ) {
            +UiText.Yki.Arvioija.passivoi
        }
        return
    }

    modalCommandButton(PASSIVOINTI_MODAL, ModalCommand.OPEN, classes = "secondary") {
        testId("passivoiArvioija")
        +UiText.Yki.Arvioija.passivoi
    }
}

/** Natiivi dialog renderoidaan nappiryhman ulkopuolelle, jottei se kuulu ryhman lapsiin. */
private fun FlowContent.passivointiDialogi(
    arvioija: YkiArvioijaEntity,
    muokkausKaytossa: Boolean,
    tanaan: LocalDate,
) {
    if (passivoinninEstonSyy(arvioija, muokkausKaytossa, tanaan) != null) return

    vahvistusdialogi(
        PASSIVOINTI_MODAL,
        UiText.Yki.Arvioija.passivoi,
        UiText.Yki.Arvioija.passivoiVahvistus,
        Links.Yki.passivoiArvioija(arvioija.id!!.toInt()),
        "vahvistaPassivointi",
    )
}

/**
 * Nappi renderoidaan aina, jotta sivulta nakee etta toiminto on olemassa; esto perustellaan
 * tooltipilla. Jo passiivista merkintaa ei saa passivoida uudelleen: passivointihetki on
 * sailytysajan alkuhetki, joten klikkaus siirtaisi paattyneen merkinnan sailytysaikaa eteenpain.
 */
private fun passivoinninEstonSyy(
    arvioija: YkiArvioijaEntity,
    muokkausKaytossa: Boolean,
    tanaan: LocalDate,
): LocalizedString? {
    val merkintaOnPassiivinen =
        arvioija.arviointioikeudet.isNotEmpty() &&
            arvioija.arviointioikeudet.all {
                Rekisterointitila.laske(it, tanaan) == Rekisterointitila.PASSIVOITU
            }

    return when {
        !muokkausKaytossa -> {
            UiText.Yki.Arvioija.kirjoitusEiKaytossa
        }

        arvioija.passivoitu != null -> {
            UiText.Yki.Arvioija.joPassivoitu
                .interpolate("pvm" to arvioija.passivoitu.toLocalDate().finnishDate())
        }

        merkintaOnPassiivinen -> {
            UiText.Yki.Arvioija.kausiPaattynyt
        }

        else -> {
            null
        }
    }
}

private const val PASSIVOINTI_MODAL = "passivoiArvioijaDialog"

private fun FlowContent.kausihistoriaTaulukko(
    kausihistoria: List<YkiArvioijaKausiEntity>,
    tanaan: LocalDate,
) {
    displayTable(
        kausihistoria,
        listOf(
            DisplayTableColumn(
                UiText.Yki.Arvioija.Kausi.toimenpide
                    .toString(),
                testId = "kausiToimenpide",
            ) { kausi ->
                kausi.toimenpide?.let { +it.nimi }
            },
            DisplayTableColumn(
                UiText.Yki.Arvioija.tutkintokieli
                    .toString(),
                testId = "kausiKieli",
            ) {
                +it.kieli.nimi
            },
            DisplayTableColumn(
                UiText.Yki.Sarake.tasot
                    .toString(),
                testId = "kausiTasot",
            ) { kausi ->
                +kausi.tasot.sorted().joinToString(", ") { it.nimi.toString() }
            },
            DisplayTableColumn(
                UiText.Yki.Sarake.tila
                    .toString(),
                testId = "kausiTila",
            ) { +Rekisterointitila.laske(it, tanaan).nimi },
            DisplayTableColumn(
                UiText.Yki.Arvioija.kaudenAlkupaiva
                    .toString(),
                testId = "kausiAlkupaiva",
            ) { kausi ->
                kausi.kaudenAlkupaiva?.let { finnishDate(it) }
            },
            DisplayTableColumn(
                UiText.Yki.Arvioija.kaudenPaattymispaiva
                    .toString(),
                testId = "kausiPaattymispaiva",
            ) { kausi ->
                kausi.kaudenPaattymispaiva?.let { finnishDate(it) }
            },
            DisplayTableColumn(
                UiText.Yki.Arvioija.jatkorekisterointi
                    .toString(),
                testId = "kausiJatkorekisterointi",
            ) { kausi ->
                +(if (kausi.jatkorekisterointi) UiText.Filter.kylla else UiText.Filter.ei)
            },
            DisplayTableColumn(
                UiText.Yki.Sarake.ashaNumero
                    .toString(),
                testId = "kausiMuutosAsha",
            ) { kausi ->
                +kausi.ashaNumero.orEmpty()
            },
            DisplayTableColumn(
                UiText.Yki.Arvioija.kirjattu
                    .toString(),
                testId = "kausiKirjattu",
            ) { kausi ->
                kausi.kirjattu?.toInstant()?.let { finnishDateTime(it) }
            },
            DisplayTableColumn(
                UiText.Yki.Arvioija.kirjaaja
                    .toString(),
                testId = "kausiKirjaaja",
            ) { kausi ->
                kausi.kirjaajaOid
                    ?.let { oid -> a(href = Links.Opintopolku.onr(oid)) { +oid.toString() } }
                    ?: +UiText.Yki.Arvioija.jarjestelma
            },
        ),
        testId = "kausihistoria",
    )
}

private fun FlowContent.kaudetTaulukko(
    arvioija: YkiArvioijaEntity,
    kaudet: List<YkiArviointikausiEntity>,
    muokkausKaytossa: Boolean,
    tanaan: LocalDate,
) {
    val arvioijaId = arvioija.id!!.toInt()

    displayTable(
        kaudet,
        listOfNotNull(
            DisplayTableColumn(
                UiText.Yki.Sarake.tila
                    .toString(),
                testId = "kausiTila",
            ) { kausi ->
                +Rekisterointitila.laske(kausi, tanaan).nimi
            },
            DisplayTableColumn(
                UiText.Yki.Arvioija.kaudenAlkupaiva
                    .toString(),
                testId = "kausiAlkupaiva",
            ) { kausi ->
                finnishDate(kausi.alkupaiva)
            },
            DisplayTableColumn(
                UiText.Yki.Arvioija.kaudenPaattymispaiva
                    .toString(),
                testId = "kausiPaattymispaiva",
            ) { kausi ->
                kausi.paattymispaiva?.let { finnishDate(it) }
            },
            DisplayTableColumn(
                UiText.Yki.Arvioija.arviointioikeudet
                    .toString(),
                testId = "kausiOikeudet",
            ) { kausi ->
                kausi.oikeudet.sortedBy { it.kieli.name }.forEach { oikeus ->
                    div {
                        +"${oikeus.kieli.nimi}: ${oikeus.tasot.sorted().joinToString(", ") { it.nimi.toString() }}"
                    }
                }
            },
            DisplayTableColumn(
                UiText.Yki.Sarake.ashaNumero
                    .toString(),
                testId = "kausiAsha",
            ) { kausi ->
                +kausi.ashaNumero.orEmpty()
            },
            if (!CurrentUser.hasAuthority(Authority.YKI_ARVIOIJAREKISTERI)) {
                null
            } else {
                DisplayTableColumn(
                    UiText.Yki.Arvioija.Kausi.toiminnot
                        .toString(),
                    testId = "kausiToiminnot",
                ) { kausi ->
                    kausitoiminnot(arvioijaId, kausi, kaudet.size, muokkausKaytossa, tanaan)
                }
            },
        ),
        testId = "arviointikaudet",
        rowTestId = { "kausi-${it.id}" },
    )
}

private fun FlowContent.kausitoiminnot(
    arvioijaId: Int,
    kausi: YkiArviointikausiEntity,
    kausiaYhteensa: Int,
    muokkausKaytossa: Boolean,
    tanaan: LocalDate,
) {
    val kausiId = kausi.id!!.toInt()

    buttonGroup {
        buttonLink(
            href = Links.Yki.muokkaaKautta(arvioijaId, kausiId),
            enabled = muokkausKaytossa,
            testId = "muokkaaKautta",
            disabledTooltip = UiText.Yki.Arvioija.kirjoitusEiKaytossa,
        ) {
            +UiText.Yki.Arvioija.Kausi.muokkaa
        }

        if (Rekisterointitila.laske(kausi, tanaan) == Rekisterointitila.AKTIIVINEN) {
            kausikomento(
                kaudenPassivointiDialogi(kausiId),
                UiText.Yki.Arvioija.Kausi.passivoi,
                "passivoiKausi",
                muokkausKaytossa,
            )
        }

        // Viimeinen kausi jattaisi arvioijan ilman arviointioikeuksia, jolloin han katoaisi listalta.
        if (kausiaYhteensa > 1) {
            kausikomento(poistoDialogi(kausiId), UiText.Yki.Arvioija.Kausi.poista, "poistaKausi", muokkausKaytossa)
        }
    }
}

private fun FlowContent.kausikomento(
    dialogId: String,
    teksti: LocalizedString,
    testId: String,
    muokkausKaytossa: Boolean,
) {
    val estonSyy =
        UiText.Yki.Arvioija.kirjoitusEiKaytossa
            .takeIf { !muokkausKaytossa }

    selitettyEsto(estonSyy) {
        modalCommandButton(dialogId, ModalCommand.OPEN, classes = "secondary") {
            testId(testId)
            disabled = !muokkausKaytossa
            +teksti
        }
    }
}

private fun FlowContent.kausidialogit(
    arvioija: YkiArvioijaEntity,
    kausi: YkiArviointikausiEntity,
    kausiaYhteensa: Int,
    tanaan: LocalDate,
) {
    val arvioijaId = arvioija.id!!.toInt()
    val kausiId = kausi.id!!.toInt()

    if (Rekisterointitila.laske(kausi, tanaan) == Rekisterointitila.AKTIIVINEN) {
        vahvistusdialogi(
            kaudenPassivointiDialogi(kausiId),
            UiText.Yki.Arvioija.Kausi.passivoi,
            UiText.Yki.Arvioija.Kausi.passivoiVahvistus,
            Links.Yki.passivoiKausi(arvioijaId, kausiId),
            "vahvistaKaudenPassivointi",
        )
    }

    if (kausiaYhteensa > 1) {
        vahvistusdialogi(
            poistoDialogi(kausiId),
            UiText.Yki.Arvioija.Kausi.poista,
            UiText.Yki.Arvioija.Kausi.poistaVahvistus,
            Links.Yki.poistaKausi(arvioijaId, kausiId),
            "vahvistaKaudenPoisto",
        )
    }
}

private fun FlowContent.vahvistusdialogi(
    dialogId: String,
    otsikko: LocalizedString,
    vahvistus: LocalizedString,
    action: String,
    vahvistusTestId: String,
) {
    modal(dialogId, otsikko.toString()) {
        p { +vahvistus }
        formPost(action) {
            footer {
                buttonGroup {
                    button(type = ButtonType.submit) {
                        testId(vahvistusTestId)
                        +otsikko
                    }
                    modalCommandButton(dialogId, ModalCommand.CLOSE, classes = "secondary") {
                        +UiText.Yki.Arvioija.peruuta
                    }
                }
            }
        }
    }
}

private fun kaudenPassivointiDialogi(kausiId: Int): String = "passivoiKausiDialog-$kausiId"

private fun poistoDialogi(kausiId: Int): String = "poistaKausiDialog-$kausiId"

/** Vanhentuneet kielet eivat kuulu kausiin, joten ne naytetaan omanaan vain luettavina. */
private fun FlowContent.vanhentuneetOikeudet(oikeudet: List<YkiArviointioikeusEntity>) {
    displayTable(
        oikeudet,
        listOf(
            DisplayTableColumn(
                UiText.Yki.Arvioija.tutkintokieli
                    .toString(),
                testId = "vanhentunutKieli",
            ) {
                +it.kieli.nimi
            },
            DisplayTableColumn(
                UiText.Yki.Sarake.tasot
                    .toString(),
                testId = "vanhentunutTasot",
            ) { oikeus ->
                +oikeus.tasot.sorted().joinToString(", ") { it.nimi.toString() }
            },
            DisplayTableColumn(
                UiText.Yki.Arvioija.kaudenAlkupaiva
                    .toString(),
                testId = "vanhentunutAlkupaiva",
            ) { oikeus ->
                oikeus.kaudenAlkupaiva?.let { finnishDate(it) }
            },
            DisplayTableColumn(
                UiText.Yki.Arvioija.kaudenPaattymispaiva
                    .toString(),
                testId = "vanhentunutPaattymispaiva",
            ) { oikeus ->
                oikeus.kaudenPaattymispaiva?.let { finnishDate(it) }
            },
        ),
        testId = "vanhentuneetOikeudet",
    )
}
