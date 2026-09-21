package fi.oph.kitu.i18n

object UiText {
    val appTitle: LocalizedString get() = tr("appTitle", fi = "Kielitutkintorekisteri")

    object Nav {
        val yki: LocalizedString get() = tr("nav.yki", fi = "Yleinen kielitutkinto")
        val kotoutumiskoulutuksenPaattotesti: LocalizedString
            get() = tr("nav.kotoutumiskoulutuksenPaattotesti", fi = "Kotoutumiskoulutuksen kielitaidon päättötesti")
        val vkt: LocalizedString get() = tr("nav.vkt", fi = "Valtionhallinnon kielitutkinto")
        val yllapito: LocalizedString get() = tr("nav.yllapito", fi = "Ylläpito")

        val suoritukset: LocalizedString get() = tr("nav.suoritukset", fi = "Suoritukset")
        val arvioijat: LocalizedString get() = tr("nav.arvioijat", fi = "Arvioijat")
        val tarkistusarvioinnit: LocalizedString get() = tr("nav.tarkistusarvioinnit", fi = "Tarkistusarvioinnit")
        val tehtavapaketit: LocalizedString get() = tr("nav.tehtavapaketit", fi = "Tehtäväpaketit")
        val kaikkiSuoritukset: LocalizedString get() = tr("nav.kaikkiSuoritukset", fi = "Kaikki suoritukset")
        val erinomaisenTaidonIlmoittautuneet: LocalizedString
            get() = tr("nav.erinomaisenTaidonIlmoittautuneet", fi = "Erinomaisen taidon ilmoittautuneet")
        val erinomaisenTaidonSuoritukset: LocalizedString
            get() = tr("nav.erinomaisenTaidonSuoritukset", fi = "Erinomaisen taidon suoritukset")
        val hyvanJaTyydyttavanSuoritukset: LocalizedString
            get() = tr("nav.hyvanJaTyydyttavanSuoritukset", fi = "Hyvän ja tyydyttävän taidon suoritukset")
        val erajojenHallinta: LocalizedString get() = tr("nav.erajojenHallinta", fi = "Eräajojen hallinta")
    }

    object Error {
        val internalServerError: LocalizedString get() = tr("error.internalServerError", fi = "Sisäinen palvelinvirhe")
        val sivuaEiLoydy: LocalizedString get() = tr("error.sivuaEiLoydy", fi = "Sivua ei löydy")
        val virheellinenPyynto: LocalizedString get() = tr("error.virheellinenPyynto", fi = "Virheellinen pyyntö")
        val virheellinenPyyntoOhje: LocalizedString
            get() =
                tr(
                    "error.virheellinenPyyntoOhje",
                    fi = "Tarkista että esimerkiksi sivun osoitteen kaikki parametrit on kirjoitettu oikein.",
                )
        val eiKayttooikeuksia: LocalizedString get() =
            tr(
                "error.eiKayttooikeuksia",
                fi = "Ei tarvittavia käyttöoikeuksia",
            )
        val katsoVirheet: LocalizedString get() = tr("error.katsoVirheet", fi = "Katso virheet")
        val jaljitystunniste: LocalizedString get() = tr("error.jaljitystunniste", fi = "Jäljitystunniste")
        val oppijaEiLoydyOnr: LocalizedString
            get() = tr("error.oppijaEiLoydyOnr", fi = "Oppijasta ei löydy tietoja Oppijanumerorekisteristä")
        val oppijanHakuOnrEpaonnistui: LocalizedString
            get() =
                tr("error.oppijanHakuOnrEpaonnistui", fi = "Oppijan tietojen haku Oppijanumerorekisteristä epäonnistui")

        fun jarjestelmassaVirheita(count: Long) =
            tr("error.jarjestelmassaVirheita", fi = "Järjestelmässä on {count} virhettä.").interpolate("count" to count)

        fun koskiSiirtoEpaonnistunut(count: Long) =
            tr("error.koskiSiirtoEpaonnistunut", fi = "{count} siirtoa KOSKI-tietovarantoon on epäonnistunut")
                .interpolate("count" to count)
    }

    object Vkt {
        val yhteensa: LocalizedString get() = tr("vkt.yhteensa", fi = "Yhteensä")
        val integraatiot: LocalizedString get() = tr("vkt.integraatiot", fi = "Integraatiot")
        val tiedoissaPuutteita: LocalizedString
            get() =
                tr(
                    "vkt.tiedoissaPuutteita",
                    fi = "Tiedoissa puutteita tai virheitä, eivätkä ole valmiit siirrettäväksi KOSKI-tietovarantoon.",
                )
        val siirtoAjastettu: LocalizedString
            get() = tr("vkt.siirtoAjastettu", fi = "Yritys tietojen siirrosta KOSKI-tietovarantoon ajastettu.")
        val tiedotSiirretty: LocalizedString
            get() = tr("vkt.tiedotSiirretty", fi = "Tiedot siirretty KOSKI-tietovarantoon.")
        val tiedonsiirtotilaVirheellinen: LocalizedString
            get() = tr("vkt.tiedonsiirtotilaVirheellinen", fi = "Tiedonsiirtotila on virheellinen.")
        val opiskeluoikeudenOid: LocalizedString get() = tr("vkt.opiskeluoikeudenOid", fi = "Opiskeluoikeuden oid")
        val tutkinnot: LocalizedString get() = tr("vkt.tutkinnot", fi = "Tutkinnot")
        val osakokeet: LocalizedString get() = tr("vkt.osakokeet", fi = "Osakokeet")
        val koskiTiedonsiirtovirheet: LocalizedString
            get() = tr("vkt.koskiTiedonsiirtovirheet", fi = "KOSKI-tiedonsiirtovirheet")
        val naytaJson: LocalizedString get() = tr("vkt.naytaJson", fi = "Näytä JSON")
        val yksiloity: LocalizedString get() = tr("vkt.yksiloity", fi = "Yksilöity")
        val yksilointiaYritetty: LocalizedString get() = tr("vkt.yksilointiaYritetty", fi = "Yksilöintiä yritetty")
        val eiYksiloity: LocalizedString get() = tr("vkt.eiYksiloity", fi = "Ei yksilöity")
        val suodata: LocalizedString get() = tr("vkt.suodata", fi = "Suodata")

        val arvioinninTila: LocalizedString get() = tr("vkt.arvioinninTila", fi = "Arvioinnin tila")
        val vainPoistettavat: LocalizedString get() = tr("vkt.vainPoistettavat", fi = "Vain poistettavat suoritukset")
        val vainEiPoistettavat: LocalizedString
            get() =
                tr("vkt.vainEiPoistettavat", fi = "Vain suoritukset, joita ei ole merkitty poistettavaksi")
        val arvioituOsittain: LocalizedString
            get() = tr("vkt.arvioituOsittain", fi = "Arvioitu osittain tai kokonaan")
        val arviointejaPuuttuu: LocalizedString get() = tr("vkt.arviointejaPuuttuu", fi = "Arviointeja puuttuu")

        val alkaen: LocalizedString get() = tr("vkt.alkaen", fi = "Alkaen")
        val paattyen: LocalizedString get() = tr("vkt.paattyen", fi = "Päättyen")
        val erinomaisenArvioinninTila: LocalizedString
            get() = tr("vkt.erinomaisenArvioinninTila", fi = "Erinomaisen tason suoritusten arvioinnin tila")
        val poistettavaksiMerkitty: LocalizedString
            get() = tr("vkt.poistettavaksiMerkitty", fi = "Poistettavaksi merkitty erinomaisen tason suoritus")
        val naytaKaikki: LocalizedString get() = tr("vkt.naytaKaikki", fi = "Näytä kaikki")
        val naytaVainPoistettavat: LocalizedString
            get() = tr("vkt.naytaVainPoistettavat", fi = "Näytä vain poistettavat suoritukset")
        val piilotaPoistettavat: LocalizedString
            get() = tr("vkt.piilotaPoistettavat", fi = "Piilota poistettavat suoritukset")
        val oppijanumeroTaiNimi: LocalizedString get() = tr("vkt.oppijanumeroTaiNimi", fi = "Oppijanumero tai nimi")
        val tutkinnonTaso: LocalizedString get() = tr("vkt.tutkinnonTaso", fi = "Tutkinnon taso")
        val kieli: LocalizedString get() = tr("vkt.kieli", fi = "Kieli")
        val koski: LocalizedString get() = tr("vkt.koski", fi = "KOSKI")
        val tutkinto: LocalizedString get() = tr("vkt.tutkinto", fi = "Tutkinto")
        val arvosana: LocalizedString get() = tr("vkt.arvosana", fi = "Arvosana")
        val arviointipaiva: LocalizedString get() = tr("vkt.arviointipaiva", fi = "Arviointipäivä")
        val arviointiPuuttuu: LocalizedString get() = tr("vkt.arviointiPuuttuu", fi = "Arviointi puuttuu")
        val arvioinnitPuuttuvat: LocalizedString get() = tr("vkt.arvioinnitPuuttuvat", fi = "Arvioinnit puuttuvat")
        val osakoePuuttuu: LocalizedString get() = tr("vkt.osakoePuuttuu", fi = "Osakoe puuttuu")
        val henkilotunnus: LocalizedString get() = tr("vkt.henkilotunnus", fi = "Henkilötunnus")
        val henkiloOid: LocalizedString get() = tr("vkt.henkiloOid", fi = "Henkilö-oid")
        val syntymaaika: LocalizedString get() = tr("vkt.syntymaaika", fi = "Syntymäaika")
        val yksilointi: LocalizedString get() = tr("vkt.yksilointi", fi = "Yksilöinti")
        val erinomainen: LocalizedString get() = tr("vkt.erinomainen", fi = "Erinomainen")
        val hylatty: LocalizedString get() = tr("vkt.hylatty", fi = "Hylätty")
        val eiSuoritusta: LocalizedString get() = tr("vkt.eiSuoritusta", fi = "Ei suoritusta")
        val osakoe: LocalizedString get() = tr("vkt.osakoe", fi = "Osakoe")
        val muutoksetTallennettu: LocalizedString
            get() = tr("vkt.muutoksetTallennettu", fi = "Muutokset tallennettu onnistuneesti.")
        val naytaVirheet: LocalizedString get() = tr("vkt.naytaVirheet", fi = "Näytä virheet")
        val merkittyKasitellyksiEiOid: LocalizedString
            get() =
                tr(
                    "vkt.merkittyKasitellyksiEiOid",
                    fi = "Suoritus on merkitty käsitellyksi, mutta sille ei ole opiskeluoikeus-oidia.",
                )
        val suoritustaEiLoytynyt: LocalizedString get() =
            tr(
                "vkt.suoritustaEiLoytynyt",
                fi = "VKT suoritusta ei löytynyt",
            )

        object Sarake {
            val ilmoittautumisenTunniste: LocalizedString
                get() = tr("vkt.sarake.ilmoittautumisenTunniste", fi = "Ilmoittautumisen tunniste")
            val sukunimi: LocalizedString get() = tr("vkt.sarake.sukunimi", fi = "Sukunimi")
            val etunimet: LocalizedString get() = tr("vkt.sarake.etunimet", fi = "Etunimet")
            val oppijanumero: LocalizedString get() = tr("vkt.sarake.oppijanumero", fi = "Oppijanumero")
            val taitotaso: LocalizedString get() = tr("vkt.sarake.taitotaso", fi = "Taitotaso")
            val tutkintokieli: LocalizedString get() = tr("vkt.sarake.tutkintokieli", fi = "Tutkintokieli")
            val tutkintopaiva: LocalizedString get() = tr("vkt.sarake.tutkintopaiva", fi = "Tutkintopäivä")
            val suorituspaikkakunta: LocalizedString
                get() = tr("vkt.sarake.suorituspaikkakunta", fi = "Suorituspaikkakunta")
            val vastaanottajanOid: LocalizedString
                get() = tr("vkt.sarake.vastaanottajanOid", fi = "Suorituksen vastaanottajan OID")
            val vastaanottaja: LocalizedString
                get() = tr("vkt.sarake.vastaanottaja", fi = "Suorituksen vastaanottaja")
            val puhuminen: LocalizedString get() = tr("vkt.sarake.puhuminen", fi = "Puhuminen")
            val puheenYmmartaminen: LocalizedString
                get() = tr("vkt.sarake.puheenYmmartaminen", fi = "Puheen ymmärtäminen")
            val kirjoittaminen: LocalizedString get() = tr("vkt.sarake.kirjoittaminen", fi = "Kirjoittaminen")
            val tekstinYmmartaminen: LocalizedString
                get() = tr("vkt.sarake.tekstinYmmartaminen", fi = "Tekstin ymmärtäminen")

            val tutkintoryhma: LocalizedString
                get() = tr("vkt.sarake.tutkintoryhma", fi = "Oppijanumero / kieli / taitotaso")
            val virhe: LocalizedString get() = tr("vkt.sarake.virhe", fi = "Virhe")
            val aikaleima: LocalizedString get() = tr("vkt.sarake.aikaleima", fi = "Aikaleima")
            val pyynto: LocalizedString get() = tr("vkt.sarake.pyynto", fi = "Pyyntö")
            val piilotus: LocalizedString get() = tr("vkt.sarake.piilotus", fi = "Piilotus")
        }
    }

    object Yki {
        val suodata: LocalizedString get() = tr("yki.suodata", fi = "Suodata")
        val arvioijiaYhteensa: LocalizedString get() = tr("yki.arvioijiaYhteensa", fi = "Arvioijia yhteensä")
        val hakusanaArvioija: LocalizedString
            get() = tr("yki.hakusanaArvioija", fi = "Nimi, oppijanumero, sähköposti tai ASHA-numero")
        val lisaaArvioija: LocalizedString get() = tr("yki.lisaaArvioija", fi = "Lisää arvioija")
        val odottaaLahetysta: LocalizedString get() = tr("yki.odottaaLahetysta", fi = "Odottaa lähetystä")
        val solkiLahetysOnnistui: LocalizedString get() = tr("yki.solkiLahetysOnnistui", fi = "Lähetetty")
        val solkiLahetysEpaonnistui: LocalizedString
            get() = tr("yki.solkiLahetysEpaonnistui", fi = "Lähetys epäonnistui")
        val solkiLahetystenVirheet: LocalizedString
            get() = tr("yki.solkiLahetystenVirheet", fi = "Vain Solki-lähetyksen virheet")
        val suorituksiaYhteensa: LocalizedString get() = tr("yki.suorituksiaYhteensa", fi = "Suorituksia yhteensä")
        val tarkistusarvioinnit: LocalizedString
            get() = tr("yki.tarkistusarvioinnit", fi = "Yleisen kielitutkinnon tarkistusarvioinnit")
        val naytaHyvaksytyt: LocalizedString
            get() = tr("yki.naytaHyvaksytyt", fi = "Näytä hyväksytyt tarkistusarvioinnit")
        val takaisinOdottaviin: LocalizedString
            get() = tr("yki.takaisinOdottaviin", fi = "Takaisin hyväksyntää odottaviin tarkistusarviointeihin")
        val tutkintotoimikunnanKokous: LocalizedString
            get() = tr("yki.tutkintotoimikunnanKokous", fi = "Tutkintotoimikunnan kokouksen päivämäärä")
        val naytaUusinVersio: LocalizedString get() = tr("yki.naytaUusinVersio", fi = "Näytä uusin versio")
        val henkilotiedot: LocalizedString get() = tr("yki.henkilotiedot", fi = "Henkilötiedot")
        val teeYksilointi: LocalizedString
            get() = tr("yki.teeYksilointi", fi = "Tee yksilöinti oppijanumerorekisterissä")
        val todistuksenPostitusosoite: LocalizedString
            get() = tr("yki.todistuksenPostitusosoite", fi = "Todistuksen postitusosoite ja kieli")
        val tutkinnonTiedot: LocalizedString get() = tr("yki.tutkinnonTiedot", fi = "Tutkinnon tiedot")
        val arviointi: LocalizedString get() = tr("yki.arviointi", fi = "Arviointi")
        val integraatiot: LocalizedString get() = tr("yki.integraatiot", fi = "Integraatiot")
        val siirrettyKoski: LocalizedString get() = tr("yki.siirrettyKoski", fi = "Siirretty KOSKI-tietovarantoon.")
        val odottaaSiirtoa: LocalizedString
            get() = tr("yki.odottaaSiirtoa", fi = "Odottaa siirtoa KOSKI-tietovarantoon.")
        val opiskeluoikeudenOid: LocalizedString get() = tr("yki.opiskeluoikeudenOid", fi = "Opiskeluoikeuden OID")
        val arviointitilaLahetetty: LocalizedString
            get() = tr("yki.arviointitilaLahetetty", fi = "Arviointitila lähetetty")
        val koskiTiedonsiirtovirheet: LocalizedString
            get() = tr("yki.koskiTiedonsiirtovirheet", fi = "KOSKI-tiedonsiirtovirheet")
        val naytaJson: LocalizedString get() = tr("yki.naytaJson", fi = "Näytä JSON")

        val henkiloOid: LocalizedString get() = tr("yki.henkiloOid", fi = "Henkilö-oid")
        val katuosoite: LocalizedString get() = tr("yki.katuosoite", fi = "Katuosoite")
        val postinumero: LocalizedString get() = tr("yki.postinumero", fi = "Postinumero")
        val postitoimipaikka: LocalizedString get() = tr("yki.postitoimipaikka", fi = "Postitoimipaikka")
        val maa: LocalizedString get() = tr("yki.maa", fi = "Maa")
        val todistuksenKieli: LocalizedString get() = tr("yki.todistuksenKieli", fi = "Todistuksen kieli")
        val jarjestaja: LocalizedString get() = tr("yki.jarjestaja", fi = "Järjestäjä")
        val arvioinninTila: LocalizedString get() = tr("yki.arvioinninTila", fi = "Arvioinnin tila")
        val tarkistusarvioinninSaapumispaiva: LocalizedString
            get() = tr("yki.tarkistusarvioinninSaapumispaiva", fi = "Tarkistusarvioinnin saapumispäivä")
        val tarkistusarvioinninAsiatunnus: LocalizedString
            get() = tr("yki.tarkistusarvioinninAsiatunnus", fi = "Tarkistusarvioinnin asiatunnus")
        val tarkistusarvioinninKasittelypaiva: LocalizedString
            get() = tr("yki.tarkistusarvioinninKasittelypaiva", fi = "Tarkistusarvioinnin käsittelypäivä")
        val tarkistusarvioidutOsakokeet: LocalizedString
            get() = tr("yki.tarkistusarvioidutOsakokeet", fi = "Tarkistusarvioidut osakokeet")
        val perustelu: LocalizedString get() = tr("yki.perustelu", fi = "Perustelu")
        val viimeksiMuokattu: LocalizedString get() = tr("yki.viimeksiMuokattu", fi = "Viimeksi muokattu")
        val koski: LocalizedString get() = tr("yki.koski", fi = "KOSKI")
        val koskiVirheet: LocalizedString get() = tr("yki.koskiVirheet", fi = "KOSKI-virheet")
        val kios: LocalizedString get() = tr("yki.kios", fi = "KIOS")
        val kiosVirhe: LocalizedString get() = tr("yki.kiosVirhe", fi = "KIOS-virhe")

        val tutkintopaivaAlkaen: LocalizedString get() = tr("yki.tutkintopaivaAlkaen", fi = "Tutkintopäivä alkaen")
        val tutkintopaivaPaattyen: LocalizedString
            get() = tr("yki.tutkintopaivaPaattyen", fi = "Tutkintopäivä päättyen")
        val naytaVersiohistoria: LocalizedString get() = tr("yki.naytaVersiohistoria", fi = "Näytä versiohistoria")

        val hakusana: LocalizedString
            get() = tr("yki.hakusana", fi = "Oppijanumero, henkilötunnus, Solki-ID tai hakusana")
        val vanhentuneetPiilotettu: LocalizedString
            get() = tr("yki.vanhentuneetPiilotettu", fi = "Vanhentuneet tietokentät piilotettu")
        val piilotaVanhentuneet: LocalizedString
            get() = tr("yki.piilotaVanhentuneet", fi = "Piilota vanhentuneet tietokentät")
        val odottavatHyvaksyntaa: LocalizedString
            get() = tr("yki.odottavatHyvaksyntaa", fi = "Odottavat tutkintotoimikunnan hyväksyntää")
        val merkitseHyvaksynta: LocalizedString
            get() = tr("yki.merkitseHyvaksynta", fi = "Merkitse hyväksyntä valituille")
        val hyvaksytytTarkistusarvioinnit: LocalizedString
            get() = tr("yki.hyvaksytytTarkistusarvioinnit", fi = "Hyväksytyt tarkistusarvioinnit")
        val korjaaHyvaksymispaiva: LocalizedString
            get() = tr("yki.korjaaHyvaksymispaiva", fi = "Korjaa hyväksymispäivämäärä valituille")
        val suoritustenTuonninVirheet: LocalizedString
            get() = tr("yki.suoritustenTuonninVirheet", fi = "Suoritusten tuonnin virheet")
        val siirtoaEiTehda: LocalizedString get() = tr("yki.siirtoaEiTehda", fi = "Siirtoa ei tehdä")

        val suoritustaEdeltavaEiLaheteta: LocalizedString
            get() = tr("yki.suoritustaEdeltavaEiLaheteta", fi = "Suoritusta edeltävää tila ei lähetetä")
        val arviointitilaaEiLahetetty: LocalizedString
            get() = tr("yki.arviointitilaaEiLahetetty", fi = "Arviointitilaa ei ole lähetetty")
        val saapunut: LocalizedString get() = tr("yki.saapunut", fi = "Pyyntö saapunut")
        val kasitelty: LocalizedString get() = tr("yki.kasitelty", fi = "Pyyntö käsitelty")
        val hyvaksytty: LocalizedString get() = tr("yki.hyvaksytty", fi = "Tulos hyväksytty")
        val arvosanaMuuttui: LocalizedString get() = tr("yki.arvosanaMuuttui", fi = "Arvosana muuttui")
        val arvosanaEiMuuttunut: LocalizedString get() = tr("yki.arvosanaEiMuuttunut", fi = "Arvosana ei muuttunut")
        val onrEiYhteytta: LocalizedString
            get() =
                tr(
                    "yki.onrEiYhteytta",
                    fi =
                        "Oppijanumerorekisteriin ei juuri nyt saatu yhteyttä, joten haku tehtiin vain annetuilla " +
                            "oideilla. Henkilön mahdollisiin muihin OID-tunnuksiin (esim. yhdistettyihin " +
                            "duplikaatteihin) liittyvät suoritukset voivat puuttua tuloksista.",
                )
        val ilmoittautumisenTiedot: LocalizedString get() =
            tr(
                "yki.ilmoittautumisenTiedot",
                fi = "Ilmoittautumisen tiedot",
            )
        val oppijanumerorekisteri: LocalizedString get() = tr("yki.oppijanumerorekisteri", fi = "Oppijanumerorekisteri")

        object Arviointitila {
            val ilmoittautunut: LocalizedString get() = tr("yki.arviointitila.ilmoittautunut", fi = "Ilmoittautunut")
            val ilmoittautuminenPeruttu: LocalizedString
                get() = tr("yki.arviointitila.ilmoittautuminenPeruttu", fi = "Ilmoittautuminen peruttu")
            val eiSuoritusta: LocalizedString get() = tr("yki.arviointitila.eiSuoritusta", fi = "Ei suoritusta")
            val suoritusArvioitavana: LocalizedString
                get() = tr("yki.arviointitila.suoritusArvioitavana", fi = "Suoritus arvioitavana")
            val arviointiValmis: LocalizedString
                get() = tr("yki.arviointitila.arviointiValmis", fi = "Arviointi valmis")
            val suoritusTarkistusarvioitavana: LocalizedString
                get() = tr("yki.arviointitila.suoritusTarkistusarvioitavana", fi = "Suoritus tarkistusarvioitavana")
            val tarkistusarviointiTehty: LocalizedString
                get() = tr("yki.arviointitila.tarkistusarviointiTehty", fi = "Tarkistusarviointi tehty")
            val tarkistusarviointiHyvaksytty: LocalizedString
                get() = tr("yki.arviointitila.tarkistusarviointiHyvaksytty", fi = "Tarkistusarviointi hyväksytty")
        }

        object ArvioijaTila {
            val aktiivinen: LocalizedString get() = tr("yki.arvioijaTila.aktiivinen", fi = "Aktiivinen")
            val passivoitu: LocalizedString get() = tr("yki.arvioijaTila.passivoitu", fi = "Passivoitu")
            val tulevaisuudessa: LocalizedString
                get() = tr("yki.arvioijaTila.tulevaisuudessa", fi = "Alkaa myöhemmin")
        }

        object Arvioija {
            val uusiArvioija: LocalizedString get() = tr("yki.arvioija.uusiArvioija", fi = "Uusi arvioija")
            val haeHenkilonTiedot: LocalizedString
                get() = tr("yki.arvioija.haeHenkilonTiedot", fi = "Hae henkilön tiedot")
            val hakuOhjeOppijanumero: LocalizedString
                get() =
                    tr(
                        "yki.arvioija.hakuOhjeOppijanumero",
                        fi = "Hae arvioijan tiedot oppijanumerorekisteristä oppijanumerolla.",
                    )
            val sukunimi: LocalizedString get() = tr("yki.arvioija.sukunimi", fi = "Sukunimi")
            val etunimet: LocalizedString get() = tr("yki.arvioija.etunimet", fi = "Etunimet")
            val oppijanumero: LocalizedString get() = tr("yki.arvioija.oppijanumero", fi = "Oppijanumero")
            val sahkopostiosoite: LocalizedString
                get() = tr("yki.arvioija.sahkopostiosoite", fi = "Sähköpostiosoite")
            val katuosoite: LocalizedString get() = tr("yki.arvioija.katuosoite", fi = "Katuosoite")
            val postinumero: LocalizedString get() = tr("yki.arvioija.postinumero", fi = "Postinumero")
            val postitoimipaikka: LocalizedString
                get() = tr("yki.arvioija.postitoimipaikka", fi = "Postitoimipaikka")
            val yhteystiedot: LocalizedString get() = tr("yki.arvioija.yhteystiedot", fi = "Yhteystiedot")
            val rekisterimerkinta: LocalizedString
                get() = tr("yki.arvioija.rekisterimerkinta", fi = "Rekisterimerkintä")
            val kaudenAlkupaiva: LocalizedString get() = tr("yki.arvioija.kaudenAlkupaiva", fi = "Kauden alkupäivä")
            val kaudenPaattymispaiva: LocalizedString
                get() = tr("yki.arvioija.kaudenPaattymispaiva", fi = "Kauden päättymispäivä")
            val kaudenPaattymispaivaOhje: LocalizedString
                get() =
                    tr(
                        "yki.arvioija.kaudenPaattymispaivaOhje",
                        fi = "Järjestelmä laskee 5 vuotta alkupäivästä",
                    )
            val jatkorekisterointi: LocalizedString
                get() = tr("yki.arvioija.jatkorekisterointi", fi = "Jatkokausi")
            val ashaNumero: LocalizedString
                get() = tr("yki.arvioija.ashaNumero", fi = "Hallintopäätöksen ASHA-numero")
            val arviointioikeudet: LocalizedString
                get() = tr("yki.arvioija.arviointioikeudet", fi = "Arviointioikeudet")
            val arviointioikeudetOhje: LocalizedString
                get() =
                    tr(
                        "yki.arvioija.arviointioikeudetOhje",
                        fi = "Valitse tutkintokielet ja -tasot. Kausi on sama kaikille valinnoille.",
                    )
            val tutkintokieli: LocalizedString get() = tr("yki.arvioija.tutkintokieli", fi = "Tutkintokieli")
            val tallenna: LocalizedString get() = tr("yki.arvioija.tallenna", fi = "Tallenna arvioija")
            val muokkaa: LocalizedString get() = tr("yki.arvioija.muokkaa", fi = "Muokkaa")
            val muokkaaArvioijaa: LocalizedString
                get() = tr("yki.arvioija.muokkaaArvioijaa", fi = "Muokkaa arvioijan tietoja")
            val tallennaMuutokset: LocalizedString
                get() = tr("yki.arvioija.tallennaMuutokset", fi = "Tallenna muutokset")
            val muutoksetTallennettu: LocalizedString
                get() = tr("yki.arvioija.muutoksetTallennettu", fi = "Arvioijan tiedot päivitettiin.")
            val peruuta: LocalizedString get() = tr("yki.arvioija.peruuta", fi = "Peruuta")
            val jorekisterissa: LocalizedString
                get() =
                    tr(
                        "yki.arvioija.joRekisterissa",
                        fi =
                            "Arvioija on jo rekisterissä. Tiedot on esitäytetty nykyisestä " +
                                "merkinnästä, ja tallennus päivittää sen.",
                    )
            val muokattuSamanaikaisesti: LocalizedString
                get() =
                    tr(
                        "yki.arvioija.muokattuSamanaikaisesti",
                        fi =
                            "Toinen käyttäjä ehti muokata arvioijan tietoja. Lataa sivu uudelleen " +
                                "ja tee muutokset uudelleen.",
                    )
            val kausihistoria: LocalizedString
                get() = tr("yki.arvioija.arviointikaudet", fi = "Arviointikaudet")
            val kirjattu: LocalizedString get() = tr("yki.arvioija.kirjattu", fi = "Kirjattu")
            val kirjaaja: LocalizedString get() = tr("yki.arvioija.kirjaaja", fi = "Kirjaaja")
            val jarjestelma: LocalizedString get() = tr("yki.arvioija.jarjestelma", fi = "Järjestelmä")
            val eiMuutoshistoriaa: LocalizedString
                get() = tr("yki.arvioija.eiMuutoshistoriaa", fi = "Ei kirjattuja muutoksia.")
            val passivoi: LocalizedString get() = tr("yki.arvioija.passivoi", fi = "Merkitse passiiviseksi")
            val passivoiVahvistus: LocalizedString
                get() =
                    tr(
                        "yki.arvioija.passivoiVahvistus",
                        fi =
                            "Arvioijan kaikki arviointioikeudet merkitään passiivisiksi. " +
                                "Toimintoa ei voi perua käyttöliittymästä.",
                    )
            val passivoitu: LocalizedString
                get() = tr("yki.arvioija.passivoitu", fi = "Arvioija merkittiin passiiviseksi.")
            val solkiinLahetetty: LocalizedString
                get() = tr("yki.arvioija.solkiinLahetetty", fi = "Lähetetty Solkiin")
            val solkiLahetysyritykset: LocalizedString
                get() = tr("yki.arvioija.solkiLahetysyritykset", fi = "Lähetysyrityksiä")
            val lahetaUudelleen: LocalizedString
                get() = tr("yki.arvioija.lahetaUudelleen", fi = "Lähetä uudelleen Solkiin")
            val lahetysjonossa: LocalizedString
                get() = tr("yki.arvioija.lahetysjonossa", fi = "Odottaa lähetystä")
            val solkiTunnus: LocalizedString
                get() = tr("yki.arvioija.solkiTunnus", fi = "Solki-tunnus")
            val solkiTunnusEiTiedossa: LocalizedString
                get() = tr("yki.arvioija.solkiTunnusEiTiedossa", fi = "Ei vielä tiedossa")
            val lahetysOnnistui: LocalizedString
                get() = tr("yki.arvioija.lahetysOnnistui", fi = "Arvioija lähetettiin Solkiin.")
            val lahetysEpaonnistui: LocalizedString
                get() = tr("yki.arvioija.lahetysEpaonnistui", fi = "Lähetys epäonnistui. Syy näkyy alla.")
            val lahetysEiKaytossa: LocalizedString
                get() =
                    tr(
                        "yki.arvioija.lahetysEiKaytossa",
                        fi = "Solki-lähetys ei ole käytössä. Rivi jää lähetysjonoon.",
                    )
            val kausiPaattynyt: LocalizedString
                get() =
                    tr(
                        "yki.arvioija.kausiPaattynyt",
                        fi = "Arviointikausi on päättynyt. Uusi arviointikausi aktivoi merkinnän.",
                    )
            val joPassivoitu: LocalizedString
                get() =
                    tr(
                        "yki.arvioija.joPassivoitu",
                        fi =
                            "Arvioija on merkitty passiiviseksi {pvm}. " +
                                "Uusi arviointikausi aktivoi merkinnän.",
                    )
            val automaattilahetysEiKaytossa: LocalizedString
                get() =
                    tr(
                        "yki.arvioija.automaattilahetysEiKaytossa",
                        fi =
                            "Automaattinen Solki-lähetys ei ole käytössä tässä ympäristössä. " +
                                "Lähetä uudelleen -painike lähettää arvioijan silti.",
                    )
            val kirjoitusEiKaytossa: LocalizedString
                get() =
                    tr(
                        "yki.arvioija.kirjoitusEiKaytossa",
                        fi = "Arvioijarekisterin ylläpito ei ole vielä käytössä tässä ympäristössä.",
                    )
            val tallennettu: LocalizedString
                get() = tr("yki.arvioija.tallennettu", fi = "Arvioija tallennettiin rekisteriin.")
            val turvakielto: LocalizedString
                get() =
                    tr(
                        "yki.arvioija.turvakielto",
                        fi = "Henkilöllä on turvakielto. Käsittele yhteystietoja huolellisesti.",
                    )
            val turvakieltoEiTiedossa: LocalizedString
                get() =
                    tr(
                        "yki.arvioija.turvakieltoEiTiedossa",
                        fi =
                            "Turvakieltoa ei voitu tarkistaa oppijanumerorekisteristä. " +
                                "Käsittele yhteystietoja huolellisesti.",
                    )
            val eiYksiloity: LocalizedString
                get() =
                    tr(
                        "yki.arvioija.eiYksiloity",
                        fi =
                            "Henkilöä ei ole yksilöity oppijanumerorekisterissä, " +
                                "joten arvioijaa ei voi lisätä.",
                    )
            val onrEiVastannut: LocalizedString
                get() =
                    tr(
                        "yki.arvioija.onrEiVastannut",
                        fi = "Oppijanumerorekisteri ei vastannut. Yritä myöhemmin uudestaan.",
                    )
            val eiLoydy: LocalizedString get() = tr("yki.arvioija.eiLoydy", fi = "Arvioijaa ei löydy")
            val eiLoytynytOnrista: LocalizedString
                get() =
                    tr(
                        "yki.arvioija.eiLoytynytOnrista",
                        fi = "Henkilöä ei löytynyt oppijanumerorekisteristä. Tarkista oppijanumero.",
                    )
            val takaisinListaan: LocalizedString
                get() = tr("yki.arvioija.takaisinListaan", fi = "Takaisin arvioijalistaan")

            object Kausi {
                val uusi: LocalizedString
                    get() = tr("yki.arvioija.arviointikausi.uusi", fi = "Lisää arviointikausi")
                val muokkaa: LocalizedString get() = tr("yki.arvioija.arviointikausi.muokkaa", fi = "Muokkaa")
                val passivoi: LocalizedString get() = tr("yki.arvioija.arviointikausi.passivoi", fi = "Passivoi")
                val poista: LocalizedString get() = tr("yki.arvioija.arviointikausi.poista", fi = "Poista")
                val peruuta: LocalizedString get() = tr("yki.arvioija.arviointikausi.peruuta", fi = "Peruuta")
                val tallenna: LocalizedString get() = tr("yki.arvioija.arviointikausi.tallenna", fi = "Tallenna")
                val toiminnot: LocalizedString get() = tr("yki.arvioija.arviointikausi.toiminnot", fi = "Toiminnot")
                val vanhentuneet: LocalizedString
                    get() = tr("yki.arvioija.arviointikausi.vanhentuneet", fi = "Vanhentuneet arviointioikeudet")
                val vanhentuneetOhje: LocalizedString
                    get() =
                        tr(
                            "yki.arvioija.kausi.vanhentuneetOhje",
                            fi =
                                "Nämä tutkintokielet eivät ole enää myönnettävissä eivätkä kuulu " +
                                    "arviointikausiin. Tiedot säilytetään sellaisenaan.",
                        )
                val toimenpide: LocalizedString get() = tr("yki.arvioija.arviointikausi.toimenpide", fi = "Toimenpide")
                val lisays: LocalizedString get() = tr("yki.arvioija.arviointikausi.toimenpide.lisays", fi = "Lisäys")
                val muokkaus: LocalizedString get() =
                    tr(
                        "yki.arvioija.arviointikausi.toimenpide.muokkaus",
                        fi = "Muokkaus",
                    )
                val passivointi: LocalizedString
                    get() = tr("yki.arvioija.arviointikausi.toimenpide.passivointi", fi = "Passivointi")
                val poisto: LocalizedString get() = tr("yki.arvioija.arviointikausi.toimenpide.poisto", fi = "Poisto")
                val tallennus: LocalizedString
                    get() = tr("yki.arvioija.arviointikausi.toimenpide.tallennus", fi = "Tallennus")
                val eiKausia: LocalizedString
                    get() = tr("yki.arvioija.arviointikausi.eiKausia", fi = "Ei kirjattuja arviointikausia.")
                val muokkaaOtsikko: LocalizedString
                    get() = tr("yki.arvioija.arviointikausi.muokkaaOtsikko", fi = "Muokkaa arviointikautta")
                val naytaMuutoshistoria: LocalizedString
                    get() = tr("yki.arvioija.arviointikausi.naytaMuutoshistoria", fi = "Näytä muutoshistoria")
                val poistaVahvistus: LocalizedString
                    get() =
                        tr(
                            "yki.arvioija.kausi.poistaVahvistus",
                            fi =
                                "Arviointikausi ja sen arviointioikeudet poistetaan pysyvästi. " +
                                    "Käytä tätä vain jos kausi on kirjattu väärälle henkilölle.",
                        )
                val passivoiVahvistus: LocalizedString
                    get() =
                        tr(
                            "yki.arvioija.kausi.passivoiVahvistus",
                            fi =
                                "Arviointikausi päätetään tähän päivään. " +
                                    "Toimintoa ei voi perua käyttöliittymästä.",
                        )
                val lisatty: LocalizedString
                    get() = tr("yki.arvioija.arviointikausi.lisatty", fi = "Arviointikausi lisättiin.")
                val paivitetty: LocalizedString
                    get() = tr("yki.arvioija.arviointikausi.paivitetty", fi = "Arviointikausi päivitettiin.")
                val passivoitu: LocalizedString
                    get() = tr("yki.arvioija.arviointikausi.passivoitu", fi = "Arviointikausi passivoitiin.")
                val poistettu: LocalizedString
                    get() = tr("yki.arvioija.arviointikausi.poistettu", fi = "Arviointikausi poistettiin.")
                val eiAktiivinen: LocalizedString
                    get() =
                        tr(
                            "yki.arvioija.kausi.eiAktiivinen",
                            fi = "Vain aktiivisen arviointikauden voi passivoida.",
                        )
                val viimeistaEiVoiPoistaa: LocalizedString
                    get() =
                        tr(
                            "yki.arvioija.kausi.viimeistaEiVoiPoistaa",
                            fi =
                                "Viimeistä arviointikautta ei voi poistaa. " +
                                    "Merkitse arvioija passiiviseksi sen sijaan.",
                        )
            }
        }

        object Taso {
            val perustaso: LocalizedString get() = tr("yki.taso.perustaso", fi = "Perustaso")
            val keskitaso: LocalizedString get() = tr("yki.taso.keskitaso", fi = "Keskitaso")
            val ylinTaso: LocalizedString get() = tr("yki.taso.ylinTaso", fi = "Ylin taso")
        }

        object Kieli {
            val suomi: LocalizedString get() = tr("yki.kieli.suomi", fi = "suomi")
            val ruotsi: LocalizedString get() = tr("yki.kieli.ruotsi", fi = "ruotsi")
            val englanti: LocalizedString get() = tr("yki.kieli.englanti", fi = "englanti")
            val saksa: LocalizedString get() = tr("yki.kieli.saksa", fi = "saksa")
            val ranska: LocalizedString get() = tr("yki.kieli.ranska", fi = "ranska")
            val italia: LocalizedString get() = tr("yki.kieli.italia", fi = "italia")
            val venaja: LocalizedString get() = tr("yki.kieli.venaja", fi = "venäjä")
            val pohjoissaame: LocalizedString get() = tr("yki.kieli.pohjoissaame", fi = "pohjoissaame")
            val espanja: LocalizedString get() = tr("yki.kieli.espanja", fi = "espanja")
            val ruotsiVanha: LocalizedString get() = tr("yki.kieli.ruotsiVanha", fi = "ruotsi (vanha koodi)")
            val kaupallinenEnglanti: LocalizedString
                get() = tr("yki.kieli.kaupallinenEnglanti", fi = "kaupallinen englanti")
            val tekninenEnglanti: LocalizedString get() = tr("yki.kieli.tekninenEnglanti", fi = "tekninen englanti")
        }

        object Sarake {
            val oppijanumero: LocalizedString get() = tr("yki.sarake.oppijanumero", fi = "Oppijanumero")
            val sukunimi: LocalizedString get() = tr("yki.sarake.sukunimi", fi = "Sukunimi")
            val etunimi: LocalizedString get() = tr("yki.sarake.etunimi", fi = "Etunimi")
            val etunimet: LocalizedString get() = tr("yki.sarake.etunimet", fi = "Etunimet")
            val sukupuoli: LocalizedString get() = tr("yki.sarake.sukupuoli", fi = "Sukupuoli")
            val henkilotunnus: LocalizedString get() = tr("yki.sarake.henkilotunnus", fi = "Henkilötunnus")
            val kansalaisuus: LocalizedString get() = tr("yki.sarake.kansalaisuus", fi = "Kansalaisuus")
            val osoite: LocalizedString get() = tr("yki.sarake.osoite", fi = "Osoite")
            val sahkoposti: LocalizedString get() = tr("yki.sarake.sahkoposti", fi = "Sähköposti")
            val tutkintopaiva: LocalizedString get() = tr("yki.sarake.tutkintopaiva", fi = "Tutkintopäivä")
            val tutkintokieli: LocalizedString get() = tr("yki.sarake.tutkintokieli", fi = "Tutkintokieli")
            val tutkintotaso: LocalizedString get() = tr("yki.sarake.tutkintotaso", fi = "Tutkintotaso")
            val kieli: LocalizedString get() = tr("yki.sarake.kieli", fi = "Kieli")
            val taso: LocalizedString get() = tr("yki.sarake.taso", fi = "Taso")
            val jarjestajanOid: LocalizedString get() = tr("yki.sarake.jarjestajanOid", fi = "Järjestäjän OID")
            val jarjestajanNimi: LocalizedString get() = tr("yki.sarake.jarjestajanNimi", fi = "Järjestäjän nimi")
            val arviointitila: LocalizedString get() = tr("yki.sarake.arviointitila", fi = "Arviointitila")
            val arviointipaiva: LocalizedString get() = tr("yki.sarake.arviointipaiva", fi = "Arviointipäivä")
            val tekstinYmmartaminen: LocalizedString
                get() = tr("yki.sarake.tekstinYmmartaminen", fi = "Tekstin ymmärtäminen")
            val kirjoittaminen: LocalizedString get() = tr("yki.sarake.kirjoittaminen", fi = "Kirjoittaminen")
            val puheenYmmartaminen: LocalizedString
                get() = tr("yki.sarake.puheenYmmartaminen", fi = "Puheen ymmärtäminen")
            val puhuminen: LocalizedString get() = tr("yki.sarake.puhuminen", fi = "Puhuminen")
            val rakenteetJaSanasto: LocalizedString
                get() = tr("yki.sarake.rakenteetJaSanasto", fi = "Rakenteet ja sanasto")
            val yleisarvosana: LocalizedString get() = tr("yki.sarake.yleisarvosana", fi = "Yleisarvosana")
            val todistuskieli: LocalizedString get() = tr("yki.sarake.todistuskieli", fi = "Todistuskieli")
            val tilaLahetetty: LocalizedString get() = tr("yki.sarake.tilaLahetetty", fi = "Tila lähetetty")
            val opiskeluoikeusOid: LocalizedString get() = tr("yki.sarake.opiskeluoikeusOid", fi = "Opiskeluoikeus-OID")
            val solkiTunniste: LocalizedString get() = tr("yki.sarake.solkiTunniste", fi = "Solki-tunniste")
            val versio: LocalizedString get() = tr("yki.sarake.versio", fi = "Versio")
            val tila: LocalizedString get() = tr("yki.sarake.tila", fi = "Tila")
            val tasot: LocalizedString get() = tr("yki.sarake.tasot", fi = "Tasot")
            val kaudenAlkupaiva: LocalizedString get() = tr("yki.sarake.kaudenAlkupaiva", fi = "Kauden alkupäivä")
            val kaudenPaattymispaiva: LocalizedString
                get() = tr("yki.sarake.kaudenPaattymispaiva", fi = "Kauden päättymispäivä")
            val jatkorekisterointi: LocalizedString
                get() = tr("yki.sarake.jatkorekisterointi", fi = "Jatkorekisteröinti")
            val rekisteriintuontiaika: LocalizedString
                get() = tr("yki.sarake.rekisteriintuontiaika", fi = "Rekisteriintuontiaika")
            val ensimmainenRekisterointipaiva: LocalizedString
                get() = tr("yki.sarake.ensimmainenRekisterointipaiva", fi = "Ensimmäinen rekisteröintipäivä")
            val ashaNumero: LocalizedString get() = tr("yki.sarake.ashaNumero", fi = "Hallintopäätöksen ASHA-numero")
            val solkiTila: LocalizedString get() = tr("yki.sarake.solkiTila", fi = "Solki-lähetys")
            val muokattu: LocalizedString get() = tr("yki.sarake.muokattu", fi = "Muokattu")
            val solkiId: LocalizedString get() = tr("yki.sarake.solkiId", fi = "Solki-ID")
            val kentta: LocalizedString get() = tr("yki.sarake.kentta", fi = "Kenttä")
            val arvoKitussa: LocalizedString get() = tr("yki.sarake.arvoKitussa", fi = "Arvo Kitussa")
            val arvoSolkissa: LocalizedString get() = tr("yki.sarake.arvoSolkissa", fi = "Arvo Solkissa")
            val havaittu: LocalizedString get() = tr("yki.sarake.havaittu", fi = "Havaittu")
            val paivamaara: LocalizedString get() = tr("yki.sarake.paivamaara", fi = "Päivämäärä")
            val asiatunnus: LocalizedString get() = tr("yki.sarake.asiatunnus", fi = "Asiatunnus")
            val tarkistusarviointi: LocalizedString
                get() = tr("yki.sarake.tarkistusarviointi", fi = "Tarkistusarviointi")
            val tarkistusarvioinninSaapumispaiva: LocalizedString
                get() =
                    tr("yki.sarake.tarkistusarvioinninSaapumispaiva", fi = "Tarkistusarvioinnin saapumispäivä")
            val tarkistusarvioinninKasittelypaiva: LocalizedString
                get() =
                    tr("yki.sarake.tarkistusarvioinninKasittelypaiva", fi = "Tarkistusarvioinnin käsittelypäivä")
            val tarkistusarviointiHyvaksytty: LocalizedString
                get() = tr("yki.sarake.tarkistusarviointiHyvaksytty", fi = "Tarkistusarviointi hyväksytty")
            val tarkistusarvioidutOsakokeet: LocalizedString
                get() = tr("yki.sarake.tarkistusarvioidutOsakokeet", fi = "Tarkistusarvioidut osakokeet")
            val arvosanaMuuttuiOsakokeet: LocalizedString
                get() = tr("yki.sarake.arvosanaMuuttuiOsakokeet", fi = "Osakokeet joiden arvosana muuttui")
            val suorituksenTunniste: LocalizedString
                get() = tr("yki.sarake.suorituksenTunniste", fi = "Suorituksen tunniste")
            val virhe: LocalizedString get() = tr("yki.sarake.virhe", fi = "Virhe")
            val aikaleima: LocalizedString get() = tr("yki.sarake.aikaleima", fi = "Aikaleima")
            val pyynto: LocalizedString get() = tr("yki.sarake.pyynto", fi = "Pyyntö")
            val piilotus: LocalizedString get() = tr("yki.sarake.piilotus", fi = "Piilotus")
        }

        object Virhesarake {
            val oppijanumero: LocalizedString get() = tr("yki.virhesarake.oppijanumero", fi = "oppijanumero")
            val hetu: LocalizedString get() = tr("yki.virhesarake.hetu", fi = "hetu")
            val nimi: LocalizedString get() = tr("yki.virhesarake.nimi", fi = "nimi")
            val virheellinenKentta: LocalizedString
                get() = tr("yki.virhesarake.virheellinenKentta", fi = "virheellinen kenttä")
            val virheellinenArvo: LocalizedString
                get() = tr("yki.virhesarake.virheellinenArvo", fi = "virheellinen arvo")
            val virheellinenRivi: LocalizedString
                get() = tr("yki.virhesarake.virheellinenRivi", fi = "virheellinen rivi")
            val virheenRivinumero: LocalizedString
                get() = tr("yki.virhesarake.virheenRivinumero", fi = "virheen rivinumero")
            val virheenLuontiaika: LocalizedString
                get() = tr("yki.virhesarake.virheenLuontiaika", fi = "virheen luontiaika")
            val lastModified: LocalizedString get() = tr("yki.virhesarake.lastModified", fi = "last modified")
        }
    }

    object Koto {
        val henkilotiedot: LocalizedString get() = tr("koto.henkilotiedot", fi = "Henkilötiedot")
        val tutkinnonTiedot: LocalizedString get() = tr("koto.tutkinnonTiedot", fi = "Tutkinnon tiedot")
        val arviointi: LocalizedString get() = tr("koto.arviointi", fi = "Arviointi")
        val integraatiot: LocalizedString get() = tr("koto.integraatiot", fi = "Integraatiot")
        val suodata: LocalizedString get() = tr("koto.suodata", fi = "Suodata")
        val suoritustenTuonninVirheet: LocalizedString
            get() = tr("koto.suoritustenTuonninVirheet", fi = "Suoritusten tuonnin virheet")
        val lataaCsv: LocalizedString get() = tr("koto.lataaCsv", fi = "Lataa tiedot CSV:nä")
        val suorituksiaYhteensa: LocalizedString get() = tr("koto.suorituksiaYhteensa", fi = "Suorituksia yhteensä")
        val virheitaYhteensa: LocalizedString get() = tr("koto.virheitaYhteensa", fi = "Virheitä yhteensä")
        val kesken: LocalizedString get() = tr("koto.kesken", fi = "Kesken")
        val kurssi: LocalizedString get() = tr("koto.kurssi", fi = "Kurssi")
        val jarjestaja: LocalizedString get() = tr("koto.jarjestaja", fi = "Järjestäjä")
        val tehtavapaketti: LocalizedString get() = tr("koto.tehtavapaketti", fi = "Tehtäväpaketti")
        val viimeksiMuokattu: LocalizedString get() = tr("koto.viimeksiMuokattu", fi = "Viimeksi muokattu")
        val suoritusaikaAlkaen: LocalizedString get() = tr("koto.suoritusaikaAlkaen", fi = "Suoritusaika alkaen")
        val suoritusaikaPaattyen: LocalizedString get() = tr("koto.suoritusaikaPaattyen", fi = "Suoritusaika päättyen")
        val hakusana: LocalizedString
            get() = tr("koto.hakusana", fi = "Oppijanumero, nimi, oppilaitoksen nimi tai muu hakusana")

        val tehtavapankki: LocalizedString get() = tr("koto.tehtavapankki", fi = "Kotoutumiskoulutuksen tehtäväpankki")
        val eiTehtavapaketteja: LocalizedString get() = tr("koto.eiTehtavapaketteja", fi = "Ei tehtäväpaketteja.")
        val siirretty: LocalizedString get() = tr("koto.siirretty", fi = "Siirretty")
        val koko: LocalizedString get() = tr("koto.koko", fi = "Koko")
        val sisalto: LocalizedString get() = tr("koto.sisalto", fi = "Sisältö")
        val naytaSisalto: LocalizedString get() = tr("koto.naytaSisalto", fi = "Näytä sisältö")
        val lataaXml: LocalizedString get() = tr("koto.lataaXml", fi = "Lataa XML")
        val lataa: LocalizedString get() = tr("koto.lataa", fi = "Lataa")
        val paketissaEiRyhmia: LocalizedString get() = tr("koto.paketissaEiRyhmia", fi = "Paketissa ei ole ryhmiä.")
        val eiTehtavia: LocalizedString get() = tr("koto.eiTehtavia", fi = "Ei tehtäviä.")
        val tehtavanTunniste: LocalizedString get() = tr("koto.tehtavanTunniste", fi = "Tehtävän tunniste")
        val vastausvaihtoehdot: LocalizedString get() = tr("koto.vastausvaihtoehdot", fi = "Vastausvaihtoehdot")
        val liitetiedostot: LocalizedString get() = tr("koto.liitetiedostot", fi = "Liitetiedostot")
        val metadata: LocalizedString get() = tr("koto.metadata", fi = "Metadata")
        val nimeton: LocalizedString get() = tr("koto.nimeton", fi = "(nimetön)")
        val tyhjaNimi: LocalizedString get() = tr("koto.tyhjaNimi", fi = "(tyhjä nimi)")
        val lahdejarjestelma: LocalizedString get() = tr("koto.lahdejarjestelma", fi = "Lähdejärjestelmä")
        val lahdeId: LocalizedString get() = tr("koto.lahdeId", fi = "Lähde-id")
        val versio: LocalizedString get() = tr("koto.versio", fi = "Versio")
        val lahdeversio: LocalizedString get() = tr("koto.lahdeversio", fi = "Lähdeversio")
        val kieli: LocalizedString get() = tr("koto.kieli", fi = "Kieli")
        val kurssinAlku: LocalizedString get() = tr("koto.kurssinAlku", fi = "Kurssin alku")
        val lahdeGeneroitu: LocalizedString get() = tr("koto.lahdeGeneroitu", fi = "Lähde generoitu")
        val ladattu: LocalizedString get() = tr("koto.ladattu", fi = "Ladattu")
        val xmlTiedosto: LocalizedString get() = tr("koto.xmlTiedosto", fi = "XML-tiedosto")
        val versioLabel: LocalizedString get() = tr("koto.versioLabel", fi = "versio")
        val generoituLabel: LocalizedString get() = tr("koto.generoituLabel", fi = "generoitu")

        object Sarake {
            val oppijanumero: LocalizedString get() = tr("koto.sarake.oppijanumero", fi = "Oppijanumero")
            val sukunimi: LocalizedString get() = tr("koto.sarake.sukunimi", fi = "Sukunimi")
            val etunimet: LocalizedString get() = tr("koto.sarake.etunimet", fi = "Etunimet")
            val kutsumanimi: LocalizedString get() = tr("koto.sarake.kutsumanimi", fi = "Kutsumanimi")
            val sahkoposti: LocalizedString get() = tr("koto.sarake.sahkoposti", fi = "Sähköposti")
            val kurssinId: LocalizedString get() = tr("koto.sarake.kurssinId", fi = "Kurssin ID")
            val kurssinNimi: LocalizedString get() = tr("koto.sarake.kurssinNimi", fi = "Kurssin nimi")
            val testikieli: LocalizedString get() = tr("koto.sarake.testikieli", fi = "Testikieli")
            val oppilaitosOid: LocalizedString get() = tr("koto.sarake.oppilaitosOid", fi = "Oppilaitos OID")
            val oppilaitos: LocalizedString get() = tr("koto.sarake.oppilaitos", fi = "Oppilaitos")
            val opettajanSahkoposti: LocalizedString
                get() = tr("koto.sarake.opettajanSahkoposti", fi = "Opettajan sähköposti")
            val suoritusaika: LocalizedString get() = tr("koto.sarake.suoritusaika", fi = "Suoritusaika")
            val luetunYmmartaminen: LocalizedString
                get() = tr("koto.sarake.luetunYmmartaminen", fi = "Luetun ymmärtäminen")
            val kuullunYmmartaminen: LocalizedString
                get() = tr("koto.sarake.kuullunYmmartaminen", fi = "Kuullun ymmärtäminen")
            val puhe: LocalizedString get() = tr("koto.sarake.puhe", fi = "Puhe")
            val kirjoittaminen: LocalizedString get() = tr("koto.sarake.kirjoittaminen", fi = "Kirjoittaminen")
            val henkilotunnus: LocalizedString get() = tr("koto.sarake.henkilotunnus", fi = "Henkilötunnus")
            val nimi: LocalizedString get() = tr("koto.sarake.nimi", fi = "Nimi")
            val organisaatio: LocalizedString get() = tr("koto.sarake.organisaatio", fi = "Organisaatio")
            val opettajanSahkopostiosoite: LocalizedString
                get() = tr("koto.sarake.opettajanSahkopostiosoite", fi = "Opettajan sähköpostiosoite")
            val virheenLuontiaika: LocalizedString
                get() = tr("koto.sarake.virheenLuontiaika", fi = "Virheen luontiaika")
            val virheviesti: LocalizedString get() = tr("koto.sarake.virheviesti", fi = "Virheviesti")
            val ratkaisuehdotus: LocalizedString get() = tr("koto.sarake.ratkaisuehdotus", fi = "Ratkaisuehdotus")
            val virheellinenKentta: LocalizedString
                get() = tr("koto.sarake.virheellinenKentta", fi = "Virheellinen kenttä")
            val virheellinenArvo: LocalizedString
                get() = tr("koto.sarake.virheellinenArvo", fi = "Virheellinen arvo")
            val valmis: LocalizedString get() = tr("koto.sarake.valmis", fi = "Valmis")
        }

        object Tehtavatyyppi {
            val monivalinta: LocalizedString get() = tr("koto.tehtavatyyppi.monivalinta", fi = "Monivalinta")
            val tosiEpatosi: LocalizedString get() = tr("koto.tehtavatyyppi.tosiEpatosi", fi = "Tosi/epätosi")
            val lyhytVastaus: LocalizedString get() = tr("koto.tehtavatyyppi.lyhytVastaus", fi = "Lyhyt vastaus")
            val numeerinenVastaus: LocalizedString
                get() = tr("koto.tehtavatyyppi.numeerinenVastaus", fi = "Numeerinen vastaus")
            val essee: LocalizedString get() = tr("koto.tehtavatyyppi.essee", fi = "Esseetehtävä")
            val yhdistaminen: LocalizedString get() = tr("koto.tehtavatyyppi.yhdistaminen", fi = "Yhdistämistehtävä")
            val cloze: LocalizedString
                get() = tr("koto.tehtavatyyppi.cloze", fi = "Sulautetut vastaukset (Cloze)")
            val lasku: LocalizedString get() = tr("koto.tehtavatyyppi.lasku", fi = "Laskutehtävä")
            val monivalintaLasku: LocalizedString
                get() = tr("koto.tehtavatyyppi.monivalintaLasku", fi = "Monivalinta-laskutehtävä")
            val yksinkertainenLasku: LocalizedString
                get() = tr("koto.tehtavatyyppi.yksinkertainenLasku", fi = "Yksinkertainen laskutehtävä")
            val ohjeteksti: LocalizedString get() = tr("koto.tehtavatyyppi.ohjeteksti", fi = "Ohjeteksti")
            val vetaPudotaTeksti: LocalizedString
                get() = tr("koto.tehtavatyyppi.vetaPudotaTeksti", fi = "Vedä ja pudota tekstiin")
            val vetaPudotaMerkit: LocalizedString
                get() = tr("koto.tehtavatyyppi.vetaPudotaMerkit", fi = "Vedä ja pudota merkit")
            val vetaPudotaKuva: LocalizedString
                get() = tr("koto.tehtavatyyppi.vetaPudotaKuva", fi = "Vedä ja pudota kuvaan")
            val valitsePuuttuvat: LocalizedString
                get() = tr("koto.tehtavatyyppi.valitsePuuttuvat", fi = "Valitse puuttuvat sanat")
            val satunnais: LocalizedString get() = tr("koto.tehtavatyyppi.satunnais", fi = "Satunnaistehtävä")
            val satunnainenLyhytYhdistaminen: LocalizedString
                get() = tr("koto.tehtavatyyppi.satunnainenLyhytYhdistaminen", fi = "Satunnainen lyhyt yhdistäminen")
            val puuttuvaTyyppi: LocalizedString get() = tr("koto.tehtavatyyppi.puuttuvaTyyppi", fi = "Puuttuva tyyppi")
            val aaninauhoitus: LocalizedString get() = tr("koto.tehtavatyyppi.aaninauhoitus", fi = "Ääninauhoitus")
            val aaniVideonauhoitus: LocalizedString
                get() = tr("koto.tehtavatyyppi.aaniVideonauhoitus", fi = "Ääni- tai videonauhoitus")
            val hahmonsovitus: LocalizedString get() = tr("koto.tehtavatyyppi.hahmonsovitus", fi = "Hahmonsovitus")
            val kemiallinenKaava: LocalizedString
                get() = tr("koto.tehtavatyyppi.kemiallinenKaava", fi = "Kemiallisen kaavan sovitus")
            val ohjelmointi: LocalizedString get() = tr("koto.tehtavatyyppi.ohjelmointi", fi = "Ohjelmointitehtävä")
            val stack: LocalizedString
                get() = tr("koto.tehtavatyyppi.stack", fi = "Matemaattinen tehtävä (STACK)")
            val jarjestaminen: LocalizedString
                get() = tr("koto.tehtavatyyppi.jarjestaminen", fi = "Järjestämistehtävä")
            val yhdistelma: LocalizedString get() = tr("koto.tehtavatyyppi.yhdistelma", fi = "Yhdistelmätehtävä")
            val kaava: LocalizedString get() = tr("koto.tehtavatyyppi.kaava", fi = "Kaavatehtävä")
            val aukko: LocalizedString get() = tr("koto.tehtavatyyppi.aukko", fi = "Aukkotehtävä")
            val saannollinenLauseke: LocalizedString
                get() = tr("koto.tehtavatyyppi.saannollinenLauseke", fi = "Säännöllinen lauseke")
            val puhetehtava: LocalizedString
                get() = tr("koto.tehtavatyyppi.puhetehtava", fi = "Automaattisesti arvioitu puhetehtävä")
            val ristikko: LocalizedString get() = tr("koto.tehtavatyyppi.ristikko", fi = "Ristikkotehtävä")
            val piirto: LocalizedString get() = tr("koto.tehtavatyyppi.piirto", fi = "Piirtotehtävä")
        }

        object Kieli {
            val fin: LocalizedString get() = tr("koto.kieli.fin", fi = "suomi")
            val swe: LocalizedString get() = tr("koto.kieli.swe", fi = "ruotsi")
            val eng: LocalizedString get() = tr("koto.kieli.eng", fi = "englanti")
            val rus: LocalizedString get() = tr("koto.kieli.rus", fi = "venäjä")
            val est: LocalizedString get() = tr("koto.kieli.est", fi = "viro")
            val ara: LocalizedString get() = tr("koto.kieli.ara", fi = "arabia")
            val fas: LocalizedString get() = tr("koto.kieli.fas", fi = "persia")
            val som: LocalizedString get() = tr("koto.kieli.som", fi = "somali")
            val ukr: LocalizedString get() = tr("koto.kieli.ukr", fi = "ukraina")
        }

        object Metatieto {
            val piilotettu: LocalizedString get() = tr("koto.metatieto.piilotettu", fi = "Piilotettu")
            val vainYksiVastaus: LocalizedString
                get() = tr("koto.metatieto.vainYksiVastaus", fi = "Vain yksi vastaus")
            val rangaistuskerroin: LocalizedString
                get() = tr("koto.metatieto.rangaistuskerroin", fi = "Rangaistuskerroin")
            val oletuspistemaara: LocalizedString
                get() = tr("koto.metatieto.oletuspistemaara", fi = "Oletuspistemäärä")
            val sekoitaVastaukset: LocalizedString
                get() = tr("koto.metatieto.sekoitaVastaukset", fi = "Sekoita vastaukset")
            val vastauksenNumeroiminen: LocalizedString
                get() = tr("koto.metatieto.vastauksenNumeroiminen", fi = "Vastauksen numeroiminen")
            val palauteOikeasta: LocalizedString
                get() = tr("koto.metatieto.palauteOikeasta", fi = "Palaute oikeasta vastauksesta")
            val yleispalaute: LocalizedString get() = tr("koto.metatieto.yleispalaute", fi = "Yleispalaute")
            val palauteVaarasta: LocalizedString
                get() = tr("koto.metatieto.palauteVaarasta", fi = "Palaute väärästä vastauksesta")
            val naytaVakioOhje: LocalizedString get() = tr("koto.metatieto.naytaVakioOhje", fi = "Näytä vakio-ohje")
            val palauteOsittain: LocalizedString
                get() = tr("koto.metatieto.palauteOsittain", fi = "Palaute osittain oikeasta vastauksesta")
            val vastausmuoto: LocalizedString get() = tr("koto.metatieto.vastausmuoto", fi = "Vastausmuoto")
            val vastauskentanRivimaara: LocalizedString
                get() = tr("koto.metatieto.vastauskentanRivimaara", fi = "Vastauskentän rivimäärä")
            val vastausPakollinen: LocalizedString
                get() = tr("koto.metatieto.vastausPakollinen", fi = "Vastaus pakollinen")
            val vastauspohja: LocalizedString get() = tr("koto.metatieto.vastauspohja", fi = "Vastauspohja")
            val sanamaaranEnimmais: LocalizedString
                get() = tr("koto.metatieto.sanamaaranEnimmais", fi = "Sanamäärän enimmäisraja")
            val sanamaaranVahimmais: LocalizedString
                get() = tr("koto.metatieto.sanamaaranVahimmais", fi = "Sanamäärän vähimmäisraja")
            val liitteidenMaara: LocalizedString
                get() = tr("koto.metatieto.liitteidenMaara", fi = "Liitteiden sallittu määrä")
            val vaadittavatLiitteet: LocalizedString
                get() = tr("koto.metatieto.vaadittavatLiitteet", fi = "Vaadittavat liitteet")
            val tiedostonEnimmaiskoko: LocalizedString
                get() = tr("koto.metatieto.tiedostonEnimmaiskoko", fi = "Tiedoston enimmäiskoko")
            val eiAanenSuodattimia: LocalizedString
                get() = tr("koto.metatieto.eiAanenSuodattimia", fi = "Ei äänen suodattimia")
            val litteroija: LocalizedString
                get() = tr("koto.metatieto.litteroija", fi = "Puheentunnistus / litteroija")
            val koodausMuunnos: LocalizedString get() = tr("koto.metatieto.koodausMuunnos", fi = "Koodaus / muunnos")
            val aanisoittimenTeema: LocalizedString
                get() = tr("koto.metatieto.aanisoittimenTeema", fi = "Äänisoittimen teema")
            val videosoittimenTeema: LocalizedString
                get() = tr("koto.metatieto.videosoittimenTeema", fi = "Videosoittimen teema")
            val opiskelijanSoitin: LocalizedString
                get() = tr("koto.metatieto.opiskelijanSoitin", fi = "Opiskelijan soitin")
            val opettajanSoitin: LocalizedString
                get() = tr("koto.metatieto.opettajanSoitin", fi = "Opettajan soitin")
            val aikaraja: LocalizedString get() = tr("koto.metatieto.aikaraja", fi = "Aikaraja")
            val vanhentumispaivat: LocalizedString
                get() = tr("koto.metatieto.vanhentumispaivat", fi = "Vanhentumispäivät")
            val tunnisteet: LocalizedString get() = tr("koto.metatieto.tunnisteet", fi = "Tunnisteet")
            val turvallinenTallennus: LocalizedString
                get() = tr("koto.metatieto.turvallinenTallennus", fi = "Turvallinen tallennus")
            val kayttotarkoitus: LocalizedString
                get() = tr("koto.metatieto.kayttotarkoitus", fi = "Käyttötarkoitus")
            val arviointiohjeet: LocalizedString
                get() = tr("koto.metatieto.arviointiohjeet", fi = "Arviointiohjeet")
        }
    }

    object Time {
        val juuriNyt: LocalizedString get() = tr("time.juuriNyt", fi = "juuri nyt")
        val eilen: LocalizedString get() = tr("time.eilen", fi = "eilen")

        fun minuuttiaSitten(count: Long) =
            tr("time.minuuttiaSitten", fi = "{count} min sitten").interpolate("count" to count)

        fun tuntiaSitten(count: Long) = tr("time.tuntiaSitten", fi = "{count} t sitten").interpolate("count" to count)

        fun paivaaSitten(count: Long) = tr("time.paivaaSitten", fi = "{count} pv sitten").interpolate("count" to count)
    }

    object Filter {
        val aikarajausPrefix: LocalizedString get() = tr("filter.aikarajausPrefix", fi = "Aikarajaus")
        val rajaaNaytettavat: LocalizedString get() = tr("filter.rajaaNaytettavat", fi = "Rajaa näytettävät tiedot")
        val tiedonRajaus: LocalizedString get() = tr("filter.tiedonRajaus", fi = "Tiedon rajaus")
        val rajaa: LocalizedString get() = tr("filter.rajaa", fi = "Rajaa")
        val peruuta: LocalizedString get() = tr("filter.peruuta", fi = "Peruuta")
        val kaikki: LocalizedString get() = tr("filter.kaikki", fi = "Kaikki")
        val kylla: LocalizedString get() = tr("filter.kylla", fi = "Kyllä")
        val ei: LocalizedString get() = tr("filter.ei", fi = "Ei")
        val piilotaHenkilotiedot: LocalizedString get() =
            tr(
                "filter.piilotaHenkilotiedot",
                fi = "Piilota henkilötiedot",
            )
        val henkilotiedotPiilotettu: LocalizedString
            get() = tr("filter.henkilotiedotPiilotettu", fi = "Henkilötiedot piilotettu")
        val naytettavatSuoritukset: LocalizedString
            get() = tr("filter.naytettavatSuoritukset", fi = "Näytettävät suoritukset")
        val valmiit: LocalizedString get() = tr("filter.valmiit", fi = "Valmiit")
        val keskeneraiset: LocalizedString get() = tr("filter.keskeneraiset", fi = "Keskeneräiset")
    }

    object Form {
        val tarkistaTiedot: LocalizedString get() = tr("form.tarkistaTiedot", fi = "Tarkista lomakkeen tiedot")
    }

    object Toiminto {
        val nayta: LocalizedString get() = tr("toiminto.nayta", fi = "Näytä")
        val palauta: LocalizedString get() = tr("toiminto.palauta", fi = "Palauta")
        val piilota: LocalizedString get() = tr("toiminto.piilota", fi = "Piilota")
    }

    object Sukupuoli {
        val mies: LocalizedString get() = tr("sukupuoli.mies", fi = "Mies")
        val nainen: LocalizedString get() = tr("sukupuoli.nainen", fi = "Nainen")
        val eiTiedossa: LocalizedString get() = tr("sukupuoli.eiTiedossa", fi = "Ei tiedossa")
    }
}

private fun tr(
    key: String,
    fi: String,
): LocalizedString {
    UiTextRegistry.record(key, fi)
    return LocalizedString.withTolgeeKey(key, fi)
}
