package fi.oph.kitu.i18n

object UiText {
    val appTitle: LocalizedString get() = tr("appTitle")

    object Nav {
        val yki: LocalizedString get() = tr("nav.yki")
        val kotoutumiskoulutuksenPaattotesti: LocalizedString
            get() = tr("nav.kotoutumiskoulutuksenPaattotesti")
        val vkt: LocalizedString get() = tr("nav.vkt")
        val yllapito: LocalizedString get() = tr("nav.yllapito")

        val suoritukset: LocalizedString get() = tr("nav.suoritukset")
        val arvioijat: LocalizedString get() = tr("nav.arvioijat")
        val tarkistusarvioinnit: LocalizedString get() = tr("nav.tarkistusarvioinnit")
        val tehtavapaketit: LocalizedString get() = tr("nav.tehtavapaketit")
        val kaikkiSuoritukset: LocalizedString get() = tr("nav.kaikkiSuoritukset")
        val erinomaisenTaidonIlmoittautuneet: LocalizedString
            get() = tr("nav.erinomaisenTaidonIlmoittautuneet")
        val erinomaisenTaidonSuoritukset: LocalizedString
            get() = tr("nav.erinomaisenTaidonSuoritukset")
        val hyvanJaTyydyttavanSuoritukset: LocalizedString
            get() = tr("nav.hyvanJaTyydyttavanSuoritukset")
        val erajojenHallinta: LocalizedString get() = tr("nav.erajojenHallinta")
        val historiaSiirtymattomat: LocalizedString
            get() = tr("nav.historiaSiirtymattomat")
    }

    object Etusivu {
        val arvioijienSolkiVirheet: LocalizedString
            get() = tr("etusivu.arvioijienSolkiVirheet")
        val koskiSiirronVirheet: LocalizedString
            get() = tr("etusivu.koskiSiirronVirheet")
        val tuonninVirheet: LocalizedString get() = tr("etusivu.tuonninVirheet")
        val kaynnissaOlevatErajot: LocalizedString
            get() = tr("etusivu.kaynnissaOlevatErajot")
        val erajotVirhetilassa: LocalizedString
            get() = tr("etusivu.erajotVirhetilassa")
        val viimeisinSaapunutSuoritus: LocalizedString
            get() = tr("etusivu.viimeisinSaapunutSuoritus")
        val tyhjennaKaannosvalimuisti: LocalizedString
            get() = tr("etusivu.tyhjennaKaannosvalimuisti")
    }

    object Error {
        val internalServerError: LocalizedString get() = tr("error.internalServerError")
        val sivuaEiLoydy: LocalizedString get() = tr("error.sivuaEiLoydy")
        val virheellinenPyynto: LocalizedString get() = tr("error.virheellinenPyynto")
        val virheellinenPyyntoOhje: LocalizedString
            get() =
                tr("error.virheellinenPyyntoOhje")
        val eiKayttooikeuksia: LocalizedString get() =
            tr("error.eiKayttooikeuksia")
        val katsoVirheet: LocalizedString get() = tr("error.katsoVirheet")
        val jaljitystunniste: LocalizedString get() = tr("error.jaljitystunniste")
        val oppijaEiLoydyOnr: LocalizedString
            get() = tr("error.oppijaEiLoydyOnr")
        val oppijanHakuOnrEpaonnistui: LocalizedString
            get() =
                tr("error.oppijanHakuOnrEpaonnistui")

        fun jarjestelmassaVirheita(count: Long) = tr("error.jarjestelmassaVirheita").interpolate("count" to count)

        fun koskiSiirtoEpaonnistunut(count: Long) =
            tr("error.koskiSiirtoEpaonnistunut")
                .interpolate("count" to count)
    }

    object Vkt {
        val yhteensa: LocalizedString get() = tr("vkt.yhteensa")
        val integraatiot: LocalizedString get() = tr("vkt.integraatiot")
        val tiedoissaPuutteita: LocalizedString
            get() =
                tr("vkt.tiedoissaPuutteita")
        val siirtoAjastettu: LocalizedString
            get() = tr("vkt.siirtoAjastettu")
        val tiedotSiirretty: LocalizedString
            get() = tr("vkt.tiedotSiirretty")
        val tiedonsiirtotilaVirheellinen: LocalizedString
            get() = tr("vkt.tiedonsiirtotilaVirheellinen")
        val opiskeluoikeudenOid: LocalizedString get() = tr("vkt.opiskeluoikeudenOid")
        val tutkinnot: LocalizedString get() = tr("vkt.tutkinnot")
        val osakokeet: LocalizedString get() = tr("vkt.osakokeet")
        val koskiTiedonsiirtovirheet: LocalizedString
            get() = tr("vkt.koskiTiedonsiirtovirheet")
        val naytaJson: LocalizedString get() = tr("vkt.naytaJson")
        val yksiloity: LocalizedString get() = tr("vkt.yksiloity")
        val yksilointiaYritetty: LocalizedString get() = tr("vkt.yksilointiaYritetty")
        val eiYksiloity: LocalizedString get() = tr("vkt.eiYksiloity")
        val suodata: LocalizedString get() = tr("vkt.suodata")

        val arvioinninTila: LocalizedString get() = tr("vkt.arvioinninTila")
        val vainPoistettavat: LocalizedString get() = tr("vkt.vainPoistettavat")
        val vainEiPoistettavat: LocalizedString
            get() =
                tr("vkt.vainEiPoistettavat")
        val arvioituOsittain: LocalizedString
            get() = tr("vkt.arvioituOsittain")
        val arviointejaPuuttuu: LocalizedString get() = tr("vkt.arviointejaPuuttuu")

        val alkaen: LocalizedString get() = tr("vkt.alkaen")
        val paattyen: LocalizedString get() = tr("vkt.paattyen")
        val erinomaisenArvioinninTila: LocalizedString
            get() = tr("vkt.erinomaisenArvioinninTila")
        val poistettavaksiMerkitty: LocalizedString
            get() = tr("vkt.poistettavaksiMerkitty")
        val naytaKaikki: LocalizedString get() = tr("vkt.naytaKaikki")
        val naytaVainPoistettavat: LocalizedString
            get() = tr("vkt.naytaVainPoistettavat")
        val piilotaPoistettavat: LocalizedString
            get() = tr("vkt.piilotaPoistettavat")
        val oppijanumeroTaiNimi: LocalizedString get() = tr("vkt.oppijanumeroTaiNimi")
        val tutkinnonTaso: LocalizedString get() = tr("vkt.tutkinnonTaso")
        val kieli: LocalizedString get() = tr("vkt.kieli")
        val koski: LocalizedString get() = tr("vkt.koski")
        val tutkinto: LocalizedString get() = tr("vkt.tutkinto")
        val arvosana: LocalizedString get() = tr("vkt.arvosana")
        val arviointipaiva: LocalizedString get() = tr("vkt.arviointipaiva")
        val arviointiPuuttuu: LocalizedString get() = tr("vkt.arviointiPuuttuu")
        val arvioinnitPuuttuvat: LocalizedString get() = tr("vkt.arvioinnitPuuttuvat")
        val osakoePuuttuu: LocalizedString get() = tr("vkt.osakoePuuttuu")
        val henkilotunnus: LocalizedString get() = tr("vkt.henkilotunnus")
        val henkiloOid: LocalizedString get() = tr("vkt.henkiloOid")
        val syntymaaika: LocalizedString get() = tr("vkt.syntymaaika")
        val yksilointi: LocalizedString get() = tr("vkt.yksilointi")
        val erinomainen: LocalizedString get() = tr("vkt.erinomainen")
        val hylatty: LocalizedString get() = tr("vkt.hylatty")
        val eiSuoritusta: LocalizedString get() = tr("vkt.eiSuoritusta")
        val osakoe: LocalizedString get() = tr("vkt.osakoe")
        val muutoksetTallennettu: LocalizedString
            get() = tr("vkt.muutoksetTallennettu")
        val naytaVirheet: LocalizedString get() = tr("vkt.naytaVirheet")
        val merkittyKasitellyksiEiOid: LocalizedString
            get() =
                tr("vkt.merkittyKasitellyksiEiOid")

        object Sarake {
            val ilmoittautumisenTunniste: LocalizedString
                get() = tr("vkt.sarake.ilmoittautumisenTunniste")
            val sukunimi: LocalizedString get() = tr("vkt.sarake.sukunimi")
            val etunimet: LocalizedString get() = tr("vkt.sarake.etunimet")
            val oppijanumero: LocalizedString get() = tr("vkt.sarake.oppijanumero")
            val taitotaso: LocalizedString get() = tr("vkt.sarake.taitotaso")
            val tutkintokieli: LocalizedString get() = tr("vkt.sarake.tutkintokieli")
            val tutkintopaiva: LocalizedString get() = tr("vkt.sarake.tutkintopaiva")
            val suorituspaikkakunta: LocalizedString
                get() = tr("vkt.sarake.suorituspaikkakunta")
            val vastaanottajanOid: LocalizedString
                get() = tr("vkt.sarake.vastaanottajanOid")
            val vastaanottaja: LocalizedString
                get() = tr("vkt.sarake.vastaanottaja")
            val puhuminen: LocalizedString get() = tr("vkt.sarake.puhuminen")
            val puheenYmmartaminen: LocalizedString
                get() = tr("vkt.sarake.puheenYmmartaminen")
            val kirjoittaminen: LocalizedString get() = tr("vkt.sarake.kirjoittaminen")
            val tekstinYmmartaminen: LocalizedString
                get() = tr("vkt.sarake.tekstinYmmartaminen")

            val tutkintoryhma: LocalizedString
                get() = tr("vkt.sarake.tutkintoryhma")
            val virhe: LocalizedString get() = tr("vkt.sarake.virhe")
            val aikaleima: LocalizedString get() = tr("vkt.sarake.aikaleima")
            val pyynto: LocalizedString get() = tr("vkt.sarake.pyynto")
            val piilotus: LocalizedString get() = tr("vkt.sarake.piilotus")
        }
    }

    object Yki {
        val suodata: LocalizedString get() = tr("yki.suodata")
        val arvioijiaYhteensa: LocalizedString get() = tr("yki.arvioijiaYhteensa")
        val hakusanaArvioija: LocalizedString
            get() = tr("yki.hakusanaArvioija")
        val lisaaArvioija: LocalizedString get() = tr("yki.lisaaArvioija")
        val odottaaLahetysta: LocalizedString get() = tr("yki.odottaaLahetysta")
        val solkiLahetysOnnistui: LocalizedString get() = tr("yki.solkiLahetysOnnistui")
        val solkiLahetysEpaonnistui: LocalizedString
            get() = tr("yki.solkiLahetysEpaonnistui")
        val solkiLahetystenVirheet: LocalizedString
            get() = tr("yki.solkiLahetystenVirheet")
        val suorituksiaYhteensa: LocalizedString get() = tr("yki.suorituksiaYhteensa")
        val tarkistusarvioinnit: LocalizedString
            get() = tr("yki.tarkistusarvioinnit")
        val naytaHyvaksytyt: LocalizedString
            get() = tr("yki.naytaHyvaksytyt")
        val takaisinOdottaviin: LocalizedString
            get() = tr("yki.takaisinOdottaviin")
        val tutkintotoimikunnanKokous: LocalizedString
            get() = tr("yki.tutkintotoimikunnanKokous")
        val naytaUusinVersio: LocalizedString get() = tr("yki.naytaUusinVersio")
        val henkilotiedot: LocalizedString get() = tr("yki.henkilotiedot")
        val teeYksilointi: LocalizedString
            get() = tr("yki.teeYksilointi")
        val todistuksenPostitusosoite: LocalizedString
            get() = tr("yki.todistuksenPostitusosoite")
        val tutkinnonTiedot: LocalizedString get() = tr("yki.tutkinnonTiedot")
        val arviointi: LocalizedString get() = tr("yki.arviointi")
        val integraatiot: LocalizedString get() = tr("yki.integraatiot")
        val siirrettyKoski: LocalizedString get() = tr("yki.siirrettyKoski")
        val odottaaSiirtoa: LocalizedString
            get() = tr("yki.odottaaSiirtoa")
        val opiskeluoikeudenOid: LocalizedString get() = tr("yki.opiskeluoikeudenOid")
        val arviointitilaLahetetty: LocalizedString
            get() = tr("yki.arviointitilaLahetetty")
        val koskiTiedonsiirtovirheet: LocalizedString
            get() = tr("yki.koskiTiedonsiirtovirheet")
        val naytaJson: LocalizedString get() = tr("yki.naytaJson")

        val henkiloOid: LocalizedString get() = tr("yki.henkiloOid")
        val katuosoite: LocalizedString get() = tr("yki.katuosoite")
        val postinumero: LocalizedString get() = tr("yki.postinumero")
        val postitoimipaikka: LocalizedString get() = tr("yki.postitoimipaikka")
        val maa: LocalizedString get() = tr("yki.maa")
        val todistuksenKieli: LocalizedString get() = tr("yki.todistuksenKieli")
        val jarjestaja: LocalizedString get() = tr("yki.jarjestaja")
        val arvioinninTila: LocalizedString get() = tr("yki.arvioinninTila")
        val tarkistusarvioinninSaapumispaiva: LocalizedString
            get() = tr("yki.tarkistusarvioinninSaapumispaiva")
        val tarkistusarvioinninAsiatunnus: LocalizedString
            get() = tr("yki.tarkistusarvioinninAsiatunnus")
        val tarkistusarvioinninKasittelypaiva: LocalizedString
            get() = tr("yki.tarkistusarvioinninKasittelypaiva")
        val tarkistusarvioidutOsakokeet: LocalizedString
            get() = tr("yki.tarkistusarvioidutOsakokeet")
        val perustelu: LocalizedString get() = tr("yki.perustelu")
        val viimeksiMuokattu: LocalizedString get() = tr("yki.viimeksiMuokattu")
        val koski: LocalizedString get() = tr("yki.koski")
        val koskiVirheet: LocalizedString get() = tr("yki.koskiVirheet")
        val kios: LocalizedString get() = tr("yki.kios")
        val kiosVirhe: LocalizedString get() = tr("yki.kiosVirhe")

        val tutkintopaivaAlkaen: LocalizedString get() = tr("yki.tutkintopaivaAlkaen")
        val tutkintopaivaPaattyen: LocalizedString
            get() = tr("yki.tutkintopaivaPaattyen")
        val naytaVersiohistoria: LocalizedString get() = tr("yki.naytaVersiohistoria")

        val hakusana: LocalizedString
            get() = tr("yki.hakusana")
        val vanhentuneetPiilotettu: LocalizedString
            get() = tr("yki.vanhentuneetPiilotettu")
        val piilotaVanhentuneet: LocalizedString
            get() = tr("yki.piilotaVanhentuneet")
        val odottavatHyvaksyntaa: LocalizedString
            get() = tr("yki.odottavatHyvaksyntaa")
        val merkitseHyvaksynta: LocalizedString
            get() = tr("yki.merkitseHyvaksynta")
        val hyvaksytytTarkistusarvioinnit: LocalizedString
            get() = tr("yki.hyvaksytytTarkistusarvioinnit")
        val korjaaHyvaksymispaiva: LocalizedString
            get() = tr("yki.korjaaHyvaksymispaiva")
        val suoritustenTuonninVirheet: LocalizedString
            get() = tr("yki.suoritustenTuonninVirheet")
        val siirtoaEiTehda: LocalizedString get() = tr("yki.siirtoaEiTehda")

        val suoritustaEdeltavaEiLaheteta: LocalizedString
            get() = tr("yki.suoritustaEdeltavaEiLaheteta")
        val arviointitilaaEiLahetetty: LocalizedString
            get() = tr("yki.arviointitilaaEiLahetetty")
        val saapunut: LocalizedString get() = tr("yki.saapunut")
        val kasitelty: LocalizedString get() = tr("yki.kasitelty")
        val hyvaksytty: LocalizedString get() = tr("yki.hyvaksytty")
        val arvosanaMuuttui: LocalizedString get() = tr("yki.arvosanaMuuttui")
        val arvosanaEiMuuttunut: LocalizedString get() = tr("yki.arvosanaEiMuuttunut")
        val onrEiYhteytta: LocalizedString
            get() =
                tr("yki.onrEiYhteytta")
        val ilmoittautumisenTiedot: LocalizedString get() =
            tr("yki.ilmoittautumisenTiedot")
        val oppijanumerorekisteri: LocalizedString get() = tr("yki.oppijanumerorekisteri")

        object Arviointitila {
            val ilmoittautunut: LocalizedString get() = tr("yki.arviointitila.ilmoittautunut")
            val ilmoittautuminenPeruttu: LocalizedString
                get() = tr("yki.arviointitila.ilmoittautuminenPeruttu")
            val eiSuoritusta: LocalizedString get() = tr("yki.arviointitila.eiSuoritusta")
            val suoritusArvioitavana: LocalizedString
                get() = tr("yki.arviointitila.suoritusArvioitavana")
            val arviointiValmis: LocalizedString
                get() = tr("yki.arviointitila.arviointiValmis")
            val suoritusTarkistusarvioitavana: LocalizedString
                get() = tr("yki.arviointitila.suoritusTarkistusarvioitavana")
            val tarkistusarviointiTehty: LocalizedString
                get() = tr("yki.arviointitila.tarkistusarviointiTehty")
            val tarkistusarviointiHyvaksytty: LocalizedString
                get() = tr("yki.arviointitila.tarkistusarviointiHyvaksytty")
        }

        object ArvioijaTila {
            val aktiivinen: LocalizedString get() = tr("yki.arvioijaTila.aktiivinen")
            val passivoitu: LocalizedString get() = tr("yki.arvioijaTila.passivoitu")
            val tulevaisuudessa: LocalizedString
                get() = tr("yki.arvioijaTila.tulevaisuudessa")
        }

        object Arvioija {
            val uusiArvioija: LocalizedString get() = tr("yki.arvioija.uusiArvioija")
            val haeHenkilonTiedot: LocalizedString
                get() = tr("yki.arvioija.haeHenkilonTiedot")
            val hakuOhjeOppijanumero: LocalizedString
                get() =
                    tr("yki.arvioija.hakuOhjeOppijanumero")
            val sukunimi: LocalizedString get() = tr("yki.arvioija.sukunimi")
            val etunimet: LocalizedString get() = tr("yki.arvioija.etunimet")
            val oppijanumero: LocalizedString get() = tr("yki.arvioija.oppijanumero")
            val sahkopostiosoite: LocalizedString
                get() = tr("yki.arvioija.sahkopostiosoite")
            val katuosoite: LocalizedString get() = tr("yki.arvioija.katuosoite")
            val postinumero: LocalizedString get() = tr("yki.arvioija.postinumero")
            val postitoimipaikka: LocalizedString
                get() = tr("yki.arvioija.postitoimipaikka")
            val yhteystiedot: LocalizedString get() = tr("yki.arvioija.yhteystiedot")
            val rekisterimerkinta: LocalizedString
                get() = tr("yki.arvioija.rekisterimerkinta")
            val kaudenAlkupaiva: LocalizedString get() = tr("yki.arvioija.kaudenAlkupaiva")
            val kaudenPaattymispaiva: LocalizedString
                get() = tr("yki.arvioija.kaudenPaattymispaiva")
            val kaudenPaattymispaivaOhje: LocalizedString
                get() =
                    tr("yki.arvioija.kaudenPaattymispaivaOhje")
            val jatkorekisterointi: LocalizedString
                get() = tr("yki.arvioija.jatkorekisterointi")
            val ashaNumero: LocalizedString
                get() = tr("yki.arvioija.ashaNumero")
            val arviointioikeudet: LocalizedString
                get() = tr("yki.arvioija.arviointioikeudet")
            val arviointioikeudetOhje: LocalizedString
                get() =
                    tr("yki.arvioija.arviointioikeudetOhje")
            val tutkintokieli: LocalizedString get() = tr("yki.arvioija.tutkintokieli")
            val tallenna: LocalizedString get() = tr("yki.arvioija.tallenna")
            val muokkaa: LocalizedString get() = tr("yki.arvioija.muokkaa")
            val muokkaaArvioijaa: LocalizedString
                get() = tr("yki.arvioija.muokkaaArvioijaa")
            val tallennaMuutokset: LocalizedString
                get() = tr("yki.arvioija.tallennaMuutokset")
            val muutoksetTallennettu: LocalizedString
                get() = tr("yki.arvioija.muutoksetTallennettu")
            val peruuta: LocalizedString get() = tr("yki.arvioija.peruuta")
            val jorekisterissa: LocalizedString
                get() =
                    tr("yki.arvioija.joRekisterissa")
            val muokattuSamanaikaisesti: LocalizedString
                get() =
                    tr("yki.arvioija.muokattuSamanaikaisesti")
            val kausihistoria: LocalizedString
                get() = tr("yki.arvioija.arviointikaudet")
            val kirjattu: LocalizedString get() = tr("yki.arvioija.kirjattu")
            val kirjaaja: LocalizedString get() = tr("yki.arvioija.kirjaaja")
            val jarjestelma: LocalizedString get() = tr("yki.arvioija.jarjestelma")
            val eiMuutoshistoriaa: LocalizedString
                get() = tr("yki.arvioija.eiMuutoshistoriaa")
            val passivoi: LocalizedString get() = tr("yki.arvioija.passivoi")
            val passivoiVahvistus: LocalizedString
                get() =
                    tr("yki.arvioija.passivoiVahvistus")
            val passivoitu: LocalizedString
                get() = tr("yki.arvioija.passivoitu")
            val solkiinLahetetty: LocalizedString
                get() = tr("yki.arvioija.solkiinLahetetty")
            val solkiLahetysyritykset: LocalizedString
                get() = tr("yki.arvioija.solkiLahetysyritykset")
            val lahetaUudelleen: LocalizedString
                get() = tr("yki.arvioija.lahetaUudelleen")
            val lahetysjonossa: LocalizedString
                get() = tr("yki.arvioija.lahetysjonossa")
            val solkiTunnus: LocalizedString
                get() = tr("yki.arvioija.solkiTunnus")
            val solkiTunnusEiTiedossa: LocalizedString
                get() = tr("yki.arvioija.solkiTunnusEiTiedossa")
            val lahetysOnnistui: LocalizedString
                get() = tr("yki.arvioija.lahetysOnnistui")
            val lahetysEpaonnistui: LocalizedString
                get() = tr("yki.arvioija.lahetysEpaonnistui")
            val lahetysEiKaytossa: LocalizedString
                get() =
                    tr("yki.arvioija.lahetysEiKaytossa")
            val kausiPaattynyt: LocalizedString
                get() =
                    tr("yki.arvioija.kausiPaattynyt")
            val joPassivoitu: LocalizedString
                get() =
                    tr("yki.arvioija.joPassivoitu")
            val automaattilahetysEiKaytossa: LocalizedString
                get() =
                    tr("yki.arvioija.automaattilahetysEiKaytossa")
            val kirjoitusEiKaytossa: LocalizedString
                get() =
                    tr("yki.arvioija.kirjoitusEiKaytossa")
            val tallennettu: LocalizedString
                get() = tr("yki.arvioija.tallennettu")
            val turvakielto: LocalizedString
                get() =
                    tr("yki.arvioija.turvakielto")
            val turvakieltoEiTiedossa: LocalizedString
                get() =
                    tr("yki.arvioija.turvakieltoEiTiedossa")
            val eiYksiloity: LocalizedString
                get() =
                    tr("yki.arvioija.eiYksiloity")
            val onrEiVastannut: LocalizedString
                get() =
                    tr("yki.arvioija.onrEiVastannut")
            val eiLoydy: LocalizedString get() = tr("yki.arvioija.eiLoydy")
            val eiLoytynytOnrista: LocalizedString
                get() =
                    tr("yki.arvioija.eiLoytynytOnrista")
            val takaisinListaan: LocalizedString
                get() = tr("yki.arvioija.takaisinListaan")

            object Kausi {
                val uusi: LocalizedString
                    get() = tr("yki.arvioija.arviointikausi.uusi")
                val muokkaa: LocalizedString get() = tr("yki.arvioija.arviointikausi.muokkaa")
                val passivoi: LocalizedString get() = tr("yki.arvioija.arviointikausi.passivoi")
                val poista: LocalizedString get() = tr("yki.arvioija.arviointikausi.poista")
                val peruuta: LocalizedString get() = tr("yki.arvioija.arviointikausi.peruuta")
                val tallenna: LocalizedString get() = tr("yki.arvioija.arviointikausi.tallenna")
                val toiminnot: LocalizedString get() = tr("yki.arvioija.arviointikausi.toiminnot")
                val vanhentuneet: LocalizedString
                    get() = tr("yki.arvioija.arviointikausi.vanhentuneet")
                val vanhentuneetOhje: LocalizedString
                    get() =
                        tr("yki.arvioija.kausi.vanhentuneetOhje")
                val toimenpide: LocalizedString get() = tr("yki.arvioija.arviointikausi.toimenpide")
                val lisays: LocalizedString get() = tr("yki.arvioija.arviointikausi.toimenpide.lisays")
                val muokkaus: LocalizedString get() =
                    tr("yki.arvioija.arviointikausi.toimenpide.muokkaus")
                val passivointi: LocalizedString
                    get() = tr("yki.arvioija.arviointikausi.toimenpide.passivointi")
                val poisto: LocalizedString get() = tr("yki.arvioija.arviointikausi.toimenpide.poisto")
                val tallennus: LocalizedString
                    get() = tr("yki.arvioija.arviointikausi.toimenpide.tallennus")
                val eiKausia: LocalizedString
                    get() = tr("yki.arvioija.arviointikausi.eiKausia")
                val muokkaaOtsikko: LocalizedString
                    get() = tr("yki.arvioija.arviointikausi.muokkaaOtsikko")
                val naytaMuutoshistoria: LocalizedString
                    get() = tr("yki.arvioija.arviointikausi.naytaMuutoshistoria")
                val poistaVahvistus: LocalizedString
                    get() =
                        tr("yki.arvioija.kausi.poistaVahvistus")
                val passivoiVahvistus: LocalizedString
                    get() =
                        tr("yki.arvioija.kausi.passivoiVahvistus")
                val lisatty: LocalizedString
                    get() = tr("yki.arvioija.arviointikausi.lisatty")
                val paivitetty: LocalizedString
                    get() = tr("yki.arvioija.arviointikausi.paivitetty")
                val passivoitu: LocalizedString
                    get() = tr("yki.arvioija.arviointikausi.passivoitu")
                val poistettu: LocalizedString
                    get() = tr("yki.arvioija.arviointikausi.poistettu")
                val eiAktiivinen: LocalizedString
                    get() =
                        tr("yki.arvioija.kausi.eiAktiivinen")
                val viimeistaEiVoiPoistaa: LocalizedString
                    get() =
                        tr("yki.arvioija.kausi.viimeistaEiVoiPoistaa")
            }
        }

        object Taso {
            val perustaso: LocalizedString get() = tr("yki.taso.perustaso")
            val keskitaso: LocalizedString get() = tr("yki.taso.keskitaso")
            val ylinTaso: LocalizedString get() = tr("yki.taso.ylinTaso")
        }

        object Kieli {
            val suomi: LocalizedString get() = tr("yki.kieli.suomi")
            val ruotsi: LocalizedString get() = tr("yki.kieli.ruotsi")
            val englanti: LocalizedString get() = tr("yki.kieli.englanti")
            val saksa: LocalizedString get() = tr("yki.kieli.saksa")
            val ranska: LocalizedString get() = tr("yki.kieli.ranska")
            val italia: LocalizedString get() = tr("yki.kieli.italia")
            val venaja: LocalizedString get() = tr("yki.kieli.venaja")
            val pohjoissaame: LocalizedString get() = tr("yki.kieli.pohjoissaame")
            val espanja: LocalizedString get() = tr("yki.kieli.espanja")
            val ruotsiVanha: LocalizedString get() = tr("yki.kieli.ruotsiVanha")
            val kaupallinenEnglanti: LocalizedString
                get() = tr("yki.kieli.kaupallinenEnglanti")
            val tekninenEnglanti: LocalizedString get() = tr("yki.kieli.tekninenEnglanti")
        }

        object Sarake {
            val oppijanumero: LocalizedString get() = tr("yki.sarake.oppijanumero")
            val sukunimi: LocalizedString get() = tr("yki.sarake.sukunimi")
            val etunimi: LocalizedString get() = tr("yki.sarake.etunimi")
            val etunimet: LocalizedString get() = tr("yki.sarake.etunimet")
            val sukupuoli: LocalizedString get() = tr("yki.sarake.sukupuoli")
            val henkilotunnus: LocalizedString get() = tr("yki.sarake.henkilotunnus")
            val kansalaisuus: LocalizedString get() = tr("yki.sarake.kansalaisuus")
            val osoite: LocalizedString get() = tr("yki.sarake.osoite")
            val sahkoposti: LocalizedString get() = tr("yki.sarake.sahkoposti")
            val tutkintopaiva: LocalizedString get() = tr("yki.sarake.tutkintopaiva")
            val tutkintokieli: LocalizedString get() = tr("yki.sarake.tutkintokieli")
            val tutkintotaso: LocalizedString get() = tr("yki.sarake.tutkintotaso")
            val kieli: LocalizedString get() = tr("yki.sarake.kieli")
            val taso: LocalizedString get() = tr("yki.sarake.taso")
            val jarjestajanOid: LocalizedString get() = tr("yki.sarake.jarjestajanOid")
            val jarjestajanNimi: LocalizedString get() = tr("yki.sarake.jarjestajanNimi")
            val arviointitila: LocalizedString get() = tr("yki.sarake.arviointitila")
            val arviointipaiva: LocalizedString get() = tr("yki.sarake.arviointipaiva")
            val tekstinYmmartaminen: LocalizedString
                get() = tr("yki.sarake.tekstinYmmartaminen")
            val kirjoittaminen: LocalizedString get() = tr("yki.sarake.kirjoittaminen")
            val puheenYmmartaminen: LocalizedString
                get() = tr("yki.sarake.puheenYmmartaminen")
            val puhuminen: LocalizedString get() = tr("yki.sarake.puhuminen")
            val rakenteetJaSanasto: LocalizedString
                get() = tr("yki.sarake.rakenteetJaSanasto")
            val yleisarvosana: LocalizedString get() = tr("yki.sarake.yleisarvosana")
            val todistuskieli: LocalizedString get() = tr("yki.sarake.todistuskieli")
            val tilaLahetetty: LocalizedString get() = tr("yki.sarake.tilaLahetetty")
            val opiskeluoikeusOid: LocalizedString get() = tr("yki.sarake.opiskeluoikeusOid")
            val solkiTunniste: LocalizedString get() = tr("yki.sarake.solkiTunniste")
            val versio: LocalizedString get() = tr("yki.sarake.versio")
            val tila: LocalizedString get() = tr("yki.sarake.tila")
            val tasot: LocalizedString get() = tr("yki.sarake.tasot")
            val kaudenAlkupaiva: LocalizedString get() = tr("yki.sarake.kaudenAlkupaiva")
            val kaudenPaattymispaiva: LocalizedString
                get() = tr("yki.sarake.kaudenPaattymispaiva")
            val jatkorekisterointi: LocalizedString
                get() = tr("yki.sarake.jatkorekisterointi")
            val rekisteriintuontiaika: LocalizedString
                get() = tr("yki.sarake.rekisteriintuontiaika")
            val ensimmainenRekisterointipaiva: LocalizedString
                get() = tr("yki.sarake.ensimmainenRekisterointipaiva")
            val ashaNumero: LocalizedString get() = tr("yki.sarake.ashaNumero")
            val solkiTila: LocalizedString get() = tr("yki.sarake.solkiTila")
            val muokattu: LocalizedString get() = tr("yki.sarake.muokattu")
            val solkiId: LocalizedString get() = tr("yki.sarake.solkiId")
            val kentta: LocalizedString get() = tr("yki.sarake.kentta")
            val paivamaara: LocalizedString get() = tr("yki.sarake.paivamaara")
            val asiatunnus: LocalizedString get() = tr("yki.sarake.asiatunnus")
            val tarkistusarviointi: LocalizedString
                get() = tr("yki.sarake.tarkistusarviointi")
            val tarkistusarvioinninSaapumispaiva: LocalizedString
                get() =
                    tr("yki.sarake.tarkistusarvioinninSaapumispaiva")
            val tarkistusarvioinninKasittelypaiva: LocalizedString
                get() =
                    tr("yki.sarake.tarkistusarvioinninKasittelypaiva")
            val tarkistusarviointiHyvaksytty: LocalizedString
                get() = tr("yki.sarake.tarkistusarviointiHyvaksytty")
            val tarkistusarvioidutOsakokeet: LocalizedString
                get() = tr("yki.sarake.tarkistusarvioidutOsakokeet")
            val arvosanaMuuttuiOsakokeet: LocalizedString
                get() = tr("yki.sarake.arvosanaMuuttuiOsakokeet")
            val suorituksenTunniste: LocalizedString
                get() = tr("yki.sarake.suorituksenTunniste")
            val virhe: LocalizedString get() = tr("yki.sarake.virhe")
            val aikaleima: LocalizedString get() = tr("yki.sarake.aikaleima")
            val pyynto: LocalizedString get() = tr("yki.sarake.pyynto")
            val piilotus: LocalizedString get() = tr("yki.sarake.piilotus")
        }

        object Historia {
            val kuvaus: LocalizedString
                get() =
                    tr("yki.historia.kuvaus")
            val rivejaYhteensa: LocalizedString get() = tr("yki.historia.rivejaYhteensa")
            val eiRiveja: LocalizedString
                get() = tr("yki.historia.eiRiveja")
            val hakusana: LocalizedString
                get() = tr("yki.historia.hakusana")
            val syy: LocalizedString get() = tr("yki.historia.syy")
            val syyluokka: LocalizedString get() = tr("yki.historia.syyluokka")
            val oidHaunSyy: LocalizedString
                get() = tr("yki.historia.oidHaunSyy")
            val oidHakuaEiYritetty: LocalizedString
                get() = tr("yki.historia.oidHakuaEiYritetty")
            val rikkinainenRivi: LocalizedString
                get() = tr("yki.historia.rikkinainenRivi")
            val lahdetiedosto: LocalizedString get() = tr("yki.historia.lahdetiedosto")
            val ladattu: LocalizedString get() = tr("yki.historia.ladattu")
            val muutosaikaleima: LocalizedString
                get() = tr("yki.historia.muutosaikaleima")
            val postinumero: LocalizedString get() = tr("yki.historia.postinumero")
            val postitoimipaikka: LocalizedString
                get() = tr("yki.historia.postitoimipaikka")
            val syyEiOppijanumeroa: LocalizedString
                get() = tr("yki.historia.syyEiOppijanumeroa")
            val syyPaikallinenValidointi: LocalizedString
                get() = tr("yki.historia.syyPaikallinenValidointi")
            val syyApiHylkasi: LocalizedString
                get() = tr("yki.historia.syyApiHylkasi")
            val syyRikkinainenRivi: LocalizedString
                get() = tr("yki.historia.syyRikkinainenRivi")
            val syyLastModifiedEiJasenny: LocalizedString
                get() = tr("yki.historia.syyLastModifiedEiJasenny")
            val syyMuu: LocalizedString get() = tr("yki.historia.syyMuu")
        }

        object Virhesarake {
            val oppijanumero: LocalizedString get() = tr("yki.virhesarake.oppijanumero")
            val hetu: LocalizedString get() = tr("yki.virhesarake.hetu")
            val nimi: LocalizedString get() = tr("yki.virhesarake.nimi")
            val virheellinenKentta: LocalizedString
                get() = tr("yki.virhesarake.virheellinenKentta")
            val virheellinenArvo: LocalizedString
                get() = tr("yki.virhesarake.virheellinenArvo")
            val virheellinenRivi: LocalizedString
                get() = tr("yki.virhesarake.virheellinenRivi")
            val virheenRivinumero: LocalizedString
                get() = tr("yki.virhesarake.virheenRivinumero")
            val virheenLuontiaika: LocalizedString
                get() = tr("yki.virhesarake.virheenLuontiaika")
            val lastModified: LocalizedString get() = tr("yki.virhesarake.lastModified")
        }
    }

    object Koto {
        val henkilotiedot: LocalizedString get() = tr("koto.henkilotiedot")
        val tutkinnonTiedot: LocalizedString get() = tr("koto.tutkinnonTiedot")
        val arviointi: LocalizedString get() = tr("koto.arviointi")
        val integraatiot: LocalizedString get() = tr("koto.integraatiot")
        val suodata: LocalizedString get() = tr("koto.suodata")
        val suoritustenTuonninVirheet: LocalizedString
            get() = tr("koto.suoritustenTuonninVirheet")
        val lataaCsv: LocalizedString get() = tr("koto.lataaCsv")
        val suorituksiaYhteensa: LocalizedString get() = tr("koto.suorituksiaYhteensa")
        val virheitaYhteensa: LocalizedString get() = tr("koto.virheitaYhteensa")
        val kesken: LocalizedString get() = tr("koto.kesken")
        val kurssi: LocalizedString get() = tr("koto.kurssi")
        val jarjestaja: LocalizedString get() = tr("koto.jarjestaja")
        val tehtavapaketti: LocalizedString get() = tr("koto.tehtavapaketti")
        val viimeksiMuokattu: LocalizedString get() = tr("koto.viimeksiMuokattu")
        val suoritusaikaAlkaen: LocalizedString get() = tr("koto.suoritusaikaAlkaen")
        val suoritusaikaPaattyen: LocalizedString get() = tr("koto.suoritusaikaPaattyen")
        val hakusana: LocalizedString
            get() = tr("koto.hakusana")

        val tehtavapankki: LocalizedString get() = tr("koto.tehtavapankki")
        val eiTehtavapaketteja: LocalizedString get() = tr("koto.eiTehtavapaketteja")
        val siirretty: LocalizedString get() = tr("koto.siirretty")
        val koko: LocalizedString get() = tr("koto.koko")
        val sisalto: LocalizedString get() = tr("koto.sisalto")
        val naytaSisalto: LocalizedString get() = tr("koto.naytaSisalto")
        val lataaXml: LocalizedString get() = tr("koto.lataaXml")
        val lataa: LocalizedString get() = tr("koto.lataa")
        val paketissaEiRyhmia: LocalizedString get() = tr("koto.paketissaEiRyhmia")
        val eiTehtavia: LocalizedString get() = tr("koto.eiTehtavia")
        val tehtavanTunniste: LocalizedString get() = tr("koto.tehtavanTunniste")
        val vastausvaihtoehdot: LocalizedString get() = tr("koto.vastausvaihtoehdot")
        val liitetiedostot: LocalizedString get() = tr("koto.liitetiedostot")
        val metadata: LocalizedString get() = tr("koto.metadata")
        val nimeton: LocalizedString get() = tr("koto.nimeton")
        val tyhjaNimi: LocalizedString get() = tr("koto.tyhjaNimi")
        val lahdejarjestelma: LocalizedString get() = tr("koto.lahdejarjestelma")
        val lahdeId: LocalizedString get() = tr("koto.lahdeId")
        val versio: LocalizedString get() = tr("koto.versio")
        val lahdeversio: LocalizedString get() = tr("koto.lahdeversio")
        val kieli: LocalizedString get() = tr("koto.kieli")
        val kurssinAlku: LocalizedString get() = tr("koto.kurssinAlku")
        val lahdeGeneroitu: LocalizedString get() = tr("koto.lahdeGeneroitu")
        val ladattu: LocalizedString get() = tr("koto.ladattu")
        val xmlTiedosto: LocalizedString get() = tr("koto.xmlTiedosto")
        val versioLabel: LocalizedString get() = tr("koto.versioLabel")
        val generoituLabel: LocalizedString get() = tr("koto.generoituLabel")

        object Sarake {
            val oppijanumero: LocalizedString get() = tr("koto.sarake.oppijanumero")
            val sukunimi: LocalizedString get() = tr("koto.sarake.sukunimi")
            val etunimet: LocalizedString get() = tr("koto.sarake.etunimet")
            val kutsumanimi: LocalizedString get() = tr("koto.sarake.kutsumanimi")
            val sahkoposti: LocalizedString get() = tr("koto.sarake.sahkoposti")
            val kurssinId: LocalizedString get() = tr("koto.sarake.kurssinId")
            val kurssinNimi: LocalizedString get() = tr("koto.sarake.kurssinNimi")
            val testikieli: LocalizedString get() = tr("koto.sarake.testikieli")
            val oppilaitosOid: LocalizedString get() = tr("koto.sarake.oppilaitosOid")
            val oppilaitos: LocalizedString get() = tr("koto.sarake.oppilaitos")
            val opettajanSahkoposti: LocalizedString
                get() = tr("koto.sarake.opettajanSahkoposti")
            val suoritusaika: LocalizedString get() = tr("koto.sarake.suoritusaika")
            val luetunYmmartaminen: LocalizedString
                get() = tr("koto.sarake.luetunYmmartaminen")
            val kuullunYmmartaminen: LocalizedString
                get() = tr("koto.sarake.kuullunYmmartaminen")
            val puhe: LocalizedString get() = tr("koto.sarake.puhe")
            val kirjoittaminen: LocalizedString get() = tr("koto.sarake.kirjoittaminen")
            val henkilotunnus: LocalizedString get() = tr("koto.sarake.henkilotunnus")
            val nimi: LocalizedString get() = tr("koto.sarake.nimi")
            val organisaatio: LocalizedString get() = tr("koto.sarake.organisaatio")
            val opettajanSahkopostiosoite: LocalizedString
                get() = tr("koto.sarake.opettajanSahkopostiosoite")
            val virheenLuontiaika: LocalizedString
                get() = tr("koto.sarake.virheenLuontiaika")
            val virheviesti: LocalizedString get() = tr("koto.sarake.virheviesti")
            val ratkaisuehdotus: LocalizedString get() = tr("koto.sarake.ratkaisuehdotus")
            val virheellinenKentta: LocalizedString
                get() = tr("koto.sarake.virheellinenKentta")
            val virheellinenArvo: LocalizedString
                get() = tr("koto.sarake.virheellinenArvo")
            val valmis: LocalizedString get() = tr("koto.sarake.valmis")
        }

        object Tehtavatyyppi {
            val monivalinta: LocalizedString get() = tr("koto.tehtavatyyppi.monivalinta")
            val tosiEpatosi: LocalizedString get() = tr("koto.tehtavatyyppi.tosiEpatosi")
            val lyhytVastaus: LocalizedString get() = tr("koto.tehtavatyyppi.lyhytVastaus")
            val numeerinenVastaus: LocalizedString
                get() = tr("koto.tehtavatyyppi.numeerinenVastaus")
            val essee: LocalizedString get() = tr("koto.tehtavatyyppi.essee")
            val yhdistaminen: LocalizedString get() = tr("koto.tehtavatyyppi.yhdistaminen")
            val cloze: LocalizedString
                get() = tr("koto.tehtavatyyppi.cloze")
            val lasku: LocalizedString get() = tr("koto.tehtavatyyppi.lasku")
            val monivalintaLasku: LocalizedString
                get() = tr("koto.tehtavatyyppi.monivalintaLasku")
            val yksinkertainenLasku: LocalizedString
                get() = tr("koto.tehtavatyyppi.yksinkertainenLasku")
            val ohjeteksti: LocalizedString get() = tr("koto.tehtavatyyppi.ohjeteksti")
            val vetaPudotaTeksti: LocalizedString
                get() = tr("koto.tehtavatyyppi.vetaPudotaTeksti")
            val vetaPudotaMerkit: LocalizedString
                get() = tr("koto.tehtavatyyppi.vetaPudotaMerkit")
            val vetaPudotaKuva: LocalizedString
                get() = tr("koto.tehtavatyyppi.vetaPudotaKuva")
            val valitsePuuttuvat: LocalizedString
                get() = tr("koto.tehtavatyyppi.valitsePuuttuvat")
            val satunnais: LocalizedString get() = tr("koto.tehtavatyyppi.satunnais")
            val satunnainenLyhytYhdistaminen: LocalizedString
                get() = tr("koto.tehtavatyyppi.satunnainenLyhytYhdistaminen")
            val puuttuvaTyyppi: LocalizedString get() = tr("koto.tehtavatyyppi.puuttuvaTyyppi")
            val aaninauhoitus: LocalizedString get() = tr("koto.tehtavatyyppi.aaninauhoitus")
            val aaniVideonauhoitus: LocalizedString
                get() = tr("koto.tehtavatyyppi.aaniVideonauhoitus")
            val hahmonsovitus: LocalizedString get() = tr("koto.tehtavatyyppi.hahmonsovitus")
            val kemiallinenKaava: LocalizedString
                get() = tr("koto.tehtavatyyppi.kemiallinenKaava")
            val ohjelmointi: LocalizedString get() = tr("koto.tehtavatyyppi.ohjelmointi")
            val stack: LocalizedString
                get() = tr("koto.tehtavatyyppi.stack")
            val jarjestaminen: LocalizedString
                get() = tr("koto.tehtavatyyppi.jarjestaminen")
            val yhdistelma: LocalizedString get() = tr("koto.tehtavatyyppi.yhdistelma")
            val kaava: LocalizedString get() = tr("koto.tehtavatyyppi.kaava")
            val aukko: LocalizedString get() = tr("koto.tehtavatyyppi.aukko")
            val saannollinenLauseke: LocalizedString
                get() = tr("koto.tehtavatyyppi.saannollinenLauseke")
            val puhetehtava: LocalizedString
                get() = tr("koto.tehtavatyyppi.puhetehtava")
            val ristikko: LocalizedString get() = tr("koto.tehtavatyyppi.ristikko")
            val piirto: LocalizedString get() = tr("koto.tehtavatyyppi.piirto")
        }

        object Kieli {
            val fin: LocalizedString get() = tr("koto.kieli.fin")
            val swe: LocalizedString get() = tr("koto.kieli.swe")
            val eng: LocalizedString get() = tr("koto.kieli.eng")
            val rus: LocalizedString get() = tr("koto.kieli.rus")
            val est: LocalizedString get() = tr("koto.kieli.est")
            val ara: LocalizedString get() = tr("koto.kieli.ara")
            val fas: LocalizedString get() = tr("koto.kieli.fas")
            val som: LocalizedString get() = tr("koto.kieli.som")
            val ukr: LocalizedString get() = tr("koto.kieli.ukr")
        }

        object Metatieto {
            val piilotettu: LocalizedString get() = tr("koto.metatieto.piilotettu")
            val vainYksiVastaus: LocalizedString
                get() = tr("koto.metatieto.vainYksiVastaus")
            val rangaistuskerroin: LocalizedString
                get() = tr("koto.metatieto.rangaistuskerroin")
            val oletuspistemaara: LocalizedString
                get() = tr("koto.metatieto.oletuspistemaara")
            val sekoitaVastaukset: LocalizedString
                get() = tr("koto.metatieto.sekoitaVastaukset")
            val vastauksenNumeroiminen: LocalizedString
                get() = tr("koto.metatieto.vastauksenNumeroiminen")
            val palauteOikeasta: LocalizedString
                get() = tr("koto.metatieto.palauteOikeasta")
            val yleispalaute: LocalizedString get() = tr("koto.metatieto.yleispalaute")
            val palauteVaarasta: LocalizedString
                get() = tr("koto.metatieto.palauteVaarasta")
            val naytaVakioOhje: LocalizedString get() = tr("koto.metatieto.naytaVakioOhje")
            val palauteOsittain: LocalizedString
                get() = tr("koto.metatieto.palauteOsittain")
            val vastausmuoto: LocalizedString get() = tr("koto.metatieto.vastausmuoto")
            val vastauskentanRivimaara: LocalizedString
                get() = tr("koto.metatieto.vastauskentanRivimaara")
            val vastausPakollinen: LocalizedString
                get() = tr("koto.metatieto.vastausPakollinen")
            val vastauspohja: LocalizedString get() = tr("koto.metatieto.vastauspohja")
            val sanamaaranEnimmais: LocalizedString
                get() = tr("koto.metatieto.sanamaaranEnimmais")
            val sanamaaranVahimmais: LocalizedString
                get() = tr("koto.metatieto.sanamaaranVahimmais")
            val liitteidenMaara: LocalizedString
                get() = tr("koto.metatieto.liitteidenMaara")
            val vaadittavatLiitteet: LocalizedString
                get() = tr("koto.metatieto.vaadittavatLiitteet")
            val tiedostonEnimmaiskoko: LocalizedString
                get() = tr("koto.metatieto.tiedostonEnimmaiskoko")
            val eiAanenSuodattimia: LocalizedString
                get() = tr("koto.metatieto.eiAanenSuodattimia")
            val litteroija: LocalizedString
                get() = tr("koto.metatieto.litteroija")
            val koodausMuunnos: LocalizedString get() = tr("koto.metatieto.koodausMuunnos")
            val aanisoittimenTeema: LocalizedString
                get() = tr("koto.metatieto.aanisoittimenTeema")
            val videosoittimenTeema: LocalizedString
                get() = tr("koto.metatieto.videosoittimenTeema")
            val opiskelijanSoitin: LocalizedString
                get() = tr("koto.metatieto.opiskelijanSoitin")
            val opettajanSoitin: LocalizedString
                get() = tr("koto.metatieto.opettajanSoitin")
            val aikaraja: LocalizedString get() = tr("koto.metatieto.aikaraja")
            val vanhentumispaivat: LocalizedString
                get() = tr("koto.metatieto.vanhentumispaivat")
            val tunnisteet: LocalizedString get() = tr("koto.metatieto.tunnisteet")
            val turvallinenTallennus: LocalizedString
                get() = tr("koto.metatieto.turvallinenTallennus")
            val kayttotarkoitus: LocalizedString
                get() = tr("koto.metatieto.kayttotarkoitus")
            val arviointiohjeet: LocalizedString
                get() = tr("koto.metatieto.arviointiohjeet")
        }
    }

    object Time {
        val juuriNyt: LocalizedString get() = tr("time.juuriNyt")
        val eilen: LocalizedString get() = tr("time.eilen")

        fun minuuttiaSitten(count: Long) = tr("time.minuuttiaSitten").interpolate("count" to count)

        fun tuntiaSitten(count: Long) = tr("time.tuntiaSitten").interpolate("count" to count)

        fun paivaaSitten(count: Long) = tr("time.paivaaSitten").interpolate("count" to count)
    }

    object Filter {
        val aikarajausPrefix: LocalizedString get() = tr("filter.aikarajausPrefix")
        val rajaaNaytettavat: LocalizedString get() = tr("filter.rajaaNaytettavat")
        val tiedonRajaus: LocalizedString get() = tr("filter.tiedonRajaus")
        val rajaa: LocalizedString get() = tr("filter.rajaa")
        val peruuta: LocalizedString get() = tr("filter.peruuta")
        val kaikki: LocalizedString get() = tr("filter.kaikki")
        val kylla: LocalizedString get() = tr("filter.kylla")
        val ei: LocalizedString get() = tr("filter.ei")
        val piilotaHenkilotiedot: LocalizedString get() =
            tr("filter.piilotaHenkilotiedot")
        val henkilotiedotPiilotettu: LocalizedString
            get() = tr("filter.henkilotiedotPiilotettu")
        val naytettavatSuoritukset: LocalizedString
            get() = tr("filter.naytettavatSuoritukset")
        val valmiit: LocalizedString get() = tr("filter.valmiit")
        val keskeneraiset: LocalizedString get() = tr("filter.keskeneraiset")
    }

    object Form {
        val tarkistaTiedot: LocalizedString get() = tr("form.tarkistaTiedot")
    }

    object Toiminto {
        val nayta: LocalizedString get() = tr("toiminto.nayta")
        val palauta: LocalizedString get() = tr("toiminto.palauta")
        val piilota: LocalizedString get() = tr("toiminto.piilota")
    }

    object Sukupuoli {
        val mies: LocalizedString get() = tr("sukupuoli.mies")
        val nainen: LocalizedString get() = tr("sukupuoli.nainen")
        val eiTiedossa: LocalizedString get() = tr("sukupuoli.eiTiedossa")
    }
}

private fun tr(key: String): LocalizedString {
    UiTextRegistry.record(key)
    return LocalizedString.withTolgeeKey(key)
}
