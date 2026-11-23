# Rekisteristä lähtevät tiedot

> Tämä sivu on tuotettu automaattisesti ohjelmakoodista, eikä sitä muokata käsin.
> Sivu muuttuu aina, kun lähetettävät tiedot muuttuvat.

Sivu kertoo, mitä tietoja Kielitutkintorekisteri lähettää muihin järjestelmiin. Jokaisesta tietovirrasta kerrotaan vastaanottaja, lähetyksen ajankohta ja tarkoitus sekä kaikki lähetettävät tiedot. Integraatioiden tekninen toteutus on kuvattu sivulla [Integraatiot](./integraatiot.md).

- [YKI-suoritukset KOSKI-järjestelmään](#koski-yki)
- [VKT-suoritukset KOSKI-järjestelmään](#koski-vkt)
- [YKI-arvioijat Solki-järjestelmään](#solki-arvioija)
- [YKI-arviointitilat ilmoittautumisjärjestelmään](#ilmoittautumisjarjestelma-arviointitila)

## Taulukoiden lukuohje

- **Tieto** kertoo, mistä tiedosta on kyse.
- **Pakollinen** kertoo, lähetetäänkö tieto aina (kyllä) vai voiko se jäädä tyhjäksi (ei).
- **Muoto ja sallitut arvot** kertoo, millainen arvo on. Jos arvo valitaan ennalta sovituista
  vaihtoehdoista, vaihtoehdot on lueteltu: ensin lähetettävä koodi ja suluissa sen merkitys.
  Tietoryhmä koostuu useasta tiedosta, jotka on lueteltu omassa taulukossaan; linkki vie siihen.
- **Tekninen nimi** on tiedon nimi lähetettävässä sanomassa. Merkintä `[]` tarkoittaa listaa.

<a id="koski-yki"></a>

## YKI-suoritukset KOSKI-järjestelmään

- **Vastaanottaja:** Opetushallituksen KOSKI-järjestelmä
- **Milloin:** Ajastetusti sen jälkeen, kun suoritus on arvioitu. Suoritus lähetetään uudelleen, jos sen tiedot muuttuvat.
- **Miksi:** Suorittaja näkee suorituksensa Oma Opintopolussa, ja KOSKI-järjestelmä tekee siitä todistuksen.
- **Huomio:** Suorittajasta lähetetään vain oppijanumero. Nimeä, henkilötunnusta tai yhteystietoja ei lähetetä.
- **Huomio:** Suoritusta ei lähetetä, jos sitä ei ole arvioitu, sillä ei ole oppijanumeroa tai jonkin osakokeen arvosana on vilppi.
- **Huomio:** Keskeytettyjä ja suorittamatta jääneitä osakokeita ei lähetetä.

| Tieto | Pakollinen | Muoto ja sallitut arvot | Tekninen nimi |
| --- | --- | --- | --- |
| Tutkinnon suorittaja | kyllä | tunniste (OID) | `henkilö.oid` |
| Suorittajan opiskeluoikeus | kyllä | lista: [tietoryhmä](#koski-yki-opiskeluoikeudet) | `opiskeluoikeudet[]` |

<a id="koski-yki-opiskeluoikeudet"></a>

### Suorittajan opiskeluoikeus

Lähetyksessä on aina yksi opiskeluoikeus. Tietoryhmän tekninen nimi sanomassa on `opiskeluoikeudet[]`.

| Tieto | Pakollinen | Muoto ja sallitut arvot | Tekninen nimi |
| --- | --- | --- | --- |
| Suorituksen tunnistetiedot | kyllä | [tietoryhmä](#koski-yki-opiskeluoikeudet-lahdejarjestelmanid) | `lähdejärjestelmänId` |
| Opiskeluoikeuden tyyppi | kyllä | aina sama arvo: `kielitutkinto` | `tyyppi` |
| Opiskeluoikeuden tilahistoria | kyllä | lista: [tietoryhmä](#koski-yki-opiskeluoikeudet-tila-opiskeluoikeusjaksot) | `tila.opiskeluoikeusjaksot[]` |
| Tutkinnon suoritus | kyllä | lista: [tietoryhmä](#koski-yki-opiskeluoikeudet-suoritukset) | `suoritukset[]` |
| KOSKI-järjestelmän opiskeluoikeudelle antama tunniste; tyhjä, kun suoritus lähetetään ensimmäisen kerran | ei | tunniste (OID) | `oid` |

<a id="koski-yki-opiskeluoikeudet-lahdejarjestelmanid"></a>

### Suorituksen tunnistetiedot

Tietoryhmän tekninen nimi sanomassa on `opiskeluoikeudet[].lähdejärjestelmänId`.

| Tieto | Pakollinen | Muoto ja sallitut arvot | Tekninen nimi |
| --- | --- | --- | --- |
| Suorituksen tunniste Kielitutkintorekisterissä | kyllä | teksti | `id` |
| Järjestelmä, josta tieto on peräisin | kyllä | aina sama arvo: `kielitutkintorekisteri` | `lähdejärjestelmä` |

<a id="koski-yki-opiskeluoikeudet-tila-opiskeluoikeusjaksot"></a>

### Opiskeluoikeuden tilahistoria

Tietoryhmän tekninen nimi sanomassa on `opiskeluoikeudet[].tila.opiskeluoikeusjaksot[]`.

| Tieto | Pakollinen | Muoto ja sallitut arvot | Tekninen nimi |
| --- | --- | --- | --- |
| Päivä, jona tila alkoi: tutkintopäivä tai arviointipäivä | kyllä | päivämäärä (vvvv-kk-pp) | `alku` |
| Opiskeluoikeuden tila | kyllä | `lasna`, `paattynyt` | `tila` |

<a id="koski-yki-opiskeluoikeudet-suoritukset"></a>

### Tutkinnon suoritus

Lähetyksessä on aina yksi tutkinnon suoritus. Tietoryhmän tekninen nimi sanomassa on `opiskeluoikeudet[].suoritukset[]`.

| Tieto | Pakollinen | Muoto ja sallitut arvot | Tekninen nimi |
| --- | --- | --- | --- |
| Suorituksen tyyppi | kyllä | aina sama arvo: `yleinenkielitutkinto` | `tyyppi` |
| Tutkinnon taso ja kieli | kyllä | [tietoryhmä](#koski-yki-opiskeluoikeudet-suoritukset-koulutusmoduuli) | `koulutusmoduuli` |
| Organisaatio, jonka alaisuudessa tutkinto on suoritettu | kyllä | tunniste (OID) | `toimipiste.oid` |
| Tutkinnon vahvistus | ei | [tietoryhmä](#koski-yki-opiskeluoikeudet-suoritukset-vahvistus) | `vahvistus` |
| Tutkinnon osat | kyllä | lista: [tietoryhmä](#koski-yki-opiskeluoikeudet-suoritukset-osasuoritukset) | `osasuoritukset[]` |
| Tutkinnon järjestäjä | ei | tunniste (OID) | `järjestäjä.oid` |
| Tutkinnon yleisarvosana; tyhjä, jos sitä ei ole annettu | ei | `1`, `2`, `3`, `4`, `5`, `6`, `alle1` (Alle 1), `alle3` (Alle 3), `alle5` (Alle 5), `9` (Ei voi arvioida), `10` (Keskeytetty), `11` (Vilppi), `12` (Ei suoritusta) | `yleisarvosana` |

<a id="koski-yki-opiskeluoikeudet-suoritukset-koulutusmoduuli"></a>

### Tutkinnon taso ja kieli

Tietoryhmän tekninen nimi sanomassa on `opiskeluoikeudet[].suoritukset[].koulutusmoduuli`.

| Tieto | Pakollinen | Muoto ja sallitut arvot | Tekninen nimi |
| --- | --- | --- | --- |
| Tutkinnon taso | kyllä | `pt`, `kt`, `yt` | `tunniste` |
| Tutkinnon kieli | kyllä | `DE` (Saksa), `EN` (Englanti), `FI` (Suomi), `FR` (Ranska), `IT` (Italia), `RU` (Venäjä), `SE` (Pohjoissaame), `ES` (Espanja), `SV` (Ruotsi) | `kieli` |

<a id="koski-yki-opiskeluoikeudet-suoritukset-vahvistus"></a>

### Tutkinnon vahvistus

Tyhjä, jos arviointipäivää ei ole. Tietoryhmän tekninen nimi sanomassa on `opiskeluoikeudet[].suoritukset[].vahvistus`.

| Tieto | Pakollinen | Muoto ja sallitut arvot | Tekninen nimi |
| --- | --- | --- | --- |
| Päivä, jona tutkinto vahvistettiin: viimeisin arviointipäivä | kyllä | päivämäärä (vvvv-kk-pp) | `päivä` |
| Tutkinnon vahvistanut organisaatio | kyllä | tunniste (OID) | `myöntäjäOrganisaatio.oid` |

<a id="koski-yki-opiskeluoikeudet-suoritukset-osasuoritukset"></a>

### Tutkinnon osat

Tietoryhmän tekninen nimi sanomassa on `opiskeluoikeudet[].suoritukset[].osasuoritukset[]`.

| Tieto | Pakollinen | Muoto ja sallitut arvot | Tekninen nimi |
| --- | --- | --- | --- |
| Tutkinnon osan tyyppi | kyllä | aina sama arvo: `yleisenkielitutkinnonosa` | `tyyppi` |
| Suoritettu tutkinnon osa | kyllä | `tekstinymmartaminen`, `kirjoittaminen`, `puheenymmartaminen`, `puhuminen`, `rakenteetjasanasto` | `koulutusmoduuli.tunniste` |
| Tutkinnon osan arviointi | kyllä | lista: [tietoryhmä](#koski-yki-opiskeluoikeudet-suoritukset-osasuoritukset-arviointi) | `arviointi[]` |

<a id="koski-yki-opiskeluoikeudet-suoritukset-osasuoritukset-arviointi"></a>

### Tutkinnon osan arviointi

Tietoryhmän tekninen nimi sanomassa on `opiskeluoikeudet[].suoritukset[].osasuoritukset[].arviointi[]`.

| Tieto | Pakollinen | Muoto ja sallitut arvot | Tekninen nimi |
| --- | --- | --- | --- |
| Arvosana | kyllä | `1`, `2`, `3`, `4`, `5`, `6`, `alle1` (Alle 1), `alle3` (Alle 3), `alle5` (Alle 5), `9` (Ei voi arvioida) | `arvosana` |
| Arviointipäivä | kyllä | päivämäärä (vvvv-kk-pp) | `päivä` |

<a id="koski-vkt"></a>

## VKT-suoritukset KOSKI-järjestelmään

- **Vastaanottaja:** Opetushallituksen KOSKI-järjestelmä
- **Milloin:** Ajastetusti sen jälkeen, kun suorituksen kaikki osakokeet on arvioitu. Suoritus lähetetään uudelleen, jos sen tiedot muuttuvat.
- **Miksi:** Suorittaja näkee suorituksensa Oma Opintopolussa.
- **Huomio:** Suorittajasta lähetetään vain oppijanumero. Nimeä, henkilötunnusta tai yhteystietoja ei lähetetä.
- **Huomio:** Suoritusta ei lähetetä, jos jonkin osakokeen arviointi tai suorituspaikkakunta puuttuu.
- **Huomio:** Lähetykseen tulevat vain ne kielitaidot, joiden kaikki osakokeet on suoritettu ja arvioitu.

| Tieto | Pakollinen | Muoto ja sallitut arvot | Tekninen nimi |
| --- | --- | --- | --- |
| Tutkinnon suorittaja | kyllä | tunniste (OID) | `henkilö.oid` |
| Suorittajan opiskeluoikeus | kyllä | lista: [tietoryhmä](#koski-vkt-opiskeluoikeudet) | `opiskeluoikeudet[]` |

<a id="koski-vkt-opiskeluoikeudet"></a>

### Suorittajan opiskeluoikeus

Lähetyksessä on aina yksi opiskeluoikeus. Tietoryhmän tekninen nimi sanomassa on `opiskeluoikeudet[]`.

| Tieto | Pakollinen | Muoto ja sallitut arvot | Tekninen nimi |
| --- | --- | --- | --- |
| Suorituksen tunnistetiedot | kyllä | [tietoryhmä](#koski-vkt-opiskeluoikeudet-lahdejarjestelmanid) | `lähdejärjestelmänId` |
| Opiskeluoikeuden tyyppi | kyllä | aina sama arvo: `kielitutkinto` | `tyyppi` |
| Opiskeluoikeuden tilahistoria | kyllä | lista: [tietoryhmä](#koski-vkt-opiskeluoikeudet-tila-opiskeluoikeusjaksot) | `tila.opiskeluoikeusjaksot[]` |
| Tutkinnon suoritus | kyllä | lista: [tietoryhmä](#koski-vkt-opiskeluoikeudet-suoritukset) | `suoritukset[]` |
| KOSKI-järjestelmän opiskeluoikeudelle antama tunniste; tyhjä, kun suoritus lähetetään ensimmäisen kerran | ei | tunniste (OID) | `oid` |

<a id="koski-vkt-opiskeluoikeudet-lahdejarjestelmanid"></a>

### Suorituksen tunnistetiedot

Tietoryhmän tekninen nimi sanomassa on `opiskeluoikeudet[].lähdejärjestelmänId`.

| Tieto | Pakollinen | Muoto ja sallitut arvot | Tekninen nimi |
| --- | --- | --- | --- |
| Suorituksen tunniste Kielitutkintorekisterissä | kyllä | teksti | `id` |
| Järjestelmä, josta tieto on peräisin | kyllä | aina sama arvo: `kielitutkintorekisteri` | `lähdejärjestelmä` |

<a id="koski-vkt-opiskeluoikeudet-tila-opiskeluoikeusjaksot"></a>

### Opiskeluoikeuden tilahistoria

Tietoryhmän tekninen nimi sanomassa on `opiskeluoikeudet[].tila.opiskeluoikeusjaksot[]`.

| Tieto | Pakollinen | Muoto ja sallitut arvot | Tekninen nimi |
| --- | --- | --- | --- |
| Päivä, jona tila alkoi: tutkintopäivä tai arviointipäivä | kyllä | päivämäärä (vvvv-kk-pp) | `alku` |
| Opiskeluoikeuden tila | kyllä | `lasna`, `paattynyt` | `tila` |

<a id="koski-vkt-opiskeluoikeudet-suoritukset"></a>

### Tutkinnon suoritus

Lähetyksessä on aina yksi tutkinnon suoritus. Tietoryhmän tekninen nimi sanomassa on `opiskeluoikeudet[].suoritukset[]`.

| Tieto | Pakollinen | Muoto ja sallitut arvot | Tekninen nimi |
| --- | --- | --- | --- |
| Suorituksen tyyppi | kyllä | aina sama arvo: `valtionhallinnonkielitutkinto` | `tyyppi` |
| Tutkinnon taso ja kieli | kyllä | [tietoryhmä](#koski-vkt-opiskeluoikeudet-suoritukset-koulutusmoduuli) | `koulutusmoduuli` |
| Organisaatio, jonka alaisuudessa tutkinto on suoritettu | kyllä | tunniste (OID) | `toimipiste.oid` |
| Tutkinnon vahvistus | ei | [tietoryhmä](#koski-vkt-opiskeluoikeudet-suoritukset-vahvistus) | `vahvistus` |
| Tutkinnon osat | kyllä | lista: [tietoryhmä](#koski-vkt-opiskeluoikeudet-suoritukset-osasuoritukset) | `osasuoritukset[]` |

<a id="koski-vkt-opiskeluoikeudet-suoritukset-koulutusmoduuli"></a>

### Tutkinnon taso ja kieli

Tietoryhmän tekninen nimi sanomassa on `opiskeluoikeudet[].suoritukset[].koulutusmoduuli`.

| Tieto | Pakollinen | Muoto ja sallitut arvot | Tekninen nimi |
| --- | --- | --- | --- |
| Tutkinnon taso | kyllä | `erinomainen`, `hyvajatyydyttava` (Hyvä ja tyydyttävä) | `tunniste` |
| Tutkinnon kieli | kyllä | `FI` (Suomi), `SV` (Ruotsi) | `kieli` |

<a id="koski-vkt-opiskeluoikeudet-suoritukset-vahvistus"></a>

### Tutkinnon vahvistus

Tyhjä, jos arviointipäivää ei ole. Tietoryhmän tekninen nimi sanomassa on `opiskeluoikeudet[].suoritukset[].vahvistus`.

| Tieto | Pakollinen | Muoto ja sallitut arvot | Tekninen nimi |
| --- | --- | --- | --- |
| Päivä, jona tutkinto vahvistettiin: viimeisin arviointipäivä | kyllä | päivämäärä (vvvv-kk-pp) | `päivä` |
| Tutkinnon vahvistanut organisaatio | kyllä | tunniste (OID) | `myöntäjäOrganisaatio.oid` |
| Kunta, jossa tutkinto suoritettiin | kyllä | koodi koodistosta `kunta` (Suomen kunnat) | `paikkakunta` |

<a id="koski-vkt-opiskeluoikeudet-suoritukset-osasuoritukset"></a>

### Tutkinnon osat

Tietoryhmän tekninen nimi sanomassa on `opiskeluoikeudet[].suoritukset[].osasuoritukset[]`.

| Tieto | Pakollinen | Muoto ja sallitut arvot | Tekninen nimi |
| --- | --- | --- | --- |
| Tutkinnon osan tyyppi | kyllä | aina sama arvo: `valtionhallinnonkielitaito` | `tyyppi` |
| Suoritettu kielitaito | kyllä | `kirjallinen`, `suullinen`, `ymmartaminen` | `koulutusmoduuli.tunniste` |
| Kielitaidon arviointi | kyllä | lista: [tietoryhmä](#koski-vkt-opiskeluoikeudet-suoritukset-osasuoritukset-arviointi) | `arviointi[]` |
| Kielitaidon osakokeet | kyllä | lista: [tietoryhmä](#koski-vkt-opiskeluoikeudet-suoritukset-osasuoritukset-osasuoritukset) | `osasuoritukset[]` |
| Kielitaidon osakokeiden varhaisin tutkintopäivä | ei | päivämäärä (vvvv-kk-pp) | `alkamispäivä` |

<a id="koski-vkt-opiskeluoikeudet-suoritukset-osasuoritukset-arviointi"></a>

### Kielitaidon arviointi

Arviointi on johdettu kielitaidon osakokeiden arvosanoista. Tietoryhmän tekninen nimi sanomassa on `opiskeluoikeudet[].suoritukset[].osasuoritukset[].arviointi[]`.

| Tieto | Pakollinen | Muoto ja sallitut arvot | Tekninen nimi |
| --- | --- | --- | --- |
| Arvosana | kyllä | `erinomainen`, `hyva`, `tyydyttava`, `hylatty`, `ei_suoritusta` | `arvosana` |
| Arviointipäivä | kyllä | päivämäärä (vvvv-kk-pp) | `päivä` |

<a id="koski-vkt-opiskeluoikeudet-suoritukset-osasuoritukset-osasuoritukset"></a>

### Kielitaidon osakokeet

Tietoryhmän tekninen nimi sanomassa on `opiskeluoikeudet[].suoritukset[].osasuoritukset[].osasuoritukset[]`.

| Tieto | Pakollinen | Muoto ja sallitut arvot | Tekninen nimi |
| --- | --- | --- | --- |
| Osakokeen tyyppi | kyllä | aina sama arvo: `valtionhallinnonkielitutkinnonosakoe` | `tyyppi` |
| Suoritettu osakoe | kyllä | `kirjoittaminen`, `puheenymmartaminen`, `puhuminen`, `tekstinymmartaminen` | `koulutusmoduuli.tunniste` |
| Osakokeen arviointi | kyllä | lista: [tietoryhmä](#koski-vkt-opiskeluoikeudet-suoritukset-osasuoritukset-osasuoritukset-arviointi) | `arviointi[]` |
| Osakokeen tutkintopäivä | ei | päivämäärä (vvvv-kk-pp) | `alkamispäivä` |

<a id="koski-vkt-opiskeluoikeudet-suoritukset-osasuoritukset-osasuoritukset-arviointi"></a>

### Osakokeen arviointi

Tietoryhmän tekninen nimi sanomassa on `opiskeluoikeudet[].suoritukset[].osasuoritukset[].osasuoritukset[].arviointi[]`.

| Tieto | Pakollinen | Muoto ja sallitut arvot | Tekninen nimi |
| --- | --- | --- | --- |
| Arvosana | kyllä | `erinomainen`, `hyva`, `tyydyttava`, `hylatty`, `ei_suoritusta` | `arvosana` |
| Arviointipäivä | kyllä | päivämäärä (vvvv-kk-pp) | `päivä` |

<a id="solki-arvioija"></a>

## YKI-arvioijat Solki-järjestelmään

- **Vastaanottaja:** Jyväskylän yliopiston Solki-järjestelmä
- **Milloin:** Aina kun arvioijan tietoja tallennetaan Kielitutkintorekisterissä. Epäonnistunut lähetys yritetään ajastetusti uudelleen.
- **Miksi:** Solki tarvitsee ajantasaiset tiedot arvioijista ja heidän arviointioikeuksistaan.
- **Huomio:** Henkilötunnusta ei lähetetä.
- **Huomio:** Puhelinnumeroa ei lähetetä.
- **Huomio:** Arviointioikeuden tilaa (aktiivinen, passivoitu) ei lähetetä, koska se vanhenisi kauden päättyessä. Vastaanottaja päättelee tilan arviointikauden päivistä; kauden päättymispäivä on kauden viimeinen voimassaolopäivä.
- **Huomio:** Syntymäpäivää ei säilytetä Kielitutkintorekisterissä, vaan se haetaan oppijanumerorekisteristä jokaista lähetystä varten.

| Tieto | Pakollinen | Muoto ja sallitut arvot | Tekninen nimi |
| --- | --- | --- | --- |
| Arvioijan oppijanumero | kyllä | teksti | `arvioijaOid` |
| Hetki, jona arvioijan tietoja on viimeksi muutettu Kielitutkintorekisterissä | ei | päivämäärä ja kellonaika | `versio` |
| Arvioijan sukunimi | kyllä | teksti | `sukunimi` |
| Arvioijan etunimet | kyllä | teksti | `etunimet` |
| Arvioijan syntymäpäivä, joka haetaan oppijanumerorekisteristä lähetyshetkellä | ei | päivämäärä (vvvv-kk-pp) | `syntymapaiva` |
| Arvioijan sähköpostiosoite | ei | teksti | `sahkopostiosoite` |
| Arvioijan katuosoite | kyllä | teksti | `katuosoite` |
| Arvioijan postinumero | kyllä | teksti | `postinumero` |
| Arvioijan postitoimipaikka | kyllä | teksti | `postitoimipaikka` |
| Päivä, jona arvioija on ensimmäisen kerran merkitty arvioijarekisteriin | ei | päivämäärä (vvvv-kk-pp) | `ensimmainenRekisterointipaiva` |
| Arvioijan arviointioikeudet | kyllä | lista: [tietoryhmä](#solki-arvioija-arviointioikeudet) | `arviointioikeudet[]` |

<a id="solki-arvioija-arviointioikeudet"></a>

### Arvioijan arviointioikeudet

Jokaisesta kielestä on oma arviointioikeutensa. Tietoryhmän tekninen nimi sanomassa on `arviointioikeudet[]`.

| Tieto | Pakollinen | Muoto ja sallitut arvot | Tekninen nimi |
| --- | --- | --- | --- |
| Kieli, jonka tutkintoja arvioija saa arvioida | kyllä | `deu` (saksa), `eng` (englanti), `fin` (suomi), `fra` (ranska), `ita` (italia), `rus` (venäjä), `sme` (pohjoissaame), `spa` (espanja), `swe` (ruotsi), `swe10` (ruotsi (vanha koodi)), `eng11` (kaupallinen englanti), `eng12` (tekninen englanti) | `kieli` |
| Tutkintotasot, joita arvioija saa arvioida tällä kielellä | kyllä | lista: `PT` (Perustaso), `KT` (Keskitaso), `YT` (Ylin taso) | `tasot[]` |
| Arviointikauden ensimmäinen voimassaolopäivä | ei | päivämäärä (vvvv-kk-pp) | `kaudenAlkupaiva` |
| Arviointikauden viimeinen voimassaolopäivä | ei | päivämäärä (vvvv-kk-pp) | `kaudenPaattymispaiva` |
| Onko arviointikausi jatkoa arvioijan aiemmalle kaudelle | kyllä | kyllä tai ei | `jatkorekisterointi` |

<a id="ilmoittautumisjarjestelma-arviointitila"></a>

## YKI-arviointitilat ilmoittautumisjärjestelmään

- **Vastaanottaja:** YKI-tutkintojen ilmoittautumisjärjestelmä (KIOS)
- **Milloin:** Heti kun suorituksen tiedot päivittyvät Kielitutkintorekisterissä. Ajastettu lähetys varmistaa, että tila menee perille myös katkoksen jälkeen.
- **Miksi:** Suorittaja näkee arvioinnin etenemisen omalla sivullaan ilmoittautumisjärjestelmässä.
- **Huomio:** Arvosanoja ei lähetetä, vain arvioinnin tila.
- **Huomio:** Suorituksesta ei lähetetä mitään, jos sillä ei ole oppijanumeroa tai siihen liittyy vilppi.

| Tieto | Pakollinen | Muoto ja sallitut arvot | Tekninen nimi |
| --- | --- | --- | --- |
| Arviointitilat | kyllä | lista, jossa on vähintään yksi: [tietoryhmä](#ilmoittautumisjarjestelma-arviointitila-tilat) | `tilat[]` |

<a id="ilmoittautumisjarjestelma-arviointitila-tilat"></a>

### Arviointitilat

Jokaisesta suorituksesta on oma arviointitilansa. Tietoryhmän tekninen nimi sanomassa on `tilat[]`.

| Tieto | Pakollinen | Muoto ja sallitut arvot | Tekninen nimi |
| --- | --- | --- | --- |
| Suorituksen tunnistetiedot | kyllä | [tietoryhmä](#ilmoittautumisjarjestelma-arviointitila-tilat-suoritus) | `suoritus` |
| Suorituksen arvioinnin tila | kyllä | `ILMOITTAUTUNUT`, `PERUTTU` (Ilmoittautuminen peruttu), `EI_SUORITUSTA` (Ei suoritusta), `ARVIOITAVA` (Suoritus arvioitavana), `ARVIOITU` (Arviointi valmis), `TARKISTUSARVIOITAVA` (Suoritus tarkistusarvioitavana), `TARKISTUSARVIOITU` (Tarkistusarviointi tehty), `TARKISTUSARVIOINTI_HYVAKSYTTY` (Tarkistusarviointi hyväksytty) | `tila` |

<a id="ilmoittautumisjarjestelma-arviointitila-tilat-suoritus"></a>

### Suorituksen tunnistetiedot

Näillä tiedoilla suoritus tunnistetaan ilmoittautumisjärjestelmässä. Tietoryhmän tekninen nimi sanomassa on `tilat[].suoritus`.

| Tieto | Pakollinen | Muoto ja sallitut arvot | Tekninen nimi |
| --- | --- | --- | --- |
| Suorittajan oppijanumero | kyllä | tunniste (OID) | `oppijanumero` |
| Tutkintopäivä | kyllä | päivämäärä (vvvv-kk-pp) | `tutkintopaiva` |
| Tutkinnon kieli | kyllä | `deu` (saksa), `eng` (englanti), `fin` (suomi), `fra` (ranska), `ita` (italia), `rus` (venäjä), `sme` (pohjoissaame), `spa` (espanja), `swe` (ruotsi), `swe10` (ruotsi (vanha koodi)), `eng11` (kaupallinen englanti), `eng12` (tekninen englanti) | `tutkintokieli` |
| Tutkinnon taso | kyllä | `PT` (Perustaso), `KT` (Keskitaso), `YT` (Ylin taso) | `tutkintotaso` |
| Osakokeet, joihin suorittaja on ilmoittautunut tai osallistunut | kyllä | lista: `PU` (Puhuminen), `KI` (Kirjoittaminen), `TY` (Tekstin ymmärtäminen), `PY` (Puheen ymmärtäminen) | `osakokeet[]` |

## Sivun päivittäminen

Kehittäjälle: tietojen kuvaukset ovat koodissa `@Kuvaus`-annotaatioissa ja tietovirtojen tiedot luokassa `Tietovirrat`. Sivu tuotetaan uudelleen hakemistossa `server/` komennolla

```shell
./mvnw test -Dtest=LahtevatTiedotDokumenttiTest -Dkitu.dokumentaatio.paivita=true
```
