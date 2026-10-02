# Integraatiot

Kielitutkintorekisteri integroituu OPH:n ja kolmansien osapuolten järjestelmiin. Tämä sivu kuvaa
kullekin integraatiolle paketin, suunnan, asiakaskerroksen ja virhemallin.

## KOSKI — suoritusten siirto

**Paketti:** `koski/` · **Suunta:** lähetys

Onnistuneet YKI- ja VKT-suoritukset lähetetään KOSKI-palveluun, joka julkaisee ne Oma
Opintopolussa.

- `KoskiService.sendYkiSuorituksetToKoski()` ja `sendVktSuorituksetToKoski()` palauttavat
  `Either<KoskiTechnicalException, KoskiTransferReport>`. Tekniset virheet palautetaan vasempana;
  tietosisällölliset virheet kerätään raporttiin ja merkitään uudelleen lähetettäviksi.
- `KoskiException`-hierarkiassa `KoskiTechnicalException` on uudelleenyritettävä.
- Eräajot `Lähetä YKI-suoritukset KOSKI-palveluun` ja VKT-vastine, ks. [Eräajot](../db-scheduler).

## KIOS — ilmoittautumisjärjestelmä

**Paketti:** `ilmoittautumisjarjestelma/` · **Suunta:** lähetys (arviointitilat), haku (VKT)

YKI-suoritusten arviointitilat ilmoitetaan KIOS:lle heti suoritustiedon päivittyessä; eräajo
`Lähetä YKI-arviointitilat KIOS-palveluun` varmistaa perille menon katkosten jälkeen.
VKT-ilmoittautumiset ja -suoritukset haetaan KIOS:sta.

- `IlmoittautumisjarjestelmaClientImpl.post(...)` → `Either<IlmoittautumisjarjestelmaException, T>`.
- Virhetyypit: `BadRequest`, `UnexpectedError`, `MalformedResponse`, `NullResponse`.

## Oppijanumerorekisteri

**Paketti:** `oppijanumero/` · **Suunta:** haku

Henkilötietojen ja oppijanumeroiden nouto.

- `OppijanumerorekisteriClient.getUser(...)` → `Either<OppijanumeroException, Oppija>`.
- Virhetyypit: `OppijaNotFoundException`, `BadRequest`, `UnexpectedError`, `MalformedResponse`,
  `NullResponse`. `HasResponse` merkitsee ne, joilla on raaka HTTP-vastaus liitteenä.

## Organisaatiopalvelu

**Paketti:** `organisaatiot/` · **Suunta:** haku

Oppilaitosten OID:t ja niiden hierarkia validoinnin tueksi.
`OrganisaatiopalveluClient.get(...)` → `Either<OrganisaatiopalveluException, T>`; virhetyypit
vastaavat oppijanumeropuolen mallia.

## Koodisto

**Paketti:** `koodisto/` · **Suunta:** haku

OPH:n keskitetty koodistopalvelu, mm. kielikoodien ja luokitusten validointiin.

## Koealusta (kotoutumiskoulutus)

**Paketti:** `kotoutumiskoulutus/koealusta/` · **Suunta:** haku + tiedoston tuonti

Moodle-pohjainen kielitestialusta, josta haetaan suoritustietoja ja Moodle-XML-tehtäväpaketteja.

- Valmiit suoritukset haetaan inkrementaalisesti (`from`-vesileima db-schedulerin tilassa),
  keskeneräiset korvataan joka ajolla kokonaan ja rajataan ilmoittautumisajan perusteella
  (`enrolledfrom`, ks. [Eräajot](../db-scheduler)).
- `KoealustaSuoritusValidator` validoi suoritusten datan →
  `Either<KoealustaMappingError.*Failure, …>`.
- `tehtavapankki/TehtavapankkiIngestService` lataa XML:t S3:sta, parsii ne ja tallentaa yleiseen
  tehtäväpankki-skeemaan.

## Solki — YKI-tietojen lähde ja arvioijarekisterin vastaanottaja

**Paketit:** `yki/`, `yki/arvioijat/solki/` · **Suunta:** kaksisuuntainen

Yleiset kielitutkinnot tulevat Jyväskylän yliopiston Solki-järjestelmästä. Arvioijarekisterin
master on kitu.

### Arvioijarekisterin lähetys (kitu → Solki)

Jokainen kitussa tehty tallennus lähetetään Solkille:

```
POST {kitu.yki.baseUrl}arvioija
Authorization:   Basic (kitu.yki.username/password, samat tunnukset kuin suoritushaussa)
Idempotency-Key: {arvioijaOid}:{versio}
```

Runko on `SolkiArvioijaRequest`; henkilötunnusta ei lähetetä. **`tila`-kenttää ei lähetetä**: tila
lasketaan kauden päivistä (`Rekisterointitila`), joten lähetetty arvo vanhenisi vastaanottajan
kopiossa kauden umpeutuessa. Vastaanottaja johtaa tilan `kaudenAlkupaiva`n ja
`kaudenPaattymispaiva`n välistä, **päättymispäivä mukaan lukien**.

`syntymapaiva` haetaan ONR:stä lähetyshetkellä (`OppijanumeroService.getHenkiloByMasterOid`); kitu
ei säilytä sitä. Kolme tapausta:

| ONR                                                | Kitu tekee                                                                   |
| -------------------------------------------------- | ---------------------------------------------------------------------------- |
| vastaa, syntymäaika on                             | lähettää kentän muodossa `yyyy-MM-dd`                                        |
| vastaa, syntymäaikaa ei ole (yksilöimätön henkilö) | jättää kentän pois (`@JsonInclude(NON_NULL)`) ja merkitsee rivin lähetetyksi |
| ei vastaa                                          | **ei lähetä lainkaan**: `merkitseLahetysvirhe`, ja rivi jää jonoon           |

Viimeinen tapaus epäonnistuu tarkoituksella kiinni: vajaana lähtenyt rivi merkittäisiin
lähetetyksi, jolloin ONR-katko jättäisi pysyvän aukon Solkin kopioon.

**Lähetysjono** elää `yki_arvioija`-taulun sarakkeissa `solkiin_lahetetty`, `solki_lahetysvirhe`,
`solki_lahetysyritykset` ja `solki_viimeisin_lahetysyritys`. Rivi on jonossa, kun
`solkiin_lahetetty IS NULL OR solkiin_lahetetty < muokattu`. Solkista sisään tullut push leimataan
heti lähetetyksi, jottei Solkin oma data kaiu takaisin sille.

**Uudelleenyritykset** (`SolkiArvioijaScheduledTasks`): yksi synkroninen yritys tallennuksen
jälkeen, sitten `FIXED_DELAY|900s` riveille `solki_lahetysyritykset < 3` ja `DAILY|02:15` kaikille
lähettämättömille.

**Vastaus:** Solki palauttaa arvioijatunnuksen, joka tallennetaan sarakkeeseen
`yki_arvioija.solki_tunnus` ja näytetään arvioijan tietosivulla. Päivitys on
`COALESCE(?, solki_tunnus)`, joten tyhjä vastaus ei pyyhi aiemmin saatua tunnusta.

**Virheilmoitus ei sisällä pyyntörunkoa** (`SolkiArvioijaException.debugString()`): osoite,
sähköposti ja syntymäaika ovat henkilötietoa, ja virhe päätyy sekä lokiin että käyttöliittymässä
näkyvään virhesarakkeeseen. Vastausrunko otetaan mukaan, mutta **katkaistaan 500 merkkiin**.

Virkailija näkee tilan arvioijan tietosivun **Integraatiot**-kortissa ja voi käynnistää lähetyksen
uudelleen. Listanäkymässä on suodatin `vainSolkiVirheet` ja etusivulla laskuri.

### Yhteystietojen päivitys (Solki → kitu)

Solki lähettää yhteystietomuutokset rajapintaan `POST /yki/api/arvioija`. Rekisterimerkintää se ei
kirjoita: nimet tulevat ONR:stä ja kaudet kitusta.

### Kytkin `kitu.yki.arvioijarekisteri.integraatio.enabled`

Ohjaa molempia suuntia sekä yöllistä `paivitaArvioijaProjektiot`-ajoa (`DAILY|03:15`), joka pitää
`yki_arviointioikeus`-projektion ajan tasalla kausimasterin kanssa.

- **Päällä:** lähetys on käytössä ja sisääntulo kavennetaan yhteystietoihin.
- **Pois:** lähetettävät rivit jäävät jonoon ja lähtevät takautuvasti kytkimen avautuessa, ja
  Solkin koko payload otetaan vastaan ja tallennetaan.
- Kytkin on erillinen osoitteesta: `kitu.yki.baseUrl` on asetettu joka ympäristössä (myös local ja
  e2e dev-stubiin), joten sen olemassaolo ei kerro, saako lähettää.
- **Virkailijan käsin käynnistämä lähetys ei katso kytkintä.** Tietosivun "Lähetä uudelleen
  Solkiin" kutsuu `SolkiArvioijaService.lahetaArvioijaKasin`ia; yksittäinen lähetys on se tapa,
  jolla integraatiota kokeillaan ennen automaattisen liikenteen avaamista. Siksi
  `SolkiArvioijaClientImpl` ja `SolkiArvioijaServiceImpl` ovat ehdottomia beaneja ja kytkin luetaan
  vasta palvelun sisällä. Painike itsessään on **muokkauskytkimen** takana.

## Ataru — YKI-arvioijahakemukset

**Paketit:** `ataru/` (asiakas), `yki/arvioijat/hakemus/` (tuonti) · **Suunta:** haku

Arvioijahakemuslomakkeen hakemuksista luodaan YKI-arvioijamerkinnät (uusi arvioija tai
jatkokausi, joka alkaa voimassa olevan kauden päättymistä seuraavana päivänä). Lomakkeen
vuoden raja alkupäivälle ei koske tällaista jatkokautta (`TallennaArvioija.automaattinenJatkokausi`).

- Lomakkeen avain ja kenttien id:t ovat `ArvioijahakemusLomake`ssa ja hyväksymisehdot
  `ArvioijahakemusKriteeri`ssä — kovakoodattuina, ei asetuksina.
- `AtaruClient.haeHakemusavaimet(...)` → `POST /lomake-editori/api/applications/list`
  (`form-key`, `option-answers`, sivukursori `sort.offset`). Tämä on atarun virkailija-UI:n reitti,
  ei sovittu ulkoinen rajapinta.
- `AtaruClient.haeHakemukset(...)` → `POST /lomake-editori/api/external/siirto?salliYksiloimattomat=true`
  (runkona hakemusavaimet, 200 kerrallaan). Ilman parametria yksikin yksilöimätön hakija kaataa
  koko kutsun 409:llä.
- Virhetyypit: `BadRequest`, `UnexpectedError`, `MalformedResponse`, `NullResponse`, `Unauthorized`.
- **Autentikointi on CAS, ei OAuth2**: ataru ei hyväksy Bearer-tokenia. `security/cas/client/`
  kirjautuu palvelukäyttäjänä (`/cas/v1/tickets` → ST palvelulle `/lomake-editori/auth/cas` →
  eväste `ring-session`) ja kirjautuu uudelleen 401:n jälkeen.
- `kitu.ataru.service.url` tyhjä = integraatio pois päältä (ei asiakasta, palvelua eikä eräajoa).

### Vaaditut käyttöoikeudet

| Oikeus                                                                                    | Missä                                    | Huom                                                                                                                                                                             |
| ----------------------------------------------------------------------------------------- | ---------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `ATARU_HAKEMUS` READ (`APP_ATARU_HAKEMUS_READ_<org>`) palvelukäyttäjälle `koto-rekisteri` | lomakkeen omistava OPH:n alaorganisaatio | Ilman oikeutta ataru **pudottaa hakemukset hiljaa** — tyhjä vastaus, ei virhettä. Oikeutta ei myönnetä OPH:n juuriorganisaatioon, koska se tekisi käyttäjästä ataru-superuserin. |
| CAS-kirjautuminen palvelukäyttäjälle `koto-rekisteri`                                     | Otuva CAS REST (`/cas/v1/tickets`)       | Käyttää `kitu.palvelukayttaja.username/password`-arvoja (`palvelukayttaja-password`); uutta tunnusta tai salaisuutta ei tarvita.                                                 |
| ONR                                                                                       | —                                        | Nykyiset oikeudet riittävät (samat kutsut kuin arvioijalomakkeella).                                                                                                             |
| Kitussa `YKI_ARVIOIJAREKISTERI`                                                           | `/yki/arvioijat/hakemukset`              | Hylättyjen hakemusten tarkistusnäkymä.                                                                                                                                           |

`yki_arvioijahakemus` on pelkkä kirjanpito käsitellyistä hakemuksista, eikä sillä ole
säilytysaikaa: rivin poistaminen saisi seuraavan ajon luomaan arvioijan uudelleen samasta hakemuksesta.

`ATARU_EDITORI`-, `VALINTA_*`- tai CRUD-oikeuksia ei tarvita, koska kitu vain lukee. Siirto
palauttaa hetun, mutta kitu ei tallenna sitä, eikä tauluun `yki_arvioijahakemus` kirjoiteta vastauksia.

## CAS + OAuth2 — virkailija-autentikointi

**Paketti:** `security/` (`security/cas/`, `security/oauth2/`) · **Suunta:** haku

Sovellus käyttää OPH:n keskitettyä Otuva-palvelua.

## Auditlokit

**Paketti:** `auditlogs/`

OPH:n auditlokitoteutus ECS-formaatissa. Lokien siirto S3:een hoidetaan infrassa
(`koski-audit-logs-integration-stack`); palvelun ylläpitää KOSKI-tiimi.
