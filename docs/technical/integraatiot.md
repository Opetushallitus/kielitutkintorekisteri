# Integraatiot

Kielitutkintorekisteri integroituu useisiin Opetushallituksen ja kolmannen
osapuolen järjestelmiin. Tämä sivu kuvaa kullekin integraatiolle vastaavan
paketin, asiakaskerroksen, virhemallin sekä vastuun rajauksen.

## KOSKI — suoritusten siirto

**Paketti:** `koski/`
**Suunta:** lähetys

Kielitutkintorekisteri lähettää onnistuneet YKI- ja VKT-suoritukset
KOSKI-palveluun, joka julkaisee ne Oma Opintopolun kautta loppukäyttäjille.

- `KoskiService.sendYkiSuorituksetToKoski()` /
  `sendVktSuorituksetToKoski()` — palauttavat
  `Either<KoskiTechnicalException, KoskiTransferReport>`. Tekniset virheet
  (esim. KOSKI ei vastaa) palautetaan vasempana; tietosisällölliset virheet kerätään
  raporttiin ja merkitään uudelleen lähetettäviksi.
- Eräajot `Lähetä YKI-suoritukset KOSKI-palveluun` ja vastaava VKT-versio
  ajavat lähetyksen aikataulutetusti — ks. [Eräajot](../db-scheduler).
- Virhetyypit: `KoskiException`-hierarkia, jossa `KoskiTechnicalException`
  on uudelleenyritettävä ja muut suoritusrajan ulkopuolisia virheitä.

## KIOS — ilmoittautumisjärjestelmä, VKT-suoritusten lähde

**Paketti:** `ilmoittautumisjarjestelma/`
**Suunta:** lähetys (arviointitilat), sisääntuleva pyyntö (vkt)

YKI-suoritusten arviointitilat ilmoitetaan KIOS-järjestelmälle. Tavallisesti
ilmoitus lähetetään välittömästi Solki-päivityksen yhteydessä; ajastetun
tehtävän `Lähetä YKI-arviointitilat KIOS-palveluun` tehtävänä on varmistaa
perille meno verkkokatkosten jälkeen.

- `IlmoittautumisjarjestelmaClientImpl.post(...)` palauttaa
  `Either<IlmoittautumisjarjestelmaException, T>`.
- Virhetyypit: `BadRequest`, `UnexpectedError`, `MalformedResponse`,
  `NullResponse`.

VKT-ilmoittautumiset ja -suoritukset tuodaan KIOS-palvelusta.

## Oppijanumerorekisteri

**Paketti:** `oppijanumero/`
**Suunta:** haku

Oppijoiden henkilötietojen ja oppijanumeroiden noutoon.

- `OppijanumerorekisteriClient.getUser(...)` palauttaa
  `Either<OppijanumeroException, Oppija>`.
- Virhetyypit: `OppijaNotFoundException`, `BadRequest`, `UnexpectedError`,
  `MalformedResponse`, `NullResponse`. `HasResponse`-rajapinta merkitsee
  ne, joilla on raaka HTTP-vastaus liitteenä.

## Organisaatiopalvelu

**Paketti:** `organisaatiot/`
**Suunta:** haku

Käytetään mm. oppilaitosten OID:eihin ja niiden hierarkian noutoon validoinnin tueksi.

- `OrganisaatiopalveluClient.get(...)` palauttaa
  `Either<OrganisaatiopalveluException, T>`.
- Virhetyypit vastaavat oppijanumeropuolen mallia.

## Koodisto

**Paketti:** `koodisto/`
**Suunta:** haku

OPH:n keskitetty koodistopalvelu — käytetään esim. kielikoodien ja
luokitusten validointiin.

## Koealusta (kotoutumiskoulutus)

**Paketti:** `kotoutumiskoulutus/koealusta/`
**Suunta:** haku + tiedoston tuonti

Koealusta on Moodle-pohjainen kielitestialusta, josta Kielitutkintorekisteri hakee
suoritustietoja ja Moodle-XML-pohjaisia tehtäväpaketteja.

- `KoealustaSuoritusValidator` — validoi yksittäisten suoritusten datan
  oppijanumerorekisteröintiä varten; palauttaa
  `Either<KoealustaMappingError.*Failure, …>`.
- `tehtavapankki/TehtavapankkiIngestService` — lataa XML:t S3:sta, parsii ne
  XmlParserilla ja tallentaa yleiseen tehtäväpankki-skeemaan.

## Solki — YKI-tietojen lähde ja arvioijarekisterin vastaanottaja

**Paketti:** `yki/`, arvioijien lähetys `yki/arvioijat/solki/`
**Suunta:** kaksisuuntainen

Yleiset kielitutkinnot lähetetään Jyväskylän yliopiston Solki-järjestelmästä.

### Arvioijarekisterin lähetys (kitu → Solki)

Kitu on arvioijarekisterin master, joten jokainen kitussa tehty tallennus lähetetään Solkille:

```
PUT {kitu.yki.baseUrl}arvioijat/{arvioijanOppijanumero}
Authorization:   Basic (kitu.yki.username/password, samat tunnukset kuin suoritushaussa)
Idempotency-Key: {arvioijanOppijanumero}:{versio}
```

Runko on `SolkiArvioijaRequest`: samat kentät kuin poistuneessa CSV-tuonnissa, ilman henkilötunnusta
(1.1.2026 lainmuutos). `tila` on **laskettu** arvo (`Rekisterointitila`), ei kannassa säilytettävä —
vastaanottajan on syytä johtaa se samoista kauden päivistä. Sopimus on kuvattu kokonaisuudessaan
`yki-arvioijarekisteri-suunnitelma.md`:n luvussa 5.1 (JYU hyväksynyt 1.9.2026, täydennetty 4.9.2026).

**`syntymaaika` on ainoa kenttä jota CSV:ssä ei ollut** — Solki johti sen henkilötunnuksesta, jota
kitu ei enää lähetä. Kitu **ei säilytä** syntymäaikaa vaan hakee sen ONR:stä lähetyshetkellä
(`OppijanumeroService.getHenkiloByMasterOid`). Kaksi tapausta on pidettävä erillään:

| ONR                                                | Kitu tekee                                                                                         |
| -------------------------------------------------- | -------------------------------------------------------------------------------------------------- |
| vastaa, syntymäaika on                             | lähettää kentän muodossa `yyyy-MM-dd`                                                              |
| vastaa, syntymäaikaa ei ole (yksilöimätön henkilö) | jättää kentän **kokonaan pois** rungosta (`@JsonInclude(NON_NULL)`) ja merkitsee rivin lähetetyksi |
| ei vastaa                                          | **ei lähetä lainkaan**: `merkitseLahetysvirhe` ja rivi jää jonoon                                  |

Viimeinen rivi on tarkoituksellinen poikkeus turvakiellon mallista, joka epäonnistuu auki: vajaana
lähtenyt rivi merkittäisiin lähetetyksi, eikä mikään lähettäisi sitä uudelleen ennen seuraavaa
muokkausta, joten ONR-katko jättäisi pysyvän aukon Solkin kopioon.

**Lähetysjono** elää `yki_arvioija`-taulun sarakkeissa `solkiin_lahetetty`, `solki_lahetysvirhe`,
`solki_lahetysyritykset` ja `solki_viimeisin_lahetysyritys`. Rivi on jonossa, kun
`solkiin_lahetetty IS NULL OR solkiin_lahetetty < muokattu`. Solkista sisään tullut push leimataan heti
lähetetyksi, jottei Solkin oma data kaiku takaisin sille.

**Uudelleenyritykset** (`SolkiArvioijaScheduledTasks`):

| Kerros                        | Toteutus                                                       |
| ----------------------------- | -------------------------------------------------------------- |
| Välitön palaute virkailijalle | yksi synkroninen yritys tallennuksen jälkeen                   |
| "3 kertaa"                    | `FIXED_DELAY\|900s`, poimii rivit `solki_lahetysyritykset < 3` |
| "sen jälkeen säännöllisesti"  | `DAILY\|02:15`, poimii kaikki lähettämättömät                  |

Virkailija näkee tilan arvioijan tietosivun **Integraatiot**-kortissa ja voi käynnistää lähetyksen
uudelleen. Listanäkymässä on suodatin `vainSolkiVirheet` ja etusivulla laskuri.

`kitu.yki.arvioijarekisteri.integraatio.enabled` ohjaa **molempia suuntia** ja on erillinen
kytkin osoitteesta: `kitu.yki.baseUrl` on asetettu joka
ympäristössä (myös local ja e2e dev-stubiin), joten sen olemassaolo ei kerro, saako lähettää. Kun
kytkin on pois, rivit jäävät jonoon ja lähtevät takautuvasti kytkimen avautuessa.

**Poikkeus: virkailijan käsin käynnistämä lähetys ei katso kytkintä.** Tietosivun
"Lähetä uudelleen Solkiin" -painike kutsuu `SolkiArvioijaService.lahetaArvioijaKasin`ia, joka
lähettää myös kytkimen ollessa pois — yksittäinen lähetys on nimenomaan se tapa, jolla integraatiota
kokeillaan ennen automaattisen liikenteen avaamista. Onnistunut lähetys poistaa rivin jonosta ja
epäonnistunut kirjaa virheen aivan kuten automaattinenkin. Painike on sen sijaan **muokkauskytkimen**
takana, koska POST-reitti on. Tämän vuoksi `SolkiArvioijaClientImpl` ja `SolkiArvioijaServiceImpl`
ovat ehdottomia beaneja: kytkin luetaan vasta palvelun sisällä.

**Virheilmoitus ei sisällä pyyntörunkoa** (`SolkiArvioijaException.debugString()`): osoite,
sähköposti ja syntymäaika ovat henkilötietoa, ja virhe päätyy sekä lokiin että virhesarakkeeseen, joka näkyy
käyttöliittymässä. Vastausrunko otetaan mukaan mutta **katkaistaan 500 merkkiin** — vastaus voi
kaiuttaa lähetetyt arvot takaisin, ja `solki_lahetysvirhe` on rajoittamaton `TEXT`.

### Yhteystietojen päivitys (Solki → kitu)

Solki lähettää yhteystietojen muutokset nykyisen `POST /yki/api/arvioija` -rajapinnan kautta.
Rekisterimerkintää se ei kirjoita: nimet tulevat ONR:stä ja kaudet kitusta. Ks. suunnitelman §4.2.

Kavennus on saman `kitu.yki.arvioijarekisteri.integraatio.enabled` -kytkimen takana kuin lähtevä
suunta. Kytkimen ollessa pois kitu ei ole vielä master, joten Solkin **koko payload** otetaan yhä
vastaan ja tallennetaan.

## CAS + OAuth2 — virkailija-autentikointi

**Paketit:** `auth/`, `oauth2client/`
**Suunta:** haku

Sovellus käyttää OPH:n keskitettyä Otuva-palvelua.

## Auditlokit

**Paketti:** `auditlogs/`

OPH:n auditlokitoteutus ECS-formaatissa. Logien siirto
S3:een hoidetaan infrassa stack-kohtaisesti
(`koski-audit-logs-integration-stack`).
KOSKI-tiimin ylläpitämä palvelu.
