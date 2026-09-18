# Kielitutkintorekisteri

Kielitutkintorekisteriin säilötään tutkintojen ja kieliosaamisen tunnustaminen -yksikön hallinnoimiin kielitutkintoihin
liittyviä rekisteritietoja. Kielitutkintorekisteriä käyttävät Opetushallituksen virkailijat, mutta rekisteritietoja
viedään KOSKI-palveluun ja siten niitä voidaan näyttää Oma Opintopolun asiakkaille.

- [Ulkoiset rajapintakuvaukset](https://virkailija.testiopintopolku.fi/kielitutkinnot/api-docs)
- [Tietokantaskeeman dokumentaatio](https://opetushallitus.github.io/kielitutkintorekisteri/db)
- Beans-komponenttikompositio: [Untuva](https://opetushallitus.github.io/kielitutkintorekisteri/uml/untuva) / [QA](https://opetushallitus.github.io/kielitutkintorekisteri/uml/qa) / [Tuotanto](https://opetushallitus.github.io/kielitutkintorekisteri/uml/prod)
- [Eräajot](./docs/db-scheduler)
- [Tekninen dokumentaatio](./docs/technical)

## Riippuvuudet

- mise
- IntelliJ IDEA
- Docker
- tmux

Loput riippuvuudet asennetaan käyttäen `mise` -työkalua. Misen asennus onnistuu homebrewlla (`brew install mise`) tai
[vaihtoehtoisilla tavoilla](https://mise.jdx.dev/getting-started.html#_1-install-mise-cli). Kehitysympäristön perustusskripti suorittaa `mise install` osana kehitysympäristön
perustusta, joten sen ajaminen erikseen ei ole tarpeen. Ajantasainen lista asennettavista riippuvuuksista on nähtävillä
`.mise.toml`-tiedostossa, jossa on myös muu Misen konfiguraatio (mukaanlukien joitain tarpeellisia ympäristömuuttujia).

## Kehittäminen

### Salaisuudet paikallisessa kehityksessä

Sovellus hakee salaisuudet AWS Secrets Managerista, kun ympäristö käynnistetään
[start_local_env.sh](./scripts/start_local_env.sh)-skriptillä: se konfiguroi `aws`-komentorivityökalun
käyttämään OPH:n AWS-tilejä ([ensure_aws_profiles.sh](./scripts/ensure_aws_profiles.sh)) ja noutaa
salaisuudet ([ensure_aws_secrets.sh](./scripts/ensure_aws_secrets.sh)).
Huomaa, että [start_local_server.sh](./scripts/start_local_server.sh) **ei** tee kumpaakaan — se vain
käynnistää palvelimen, joten salaisuuksien pitää olla jo ympäristössä.
Voit konfiguroida AWS-profiilit myös erikseen [ensure_aws_profiles.sh](./scripts/ensure_aws_profiles.sh)-komennolla.

### Kehitysympäristön perustaminen

Paikallisen kehitysympäristön perustamiseen käytetään skriptiä `start_local_env.sh`. Skriptiä sovelletaan myös
kehitysympäristön riippuvuuksien ja perustusvaiheiden dokumentaationa.

```shell
./scripts/start_local_env.sh
```

Skripti perustaa kehitysympäristön ja oletuksena avaa uuden `tmux` session, jonka eri ikkunoihin esim. tietokantaan ja
taustapalvelimeen liittyvät prosessit käynnistetään.

Mikäli et halua avata `tmux`-sessiota ja haluat käynnistää tietokannan ja taustapalvelimen yms. jollain muulla tavoin,
skriptille voi antaa `--setup-only` parametrin. Tällöin suoritetaan kehitysympäristön perustus ja konfigurointi, mutta
`tmux`-session perustaminen, sekä Docker-konttien ja palvelinten käynnistäminen jätetään tekemättä.

```shell
./scripts/start_local_env.sh --setup-only
```

### Offline-kehitys

Kun internet-yhteyttä ei ole saatavilla (esim. lentokoneessa tai junassa), sovelluksen voi käynnistää
ilman AWS Secrets Manager -kutsuja ja ilman Untuva Opintopolun saavutettavuutta. Tämä käyttää
`local-opintopolku`-profiilia, joka mockaa Koodisto-, Oppijanumero-, Organisaatio- ja
Tehtäväpankki-palvelut sekä ohittaa Otuvan JWT-validoinnin paikallisella avaimella.

Ennakkovaatimukset (tee verkossa ollessasi):

- Maven-, mise- ja Docker-image-välimuistit ovat valmiina paikallisesti (`./mvnw package` ja `docker compose pull` kerran).

Käyttö:

```shell
# Käynnistä tietokanta (Docker-image pitää olla välimuistissa)
docker compose up -d db

# Avaa IDEA offline-profiilien kanssa
./scripts/start_offline_dev.sh idea .

# Tai käynnistä palvelin suoraan
./scripts/start_offline_dev.sh ./scripts/start_local_server.sh

# Tarvittaessa offline-tilassa myös Mavenin osalta
./scripts/start_offline_dev.sh ./mvnw -o spring-boot:run
```

Kirjautumaton pyyntö ohjataan automaattisesti mock-kirjautumissivulle
`http://localhost:8080/kielitutkinnot/dev/login`, josta voi valita käyttäjätilin
(roolit määritelty `MockUser`-enumissa). Tilin voi valita myös suoraan URL:lla
`http://localhost:8080/kielitutkinnot/dev/mocklogin/ROOT`.

Rajoitukset offline-tilassa:

- KOSKI-, YKI- ja Koealusta-tuontien ajastetut tehtävät on poistettu käytöstä.
- Ulkoiset integraatiot palauttavat mock-dataa eivätkä kutsu oikeita palveluita.
- E2E-testit (Playwright) eivät kuulu offline-tukeen.

### IDEA

#### Laajennukset

1. Navigoi `Settings` -> `Plugins`
2. Asenna tai varmista, että ainakin seuraavat pluginit on asennettu:
   - Ktlint
   - Kotlin
   - Maven
   - Flyway
   - Spring
   - Spring Boot

#### Koodin tyyli ja muotoilu

Sovelluksessa käytetään `ktlint` - teknologiaa kotlin - tiedostojen tyylittämiseen. `mise` asentaa Ktlintin
kehitysympäristön perustamisen yhteydessä. IntelliJ IDEA:aan saa Ktlint-liitännäisen, jonka asentamisen jälkeen IDEA:n
voi laittaa muotoilemaan koodin tallentamisen yhteydessä.

1. Navigoi `Settings` -> `Tools` -> `KtLint`
2. Tämän valikon alta, aseta `Mode: Distract free` ja varmista että `Format: on save` -valintaruutu on valittu.

Sen lisäksi kotlinista on hyvä olla [K2](https://blog.jetbrains.com/idea/2024/11/k2-mode-becomes-stable/) - moodi päällä.

1. Navigoi `Settings` -> `Languages & Frameworks` -> `Kotlin`
2. Täällä valitse `Enable K2 mode` - checkbox ja käynnistä IDEA uudelleen.

## Ympäristöt

Sovellus julkaistaan kolmelle AWS-tilille. Niiden lisäksi on jaettu util-tili, jolla sijaitsevat
yhteinen ECR-repo ja GitHub Actions -roolit:

| Nimi | AWS-tili     | Käyttötarkoitus                            |
| ---- | ------------ | ------------------------------------------ |
| dev  | 682033502734 | Untuva-ympäristö                           |
| test | 961341546901 | QA-ympäristö                               |
| prod | 515966535475 | Tuotanto                                   |
| util | 961341524988 | Yhteinen ECR-repo + GitHub Actions -roolit |

### Ensimmäinen julkaisu

Ensimmäinen julkaisu AWS-tilille, jolle ei ole vielä julkaistu palvelua vaatii, että kehittäjä käy manuaalisesti antamassa luvan AWS Chatbotille päästä käsiksi Slack-kanavaan jonne hälytykset ohjataan. Tämä tapahtuu AWS Chatbot -palvelun konsolista kohdasta "Configure a chat client" ja valitsemalla Slackin.

### Automaattinen julkaisu

Julkaisu tapahtuu automaattisesti GitHub Actions -palvelussa [Build](./.github/workflows/build.yml)-tiedoston mukaisesti jokaisella `main`-haaran päivityksellä.

### Manuaalinen julkaisu

Julkaisun voi myös ajaa omalta koneelta komennoilla:

[//]: # "TODO: luo skripti tätä varten"

```shell
(cd infra && TAG=$(git rev-parse HEAD) npx cdk deploy 'Util/**')
(cd infra && TAG=$(git rev-parse HEAD) npx cdk deploy 'Dev/**')
(cd infra && TAG=$(git rev-parse HEAD) npx cdk deploy 'Test/**')
(cd infra && TAG=$(git rev-parse HEAD) npx cdk deploy 'Prod/**')
```

### Salaisuudet AWS-ympäristöissä

Seuraavat salaisuudet pitää luoda manuaalisesti AWS Secret Manageriin.

Lista vastaa `secrets`-lohkoa tiedostossa [service-stack.ts](infra/lib/service-stack.ts) — deploy ei
onnistu, jos jokin näistä puuttuu tililtä.

- `kielitesti-token`: Koealustan salaisuus. Ks. [fi.oph.kitu.kielitesti-paketti](server/src/main/kotlin/fi/oph/kitu/kotoutumiskoulutus).
- `palvelukayttaja-password`: OPH:n palvelukäyttäjän salasana, jolla mm. oppijanumerorekisteriä
  kutsutaan. Ks. [fi.oph.kitu.oppijanumero-paketti](server/src/main/kotlin/fi/oph/kitu/oppijanumero).
- `palvelukayttaja-oauth-password`: saman palvelukäyttäjän OAuth2-salaisuus.
- `yki-api-user` ja `yki-api-password`: Solkin YKI-rajapinnan tunnukset.
- `tolgee-api-key`: Tolgee Cloudin **Project API Key** käännösavainten synkronointiin. Vain Test-tilille
  `961341546901` (QA), regioona `eu-west-1`. Tarvittavat scopet: `keys.view`, `keys.create`,
  `keys.delete`, `translations.view`. Personal Access Token ei kelpaa — projekti tulee avaimesta.
  Ks. [fi.oph.kitu.i18n.tolgee-paketti](server/src/main/kotlin/fi/oph/kitu/i18n/tolgee).

## Käyttöliittymän käännökset (lokalisointi)

Virkailijakäyttöliittymän tekstit ovat oletuksena suomeksi koodissa
([UiText.kt](server/src/main/kotlin/fi/oph/kitu/i18n/UiText.kt)). Ruotsin- ja englanninkieliset
käännökset haetaan Tolgeesta OPH:n lokalisointipalvelun kautta sovelluksen käynnistyessä (ja
ajoittain uudelleen) polusta `/lokalisointi/tolgee/kielitutkintorekisteri/{fi,sv,en}.json`.

Haku on käytössä ympäristöissä, joissa `kitu.lokalisointi.namespace` on asetettu (untuva, qa,
tuotanto sekä paikallinen `local`-profiili, joka lukee untuvan proxysta). Nimiavaruus (namespace)
erottaa eri sovellusten käännökset jaetussa lokalisointipalvelussa; kitun nimiavaruus on
`kielitutkintorekisteri` (vrt. KOSKIn `koski`). Testeissä, e2e:ssä ja `local-opintopolku`-profiilissa
nimiavaruus on tyhjä, jolloin käytetään koodin suomenkielisiä oletuksia eikä proxya kutsuta.
Erillistä salaisuutta ei lukemiseen tarvita, koska proxy tarjoilee julkaistut käännöstiedostot.

### Avainten synkronointi Tolgeehen

Koodin avainluettelo viedään Tolgeehen automaattisesti: sovellus vertaa käynnistyessään koodin
avaimia (`UiTextRegistry`, lämmitetty `UiTextWarmup`illa) Tolgeen nimiavaruuden avaimiin ja

- luo Tolgeehen avaimet, joita sieltä puuttuu, suomenkielisen oletustekstin kanssa
  (`resolution: NEW`, eli olemassa olevaa käännöstä ei koskaan ylikirjoiteta), ja
- poistaa Tolgeesta avaimet, joita koodissa ei enää ole.

Synkronointi kirjoittaa **suoraan Tolgee Cloudiin** (`https://app.tolgee.io`), ei lokalisointipalvelun
proxyn kautta — proxy on read-only eikä siinä ole poistoreittiä. Käännösten _luku_ pysyy proxyssa.

Ajo tapahtuu **vain QA-ympäristössä** ja vain sovelluksen käynnistyessä. Ympäristörajaus on
salaisuus itse: koko synkronointipaketti on gatettu `kitu.tolgee.apiKey`-propertyyn, ja
`TOLGEE_API_KEY` injektoidaan vain Test-tilin (QA) taskiin. Muualla property on tyhjä eivätkä beanit
edes instantioidu.

Poistoja rajoittaa turvaraja `max(kitu.tolgee.sync.minDeleteAllowance, kitu.tolgee.sync.maxDeleteRatio ×
Tolgeen avainmäärä)` (oletuksena `max(5, 10 %)`). Jos poistettavia on enemmän, **uudet avaimet viedään
silti, poistot ohitetaan** ja etusivulle tulee varoitus. Sama varoitus näkyy, jos synkronointi
epäonnistui tai koodin avainrekisteri oli tyhjä. Virhepoistosta toipuu Tolgeen roskakorin kautta
(Tolgee UI → Trash → Restore).

Vianselvitykseen: `kitu.tolgee.sync.dryRun=true` laskee ja lokittaa diffin kirjoittamatta mitään.

Koska suomi on kanoninen koodissa, `resolution: NEW` ei päivitä Tolgeehen jo vietyä lähdetekstiä.
Kun tekstin **merkitys** muuttuu, vaihda avaimen nimi — muuten kääntäjä näkee vanhan lähdetekstin.

Tolgeen monikkoeditori (ICU, `{count, plural, …}`) ei toimi kitussa: `LocalizedString.interpolate` on
pelkkä `{name}`-korvaus, joten ICU-syntaksi renderöityisi raakana. Tämä koskee `{count}`-avaimia
`error.jarjestelmassaVirheita`, `error.koskiSiirtoEpaonnistunut` ja `time.{minuuttia,tuntia,paivaa}Sitten`.

## Hyödyllisiä komentoja

```shell
# Jos haluat lisätä formatointitarkastuksen commitin luonnin yhteyteen
./scripts/setup-hooks.sh

# Tarkista formatointi. Voit formatoida koodin ajamalla `ktlint --format`
./scripts/check-formatting.sh

# paketoi projektin.
mvn package

# Voit käyttää tätä jos ajat ympäristöä terminaalin kautta
./scripts/start_local_server.sh

# e2e-testien ajaminen e2e-hakemistossa
# Playwrightin UI testien ajamiseen --ui flagilla
npx playwright test

# e2e-testien ajaminen rinnakkain. Määritä haluttu workerien lukumäärä ympäristömuuttujalla TEST_WORKERS.
# Oletusarvo on 1, joka poistaa testien rinnakkaisuuden käytöstä. Ylärajaa ei ole konfiguraatiossa, mutta
# jokainen worker käynnistää oman palvelimensa porttiin 8080+worker-indeksi, joten 4 on käytännön katto.
# CI ajaa aina yhdellä workerilla.
TEST_WORKERS=4 npx playwright test
```
