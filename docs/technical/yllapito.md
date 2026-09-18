# Ylläpito ja havainnointi

Tämä sivu kerää yhteen Kielitutkintorekisterin (kitu) ympäristöjen valvontaan,
deployaukseen ja salaisuuksien hallintaan liittyvät käytännöt.

## Havainnointi (observability)

### Trace-data: OpenTelemetry → Jaeger

- Sovellus käyttää Spring Bootin `spring-boot-starter-opentelemetry`-startteria
  ja eksplisiittistä `opentelemetry-spring-boot-autoconfigure`-riippuvuutta.
  Jälkimmäinen tarjoaa `@WithSpan`-aspectin, jonka Springin pelkkä starter
  jättää pois.
- Spannejä luodaan eksplisiittisesti `tracer.spanBuilder(...).use { … }`
  -patternilla (`observability/`-paketin extension-funktiot).
- Paikallinen Jaeger-instanssi ajetaan `docker-compose.yml`:n kautta;
  käyttöliittymä on saatavilla porttiin `16686`.
- Tuotannossa traceja lähetetään OTLP:llä OPH:n keskitettyyn telemetriapinoon.

### Strukturoidut lokit

Lokit tulostetaan ECS-formaatissa (Elastic Common Schema), jotta ne
indeksoituvat OPH:n yhteiseen Kibana-näkymään. `auditlogs/`-paketti
tuottaa tämän erikseen autentikointitapahtumille ja KOSKI-siirroille.

Paikallisesti voi käyttää `humanlog`-työkalua ECS-JSON-rivien
luettavaksi muuntamiseen — `start_local_server.sh` käärii sen
automaattisesti spring-boot:runin ympärille.

### Hälytykset

- Slack-yhteys on AWS Chatbot (`infra/lib/alarms-stack`), ei webhookia.
- CloudWatch-pohjaiset hälytykset määritellään `infra/lib/alarms-stack`issa
  ja `infra/lib/koski-audit-logs-integration-stack`issa.
- AWS Health -tapahtumat (ajonaikojen ja versioiden elinkaari) reititetään
  samaan Slack-kanavaan `alarms-stack`in EventBridge-säännöllä.

## Salaisuudet

- **Älä koskaan tallenna salaisuuksia lähdekoodiin.**
- Paikallinen kehitys hakee salaisuudet AWS Secrets Managerista
  `scripts/ensure_aws_secrets.sh`-skriptin kautta. Skriptiä kutsutaan
  automaattisesti, kun käynnistät ympäristön `./scripts/start_local_env.sh`-komennolla.
- Manuaalisesti perustettavat salaisuudet per AWS-tili
  (pää-README sisältää aina ajantasaisimman listan):
  - `kielitesti-token`
  - `palvelukayttaja-password` (mm. oppijanumerorekisterin kutsut)
  - `palvelukayttaja-oauth-password`
  - `yki-api-user` ja `yki-api-password`
  - `tolgee-api-key` (vain Test-tili / QA; käännösavainten synkronointi Tolgeehen)

## AWS-tilit ja roolit

| Tili     | ID             | Käyttötarkoitus                            |
| -------- | -------------- | ------------------------------------------ |
| **Util** | `961341524988` | Yhteinen ECR-repo + GitHub Actions -roolit |
| **Dev**  | `682033502734` | Untuva-ympäristö                           |
| **Test** | `961341546901` | QA-ympäristö                               |
| **Prod** | `515966535475` | Tuotanto                                   |

Ympäristön mise-konfiguraatio asettaa oletukseksi `AWS_PROFILE=oph-ktr-dev`.
Profiilit konfiguroidaan komennolla `scripts/ensure_aws_profiles.sh`.

## Deploy-putki

`main`-haaran päivitykset ajavat `.github/workflows/build.yml`-workflowin:

1. **`server_tests` + `frontend_tests` + `lint`** — rinnakkain.
2. **`build_image`** — Docker-kuva työnnetään Util-tilin ECR-repoon
   (`961341524988`) tagilla `${git sha}`.
3. **`deploy_util` → `deploy_dev` → `deploy_test` → `deploy_prod`** — yhteinen
   workflow `_deploy-env.yml` ottaa CDK-stack-kohtaisen ympäristöparametrin.
4. **`build_docs`** — viimeisenä rakentaa dokumentaatiosivuston (tämä sivu
   mukaan lukien) ja deployaa sen GitHub Pages -palveluun.

Tag-pohjainen manuaalinen deploy onnistuu paikallisesti:

```shell
cd infra
TAG=$(git rev-parse HEAD) npx cdk deploy 'Dev/**'
```

### Infrastruktuurin muutokset

`infra/lib/`-koodi on AWS CDK -pohjainen TypeScript-sovellus. **Muista
päivittää `infra/README.md` samassa committissa**, kun lisäät tai poistat
stackeja tai muutat IAM-identiteettejä, ristiintilejen viittauksia,
manuaalisia esivaatimuksia (DNS-vyöhykkeet, salaisuudet, KOSKI-puolen resurssit)
tai Slack/hälytys-/tutkintaputkea.

## Tietokannan ylläpito

- Migraatiot ajetaan automaattisesti sovelluksen käynnistyessä Flywayn kautta.
  V-numerojen aukot ovat tahallisia — älä käytä uudelleen ohitettuja numeroita.
- Skeeman dokumentaatio generoidaan SchemaSpy-työkalulla CI:n `build_docs`-vaiheessa
  (`docs/build.sh`).

## Päivittäiset eräajot

Ks. erillinen [Eräajot](../db-scheduler) -sivu.
