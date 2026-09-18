# Ylläpito ja havainnointi

Kielitutkintorekisterin (kitu) ympäristöjen valvontaan, deployaukseen ja salaisuuksien hallintaan
liittyvät käytännöt.

## Havainnointi (observability)

### Trace-data: OpenTelemetry → Jaeger

- Sovellus käyttää `spring-boot-starter-opentelemetry`-startteria ja eksplisiittistä
  `opentelemetry-spring-boot-autoconfigure`-riippuvuutta; jälkimmäinen tuo `@WithSpan`-aspectin,
  jonka pelkkä starter jättää pois.
- Spanit luodaan `tracer.spanBuilder(...).use { … }` -patternilla (`observability/`-paketin
  extension-funktiot).
- Paikallinen Jaeger nousee `docker-compose.yml`:stä, käyttöliittymä portissa `16686`.
  Tuotannossa tracet lähetetään OTLP:llä OPH:n keskitettyyn telemetriapinoon.

### Strukturoidut lokit

Lokit tulostetaan ECS-formaatissa (Elastic Common Schema), jotta ne indeksoituvat OPH:n yhteiseen
Kibana-näkymään; `auditlogs/` tuottaa tämän erikseen autentikointitapahtumille ja
KOSKI-siirroille. Paikallisesti `humanlog` muuntaa ECS-JSON-rivit luettavaksi —
`start_local_server.sh` käärii sen automaattisesti `spring-boot:run`in ympärille.

### Hälytykset

- Slack-yhteys on AWS Chatbot (`infra/lib/alarms-stack`), ei webhookia.
- CloudWatch-hälytykset määritellään `alarms-stack`issa ja
  `koski-audit-logs-integration-stack`issa.
- AWS Health -tapahtumat reititetään samaan Slack-kanavaan `alarms-stack`in
  EventBridge-säännöllä.

## Salaisuudet

- **Älä koskaan tallenna salaisuuksia lähdekoodiin.**
- Paikallinen kehitys hakee salaisuudet AWS Secrets Managerista
  `scripts/ensure_aws_secrets.sh`-skriptillä, jota `./scripts/start_local_env.sh` kutsuu
  automaattisesti.
- Manuaalisesti perustettavat salaisuudet per AWS-tili on lueteltu pää-README:ssä. Lista vastaa
  `infra/lib/service-stack.ts`:n `secrets`-lohkoa — pidä ne synkassa.

## AWS-tilit ja roolit

| Tili     | ID             | Käyttötarkoitus                            |
| -------- | -------------- | ------------------------------------------ |
| **Util** | `961341524988` | Yhteinen ECR-repo + GitHub Actions -roolit |
| **Dev**  | `682033502734` | Untuva-ympäristö                           |
| **Test** | `961341546901` | QA-ympäristö                               |
| **Prod** | `515966535475` | Tuotanto                                   |

mise asettaa oletukseksi `AWS_PROFILE=oph-ktr-dev`; profiilit konfiguroidaan komennolla
`scripts/ensure_aws_profiles.sh`.

## Deploy-putki

`main`-haaran päivitykset ajavat `.github/workflows/build.yml`-workflowin:

1. **`server_tests` + `frontend_tests` + `lint`** rinnakkain.
2. **`build_image`** — Docker-kuva Util-tilin ECR-repoon tagilla `${git sha}`.
3. **`deploy_util` → `deploy_dev` → `deploy_test` → `deploy_prod`** — kukin kutsuu yhteistä
   `_deploy-env.yml`-workflowia ympäristöparametrilla.
4. **`build_docs`** — rakentaa dokumentaatiosivuston (tämä sivu mukaan lukien) ja deployaa sen
   GitHub Pagesiin.

Manuaalinen deploy paikallisesti:

```shell
cd infra
npm run deploy/dev   # = TAG=$(git rev-parse main) npx cdk deploy --exclusively 'Dev/**'
```

`TAG`in on osoitettava committiin, jonka image löytyy ECR:stä — käytännössä `main`iin, koska
imaget rakennetaan vain siitä.

### Infrastruktuurin muutokset

`infra/lib/` on AWS CDK -pohjainen TypeScript-sovellus. **Muista päivittää `infra/README.md`
samassa committissa**, kun lisäät tai poistat stackeja tai muutat IAM-identiteettejä,
ristiintilien viittauksia, manuaalisia esivaatimuksia (DNS-vyöhykkeet, salaisuudet, KOSKI-puolen
resurssit) tai Slack-/hälytysputkea.

## Tietokannan ylläpito

- Migraatiot ajetaan automaattisesti käynnistyksessä Flywayn kautta. V-numeroiden aukot ovat
  tahallisia — älä käytä ohitettuja numeroita uudelleen.
- Skeeman dokumentaatio generoidaan SchemaSpy-työkalulla `build_docs`-vaiheessa (`docs/build.sh`).

## Päivittäiset eräajot

Ks. [Eräajot](../db-scheduler).
