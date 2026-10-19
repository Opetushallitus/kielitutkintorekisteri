# Arkkitehtuuri

Kielitutkintorekisteri on virkailijoille suunnattu Spring Boot -palvelu, joka ylläpitää OPH:n
hallinnoimien kielitutkintojen rekisteritietoja ja välittää osajoukon niistä KOSKI-palveluun.
Sovellus julkaisee palvelimella renderöityjä HTML-sivuja sekä JSON-rajapintoja.

## Repositorion rakenne

| Hakemisto  | Sisältö                                                                                                                                                                         |
| ---------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `server/`  | Spring Boot 4 + Kotlin -taustapalvelu. Lähdekansiot `src/main/kotlin` ja `src/test/kotlin`; tarkat versiot `server/pom.xml`:n `kotlin.version`- ja `java.version`-propertyissä. |
| `infra/`   | AWS CDK -sovellus (TypeScript). Ympäristöt `Util`, `Dev`, `Test`, `Prod`.                                                                                                       |
| `e2e/`     | Playwright-testit oikeaa palvelinta ja Postgresia vasten.                                                                                                                       |
| `scripts/` | Paikallisen kehityksen skriptit.                                                                                                                                                |
| `docs/`    | GitHub Pages -lähde: SchemaSpy-, UML- ja tekstidokumentaatio.                                                                                                                   |

## Taustapalvelun pakettijakautuma

Paketit hakemistossa `server/src/main/kotlin/fi/oph/kitu/` on jaoteltu **toiminta-alueittain**
(feature domain), ei kerroksittain. Tyypillinen paketti sisältää omat `*ApiController`-,
`*ViewController`- (kotlinx.html), `*Service`-, `*Repository`- ja `*ScheduledTasks`-komponenttinsa.

### Toiminta-alueet

- **`vkt/`** — Valtionhallinnon kielitutkinnot
- **`yki/`** — Yleiset kielitutkinnot (suoritukset ja arvioijat; arvioijahakemusten tuonti atarusta
  paketissa `yki/arvioijat/hakemus/`)
- **`kotoutumiskoulutus/`** — Kotoutumiskoulutuksen päättötestit; sisältää Koealusta-integraation
  ja Moodle-XML-tehtäväpakettien tuonnin (`koealusta/tehtavapankki/`)
- **`koski/`** — Lähtevä KOSKI-integraatio (YKI- ja VKT-suoritukset)
- **`ilmoittautumisjarjestelma/`** — YKI-arviointitilojen lähetys ilmoittautumisjärjestelmään
  (KIOS). Suuntaan KIOS → kitu kulkevat VKT-suoritukset ovat paketissa `vkt/`.
- **`oppijanumero/`**, **`organisaatiot/`**, **`koodisto/`** — OPH:n perusrekisteri-integraatiot
- **`ataru/`** — Hakemuspalvelu atarun asiakas (CAS-istunto) arvioijahakemusten hakuun
- **`yhteystiedot/`** — Suorittajan yhteystietojen haku (KOSKI-järjestelmän tiedotteet: todistuskieli ja paperipostin osoite)
- **`security/`** — CAS- ja OAuth2-autentikointi (`security/cas/`, `security/oauth2/`)

Integraatioiden yksityiskohdat: [Integraatiot](./integraatiot.md).

### Läpileikkaavat paketit

- **`csv/`** — CSV-latausvastaus (`csvAttachmentResponse`) ja Excel-yhteensopiva alkutavu (BOM).
  Kaikki CSV-viennit tuotetaan sarake-enumeista `html/table/`-paketin `DisplayTableCsvRenderer`illa.
- **`tiedontuontischema/`** — Suoritusten tuontirajapintojen tietomallit ja validoinnit
- **`html/`** — kotlinx-html-sivupohjat ja komponentit
- **`i18n/`** — Käännöstuki
- **`observability/`**, **`auditlogs/`** — OpenTelemetry ja ECS-strukturoidut lokit
- **`restclient/`** — `RestClient`-konfiguraatio ja apufunktiot
- **`tehtavapankki/`** — tehtäväpankin entiteetit ja repository (logiikka ja näkymät ovat
  paketissa `kotoutumiskoulutus/koealusta/tehtavapankki/`)
- **`etusivu/`** — etusivun kojelauta, joka kokoaa luvut kaikista toiminta-alueista
- **`config/`**, **`jdbc/`**, **`oid/`**, **`openapi/`**, **`util/`**, **`webmvc/`**, **`dev/`** —
  konfiguraatio-, apu- ja kehitysaikaiset paketit

Pakettijaon visualisointi Spring beans -tasolla:
[UML-sivut](https://opetushallitus.github.io/kielitutkintorekisteri/uml/prod).

## Spring-profiilit ja konfiguraatio

`application.properties` on perusta; ympäristökohtaiset asetukset ovat tiedostoissa
`application-{local,local-opintopolku,untuva,qa,prod,e2e}.properties`. Ajastettujen tehtävien
aktivointia ohjaavat ympäristöittäin `kitu.{yki,vkt,koski,cas,lokalisointi}.scheduling.*`,
`kitu.kotoutumiskoulutus.koealusta.scheduling.*` ja `kitu.ataru.arvioijahakemus.schedule`.

## Tietokanta ja migraatiot

- **Spring JDBC** PostgreSQL:n päällä — Hibernate/JPA ei ole käytössä.
- **Flyway**-migraatiot hakemistossa `server/src/main/resources/db/migration` muodossa `V*__*.sql`.
- V-numeroinnissa on aukkoja (V7, V38, V46–V55, V65, V104, V105). **Älä käytä uudelleen
  ohitettuja numeroita — jatka aina suurimmasta eteenpäin**
  (`ls server/src/main/resources/db/migration | sort -V | tail -1`).
- Skeeman visuaalinen dokumentaatio:
  [SchemaSpy](https://opetushallitus.github.io/kielitutkintorekisteri/db).

## Ajastetut tehtävät

Ajastus toteutetaan `db-scheduler`-kirjastolla, ja tehtävien aktivointi ohjataan
profiilikohtaisilla propertyilla. Sovellus tarjoaa db-scheduler UI:n sisäänkirjautuneille
virkailijoille. Tehtäväluettelo: [Eräajot](../db-scheduler).

`db-scheduler-log`-laajennus on kopioitu paikallisesti hakemistoon
`server/src/main/kotlin/io/rocketbase/extension/` (0.7.0 relocatoituna Spring Boot 4:ään), kunnes
upstream julkaisee SB4-yhteensopivan version. Paluuohjeet ovat paketin omassa README:ssä.

## Frontend

Sovellus on **palvelinrenderöity**: HTML rakennetaan `kotlinx-html-jvm`-kirjastolla, tyylitys
[Pico.css](https://picocss.com/) + projektin oma `style.css`. SPA-rakenteita ei käytetä, eikä
selainpuolen JavaScriptille ole käännösvaihetta.

## Deploy-topologia

`main`-haaran päivitykset deployataan automaattisesti: testit ja lint rinnakkain → Docker-image
Util-tilin ECR:ään → `deploy_util` → `deploy_dev` → `deploy_test` → `deploy_prod` → `build_docs`.
Putki, AWS-tilit ja hälytykset: [Ylläpito ja havainnointi](./yllapito.md).
