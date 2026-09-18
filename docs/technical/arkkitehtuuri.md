# Arkkitehtuuri

Kielitutkintorekisteri on virkailijoille suunnattu Spring Boot -palvelu, joka
ylläpitää OPH:n hallinnoimien kielitutkintojen rekisteritietoja ja välittää
osajoukon niistä KOSKI-palveluun. Sovellus julkaisee palvelimella renderöityjä
HTML-sivuja sekä JSON-rajapintoja.

## Repositorion rakenne

| Hakemisto  | Sisältö                                                                                                                                                                      |
| ---------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `server/`  | Spring Boot 4.1 + Kotlin 2.4 -taustapalvelu (versiot: `kotlin.version` ja `java.version` tiedostossa `server/pom.xml`). Lähdekansiot `src/main/kotlin` ja `src/test/kotlin`. |
| `infra/`   | AWS CDK -sovellus (TypeScript). Ympäristöt: `Util`, `Dev`, `Test`, `Prod`.                                                                                                   |
| `e2e/`     | Playwright-end-to-end-testit, jotka ajetaan oikeaa palvelinta + Postgresia vasten.                                                                                           |
| `scripts/` | Skriptejä paikallisen kehitystyötä varten.                                                                                                                                   |
| `docs/`    | GitHub Pages -lähde: SchemaSpy-, UML- ja tekstidokumentaatio.                                                                                                                |

## Taustapalvelun pakettijakautuma

Paketit hakemistossa `server/src/main/kotlin/fi/oph/kitu/` on jaoteltu
**toiminta-alueittain** (feature domain), ei kerroksittain. Tyypillinen paketti
sisältää oman `*ApiController`-, `*ViewController`- (kotlinx.html-renderöinti),
`*Service`-, `*Repository`- ja `*ScheduledTasks`-komponenttinsa.

### Toiminta-alueet

- **`vkt/`** — Valtionhallinnon kielitutkinnot
- **`yki/`** — Yleiset kielitutkinnot (suoritukset ja arvioijat)
- **`kotoutumiskoulutus/`** — Kotoutumiskoulutuksen päättötestit. Sisältää integraation Koealusta-kielitestipalveluun, mukaan lukien
  Moodle-XML-pohjaisten tehtäväpakettien tuonti (`koealusta/tehtavapankki/`)
- **`koski/`** — Lähtevä KOSKI-integraatio (YKI- ja VKT-suoritusten siirto)
- **`ilmoittautumisjarjestelma/`** — Lähtevä KIOS-integraatio (arviointitilat)
- **`oppijanumero/`** — Oppijanumerorekisterin-integraatio
- **`organisaatiot/`** — Organisaatiopalvelu-integraatio
- **`koodisto/`** — Koodistopalvelu-integraatio
- **`yhteystiedot/`** — Rajapinta kielitutkintosuorituksen tekijän yhteystietojen hakuun (KOSKI-palvelu käyttää digitodistusten postitusta varten)
- **`security/`** — CAS- ja OAuth2-autentikointi (`security/cas/`, `security/oauth2/`). Aiemmat paketit `auth/` ja `oauth2client/` on poistettu.

### Läpileikkaavat paketit

- **`csvparsing/`** — Jackson CSV-formaatin yli rakennettu apukerros. Vain **vientiin** (listanäkymien CSV-lataukset); Solkin CSV-tuontiputki on poistettu, ja tiedot tulevat sisään JSON-rajapintojen kautta
- **`tiedontuontischema/`** — Tietomallit ja validaatiot rajapinnoille, joilla tuodaan suorituksia
- **`html/`** — kotlinx-html-pohjaiset sivupohjat ja komponentit
- **`i18n/`** — Käännöstuki
- **`observability/`**, **`auditlogs/`** — OpenTelemetry- ja ECS-strukturoidut lokit
- **`restclient/`** — Spring `RestClient` -konfiguraatio ja apufunktiot
- **`tehtavapankki/`** — tehtäväpankin entiteetit ja repository. Huom: eri paketti kuin
  `kotoutumiskoulutus/koealusta/tehtavapankki/`, jossa on logiikka ja näkymät
- **`config/`**, **`jdbc/`**, **`oid/`**, **`openapi/`**, **`util/`**, **`webmvc/`**, **`dev/`** —
  konfiguraatio-, apu- ja kehitysaikaiset paketit

Pakettijaon visualisointi Spring beans -tasolla löytyy [UML-sivuilta](https://opetushallitus.github.io/kielitutkintorekisteri/uml/prod).

## Spring-profiilit ja konfiguraatio

`application.properties` toimii perustana; ympäristökohtaiset asetukset ovat
tiedostoissa `application-{local,local-opintopolku,untuva,qa,prod,e2e}.properties`.

- `kitu.yki.scheduling.*` ja `kitu.vkt.scheduling.*` -propertyt ohjaavat
  ajastettujen tehtävien aktivointia ympäristöittäin.

## Tietokanta ja migraatiot

- **Spring JDBC** PostgreSQL-tietokannan päällä — Hibernate/JPA EI ole käytössä.
- **Flyway**-migraatiot löytyvät hakemistosta
  `server/src/main/resources/db/migration` muodossa `V*__*.sql`.
- V-numeroinnissa on aukkoja (V7, V38, V46–V55, V65, V104, V105) aiempien
  virheiden jäljiltä — V65 nimettiin V67:ksi rikkoutuneen deployjärjestyksen
  korjaamiseksi (commit `04b1d07d`), ja V104/V105 numeroitiin samasta syystä
  uudelleen V108:ksi ja V109:ksi (commitit `3b2c24bd` ja `021153cd`).
  Suurin mainissa käytössä oleva numero on **V126**. **Älä käytä uudestaan
  ohitettuja numeroita; jatka aina suurimmasta käytetystä eteenpäin.**
- Tietokantakaavion visuaalinen dokumentaatio on
  [SchemaSpy-sivulla](https://opetushallitus.github.io/kielitutkintorekisteri/db).

## Ajastetut tehtävät

Ajastus toteutetaan `db-scheduler`-kirjastolla. Tehtävien aktivointi
ohjataan profiilikohtaisilla propertyilla. Sovellus tarjoaa db-scheduler UI:n
sisäänkirjautuneille virkailijoille. Lisätiedot:
[Eräajot](../db-scheduler).

`db-scheduler-log`-laajennus on toistaiseksi **kopioitu paikallisesti**
hakemistoon `server/src/main/kotlin/io/rocketbase/extension/` (versio 0.7.0
relocatoituna Spring Boot 4:ään), kunnes upstream julkaisee SB4-yhteensopivan
version. Paluuohjeet löytyvät kyseisen paketin omasta README-tiedostosta.

## Frontend

Sovellus on **palvelinrenderöity**: HTML rakennetaan `kotlinx-html-jvm`
-kirjastolla, tyylitys [Pico.css](https://picocss.com/) + projektin oma
`style.css`. SPA-rakenteita ei käytetä — eikä selainpuolen JavaScriptille ole
lainkaan käännösvaihetta: `server/src/main` ei sisällä yhtään `.js`- tai
`.ts`-tiedostoa, eikä `esbuild`ia kutsuta `pom.xml`:stä, `scripts/`-hakemistosta,
`Dockerfile`sta tai workfloweista. (`.mise.toml` asentaa `esbuild`in, mutta se on
tällä hetkellä käyttämätön.)

## Deploy-topologia

`main`-haaran päivitykset deployataan automaattisesti:

1. Server-, e2e- ja lint-jobit ajetaan rinnakkain.
2. Onnistuneen ajon jälkeen Docker-image rakennetaan ja työnnetään
   Util-tilin ECR:ään.
3. Util → Dev → Test → Prod -ketju ajetaan `_deploy-env.yml`-workflowin kautta.

AWS-tilit:

| Tili | ID             | Käyttötarkoitus                              |
| ---- | -------------- | -------------------------------------------- |
| Util | `961341524988` | Yhteinen ECR-repo ja GitHub Actions -roolit. |
| Dev  | `682033502734` | Untuva-ympäristö                             |
| Test | `961341546901` | QA-ympäristö                                 |
| Prod | `515966535475` | Tuotanto                                     |

Lisää ylläpidollista tietoa: [Ylläpito ja havainnointi](./yllapito.md).
