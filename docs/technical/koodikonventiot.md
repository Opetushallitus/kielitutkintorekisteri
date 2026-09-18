# Koodikonventiot

Tämä sivu kokoaa toistuvat tekniset valinnat, joiden noudattaminen on tärkeää
koodikannan eheyden kannalta. Pintaa raapaisevammat kuvaukset löytyvät
[arkkitehtuurisivulta](./arkkitehtuuri.md).

## Virheenkäsittely: Arrow `Either`

Domain-virheet välitetään palauttamalla `arrow.core.Either<E, V>`
(vasen = virhe, oikea = onnistunut arvo). Älä heitä poikkeuksia palvelukerroksesta;
sen sijaan kuljeta tieto virheestä eksplisiittisenä tyyppinä.

Apufunktiot löytyvät paketista `util/result/EitherExtensions.kt`:

- `getOrThrow()` — käytä vain domain-rajalla (esim. ajastetun tehtävän
  ylimmällä tasolla), missä Eitherin avaaminen poikkeukseksi on perusteltua.
- `splitIntoValuesAndErrors()` — pilkkoo `List<Either<E, V>>` pariksi
  `(List<V>, List<E>)`. Käytetään eräajojen ja joukkolähetysten tulosten
  käsittelyssä (`KoskiService`, `YkiViewController`).

## Validointi: `Validation<T>` ja Arrow Raise

Lomake- tai rajapintadataa validoivat luokat toteuttavat oman
`Validation<T>`-rajapinnan (`util/validation/Validation.kt`) ja ylikirjoittavat
muodot:

- `ValidationRaise.validateBeforeEnrichment`
- `enrich`
- `ValidationRaise.validateAfterEnrichment`

Vaiheet ajetaan järjestyksessä (`validateAndEnrich`): ensin pre-validointi,
sitten enrichment, lopuksi post-validointi.

`ValidationRaise` on aliperuste tyypille `Raise<NonEmptyList<ValidationError>>`;
sen rinnakkainen `ValidationResult<T>` on `Either<NonEmptyList<ValidationError>, T>`.
Molemmat määritellään samassa `Validation.kt`-tiedostossa.

**Pidä vaiheet erillään:** `validate*`-vaiheet eivät muunna arvoa, vaan
ainoastaan tarkistavat sen — ne palauttavat `Unit`, jolloin tyyppijärjestelmä
estää muunnokset. Kaikki johdantatieto ja muunnokset (mm. default-arvojen
täydentäminen, välitilojen suodatus) elävät `enrich`-vaiheessa, jolla ei ole
`Raise`-vastaanotinta eikä se voi epäonnistua. Jos tarkistus riippuu enrichmentin
tuottamasta tilasta, se kuuluu `validateAfterEnrichment`iin — esimerkkinä
`YkiSuoritusValidation`, jossa `enrich` suodattaa pois ne osakokeet joihin ei
ole tultu paikalle (arvosana `12`) ja `validateAfterEnrichment` varmistaa
suodatuksen jälkeen "vähintään yksi osakoe" -invariantin.

Käytä `accumulate` / `accumulating` -funktioita kun tarve on kerätä
samaan tietueeseen useampi virheilmoitus; käytä `ensure` ja `ensureNotNull`
predikaattien käsittelyyn. Alivalidaattori joka itse akkumuloi virheitä ottaa
`RaiseAccumulate<ValidationError>`-vastaanottimen ja sisältää sisäkkäisiä
`accumulating { … }` -lohkoja.

Kontrollerikerros kutsuu validointia muodossa `validation.validateAndEnrich(…).getOrThrow()`.
Tämä poikkeus napataan `GlobalControllerExceptionHandler`issa ja muunnetaan 400-vastaukseksi.

## Jackson 3

Projekti käyttää **Jackson 3** -kirjastoa (`tools.jackson.*`-paketit,
esim. `tools.jackson.databind.json.JsonMapper`). Annotaatiot ovat edelleen
peräisin `com.fasterxml.jackson.annotation`-paketista yhteensopivuussyistä.

## RestClient ja message converterit

**Tärkeää:** jokainen `RestClient`, joka lukee JSON-vastauksen `String`-tyyppiin
(mukaan lukien `retrieveEntitySafely(String::class.java)`), **on kutsuttava
`.withLenientStringConverter()`** (`restclient/RestClientExtensions.kt`).
Spring 7:n oletus-`StringHttpMessageConverter` mainostaa vain `text/*`-tyyppejä,
joten ilman apufunktiota Jackson kaatuu vastauksen vastaanottoon.

## Spring HATEOAS 3 `linkTo`

HATEOAS-DSL seuraa kontrollerikutsua **lambdan paluuarvon kautta** (ei thread-localin),
joten lambdan signatuurin on oltava `(C) -> Any`. Vastaanotinmuotoinen
`C.() -> Unit` palauttaa `Unit` ja ohittaa proxyn `LastInvocationAware`-seurannan.
Katso esimerkki: `html/Navigation.kt`.

## Testcontainers 2.x

- Artefaktinimet ovat etuliitettyjä: `testcontainers-postgresql`,
  `testcontainers-junit-jupiter`.
- `PostgreSQLContainer` siirtyi pakettiin
  `org.testcontainers.postgresql.PostgreSQLContainer` ilman geneeristä parametria.
- macOS:lla aseta `DOCKER_HOST="unix://${HOME}/.docker/run/docker.sock"`,
  jos socketia ei tunnisteta automaattisesti.

## Formatointi ja staattinen analyysi

- **Kotlin:** ktlint (K2-tila, IDEA:n format-on-save), aja `./scripts/format.sh`.
- **TS/JSON/YAML/MD:** Prettier.
- **Shell:** ShellCheck (`scripts/*.sh`).
- CI ajaa skriptin `./scripts/check-formatting.sh`, joka heijastaa täsmälleen
  paikallista format.sh:ta read-only-tarkistuksena.
- `.git-blame-ignore-revs` seuraa pelkkiä formatointi-commiteja — lisää uudet
  laaja-alaiset uudelleenformatoinnit listaan.

## Salaisuudet

Salaisuuksia ei tallenneta lähdekoodiin. Paikallinen kehitys hakee ne AWS
Secrets Managerista skriptin `scripts/ensure_aws_secrets.sh` kautta.
Pää-README sisältää listan salaisuuksista, jotka on perustettava manuaalisesti
per AWS-tili (`kielitesti-token`, `palvelukayttaja-password`,
`palvelukayttaja-oauth-password`, `yki-api-user`, `yki-api-password` sekä vain
Test-tilille `tolgee-api-key`). Lista vastaa `service-stack.ts`:n
`secrets`-lohkoa.
