# Koodikonventiot

Toistuvat tekniset valinnat, joiden noudattaminen on tärkeää koodikannan eheyden kannalta.
Laajempi rakennekuvaus: [Arkkitehtuuri](./arkkitehtuuri.md).

## Virheenkäsittely: Arrow `Either`

Domain-virheet välitetään palauttamalla `arrow.core.Either<E, V>` (vasen = virhe, oikea = arvo).
Älä heitä poikkeuksia palvelukerroksesta, vaan kuljeta virhe eksplisiittisenä tyyppinä.

Apufunktiot ovat paketissa `util/result/EitherExtensions.kt`:

- `getOrThrow()` — vain domain-rajalla (esim. ajastetun tehtävän ylimmällä tasolla).
- `splitIntoValuesAndErrors()` — pilkkoo `List<Either<E, V>>` pariksi `(List<V>, List<E>)`.
  Käytössä eräajojen ja joukkolähetysten tuloksissa (`KoskiService`, `YkiViewController`).

## Validointi: `Validation<T>` ja Arrow Raise

Lomake- ja rajapintadataa validoivat luokat toteuttavat `Validation<T>`-rajapinnan
(`util/validation/Validation.kt`). `validateAndEnrich` ajaa kolme vaihetta järjestyksessä:

1. `ValidationRaise.validateBeforeEnrichment`
2. `EnrichmentRaise.enrich`
3. `ValidationRaise.validateAfterEnrichment`

Samassa tiedostossa määritellään `ValidationRaise` (= `Raise<NonEmptyList<ValidationError>>`),
`EnrichmentRaise` (= `Raise<ValidationError.EnrichmentError>`) ja `ValidationResult<T>`
(= `Either<NonEmptyList<ValidationError>, T>`).

**Pidä vaiheet erillään:** `validate*`-vaiheet palauttavat `Unit`, joten tyyppijärjestelmä estää
arvon muuntamisen niissä. Kaikki muunnokset ja johdetut arvot kuuluvat `enrich`-vaiheeseen, joka
voi epäonnistua — mutta vain yhdellä `EnrichmentError`illa, ei virhelistalla. Tarkistus, joka
riippuu enrichmentin tuottamasta muodosta, kuuluu `validateAfterEnrichment`iin. Esimerkki
`YkiSuoritusValidation`: `enrich` laskee `arviointitila`n uudelleen, kun
`kitu.yki.convertLegacyArviointitila.enabled` on päällä, ja `validateAfterEnrichment` varmistaa
mm. "vähintään yksi osakoe" -invariantin sekä sen, että arviointitila vastaa arvosanoja.

Kerää useampi virhe samasta tietueesta `accumulate` / `accumulating` -funktioilla; käytä `ensure`
ja `ensureNotNull` predikaatteihin. Alivalidaattori, joka itse akkumuloi, ottaa
`RaiseAccumulate<ValidationError>`-vastaanottimen ja sisältää sisäkkäisiä `accumulating { … }`
-lohkoja.

Kontrollerikerros kutsuu `validation.validateAndEnrich(…).getOrThrow()`; syntyvä poikkeus napataan
`GlobalControllerExceptionHandler`issa ja muunnetaan 400-vastaukseksi.

## Jackson 3

Projekti käyttää **Jackson 3**:a (`tools.jackson.*`, esim.
`tools.jackson.databind.json.JsonMapper`). Annotaatiot tulevat edelleen paketista
`com.fasterxml.jackson.annotation` yhteensopivuussyistä.

## RestClient ja message converterit

Jokainen `RestClient`, joka lukee JSON-vastauksen `String`-tyyppiin (mukaan lukien
`retrieveEntitySafely(String::class.java)`), **on kutsuttava `.withLenientStringConverter()`**
(`restclient/RestClientExtensions.kt`). Spring 7:n oletus-`StringHttpMessageConverter` mainostaa
vain `text/*`-tyyppejä, joten ilman apufunktiota Jackson kaatuu vastaanotossa.

## Spring HATEOAS 3 `linkTo`

HATEOAS-DSL seuraa kontrollerikutsua **lambdan paluuarvon kautta**, ei thread-localin, joten
lambdan signatuurin on oltava `(C) -> Any`. Vastaanotinmuotoinen `C.() -> Unit` palauttaa `Unit` ja
ohittaa proxyn `LastInvocationAware`-seurannan. Esimerkki: `html/Navigation.kt`.

## Testcontainers 2.x

- Artefaktinimet ovat etuliitettyjä: `testcontainers-postgresql`, `testcontainers-junit-jupiter`.
- `PostgreSQLContainer` on paketissa `org.testcontainers.postgresql` ilman geneeristä parametria.
- macOS:lla aseta `DOCKER_HOST="unix://${HOME}/.docker/run/docker.sock"`, jos socketia ei tunnisteta.

## Formatointi ja staattinen analyysi

- **Kotlin:** ktlint (K2-tila, IDEA:n format-on-save). **TS/JSON/YAML/MD:** Prettier.
  **Shell:** ShellCheck (`scripts/*.sh`, `infra/scripts/*.sh`).
- Aja `./scripts/format.sh` ennen committia; CI ajaa saman read-onlynä
  (`./scripts/check-formatting.sh`).
- `.git-blame-ignore-revs` seuraa pelkkiä formatointi-committeja — lisää uudet laaja-alaiset
  uudelleenformatoinnit listaan.

## Salaisuudet

Salaisuuksia ei tallenneta lähdekoodiin; paikallinen kehitys hakee ne AWS Secrets Managerista
skriptillä `scripts/ensure_aws_secrets.sh`. Manuaalisesti perustettavat salaisuudet on lueteltu
pää-README:ssä. Ks. myös [Ylläpito ja havainnointi](./yllapito.md).
