# Verify: run the server locally without AWS and drive it over HTTP

Recipe for runtime-verifying backend changes without AWS credentials or CAS, using
the self-contained `e2e` Spring profile.

## Launch

```shell
docker compose up -d db     # Postgres on :5432, pre-creates kitu-test-e2e-0..3

cd server
SPRING_PROFILES_ACTIVE=e2e TEST_PARALLEL_INDEX=1 TEST_WORKER_PORT=8331 ./mvnw spring-boot:run
# health: curl http://localhost:8331/kielitutkinnot/actuator/health
```

- Context path is `/kielitutkinnot`.
- The `e2e` profile needs no secrets, disables CSRF, opens `/dev/**`, and replaces
  ONR/organisaatiopalvelu/koodisto with in-process mocks (`MockOppijanumeroService`,
  `MockOrganisaatioService`, `MockKoodistoService`).
- **Run via `spring-boot:run`, not `java -jar`**: the mock services read their fixtures
  with `ClassPathResource(...).file`, which throws inside a fat jar — ONR lookups then
  fail and tiedonsiirto POSTs fail on "Oppijanumeroa ei löydy".
- Fresh DB when migration behavior matters:
  `docker compose exec db psql -U kitu -d postgres -c 'DROP DATABASE IF EXISTS "kitu-test-e2e-1"' -c 'CREATE DATABASE "kitu-test-e2e-1"'`
- To boot the DB at a pre-change migration level, build the jar, `zip -d` the new
  `BOOT-INF/classes/db/migration/V*.sql` out of a copy, boot that copy once, seed via
  psql, then boot the real build to watch the migration apply.

## Drive

- Browser-session auth: `curl -c cookies.txt .../kielitutkinnot/dev/mocklogin` (302 = ok),
  then send the cookie jar to any virkailija view (`/yki/suoritukset`, POST
  `/yki/tarkistusarvioinnit` with `suoritukset=<solkiId>&hyvaksyttyPvm=...`, etc.).
- API bearer token: `POST /kielitutkinnot/dev/oauth/token` with
  `grant_type=client_credentials&client_id=DEFAULT&client_secret=x` → `access_token`,
  accepted by `/yki/api/**` and other oauth2 endpoints.
- Tiedonsiirto body: start from `server/src/test/resources/yki-tiedonsiirto-example.json`;
  `suoritus.lahdejarjestelmanId` is an object `{"id": "...", "lahde": "Solki"}`, and the
  example carries a `tarkistusarviointi` that only fits tarkistus-tilat — drop it when
  posting plain ARVIOITU variants.
- Seed data: `GET /dev/mockdata/yki/suoritus/{count}` writes through the real repository
  and returns the saved entities as JSON. Note: generated `tutkintopaiva` is the min of 5
  random dates since 2000, so it's practically always years in the past.
- DB inspection: `docker compose exec db psql -U kitu -d kitu-test-e2e-1 -c '...'`.

## Gotchas

- Flyway runs every pending migration at boot, so DB state can change between restarts for
  reasons unrelated to your change — diff against a freshly created database when in doubt.
  (The old `YkiArviointitilaMigration` boot-time rewrite is gone; `Arviointitila` conversion is
  now opt-in via `kitu.yki.convertLegacyArviointitila.enabled`, off outside prod.)
- Anything owning port 8331/8080 or the e2e DBs: prior Playwright runs use the same
  databases (`TEST_PARALLEL_INDEX` 0-3).
- Screenshots: browse via `http://127.0.0.1:8331`, **not** `localhost`. The `e2e` profile's
  `kitu.appUrl` is `http://127.0.0.1:…`, so `pico.min.css`/`style.css` are linked by absolute
  127.0.0.1 URLs; a session started on `localhost` renders the pages completely unstyled, which
  makes any layout/CSS check meaningless (and Pico's dropdown overlay simply absent).
