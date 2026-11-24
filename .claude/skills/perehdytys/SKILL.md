---
name: perehdytys
description: Write or update an onboarding tutorial / knowledge-sharing session page in docs/perehdytys/ (the series for the team taking over Kielitutkintorekisteri maintenance — YKI-suoritukset, kotoutumiskoulutuksen päättötesti, and the ones still marked "tulossa" in its README). Use when asked to prepare a tiedonjakosessio, knowledge sharing, lesson, tutorial or perehdytys on some part of the system.
---

# Perehdytys: write the next tutorial

Each page in `docs/perehdytys/` is both a self-study tutorial and the agenda of a live
knowledge-sharing session. `docs/perehdytys/README.md` lists the planned topics and defines the
nine-section skeleton. The existing pages are the model: read the README and the most recent
tutorial in full before writing anything.

## Steps

1. **Branch from `main`**, not from whatever feature branch is checked out — a tutorial describes
   what is in production code. Run `./mvnw clean` in `server/` after switching.
2. **Read the code, not just `CLAUDE.md`.** `CLAUDE.md` tells you where the traps are; the
   tutorial's code-path tables need the actual class and method names. Read the feature package,
   its tests (test names become the "Testit" lines), the migrations for the tables, the
   `application*.properties` keys and the relevant section of `docs/technical/integraatiot.md`.
3. **Build the "Kokeile itse" demo first and run it.** It decides what the page can show. It must
   work on the offline profile (`./scripts/start_offline_dev.sh ./scripts/start_local_server.sh`)
   with no AWS credentials. Verify every command and every stated outcome by running it; point
   the server at a throwaway database (`SPRING_DATASOURCE_URL=…/kitu-perehdytys-demo`, created
   and dropped with `docker compose exec db psql`) instead of writing demo rows into `kitu-dev`,
   and launch through `mise exec -- env …` so mise does not reset the profile.
4. **Write the page** following the skeleton, then link it from the README list (replace the
   "(tulossa)" entry) and add the file to the page check in `.github/workflows/build.yml`
   ("Verify technical documentation pages exist").
5. **Run `./scripts/format.sh`** (prettier re-pads the tables) and, since a workflow changed,
   `./scripts/check-workflows.sh`.

## Content rules

- Finnish. Write "Kielitutkintorekisteri", never "kitu". "KOSKI" is not inflected: "KOSKI-järjestelmään".
- Name classes and methods without line numbers; packages relative to
  `server/src/main/kotlin/fi/oph/kitu/`.
- **Describe the normal case.** Leave out edge cases that practically never happen, historical
  incidents and how long things took. "Ansat ja päätökset" is for rules a maintainer could
  plausibly break: each item is Sääntö / Miksi / Testit.
- Only state what the code or a run shows. Domain facts that the repo does not contain (who does
  what at OPH, what an abbreviation stands for) are either confirmed with the user or left out;
  list any you included unconfirmed when reporting back.
- Don't present something as live that the user has said is going away.
- The opening agenda table mirrors the nine section headings plus "Kysymykset" before section 9.
- Reuse the data-flow diagram from `docs/technical/kaaviot/` for section 1. A new diagram
  (state machine, process) is generated with the `integraatiokaaviot` skill, never hand-drawn.

## Demo patterns that work offline

- **Inbound push API** (YKI): token from `POST /dev/oauth/token`, body from
  `/schema-examples/*.json`, `curl` to the real route.
- **Outbound fetch from an external system** (Koealusta): serve a static JSON file with
  `python3 -m http.server` and point the client's base URL at it with an environment variable
  (`KITU_…_BASEURL=http://localhost:9999`). The file is committed under
  `docs/perehdytys/esimerkit/`. Give the eräajo the rare cron `0 0 0 29 2 ?` the same way so it
  can be started with "Run now" in `/db-scheduler` (a `-` schedule hides the task).
- `MockOppijanumeroService` resolves only two name + hetu pairs (see its `oppijaToOid`), plus the
  special hetus `INVALID_HETU` and `WRONG_HETU`; demo data has to use those.
- Random list data: `/dev/mockdata/<alue>/suoritus/{count}`.
