---
name: perehdytys
description: Write or update an onboarding tutorial / knowledge-sharing session page in docs/perehdytys/ (the series for the team taking over Kielitutkintorekisteri maintenance — YKI-suoritukset, kotoutumiskoulutuksen päättötesti, and the ones still marked "tulossa" in its README). Use when asked to prepare a tiedonjakosessio, knowledge sharing, lesson, tutorial or perehdytys on some part of the system.
---

# Perehdytys: write the next tutorial

Each page in `docs/perehdytys/` is both a self-study tutorial and the agenda of a **live,
time-boxed knowledge-sharing session**: the headings are the agenda. `docs/perehdytys/README.md`
lists the planned topics. Read the README and the most recent tutorial
(`koto-paattotesti.md`) in full before writing anything — it is the current model, more than
the older `yki-suoritukset.md`.

## Sections

The README still describes a nine-section skeleton. In practice the session has no time for
code-level detail, so the koto tutorial dropped two parts (Ilkka, 24.11.2026). The default is:

1. **Mikä tämä on ja kuka tekee mitä** — domain, parties table, the data-flow diagram, and the
   two or three things that set this area apart
2. **Sanasto** — term / meaning / name in code
3. **Elinkaari** — what happens to the data, as tables of "tilanne → mitä tapahtuu"
4. **Tekninen yleiskuva** — packages, tables, external interfaces, views, eräajot, rights and secrets
5. **Ansat ja päätökset** — each item **Sääntö / Miksi only**
6. **Ylläpito** — where to look, typical support requests, manual operations
7. **Kokeile itse** — the demo
8. **Lisälukemista** — docs and a table of tests to read; not covered in the session

The agenda table at the top lists these headings, with "Kysymykset" before the last one and the
last marked "(ei käydä läpi)".

**Left out by default:** a "Koodipolut" section (step-by-step class tables) and "Testit" lines
under the traps, plus traps that only concern how the test suite is set up. Ask the user before
adding any of them back, and if the series skeleton in the README should follow, offer to update it.

## Steps

1. **Branch from `main`**, not from whatever feature branch is checked out — a tutorial describes
   production code. Run `./mvnw clean` in `server/` after switching. Tell the user the branch name.
2. **Read the code, not just `CLAUDE.md`.** `CLAUDE.md` points at the traps; the page needs the
   real class names and behaviour. Read the feature package, its tests, the migrations for its
   tables, the `application*.properties` keys and the matching part of
   `docs/technical/integraatiot.md`. Use `git log -S` on key functions to understand why
   something is the way it is.
3. **Build the "Kokeile itse" demo first and run it.** It decides what the page can show (see
   "Demo" below). Verify every command and every stated outcome by running it.
4. **Bugs found while writing are a separate PR.** If the code doesn't do what the feature is for
   (the koto keskeneräiset never called ONR), report it with its git history, fix it on its own
   branch from `main` with tests, and open that PR. If the tutorial should describe the fixed
   behaviour, move the tutorial branch on top of the fix branch, re-run the demo there, and open
   the tutorial PR **against the fix branch**, saying so in the description.
5. **Write the page**, link it from the README list (replace the "(tulossa)" entry) and add the
   file to the page check in `.github/workflows/build.yml` ("Verify technical documentation
   pages exist").
6. **Run `./scripts/format.sh`** (prettier re-pads the tables), `./scripts/check-formatting.sh`
   and, since a workflow changed, `./scripts/check-workflows.sh`.
7. **Don't commit or open the PR until asked.** The user reviews and edits the page in between —
   treat their edits in the file as the current state. When committing: files under `.claude/`
   need `git add -f` (the directory is in `.git/info/exclude`), and give them a **separate**
   `git add` call — an ignored path makes `git add` exit non-zero and stops an `&&` chain before
   the commit. Open the PR as a draft.

## Content rules

- Finnish. Write "Kielitutkintorekisteri", never "kitu". "KOSKI" is not inflected: "KOSKI-järjestelmään".
- Name classes and methods without line numbers; packages relative to
  `server/src/main/kotlin/fi/oph/kitu/`.
- **Describe the normal case.** Leave out edge cases that practically never happen, historical
  incidents and how long things took. A trap is a rule a maintainer could plausibly break.
- Only state what the code or a run shows. Domain facts the repo doesn't contain (who does what
  at OPH, what an abbreviation means, which company runs an external system) are confirmed with
  the user or left out — vendor names were removed from the koto page and its diagrams on request.
  List anything you included unconfirmed when reporting back.
- Don't present something as live that the user has said is going away.
- Reuse the data-flow diagram from `docs/technical/kaaviot/` in section 1. A new diagram (state
  machine, process) or a label change is made with the `integraatiokaaviot` skill, never by
  hand-editing the SVG.

## Demo

It must run on the offline profile with no AWS credentials and no external systems:

```shell
docker compose up -d db
./scripts/start_offline_dev.sh ./scripts/start_local_server.sh
```

When verifying it yourself, point the server at a throwaway database instead of writing demo
rows into `kitu-dev` (which may also be ahead of your branch's migrations), and launch through
`mise exec -- env …` so mise doesn't reset the profile:

```shell
docker compose exec -T db psql -U kitu -d postgres -c 'CREATE DATABASE "kitu-perehdytys-demo"'
cd server && mise exec -- env SPRING_PROFILES_ACTIVE=local,local-opintopolku \
  SPRING_DATASOURCE_URL='jdbc:postgresql://localhost:5432/kitu-perehdytys-demo?stringtype=unspecified' \
  … ./mvnw spring-boot:run
```

Log in with `curl -c cookies $B/dev/mocklogin`. Stop the server and drop the database afterwards.

Patterns that work:

- **Inbound push API** (YKI): token from `POST /dev/oauth/token`, body from
  `/schema-examples/*.json`, `curl` to the real route.
- **Outbound fetch from an external system** (Koealusta): serve a static JSON file with
  `python3 -m http.server` and point the client's base URL at it with an environment variable
  (`KITU_…_BASEURL=http://localhost:9999`). Commit the file under `docs/perehdytys/esimerkit/`,
  and say in the page that the fake server ignores query parameters.
- **Eräajot:** give the task the rare cron `0 0 0 29 2 ?` through its schedule property, so it can
  be started with "Run now" in `/db-scheduler`; a `-` schedule hides the task. To trigger it from
  a script: `POST /db-scheduler-api/tasks/rerun?id=recurring&name=<url-encoded task name>&scheduleTime=<ISO instant>`
  with the session cookie (without `scheduleTime` the call returns 400).
- `MockOppijanumeroService` resolves only two name + hetu pairs (see its `oppijaToOid`), plus the
  special hetus `INVALID_HETU` (400) and `WRONG_HETU` (not identified). Any other hetu is 404,
  which is useful for showing an error.
- Random list data: `/dev/mockdata/<alue>/suoritus/{count}`.
