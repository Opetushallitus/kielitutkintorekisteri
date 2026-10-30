---
name: integraatiokaaviot
description: Update the six integration data-flow diagrams (docs/technical/kaaviot/*.svg — YKI suoritukset, YKI arvioijarekisteri, VKT erinomainen, VKT hyvä & tyydyttävä, kotoutumiskoulutus suoritukset, kotoutumiskoulutus tehtäväpankki) when an integration between kitu, KIOS, Solki, Koealusta/Mediamaisteri, KOSKI (todistukset, Tiedotuspalvelu), ONR etc. changes, or when asked to refresh/verify them.
---

# Integraatiokaaviot: update the data-flow diagrams

Six hand-editable SVGs in `docs/technical/kaaviot/`, embedded in
`docs/technical/integraatiot.md` (section "Kaaviot"):

| File                                   | Scope                                                                       |
| -------------------------------------- | --------------------------------------------------------------------------- |
| `yki-suoritukset.svg`                  | YKI results: KIOS ↔ Solki → kitu → KOSKI (todistus, tiedote, Suomi.fi)      |
| `yki-arvioijat.svg`                    | YKI arvioijarekisteri: Solki ↔ kitu, Ataru, virkailija                      |
| `vkt-erinomainen.svg`                  | VKT ET: KIOS sends ilmoittautuminen without arvosanat, arvosanat in kitu    |
| `vkt-hyva-ja-tyydyttava.svg`           | VKT HTT: tutkinnon vastaanottaja grades in KIOS, KIOS sends finished result |
| `kotoutumiskoulutus-suoritukset.svg`   | Koealusta (Moodle, Mediamaisteri) → kitu: valmiit + keskeneräiset           |
| `kotoutumiskoulutus-tehtavapankki.svg` | Koealusta → kitu → S3; virkailija downloads files via presigned S3 URL      |

They are **generated** by `kaaviot.py` in this skill directory. The script is the source of
truth for layout; every coordinate is set by hand in it (no auto-layout).

## Workflow

1. **Find out what changed.** Re-verify facts from code, not from the current diagram:
   - kitu (this repo): `ilmoittautumisjarjestelma/`, `vkt/VktApiController.kt`, `yki/YkiApiController.kt`,
     `yki/arvioijat/solki/`, `koski/`, `yhteystiedot/`, `ataru/`, `kotoutumiskoulutus/koealusta/`,
     `oppijanumero/`, `organisaatiot/`, `koodisto/`, `security/WebSecurityConfig.kt`, schedules in
     `server/src/main/resources/application-prod.properties`. Also `docs/technical/integraatiot.md`.
   - KIOS, repo `Opetushallitus/kieli-ja-kaantajatutkinnot`, branch **`dev`** (no `main`). Read it
     with `gh api repos/Opetushallitus/kieli-ja-kaantajatutkinnot/contents/<path>?ref=dev` or
     `gh search code --repo ...`; don't clone. VKT: `backend/vkt/src/main/java/fi/oph/vkt/`
     (`scheduled/SyncRegisterEnrollments.java`, `service/RegisterEnrollmentService.java`,
     `payment/`, `service/onr/`, `service/koski/`, `service/email/`, `service/aws/`). YKI Java
     backend: `backend/yki/src/main/java/fi/oph/yki/` (`api/oauth2/OAuth2RegistrationController.java`).
   - Old YKI backend `Opetushallitus/yki` (Clojure, branch `dev`): it still does the **Solki sync**
     (`src/yki/boundary/yki_register.clj`, `src/yki/job/scheduled_tasks.clj`), Paytrail and Suomi.fi
     login. Both YKI backends share one database.
   - KOSKI, repo `Opetushallitus/koski` (`master`): `src/main/scala/fi/oph/koski/todistus/`
     (todistuspalvelu: PDF + Swisscom AIS signing) and `todistus/tiedote/` (hourly tiedote job; the
     only caller of kitu's `yhteystiedot` API). Tiedotuspalvelu, repo `Opetushallitus/tiedotuspalvelu`
     (`main`, ONR team): Suomi.fi-viestit, paper-mail fallback via Posti. KOSKI's per-env flags
     (`tiedote.enabled`) live in AWS AppConfig, not in the repo.
   - For broad sweeps, delegate to Explore agents (one per repo/area) and ask for
     source → target, push/pull, endpoint, auth, schedule, data.
2. **Check for hand edits before regenerating.** The user may have edited SVGs in Inkscape/draw.io:
   ```shell
   python3 -I .claude/skills/integraatiokaaviot/kaaviot.py "$SCRATCH" &&
     for f in docs/technical/kaaviot/*.svg; do cmp -s "$f" "$SCRATCH/$(basename "$f")" || echo "differs: $f"; done
   ```
   (`$SCRATCH` = a scratchpad dir.) If a file differs from the current generator output, it was
   hand-edited (check `git log -p -- docs/technical/kaaviot/`). Do **not** overwrite it: either port
   the hand edits into `kaaviot.py` first, or edit the SVG directly for that change — ask the user
   which they prefer if the edits are substantial.
3. **Edit `kaaviot.py`.** One function per diagram (`yki_suoritukset`, `yki_arvioijat`, `vkt_et`, `vkt_htt`, `koto_suoritukset`, `koto_tehtavapankki`). `Svg(..., width=)` defaults to 1600; `yki_suoritukset` uses 1900 for its KOSKI lane. Bump
   `PAIVAYS` to today's date (shown in each subtitle).
4. **Regenerate and look at it:**
   ```shell
   python3 -I .claude/skills/integraatiokaaviot/kaaviot.py docs/technical/kaaviot
   for f in docs/technical/kaaviot/*.svg; do xmllint --noout "$f" && rsvg-convert "$f" -o "$SCRATCH/$(basename "$f" .svg).png"; done
   ```
   Then **Read each PNG** and check: no label overlaps another label or box, arrowheads touch box
   edges, and label backgrounds don't hide arrowheads. Iterate on coordinates until clean.
5. **Keep the prose in sync:** update `docs/technical/integraatiot.md` (and CLAUDE.md if a
   non-obvious fact changed). Run `./scripts/format.sh`, then `./scripts/check-formatting.sh`.

## Conventions in the diagrams

- Finnish labels. **Never inflect "KOSKI"** (no "KOSKEen", "KOSKIin"): write "KOSKI-järjestelmään",
  "KOSKI-järjestelmästä" etc. Arrow label = data + mechanism (`PUT /api/vkt/kios · CAS · tunneittain`).
- Arrow direction = **direction the data moves**. `both=True` for genuinely two-way exchanges
  (payments, S3, signing). Every `s.arrow(...)` takes an explicit `kind=`, and the legend
  (`NUOLITYYPIT`) is shared by all diagrams:
  - `heti` (teal): system push triggered by an event, e.g. kitu → KIOS arviointitila right after
    ingest, or kitu → Solki right after a save. Retries on a timer don't change this.
  - `ajastettu` (amber): system push run as a batch/scheduled job (KIOS → Solki every 59 min,
    KIOS → kitu VKT hourly, kitu → KOSKI YKI every 15 min and VKT nightly, KOSKI tiedotteet hourly, email queues polled
    every 10 s).
  - `push` (dark grey): system push whose timing we **haven't verified in code**, e.g. Solki → kitu
    (Solki decides when), Tiedotuspalvelu → Suomi.fi-viestit, YKI emails from the old Clojure
    backend. Don't guess `heti`/`ajastettu`: confirm it from the sender's code, then reclassify.
  - `pull` (dashed): the receiver fetches, including UIs reading on demand (Oma Opintopolku ←
    KOSKI is a pull, not a "julkaisu" push).
  - `kayttaja` (light grey): a person's action in a UI. It's not a system push, so it never uses
    the three push styles.
- Classes: `kitu`, `kios`, `oph` (other OPH service), `ulkoinen` (Solki, Koealusta, Paytrail,
  Suomi.fi), `infra` (S3), `toimija` (people). `ei-kaytossa` greys out integrations that exist in
  code but are off in every real environment (currently Ataru: `kitu.ataru.service.url` empty).
- Lanes: Käyttäjät | KIOS side (or Kielitestialusta) | kitu + KOSKI + Oma Opintopolku; shared OPH
  services (ONR, Koodisto, Organisaatiopalvelu, Otuva) are text boxes in a bottom strip, **without
  arrows** — say who uses them for what, prefixed `KIOS:` / `kitu:`.
- **No notice/"Huom" boxes** in the diagrams (the user removed them). Surprising facts and open
  questions go in the prose of `integraatiot.md` instead. Canvas height follows the bottom strip
  (`shared` sets `s.h`).
- Keep SVGs editable: plain `<text>`/`<tspan>`, one `<style>` block, every box/arrow a `<g id>`;
  no `<foreignObject>`, no embedded fonts.
- **Embed every diagram in `integraatiot.md` as an image wrapped in a raw link**:
  `[![Alt](./kaaviot/x.svg)](https://raw.githubusercontent.com/Opetushallitus/kielitutkintorekisteri/refs/heads/main/docs/technical/kaaviot/x.svg)`.
  A bare `![Alt](./kaaviot/x.svg)` makes GitHub's renderer wrap the image in a link to the
  `/blob/` file-viewer page. That can't be configured, but GitHub keeps an explicit link. The
  relative `src` keeps the image working on both github.com and Pages. Check with
  `gh api markdown -f mode=gfm -F text=@file`.

## Facts that are easy to get wrong

- KIOS **pushes** VKT to kitu (hourly, CAS service ticket — why `j_spring_cas_security_check`
  exists). Kitu never sends VKT back to KIOS.
- VKT ET: KIOS sends only after the tutkintopäivä, without arvosanat; kitu sends to KOSKI once every
  osakoe is graded. VKT HTT: arvosanat are entered in KIOS by the tutkinnon vastaanottaja.
- YKI arviointitila kitu → KIOS is display-only in KIOS (citizen's own page).
- Open question (raised in PR #3545, not in any diagram): KIOS doesn't bump the appointment's
  `modifiedAt` when an HTT grade is edited, so corrected grades likely never re-sync to kitu.
- Kotoutumiskoulutus data goes nowhere outside kitu (no KOSKI, no KIOS).
- YKI arvioija → Solki automatic sending is off everywhere (`kitu.yki.arvioijarekisteri.integraatio.enabled`
  defaults to false, no env overrides); only the virkailija's manual send works. The
  arrow is still drawn as `heti`, because that is how it behaves once switched on.
