# YKI historia -migraatio — runbook

Analyse a historical YKI suoritus CSV in S3 and migrate it into the register.
Two scripts cover the flow:

| Phase       | Script                            | Purpose                                              |
| ----------- | --------------------------------- | ---------------------------------------------------- |
| 1. Analysis | `scripts/duckdb_session.sh`       | Query the CSV in place with DuckDB (read-only).      |
| 2. Migrate  | `scripts/migrate_yki_historia.py` | Map each row → JSON and POST to `/yki/api/suoritus`. |

The migrate script's pure helpers have unit tests (stdlib only, not in CI):

```bash
python3 -m unittest discover -s scripts -p 'test_*.py'
```

## Safety boundary (read first)

The CSV is **sensitive personal data**. Keep it inside AWS end to end:

- Run **both** scripts from **AWS CloudShell in the account that owns the bucket**
  (prod = `515966535475`, profile `oph-ktr-prod`, `eu-west-1`). The data never
  touches a laptop or any external service.
- Analysis is read-only. Migration writes to prod — **take an Aurora snapshot first**,
  and go dry-run → smoke test → full.
- Keep the report / payload files in CloudShell; don't `aws s3 cp` them out. The
  `--unresolved-out` CSV carries hetus, names and addresses — same boundary.
  The `--oid-map` JSONL deliberately carries **no** personal data (solki_id, oid,
  reason only), so it is the safe one to grep and share.
- Migration goes through the validated API (`POST /yki/api/suoritus`), never a raw DB
  write — so validation, dedup, KOSKI forwarding and ilmoittautumisjärjestelmä
  notification all run.

## The data

The 30-column headerless layout (`YkiSuoritusCsv` order, comma-separated, `"`-quoted,
MySQL's `\N` as the NULL sentinel) is stable across exports, and both scripts assume it.
The migrate script auto-detects the delimiter — tab/comma/`;`/`|` — by which one yields
30 fields; override with `--delimiter`.

**Everything else is per-export and must be re-verified** with the Phase 1 queries. For
the record, the **2011–2020 export** had: 76,848 rows; valid enum encodings throughout
(`M/N/E`, `fin/swe/eng/…`, `PT/KT/YT`) with no legacy codes; all dates well-formed once
`\N`→NULL; 204 rows with a real tarkistusarviointi; no duplicate Solki ids and no
natural-key collisions. Do **not** inherit those facts — a re-export that drops a filter
contains rows the old one never showed, so the "no legacy codes" claim in particular has
to be re-established.

## Source export completeness (rows the export SQL silently drops)

The 2011–2020 export was produced with
`FROM suoritus s JOIN osallistuja o ON s.osallistuja = o.nro JOIN jarjestaja j ON s.jarjestaja = j.oid WHERE o.oid IS NOT NULL AND YEAR(s.pvm) BETWEEN 2011 AND 2020`.
Four of its filters drop rows **without any error**:

| Clause                      | Silently drops                                                                                                          |
| --------------------------- | ----------------------------------------------------------------------------------------------------------------------- |
| `WHERE o.oid IS NOT NULL`   | all suoritukset of participants with no oppijanumero — **dropped by the 2011–2020 export, included in the current one** |
| inner join to `jarjestaja`  | suoritukset whose `jarjestaja` is NULL or unmatched                                                                     |
| inner join to `osallistuja` | orphaned suoritukset with no matching participant row                                                                   |
| `YEAR(s.pvm)`               | rows with NULL `pvm` (`YEAR(NULL)` is NULL, never in the range)                                                         |

Quantify against the source MySQL before accepting an export as complete:

```sql
-- baseline, ignoring every join/filter
SELECT COUNT(*) FROM suoritus s WHERE YEAR(s.pvm) BETWEEN 2011 AND 2020;

-- dropped by the oid filter; the has_hetu split shows how many are recoverable via ONR
SELECT COUNT(*)                                                    AS after_osallistuja_join,
       SUM(o.oid IS NULL)                                          AS dropped_oid_null,
       SUM(o.oid IS NULL AND o.hetu IS NOT NULL AND o.hetu <> '')  AS dropped_oid_null_but_has_hetu
FROM suoritus s JOIN osallistuja o ON s.osallistuja = o.nro
WHERE YEAR(s.pvm) BETWEEN 2011 AND 2020;

-- dropped by the jarjestaja inner join
SELECT COUNT(*) FROM suoritus s
JOIN osallistuja o ON s.osallistuja = o.nro
LEFT JOIN jarjestaja j ON s.jarjestaja = j.oid
WHERE YEAR(s.pvm) BETWEEN 2011 AND 2020 AND o.oid IS NOT NULL AND j.oid IS NULL;

-- orphaned suoritukset + NULL exam dates
SELECT SUM(o.nro IS NULL) FROM suoritus s
LEFT JOIN osallistuja o ON s.osallistuja = o.nro
WHERE YEAR(s.pvm) BETWEEN 2011 AND 2020;
SELECT COUNT(*) FROM suoritus WHERE pvm IS NULL;
```

Rows without an oppijanumero cannot be POSTed as-is — `henkilo.oid` is a non-nullable
`Oid` in `POST /yki/api/suoritus`, so a null one is a JSON parse error, and the suoritus
API only _verifies_ a given OID against ONR, never resolves one from the hetu.
Recovering those rows is what the OID backfill is for (see Phase 2).

## Phase 1 — analysis

```bash
# fresh CloudShell: the launcher installs DuckDB, wires up S3, and builds the views.
./scripts/duckdb_session.sh s3://kitu-yki-historia-upload-prod/<key>.csv
```

Two views are created over the 30 named columns:

- **`raw`** — trimmed, with `\N` mapped to NULL, **byte-for-byte the same normalization
  the migrate script's `to_null()` does**. Use this for everything.
- **`raw_verbatim`** — the fields exactly as read, for whitespace forensics.

The distinction matters: `nullstr := '\N'` alone (what `raw` used to do) only matches a
field that is _exactly_ `\N`, and this export carries stray trailing whitespace in
`sukunimi`, `etunimet`, `katuosoite` and `postitoimipaikka`. So `'\N '` used to read as a
value and `'M '` used to split a `GROUP BY` — which is precisely what the OID-less
sizing below depends on.

```sql
-- shape
SELECT count(*) FROM raw;
DESCRIBE raw;

-- the backfill funnel: how many rows need an OID, and how many ONR calls that costs
SELECT count(*)                                                              AS rivit,
       count(*) FILTER (WHERE suorittajan_oid IS NULL)                       AS ilman_oidia,
       count(*) FILTER (WHERE suorittajan_oid IS NULL AND hetu IS NOT NULL
                          AND etunimet IS NOT NULL AND sukunimi IS NOT NULL) AS haettavissa,
       count(DISTINCT (lower(hetu),
                       lower(regexp_replace(etunimet, '\s+', ' ', 'g')),
                       lower(regexp_replace(sukunimi, '\s+', ' ', 'g'))))
         FILTER (WHERE suorittajan_oid IS NULL AND hetu IS NOT NULL)         AS eri_henkiloa
FROM raw
WHERE TRY_CAST(last_modified AS TIMESTAMP) < '2017-01-01';
```

`eri_henkiloa` **is the ONR call budget** — the migrate script resolves one OID per
person, not per suoritus row, so `haettavissa - eri_henkiloa` is what that cache saves.
The `lower(regexp_replace(...))` is not decoration: it mirrors the script's `person_key`,
which casefolds and collapses **inner** whitespace. `raw` only trims the ends, so a plain
`count(DISTINCT (hetu, etunimet, sukunimi))` counts `Anna Maria` and `Anna  Maria` as two
people and over-estimates the budget (verified: 4 vs the script's 3 on a test file).
Cross-check the number against `--backfill-oids --dry-run`, which prints it directly.

```sql
-- Name variants. eri_henkiloa counts (hetu, etunimet, sukunimi) triples, so a person
-- written two ways is looked up twice. If eri_hetua is meaningfully below eri_henkiloa,
-- the gap is wasted ONR calls -- and a hint that the same human sits behind several
-- osallistuja rows.
SELECT count(DISTINCT lower(hetu))
         FILTER (WHERE suorittajan_oid IS NULL AND hetu IS NOT NULL) AS eri_hetua
FROM raw
WHERE TRY_CAST(last_modified AS TIMESTAMP) < '2017-01-01';
```

Hetu _validity_ is not checked here on purpose (one checksum implementation, in Python);
`--backfill-oids --dry-run` classifies those, see Phase 2.

```sql
-- enum domains (expect only M/N/E ; PT/KT/YT ; fin/swe/eng/deu/fra/ita/rus/sme/spa)
SELECT sukupuoli, count(*) FROM raw GROUP BY 1 ORDER BY 2 DESC;
SELECT tutkintotaso, count(*) FROM raw GROUP BY 1 ORDER BY 2 DESC;
SELECT tutkintokieli, count(*) FROM raw GROUP BY 1 ORDER BY 2 DESC;

-- date parseability (non-zero = would be rejected on import)
SELECT
  count(*) FILTER (WHERE last_modified IS NOT NULL AND TRY_CAST(last_modified AS TIMESTAMPTZ) IS NULL) AS bad_last_modified,
  count(*) FILTER (WHERE tutkintopaiva IS NOT NULL AND TRY_CAST(tutkintopaiva AS DATE) IS NULL)        AS bad_tutkintopaiva
FROM raw;

-- duplicate Solki ids: these DO interact, via YkiSuoritusRepository.save's
-- findLatestBySolkiIds guard, so the second row is compared against the first
SELECT count(*) FROM (SELECT suoritus_id FROM raw GROUP BY 1 HAVING count(*) > 1);

-- natural-key duplicates. NOTE: nothing in the app collapses these. The
-- UNIQUE (suorittajan_oppijanumero, tutkintopaiva, tutkintokieli, tutkintotaso)
-- constraint from V6 was DROPPED in V12 in favour of UNIQUE (suoritus_id,
-- last_modified), and save() guards on solkiId alone -- so each row loads as its
-- own suoritus and gets its own KOSKI opiskeluoikeus. A hit here is a data-quality
-- question for OPH (two identical records in Oma Opintopolku), not an import blocker.
-- `suorittajan_oid IS NOT NULL` is essential: GROUP BY treats NULLs as equal, so
-- without it every OID-less row collapses with unrelated people who happen to share
-- the same date/language/level.
SELECT count(*) AS ryhmat, coalesce(sum(n), 0) AS rivit
FROM (
  SELECT count(*) AS n FROM raw
  WHERE suorittajan_oid IS NOT NULL
  GROUP BY suorittajan_oid, tutkintopaiva, tutkintokieli, tutkintotaso
  HAVING count(*) > 1
);

-- The oid-based check above is BLIND to the collisions the backfill itself creates: it
-- excludes precisely the rows about to receive an oppijanumero. Group by hetu instead to
-- see the whole file. `oidittomia > 0` marks a group the backfill would bring into being,
-- either among newly-resolved rows or between one of them and a row that already had
-- that oid. Note this only covers collisions WITHIN the file -- a collision against rows
-- already in the register can only be checked afterwards, by joining the resolved oids
-- from --oid-map (which holds no personal data) against yki_suoritus.
SELECT count(*) AS ryhmat, coalesce(sum(n), 0) AS rivit,
       count(*) FILTER (WHERE oidittomia > 0) AS taydennyksen_synnyttamat
FROM (
  SELECT count(*) AS n, count(*) FILTER (WHERE suorittajan_oid IS NULL) AS oidittomia
  FROM raw WHERE hetu IS NOT NULL
  GROUP BY lower(hetu), tutkintopaiva, tutkintokieli, tutkintotaso
  HAVING count(*) > 1
);

SELECT lower(hetu) AS hetu, tutkintopaiva, tutkintokieli, tutkintotaso,
       count(*) AS rivit, list(suoritus_id) AS solki_idt,
       count(*) FILTER (WHERE suorittajan_oid IS NULL) AS oidittomia,
       count(DISTINCT (as_ty, as_ki, as_rs, as_py, as_pu, as_yl)) AS eri_arvosanayhdistelmia
FROM raw WHERE hetu IS NOT NULL
GROUP BY 1, 2, 3, 4 HAVING count(*) > 1
ORDER BY rivit DESC LIMIT 20;

-- A hetu-level group whose rows ALL carry an oid, yet which the oid-based check did not
-- report, means one hetu with two different oppijanumerot. That is the nastiest case:
-- HenkilosuoritusValidation.enrichHenkilo replaces henkilo.oid with
-- mapHenkiloOidToMasterOid(...), so if the two oids are LINKED in ONR both rows collapse
-- onto the same master oppijanumero and become a real duplicate in the register -- while
-- in the file they look like two different people. Take the oids to ONR and ask.
SELECT count(*) AS hetuja_monella_oidilla
FROM (SELECT 1 FROM raw
      WHERE hetu IS NOT NULL AND suorittajan_oid IS NOT NULL
      GROUP BY lower(hetu) HAVING count(DISTINCT suorittajan_oid) > 1);

SELECT lower(hetu) AS hetu, tutkintopaiva, tutkintokieli, tutkintotaso,
       count(*) AS rivit,
       count(DISTINCT suorittajan_oid)                            AS eri_oideja,
       list(DISTINCT suorittajan_oid)                             AS oidit,
       list(suoritus_id)                                          AS solki_idt,
       count(DISTINCT (as_ty, as_ki, as_rs, as_py, as_pu, as_yl)) AS eri_arvosanayhdistelmia
FROM raw WHERE hetu IS NOT NULL
GROUP BY 1, 2, 3, 4
HAVING count(*) > 1 AND count(*) FILTER (WHERE suorittajan_oid IS NULL) = 0
ORDER BY rivit DESC;

-- eri_arvosanayhdistelmia is the discriminator on every duplicate group above:
--   = 1  identical records, i.e. a Solki-side duplicate
--   > 1  the grades differ, so it is a correction recorded as a second row, and the
--        question becomes whether history should show both
--
-- NOTE: none of the duplicate queries in this block carry the scope filter, so they span
-- the whole file. Add `AND TRY_CAST(last_modified AS TIMESTAMP) < '2017-01-01'` inside the
-- subquery for the count that applies to the run you are about to make. Once you have
-- decided which row of each pair to drop, feed those ids to the migrate script's
-- --skip-solki-ids rather than editing the source CSV.

-- henkilö fields YkiSuoritusEntity.from requires (a null here is a guaranteed 400).
-- joista_oidittomia predicts the backfill's skipped_issue count: rows no oppijanumero
-- can rescue, so they come off the ONR budget before a single lookup is spent.
-- Mind the parentheses -- the OR chain must be grouped before the scope filter.
SELECT count(*)                                        AS puutteellisia,
       count(*) FILTER (WHERE suorittajan_oid IS NULL)  AS joista_oidittomia
FROM raw
WHERE (sukupuoli IS NULL OR sukunimi IS NULL OR etunimet IS NULL OR kansalaisuus IS NULL
    OR katuosoite IS NULL OR postinumero IS NULL OR postitoimipaikka IS NULL)
  AND TRY_CAST(last_modified AS TIMESTAMP) < '2017-01-01';

-- whitespace forensics, if a count looks wrong
SELECT count(*) FROM raw_verbatim WHERE sukunimi <> trim(sukunimi);
```

`.quit` to exit. See the script header for options (`AWS_REGION`, header/positional fallback).

## Phase 2 — migration

This migration only loads records last modified before 2017, hence
`--modified-before 2017-01-01` on every command.

> **Use a different `--out` for dry runs than for live runs.** `--out` is append-only and
> doubles as the resume ledger. Only `action: "posted"` records count as done (a dry-run
> record carries `ok: true` too, and _used_ to be accepted — which silently turned the
> live run into a no-op reporting `skipped_done` for the whole file). Separate files keep
> the report readable regardless.

```bash
# 1. Dry run: map + local-check all rows, POST nothing.
./scripts/migrate_yki_historia.py --source s3://kitu-yki-historia-upload-prod/<key>.csv \
    --modified-before 2017-01-01 --dry-run --out dry-report.jsonl --emit-payloads payloads.jsonl
grep '"ok": false' dry-report.jsonl        # rows that would be rejected, with reasons

# 2. Smoke test: 5 real rows against prod → expect HTTP 200.
./scripts/migrate_yki_historia.py --source s3://.../<key>.csv --env prod --confirm-prod \
    --modified-before 2017-01-01 --client-id "$CID" --client-secret "$CSECRET" --limit 5 --out report.jsonl

# 3. Full run: resumable — re-running skips rows already POSTed ok.
./scripts/migrate_yki_historia.py --source s3://.../<key>.csv --env prod --confirm-prod \
    --modified-before 2017-01-01 --client-id "$CID" --client-secret "$CSECRET" \
    --unresolved-out unresolved.csv --out report.jsonl
```

- `--modified-before YYYY-MM-DD` migrates only rows whose `last_modified` is strictly
  before that date (UTC); the rest are counted as `filtered` in the run summary. Omit it
  to migrate every row.
- Credentials: pass `--client-id/--client-secret` or set `KITU_CLIENT_ID` /
  `KITU_CLIENT_SECRET`. This is the palvelukäyttäjä OAuth client allowed to POST YKI
  suoritukset (`YKI_TALLENNUS`, the same authority the haku endpoint needs).
  **The OAuth token is refreshed automatically on a 401** — a full pass runs for hours
  and will outlive its token. Persistent 401 or any 403 aborts the run: those are
  credential problems, not row problems.
- The report (`report.jsonl`) has one line per row: `{solki_id, ok, action, http,
response|issues|reason}`. `action` is one of `posted`, `dry-run`, `skipped`
  (local validation), `unresolved` (no oppijanumero) or `transient`.
- Local pre-checks skip rows the server would 400 so they're reported without a wasted
  POST — and the saving is larger than it looks: `HenkilosuoritusValidation.enrich`
  verifies the OID against ONR **before** any YKI-specific validation runs, so every
  local check saves a full ONR round-trip, not just a POST. Covered: no osat, unknown
  tutkintotaso, arvosana invalid for the taso, `arvosanaMuuttui ⊄ tarkistetut`,
  tarkistusarvioinnin `käsittelypäivä < saapumispäivä`, and the seven henkilö fields
  `YkiSuoritusEntity.from` requires (`sukupuoli, sukunimi, etunimet, kansalaisuus,
katuosoite, postinumero, postitoimipaikka`). Still **not** covered: the legacy
  kielikoodi rule (`swe10/eng11/eng12` need `tutkintopaiva < 2017-01-01`) and the
  enum domains of `sukupuoli`/`tutkintokieli`, which fail as JSON parse errors.
- `--skip-solki-ids PATH` leaves listed rows out of the run entirely — one solki_id per
  line, `#` starts a comment. It applies to **both** the backfill pre-pass (so no ONR
  lookup is spent on an excluded row) and the migration run, and excluded rows are
  counted as `excluded` and written to neither the report nor `--unresolved-out`. Intended
  for rows a human has ruled out, e.g. one half of a duplicate pair found by the Phase 1
  hetu-level duplicate check — decided deliberately, without editing a CSV full of hetus.
- `--sleep N` throttles between API calls (recommended: `0.1`); `--limit N` caps the
  input rows read; `--timeout` sets the per-request socket timeout (default 180 s,
  deliberately generous — see the fan-out below); `--delimiter` overrides the
  auto-detected field delimiter (`tab`, `comma`, `;`, `|`, or a literal char).
- Ten consecutive transient failures abort the run rather than grinding through the
  rest of the file against a service that is down.
- **Exit codes:** `0` clean, `1` aborted (transient run or a credential problem),
  `2` rows were diverted but no `--unresolved-out` was given, so they were not kept.

> Note: `--limit N` caps the number of input rows read, before filtering — so with
> `--modified-before`, a small `--limit` may migrate fewer than N rows if early rows are
> filtered out. For the backfill pass use `--max-lookups` instead, which caps actual
> ONR lookups.

### OID backfill (rows without oppijanumero)

For a source CSV that contains rows without `suorittajan_oid`, a pre-pass resolves OIDs
from hetu + names through kitu's `POST /yki/api/oppijanumero-haku`. It is **resolve-only**:
people ONR has never seen stay unresolved, since kitu has no ONR-create capability.

**Cost model — read this before choosing `--sleep`.** The endpoint takes **one person per
request** and returns one `oid`. On a miss the server fans out through
`OppijanumeroTroubleshootingService`: each etunimi as kutsumanimi, then the same list
again with etunimet/sukunimi swapped, then a re-query of the winner — **1 + 2N + 1** ONR
calls for N etunimet, each wrapped in `@RetryOutboundIntegration` (3 attempts, 1s/2s/5s
backoff). So **the people who cannot be resolved are the most expensive ones.** Two
things keep this affordable: the script resolves **one OID per person**, not per suoritus
row (an in-memory cache keyed on hetu + names; a person's rows all come from one
`osallistuja` row), and it rejects hopeless hetus locally before spending a lookup.

```bash
# 0. Sizing pass: classify rows locally. No API calls, no credentials, writes nothing
#    to the map. Prints the funnel and the distinct-person count = the ONR call budget.
./scripts/migrate_yki_historia.py --source s3://.../<key>.csv --dry-run \
    --modified-before 2017-01-01 \
    --backfill-oids --oid-map oid_map.jsonl --unresolved-out unresolved.csv

# 1. Pre-pass: resolve OIDs for OID-less rows into a resumable map. POSTs no suoritukset.
./scripts/migrate_yki_historia.py --source s3://.../<key>.csv --env prod --confirm-prod \
    --modified-before 2017-01-01 --client-id "$CID" --client-secret "$CSECRET" \
    --backfill-oids --oid-map oid_map.jsonl --unresolved-out unresolved.csv --sleep 0.1

# 2. Migration run: --oid-map injects the resolved OIDs; rows still without an OID are
#    diverted to --unresolved-out and reported as action=unresolved, never POSTed.
./scripts/migrate_yki_historia.py --source s3://.../<key>.csv --env prod --confirm-prod \
    --modified-before 2017-01-01 --client-id "$CID" --client-secret "$CSECRET" \
    --oid-map oid_map.jsonl --unresolved-out unresolved.csv --out report.jsonl
```

- `--backfill-oids` **requires** `--oid-map` and `--unresolved-out` — without the latter
  the rows it cannot resolve would not be kept for further processing.
- `--max-lookups N` caps real ONR lookups (a backfill smoke test); `--no-hetu-check`
  skips the local hetu validation and lets ONR judge every hetu.
- The pre-pass respects `--modified-before` and `--limit`, so it only resolves rows the
  migration would actually load.

#### What each outcome means

The map (`{solki_id, oid, reason}`, one line per **attempted** row) records only
**permanent** outcomes, so a re-run skips them:

| `reason`                                | Meaning                                             | What to do                                                          |
| --------------------------------------- | --------------------------------------------------- | ------------------------------------------------------------------- |
| _(null, with an `oid`)_                 | resolved                                            | nothing                                                             |
| `hetu puuttuu`                          | no hetu in the source row                           | unresolvable by this route; needs a source fix or a domain decision |
| `etunimet puuttuu` / `sukunimi puuttuu` | ONR needs hetu **and** both names                   | source fix                                                          |
| `virheellinen hetun muoto`              | not `DDMMYY` + separator + `NNN` + check char       | source fix                                                          |
| `virheellinen hetun päivämäärä`         | shape ok, day/month impossible (e.g. `000000-0000`) | source fix                                                          |
| `virheellinen hetun tarkiste`           | checksum mismatch — almost always a typo            | source fix; `--no-hetu-check` to try it against ONR anyway          |
| `ei löytynyt oppijanumerorekisteristä`  | ONR 404: unknown hetu, or found but not yksilöity   | retry in a later round once ONR knows the person                    |
| `haku hylkäsi pyynnön: …`               | endpoint 400 — a blank field slipped through        | read the body; should not happen after the local checks             |
| _not in the map at all_                 | **transient** (502/5xx/network/timeout)             | just re-run the pass; nothing was recorded, so it is retried        |

Rows that fail _local validation_ (a missing `kansalaisuus`, say) are **not** written to
the map at all — that file records OID outcomes only, so a later round still resolves
them if the source data gets fixed. They are counted as `skipped_issue` and captured to
`--unresolved-out`, and the migration run reports them as `action: "skipped"` with the
`issues` list plus `oid_missing: true`.

**Why a bad hetu and an ONR outage look the same over HTTP.** The endpoint answers 404
only for not-found/not-identified; **every** other `OppijanumeroException` becomes a
**502**, because `OppijanumerorekisteriClient` maps any non-404 4xx from ONR — including
the 4xx it returns for a malformed hetu — to `BadRequest`. That was left alone on
purpose: ONR answering 401/403 _to kitu_ also lands in `BadRequest`, so reporting it as a
client 400 would tell the operator "your data is bad" during a kitu-side credential
failure, and a permanent classification would then blacklist thousands of good rows. The
local hetu check is what removes the ambiguity in practice — with it, a 502 really does
mean "ONR is unhappy", not "row 900 has a typo". The class name is in the 502 body and in
the script's log line.

#### Later rounds

```bash
# unresolved.csv is verbatim 30-column, deduplicated on solki_id → reusable as --source.
./scripts/migrate_yki_historia.py --source unresolved.csv --env prod --confirm-prod \
    --client-id "$CID" --client-secret "$CSECRET" \
    --backfill-oids --oid-map oid_map_round2.jsonl --unresolved-out unresolved2.csv
./scripts/migrate_yki_historia.py --source unresolved.csv --env prod --confirm-prod \
    --client-id "$CID" --client-secret "$CSECRET" \
    --oid-map oid_map_round2.jsonl --unresolved-out unresolved2.csv --out report2.jsonl
```

- A later retry round needs a **fresh** map, or the recorded `ei löytynyt
oppijanumerorekisteristä` rows are skipped instead of retried. Carry the successes
  over so you don't re-spend those lookups:
  `jq -c 'select(.oid != null)' oid_map.jsonl > oid_map_round2.jsonl`.
- **Rotate the map and the unresolved CSV together.** On a resumed pre-pass, rows already
  in the map are skipped _before_ the capture step, so keeping one and rotating the other
  loses rows. The migration run re-derives the full OID-less set on every pass, so treat
  it as the authoritative producer of `unresolved.csv`.
- Plan the session: thousands of people at a second or more each means hours. CloudShell
  caps sessions at 12 h and drops on inactivity, and `$HOME` is 1 GB — run under `tmux`,
  or chunk with `--max-lookups` and resume.

### Column → JSON mapping (reference)

CSV row → `Henkilosuoritus<YkiSuoritus>`. Every field is trimmed of surrounding
whitespace/tabs and `\N`/empty is mapped to null.

- `henkilo`: suorittajan_oid→oid, plus hetu, sukupuoli, sukunimi, etunimet, kansalaisuus,
  katuosoite, postinumero, postitoimipaikka, email. (`maa` omitted — not in the export.)
- `suoritus`: tyyppi=`yleinenkielitutkinto`, tutkintotaso, kieli, todistuskieli=null,
  jarjestaja{oid,nimi}, tutkintopaiva, arviointipaiva, lahdejarjestelmanId={id: suoritus_id,
  lahde: `Solki`}.
- `osat`: one `{tyyppi, arvosana}` per **non-null** grade column
  (as_ty→TY, as_ki→KI, as_rs→RS, as_py→PY, as_pu→PU, as_yl→YL).
- `arviointitila`: `ARVIOITU` when arviointipaiva present else `ARVIOITAVA`. Prod runs
  `kitu.yki.convertLegacyArviointitila.enabled=true`, so enrichment re-derives the real
  state (`laskeArviointitila`): tarkistusarviointi with käsittelypäivä →
  `TARKISTUSARVIOITU`, without → `TARKISTUSARVIOITAVA`; otherwise all arvosanat ≥ 9 →
  `EI_SUORITUSTA`, else `ARVIOITU`. The derived state always satisfies the
  arviointitila↔arvosanat and tarkistusarviointi-sallittu validations.
- `tarkistusarviointi` (only when tark_saapumis_pvm present): saapumispaiva, kasittelypaiva,
  asiatunnus, perustelu, and the **bitmask** ints tark_osakokeet / arvosana_muuttui decoded
  to osa-code lists (`PU=1, KI=2, TY=4, PY=8`; RS/YL cannot appear).

## Open items / decisions

- **Prod host + OAuth token URL are a best guess** in the script's `ENV_PRESETS`,
  extrapolated from the dev/test values in `scripts/upload_yki_suoritus.sh`. Verify them,
  or override with `--host` / `--token-url`. The script prints the resolved prod URLs and
  refuses to POST to prod without `--confirm-prod`.
- **Tarkistus rows land as `TARKISTUSARVIOITU`** (those with a käsittelypäivä; any
  without one land as `TARKISTUSARVIOITAVA`), **not `TARKISTUSARVIOINTI_HYVAKSYTTY`.**
  The JSON import path cannot set the "approved" state. If these historical tarkistukset
  should be `HYVAKSYTTY`, do a follow-up DB update after import or adjust the import — a
  domain decision.
- **People ONR has never seen cannot be backfilled** — the haku endpoint is
  resolve-only. Whether the final unresolved remainder should be created in ONR (a
  capability kitu doesn't have) or documented as a permanent gap is a domain decision.
  The same question applies to participants who have no Finnish hetu at all.
- **Rows whose `suorittajan_oid` is present but unknown to ONR** fail in
  `mapHenkiloOidToMasterOid` with HTTP 400 and are reported as `action: "posted"`,
  `http: 400` — they are **not** captured to `unresolved.csv`, because the row does carry
  an OID. In principle they could be re-resolved from the hetu like the OID-less ones;
  worth a follow-up round if `grep 'ei löydy Oppijanumerorekisteristä' report.jsonl`
  returns a material count.
- **Structurally invalid hetus need a source-system fix.** The backfill reports them
  by class (muoto / päivämäärä / tarkiste) so the counts can go back to Solki.
- **Duplicate suoritukset the backfill and OID mastering create.** Nothing in the app
  collapses natural-key duplicates (see Phase 1), so each loads as its own suoritus with
  its own KOSKI opiskeluoikeus. Two mechanisms produce them, and the oid-based duplicate
  check sees neither: an OID-less row resolving onto the same person as an existing row,
  and one hetu carrying two oppijanumerot that ONR masters to the same oppijanumero.
  Review the Phase 1 hetu-level groups, decide per pair, and exclude the losing side with
  `--skip-solki-ids`. Collisions against suoritukset **already in the register** are not
  visible in the file at all — check those after the pre-pass by joining the resolved oids
  from `--oid-map` (no personal data) against `yki_suoritus`.
- **The haku endpoint (`POST /yki/api/oppijanumero-haku`) is a single-person hetu→OID
  resolve**, kept at the same trust level as suoritus creation (`YKI_TALLENNUS`).
  Consider removing it once the migration is complete.

## Troubleshooting

- `glob('s3://…')` errors / boto3 `AccessDenied` → wrong account or expired session; check
  you're in CloudShell in the bucket's account, or `aws sso login`.
- The live run reports `skipped_done` for everything → you reused a dry run's `--out`.
  Only `action: "posted"` records resume now, but an old report file may predate that;
  use a fresh `--out`.
- Migration rows returning HTTP 400 → read `response` in the report; the field path in the
  `TiedonsiirtoFailure` points at the offending value. HTTP 401 is retried once after a
  token refresh; a second 401, or any 403, aborts — wrong client credentials or the
  palvelukäyttäjä lacks YKI rights.
- Backfill rows returning 502 → ONR is unhappy. With the local hetu check on, suspect the
  service rather than the data; the exception class name is in the 502 body. Transient
  failures are **not** written to the map, so re-running the pass retries exactly those
  rows. Ten in a row aborts the pass.
- Run aborted part-way → just re-run the same command. The map, the report and the
  unresolved CSV are all append-and-flush, and all three deduplicate on re-read.
- `unresolved.csv: erotinta ei tunnistettu` → the leftover file isn't the 30-column
  layout, so the script cannot tell which rows it already wrote and refuses to append
  rather than silently duplicating them. Move it aside or fix its shape.
- Migrate script errors `could not auto-detect a delimiter giving 30 columns` → it prints
  the field counts per candidate delimiter; pass `--delimiter` explicitly, or the file
  isn't the expected layout at all.
- DuckDB reads garbage / wrong column count → the file isn't the 30-column Solki layout;
  re-read positionally (`header := false`, no `names`) and re-derive the mapping.
