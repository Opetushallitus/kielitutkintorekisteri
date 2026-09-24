# YKI historia -migraatio — runbook

Analyse a historical YKI suoritus CSV in S3 and migrate it into the register.
Two scripts cover the flow:

| Phase       | Script                            | Purpose                                              |
| ----------- | --------------------------------- | ---------------------------------------------------- |
| 1. Analysis | `scripts/duckdb_session.sh`       | Query the CSV in place with DuckDB (read-only).      |
| 2. Migrate  | `scripts/migrate_yki_historia.py` | Map each row → JSON and POST to `/yki/api/suoritus`. |

## Safety boundary (read first)

The CSV is **sensitive personal data**. Keep it inside AWS end to end:

- Run **both** scripts from **AWS CloudShell in the account that owns the bucket**
  (prod = `515966535475`, profile `oph-ktr-prod`, `eu-west-1`). The data never
  touches a laptop or any external service.
- Analysis is read-only. Migration writes to prod — **take an Aurora snapshot first**,
  and go dry-run → smoke test → full.
- Keep the report / payload files in CloudShell; don't `aws s3 cp` them out.
- Migration goes through the validated API (`POST /yki/api/suoritus`), never a raw DB
  write — so validation, dedup, KOSKI forwarding and ilmoittautumisjärjestelmä
  notification all run.

## The data (as verified for the 2011–2020 export)

- 76,848 rows, headerless, comma-separated, `"`-quoted, **30 columns** in
  `YkiSuoritusCsv` order. (The migrate script auto-detects the delimiter —
  tab/comma/`;`/`|` — by which one yields 30 fields; override with `--delimiter`.)
- NULLs are the MySQL sentinel **`\N`** (from `SELECT … INTO OUTFILE`).
- Enum encodings already valid (`M/N/E`, `fin/swe/eng/…`, `PT/KT/YT`); no legacy codes.
- All dates well-formed once `\N`→NULL. **204 rows** carry a real tarkistusarviointi.
- No duplicate Solki ids, no natural-key collisions → straight 1:1 load.

If you point the tools at a **different** export, re-run the checks below before trusting
the layout — the column names and `\N` handling are baked in on the assumption of this
30-column Solki format.

## Source export completeness (rows the export SQL silently drops)

The export was produced with
`FROM suoritus s JOIN osallistuja o ON s.osallistuja = o.nro JOIN jarjestaja j ON s.jarjestaja = j.oid WHERE o.oid IS NOT NULL AND YEAR(s.pvm) BETWEEN 2011 AND 2020`.
Four of its filters drop rows **without any error**:

| Clause                      | Silently drops                                                  |
| --------------------------- | --------------------------------------------------------------- |
| `WHERE o.oid IS NOT NULL`   | all suoritukset of participants with no oppijanumero            |
| inner join to `jarjestaja`  | suoritukset whose `jarjestaja` is NULL or unmatched             |
| inner join to `osallistuja` | orphaned suoritukset with no matching participant row           |
| `YEAR(s.pvm)`               | rows with NULL `pvm` (`YEAR(NULL)` is NULL, never in the range) |

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

Rows without an oppijanumero could not be POSTed as-is anyway — `henkilo.oid` is a
required field of `POST /yki/api/suoritus`, and the suoritus API only verifies a given
OID against ONR, it never resolves one from the hetu. Recovering those rows is what
the OID backfill flow is for (see "OID backfill" under Phase 2).

## Phase 1 — analysis

```bash
# fresh CloudShell: the launcher installs DuckDB, wires up S3, and builds a
# `raw` view with the 30 named columns (\N mapped to NULL).
./scripts/duckdb_session.sh s3://kitu-yki-historia-upload-prod/<key>.csv
```

Then write your own SQL against `raw`. Useful checks:

```sql
-- shape
SELECT count(*) FROM raw;
DESCRIBE raw;

-- enum domains (expect only M/N/E ; PT/KT/YT ; fin/swe/eng/deu/fra/ita/rus/sme/spa)
SELECT sukupuoli, count(*) FROM raw GROUP BY 1 ORDER BY 2 DESC;
SELECT tutkintotaso, count(*) FROM raw GROUP BY 1 ORDER BY 2 DESC;
SELECT tutkintokieli, count(*) FROM raw GROUP BY 1 ORDER BY 2 DESC;

-- date parseability (non-zero = would be rejected on import)
SELECT
  count(*) FILTER (WHERE last_modified IS NOT NULL AND TRY_CAST(last_modified AS TIMESTAMPTZ) IS NULL) AS bad_last_modified,
  count(*) FILTER (WHERE tutkintopaiva IS NOT NULL AND TRY_CAST(tutkintopaiva AS DATE) IS NULL)        AS bad_tutkintopaiva
FROM raw;

-- duplicates the app would collapse on upsert
SELECT count(*) FROM (SELECT suoritus_id FROM raw GROUP BY 1 HAVING count(*) > 1);
SELECT count(*) FROM (SELECT 1 FROM raw GROUP BY suorittajan_oid, tutkintopaiva, tutkintokieli, tutkintotaso HAVING count(*) > 1);

-- tarkistus rows the server would 400 (käsittelypäivä before saapumispäivä);
-- the migrate script does NOT pre-check this locally
SELECT count(*) FROM raw
WHERE TRY_CAST(tark_kasittely_pvm AS DATE) < TRY_CAST(tark_saapumis_pvm AS DATE);
```

`.quit` to exit. See the script header for options (`AWS_REGION`, header/positional fallback).

## Phase 2 — migration

This migration only loads records last modified before 2017, hence
`--modified-before 2017-01-01` on every command.

```bash
# 1. Dry run: map + local-check all rows, POST nothing.
./scripts/migrate_yki_historia.py --source s3://kitu-yki-historia-upload-prod/<key>.csv \
    --modified-before 2017-01-01 --dry-run --out report.jsonl --emit-payloads payloads.jsonl
grep '"ok": false' report.jsonl        # rows that would be rejected, with reasons

# 2. Smoke test: 5 real rows against prod → expect HTTP 200.
./scripts/migrate_yki_historia.py --source s3://.../<key>.csv --env prod --confirm-prod \
    --modified-before 2017-01-01 --client-id "$CID" --client-secret "$CSECRET" --limit 5 --out report.jsonl

# 3. Full run: resumable — re-running skips rows already recorded ok.
./scripts/migrate_yki_historia.py --source s3://.../<key>.csv --env prod --confirm-prod \
    --modified-before 2017-01-01 --client-id "$CID" --client-secret "$CSECRET" --out report.jsonl
```

Only rows that were really POSTed (`action=posted`, `ok=true`) resume as `skipped_done`,
so pointing the dry run, the smoke test and the full run at the same `report.jsonl` is
safe: the dry run's records are ignored, and the smoke test's five rows are correctly
skipped. A row that failed to POST is retried on the next run.

- `--modified-before YYYY-MM-DD` migrates only rows whose `last_modified` is strictly
  before that date (UTC); the rest are counted as `filtered` in the run summary. Omit it
  to migrate every row.
- Credentials: pass `--client-id/--client-secret` or set `KITU_CLIENT_ID` /
  `KITU_CLIENT_SECRET`. This is the palvelukäyttäjä OAuth client allowed to POST YKI
  suoritukset.
- The report (`report.jsonl`) has one line per row: `{solki_id, ok, action, http,
response|issues}`. Re-running with the same `--out` skips rows already POSTed
  (idempotent anyway — the API upserts on the Solki id).
- `--failed-out` kerää **kaikki siirtämättä jääneet rivit yhteen tiedostoon** sellaisenaan
  ja lisää syyn 31. sarakkeeksi: puuttuva oppijanumero, paikallisten tarkistusten hylkäämät,
  API:n hylkäämät POSTit, rikkinäiset rivit ja jäsentymättömät `last_modified`-arvot. Tämä on
  se tiedosto josta kannattaa rakentaa siivoustaulu jatkokäsittelyä varten. Syysarake on
  tarkoituksella ylimääräinen: jos siivottu tiedosto syötetään takaisin `--source`:ksi ilman
  sarakkeen poistoa, ajo kaatuu sarakemäärään sen sijaan että lukisi datan väärin.
  (`--unresolved-out` on edelleen eri asia: vain oppijanumerottomat, 30 saraketta, suoraan
  uudelleen syötettävissä.)
- Local pre-checks skip rows that would 400 (no osat, invalid arvosana for the taso,
  `arvosanaMuuttui ⊄ tarkistetut`) so they're reported without a wasted POST. They do
  **not** cover everything the server checks — notably `tark_kasittely_pvm <
tark_saapumis_pvm` is rejected server-side only (see the Phase 1 pre-check).
- `--sleep N` throttles between POSTs; `--limit N` caps the run; `--delimiter` overrides
  the auto-detected field delimiter (accepts `tab`, `comma`, `;`, `|`, or a literal char).

> Note: `--limit N` caps the number of input rows read, before filtering — so with
> `--modified-before`, a small `--limit` may migrate fewer than N rows if early rows are
> filtered out.

### OID backfill (rows without oppijanumero)

For a source CSV that contains rows without `suorittajan_oid` (see "Source export
completeness"), a pre-pass resolves OIDs from hetu + names through kitu's
`POST /yki/api/oppijanumero-haku` (same `YKI_TALLENNUS` OAuth2 client as the suoritus
POST; the endpoint queries ONR `yleistunniste/hae` and falls back to
`OppijanumeroTroubleshootingService` name combinations — each etunimi as kutsumanimi,
swapped etunimet/sukunimi). It is resolve-only: people ONR has never seen stay
unresolved, since kitu has no ONR-create capability.

```bash
# Pre-pass: resolve OIDs for OID-less rows into a resumable map. POSTs no suoritukset.
./scripts/migrate_yki_historia.py --source s3://.../<key>.csv --env prod --confirm-prod \
    --client-id "$CID" --client-secret "$CSECRET" \
    --backfill-oids --oid-map oid_map.jsonl --unresolved-out unresolved.csv --sleep 0.1

# Migration run: --oid-map injects the resolved OIDs; rows still without an OID are
# diverted to --unresolved-out and reported as action=unresolved, never POSTed.
./scripts/migrate_yki_historia.py --source s3://.../<key>.csv --env prod --confirm-prod \
    --client-id "$CID" --client-secret "$CSECRET" \
    --oid-map oid_map.jsonl --unresolved-out unresolved.csv --out report.jsonl
```

- `--oid-map` (JSONL, one `{solki_id, oid, reason}` per attempted row) is resumable:
  re-running the pre-pass skips already-attempted solki_ids. For a **later retry round**
  use a **fresh** map file — the map records failed attempts too, so reusing it would
  skip people who exist in ONR by now.
- `--unresolved-out` accumulates the still-unresolved rows verbatim in the same
  30-column headerless layout, deduplicated on solki_id, so the file is **directly
  usable as `--source`** in a later round. It contains hetus, names and addresses —
  same safety boundary as the source CSV, keep it in AWS.
- The pre-pass respects `--modified-before` and `--limit`, so it only resolves rows the
  migration would actually load.
- **A 502 `(BadRequest)` is a per-row data fault, not an outage.** kitu maps every ONR
  answer except not-found to 502 with the text "Yritä myöhemmin uudestaan", so the
  exception name in the body is the only signal: `(BadRequest)` means ONR rejected that
  row (a malformed hetu, usually) and retrying gives the same 400. Such rows are recorded
  in the map with a reason and the pass continues. Genuine transients (other 502s, 5xx,
  connection errors) are counted as `transient` and deliberately **not** written to the
  map, so a resume retries them. 401 refreshes the token once; 403 or a second 401 stops
  the pass (exit 1).
- Hetus are checked locally (format, century marker, checksum, calendar date) before a
  lookup is spent, and recorded as `virheellinen hetu (...)`. `--no-hetu-check` bypasses
  it if you would rather let ONR decide.
- Lookups are cached per person — `(hetu, etunimet, sukunimi)`, case-folded with inner
  whitespace collapsed — so one person's several suoritukset cost one ONR call. The
  `cache_hits` counter shows how many rows were answered without a call; the oid map still
  gets one record per row, so resuming is unaffected.

#### `--hetu-batch`: haku pelkillä henkilötunnuksilla

Nimillä tehty haku vastaa **409:llä aina kun rekisterin nimet eroavat lähdeaineiston
nimistä** — vanhassa datassa se on pikemminkin sääntö kuin poikkeus, ja ONR:n koodi
osoittaa 409:n tarkoittavan "henkilö on olemassa, nimet eivät täsmää". `--hetu-batch`
ohittaa nimet kokonaan ja ratkaisee oppijanumerot erissä (1 000 hetua/kutsu):

```bash
./scripts/migrate_yki_historia.py --source unresolved_r2.csv --env prod --confirm-prod \
    --modified-before 2017-01-01 --client-id "$CID" --client-secret "$CSECRET" \
    --backfill-oids --hetu-batch \
    --oid-map oid_map_hetut.jsonl --unresolved-out unresolved_r3.csv
```

- Vaatii palvelinpuolen `POST /yki/api/oppijanumero-haku-hetulista` -rajapinnan **ja**
  oppijanumerorekisterin oikeuden **`rekisterinpitäjä read` (`REKISTERINPITAJA_READ`) OPH:n
  juuriorganisaatioon `1.2.246.562.10.00000000001`**. Muuhun organisaatioon myönnettynä
  kutsu onnistuu mutta rekisteri suodattaa rivit pois, jolloin vastaus on tyhjä — skripti
  varoittaa erikseen tästä tilanteesta.
- **403 ja 502 tarkoittavat eri asioita.** Kitu kääntää oppijanumerorekisterin virheet
  502:ksi, joten paljas 403 tulee aina kitun omasta valtuutuksesta: hetulistahaku on rajattu
  nimettyihin kutsujiin (`kitu.yki.hetulistahaku.sallitutKutsujat`), koska pelkällä
  hetulla tehtävää hakua ei voi avata `YKI_TALLENNUS`-oikeudelle — se on myönnetty myös
  Solkin tiedonsiirrolle. 502, jonka tekstissä lukee "oppijanumerorekisteri vastasi HTTP
  403", kertoo puuttuvasta `REKISTERINPITAJA_READ`-oikeudesta, ja 404 siitä ettei rajapinta
  ole vielä ympäristössä. Kaikki kolme pysäyttävät ajon.
- Hetut lähetetään myös niiltä riveiltä jotka eivät läpäise paikallista tarkistusta: erän
  paikka ei maksa mitään, ja virheellinen tarkistusmerkki jää vain löytymättä. Näin myös
  kirjoitusvirheelliset hetut tulevat kokeilluiksi kerran.
- Saman henkilön monta suoritusta vievät yhden paikan erässä, mutta oid-karttaan kirjoitetaan
  rivikohtainen tietue, joten resume toimii ennallaan. Erä joka ei vastannut jätetään
  **kokonaan kirjaamatta**, jolloin seuraava ajo yrittää sen uudelleen.

```bash
# Later round, when ONR knows more people: feed the leftover file back in.
./scripts/migrate_yki_historia.py --source unresolved.csv --env prod --confirm-prod \
    --client-id "$CID" --client-secret "$CSECRET" \
    --backfill-oids --oid-map oid_map_round2.jsonl --unresolved-out unresolved2.csv
./scripts/migrate_yki_historia.py --source unresolved.csv --env prod --confirm-prod \
    --client-id "$CID" --client-secret "$CSECRET" \
    --oid-map oid_map_round2.jsonl --unresolved-out unresolved2.csv --out report2.jsonl
```

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
- **The 204 tarkistus rows land as `TARKISTUSARVIOITU`** (those with a käsittelypäivä; any
  without one land as `TARKISTUSARVIOITAVA`), **not `TARKISTUSARVIOINTI_HYVAKSYTTY`.**
  The JSON import path cannot set the "approved" state. If these historical tarkistukset
  should be `HYVAKSYTTY`, do a follow-up DB update after import or adjust the import — a
  domain decision.
- **The current 2011–2020 export contains no OID-less rows** (`WHERE o.oid IS NOT NULL`
  filtered them out at the source), so the OID backfill only matters once a re-export
  without that filter is available — see "Source export completeness" for the queries
  that quantify what it excluded.
- **People ONR has never seen cannot be backfilled** — the haku endpoint is
  resolve-only. Whether the final unresolved remainder should be created in ONR (a
  capability kitu doesn't have) or documented as a permanent gap is a domain decision.
- **The haku endpoint (`POST /yki/api/oppijanumero-haku`) is a bulk hetu→OID lookup**
  kept at the same trust level as suoritus creation (`YKI_TALLENNUS`). Consider
  removing it once the migration is complete.

## Troubleshooting

- `glob('s3://…')` errors / boto3 `AccessDenied` → wrong account or expired session; check
  you're in CloudShell in the bucket's account, or `aws sso login`.
- Migration rows returning HTTP 400 → read `response` in the report; the field path in the
  `TiedonsiirtoFailure` points at the offending value. HTTP 401 → wrong client credentials
  or the palvelukäyttäjä lacks YKI rights.
- Migrate script errors `could not auto-detect a delimiter giving 30 columns` → it prints
  the field counts per candidate delimiter; pass `--delimiter` explicitly, or the file
  isn't the expected layout at all.
- DuckDB reads garbage / wrong column count → the file isn't the 30-column Solki layout;
  re-read positionally (`header := false`, no `names`) and re-derive the mapping.
