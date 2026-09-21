#!/usr/bin/env python3
"""Migrate historical YKI suoritukset from a Solki CSV export into the kitu
register via POST /yki/api/suoritus.

Run this inside AWS CloudShell in the account that owns the bucket, so the
sensitive CSV never leaves AWS. It is the migration counterpart to the analysis
tool in scripts/duckdb_session.sh.

The CSV is the fixed 30-column headerless Solki layout (YkiSuoritusCsv order),
with MySQL's \\N as the NULL sentinel. Each row is mapped to a
Henkilosuoritus<YkiSuoritus> JSON body and POSTed with an OAuth2
client_credentials token. The endpoint validates, dedups (upsert on the Solki
id, so re-runs are safe), forwards to KOSKI and notifies ilmoittautumisjärjestelmä.

Typical use:
    # 1. Dry run: map every row, run local checks, write a report, POST nothing.
    ./migrate_yki_historia.py --source s3://kitu-yki-historia-upload-prod/<key>.csv \\
        --dry-run --out report.jsonl --emit-payloads payloads.jsonl

    # 2. Smoke test against prod with a handful of rows.
    ./migrate_yki_historia.py --source s3://.../<key>.csv --env prod --confirm-prod \\
        --client-id "$CID" --client-secret "$CSECRET" --limit 5 --out report.jsonl

    # 3. Full run (resumable: re-running skips rows already recorded ok).
    ./migrate_yki_historia.py --source s3://.../<key>.csv --env prod --confirm-prod \\
        --client-id "$CID" --client-secret "$CSECRET" --out report.jsonl

Rows without a suorittajan OID: a --backfill-oids pre-pass resolves them from hetu +
names via kitu's POST /yki/api/oppijanumero-haku into a resumable --oid-map (JSONL),
which the normal run then injects. Rows that still lack an OID are diverted to
--unresolved-out as a 30-column CSV, directly reusable as --source in a later round.
Add --dry-run to the pre-pass to only classify the rows (no ONR calls, no credentials
needed) and see how many are even attemptable:

    ./migrate_yki_historia.py --source s3://.../<key>.csv --dry-run \\
        --backfill-oids --oid-map oid_map.jsonl --unresolved-out unresolved.csv
    ./migrate_yki_historia.py --source s3://.../<key>.csv --env prod --confirm-prod \\
        --client-id "$CID" --client-secret "$CSECRET" \\
        --backfill-oids --oid-map oid_map.jsonl --unresolved-out unresolved.csv
    ./migrate_yki_historia.py --source s3://.../<key>.csv --env prod --confirm-prod \\
        --client-id "$CID" --client-secret "$CSECRET" \\
        --oid-map oid_map.jsonl --unresolved-out unresolved.csv --out report.jsonl
"""

import argparse
import csv
import io
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone

# YkiSuoritusCsv @JsonPropertyOrder, renamed from the source export SQL.
COLUMNS = [
    "suorittajan_oid", "hetu", "sukupuoli", "sukunimi", "etunimet", "kansalaisuus",
    "katuosoite", "postinumero", "postitoimipaikka", "email", "suoritus_id",
    "last_modified", "tutkintopaiva", "tutkintokieli", "tutkintotaso",
    "jarjestajan_oid", "jarjestajan_nimi", "arviointipaiva", "as_ty", "as_ki",
    "as_rs", "as_py", "as_pu", "as_yl", "tark_saapumis_pvm", "tark_asiatunnus",
    "tark_osakokeet", "arvosana_muuttui", "perustelu", "tark_kasittely_pvm",
]

LAST_MODIFIED_IDX = COLUMNS.index("last_modified")
SUORITTAJAN_OID_IDX = COLUMNS.index("suorittajan_oid")
HETU_IDX = COLUMNS.index("hetu")
ETUNIMET_IDX = COLUMNS.index("etunimet")
SUKUNIMI_IDX = COLUMNS.index("sukunimi")
SUORITUS_ID_IDX = COLUMNS.index("suoritus_id")

# grade column -> TutkinnonOsa code
OSA_COLUMNS = [
    ("as_ty", "TY"), ("as_ki", "KI"), ("as_rs", "RS"),
    ("as_py", "PY"), ("as_pu", "PU"), ("as_yl", "YL"),
]

# TutkinnonOsa.bitmask; RS and YL are 0 and cannot appear in the tarkistus bitmasks.
BITMASK = [("PU", 1), ("KI", 2), ("TY", 4), ("PY", 8)]

# Koodisto.YkiArvosana.validIntegersFor(tutkintotaso)
VALID_ARVOSANA = {
    "PT": {0, 1, 2, 9, 10, 11, 12},
    "KT": {0, 1, 2, 3, 4, 9, 10, 11, 12},
    "YT": {0, 1, 2, 3, 4, 5, 6, 9, 10, 11, 12},
}

# YkiSuoritusEntity.from throws IllegalArgumentException ("<Field> puuttuu") on each of
# these, i.e. HTTP 400 — but only after validation has already spent an ONR call
# verifying the oid, so they are worth catching locally.
REQUIRED_HENKILO_FIELDS = [
    "sukupuoli", "sukunimi", "etunimet", "kansalaisuus",
    "katuosoite", "postinumero", "postitoimipaikka",
]

# (host, oauth token url). dev/test taken from scripts/upload_yki_suoritus.sh.
# PROD values are a best guess from the dev/test pattern — VERIFY before a real run
# (or pass --host/--token-url explicitly).
ENV_PRESETS = {
    "dev": (
        "https://virkailija.untuvaopintopolku.fi/kielitutkinnot",
        "https://dev.otuva.opintopolku.fi/kayttooikeus-service/oauth2/token",
    ),
    "test": (
        "https://virkailija.testiopintopolku.fi/kielitutkinnot",
        "https://qa.otuva.opintopolku.fi/kayttooikeus-service/oauth2/token",
    ),
    "prod": (
        "https://virkailija.opintopolku.fi/kielitutkinnot",
        "https://virkailija.opintopolku.fi/kayttooikeus-service/oauth2/token",
    ),
}


def log(msg):
    print(msg, file=sys.stderr, flush=True)


def to_null(value):
    """Normalise a CSV field: trim surrounding whitespace/tabs (the export has
    stray trailing whitespace in e.g. sukunimi, etunimet, katuosoite,
    postitoimipaikka), then treat MySQL's \\N and empty string as NULL."""
    if value is None:
        return None
    value = value.strip()
    return None if value in ("", "\\N") else value


def parse_int(value):
    value = to_null(value)
    return None if value is None else int(value)


def decode_bitmask(value):
    n = parse_int(value)
    if not n:
        return []
    return [code for code, bit in BITMASK if n & bit]


HETU_PATTERN = re.compile(r"^(\d{6})([+\-YXWVUABCDEF])(\d{3})([0-9A-Y])$", re.IGNORECASE)
HETU_CHECKSUM_TABLE = "0123456789ABCDEFHJKLMNPRSTUVWXY"


def hetu_issue(hetu):
    """None when the hetu is well-formed, else the reason it is not. ONR matches on the
    hetu exactly, so a hetu that fails this cannot resolve — and it would come back as a
    502 indistinguishable from an ONR outage, after a full name-combination fan-out."""
    match = HETU_PATTERN.match(hetu)
    if match is None:
        return "virheellinen hetun muoto"
    paivat, _, yksilonumero, tarkiste = match.groups()
    paiva, kuukausi = int(paivat[0:2]), int(paivat[2:4])
    if not (1 <= paiva <= 31 and 1 <= kuukausi <= 12):
        return "virheellinen hetun päivämäärä"
    if HETU_CHECKSUM_TABLE[int(paivat + yksilonumero) % 31] != tarkiste.upper():
        return "virheellinen hetun tarkiste"
    return None


LAST_MODIFIED_FORMATS = (
    "%Y-%m-%dT%H:%M:%S%z",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%d %H:%M:%S%z",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d",
)


def parse_last_modified(value):
    value = to_null(value)
    if value is None:
        return None
    for fmt in LAST_MODIFIED_FORMATS:
        try:
            dt = datetime.strptime(value, fmt)
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def build_payload(row):
    d = dict(zip(COLUMNS, row))

    osat = []
    for col, code in OSA_COLUMNS:
        arvosana = parse_int(d[col])
        if arvosana is not None:
            osat.append({"tyyppi": code, "arvosana": arvosana})

    arviointipaiva = to_null(d["arviointipaiva"])
    suoritus = {
        "tyyppi": "yleinenkielitutkinto",
        "tutkintotaso": to_null(d["tutkintotaso"]),
        "kieli": to_null(d["tutkintokieli"]),
        "todistuskieli": None,
        "jarjestaja": {
            "oid": to_null(d["jarjestajan_oid"]),
            "nimi": to_null(d["jarjestajan_nimi"]),
        },
        "tutkintopaiva": to_null(d["tutkintopaiva"]),
        "arviointipaiva": arviointipaiva,
        "arviointitila": "ARVIOITU" if arviointipaiva else "ARVIOITAVA",
        "osat": osat,
        "lahdejarjestelmanId": {"id": to_null(d["suoritus_id"]), "lahde": "Solki"},
    }

    if to_null(d["tark_saapumis_pvm"]):
        suoritus["tarkistusarviointi"] = {
            "saapumispaiva": to_null(d["tark_saapumis_pvm"]),
            "kasittelypaiva": to_null(d["tark_kasittely_pvm"]),
            "asiatunnus": to_null(d["tark_asiatunnus"]) or "",
            "tarkistusarvioidutOsakokeet": decode_bitmask(d["tark_osakokeet"]),
            "arvosanaMuuttui": decode_bitmask(d["arvosana_muuttui"]),
            "perustelu": to_null(d["perustelu"]) or "",
        }

    henkilo = {
        "oid": to_null(d["suorittajan_oid"]),
        "sukunimi": to_null(d["sukunimi"]),
        "etunimet": to_null(d["etunimet"]),
        "hetu": to_null(d["hetu"]),
        "sukupuoli": to_null(d["sukupuoli"]),
        "kansalaisuus": to_null(d["kansalaisuus"]),
        "katuosoite": to_null(d["katuosoite"]),
        "postinumero": to_null(d["postinumero"]),
        "postitoimipaikka": to_null(d["postitoimipaikka"]),
        "email": to_null(d["email"]),
    }
    return {"henkilo": henkilo, "suoritus": suoritus}


def local_issues(payload):
    """Cheap checks that mirror the server's validation, so obviously-bad rows
    are reported without a wasted POST."""
    s = payload["suoritus"]
    issues = []
    if not s["osat"]:
        issues.append("ei yhtään osakoetta")
    if s["tutkintotaso"] not in VALID_ARVOSANA:
        issues.append(f"tuntematon tutkintotaso: {s['tutkintotaso']}")
    else:
        valid = VALID_ARVOSANA[s["tutkintotaso"]]
        bad = sorted({o["arvosana"] for o in s["osat"] if o["arvosana"] not in valid})
        if bad:
            issues.append(f"virheellinen arvosana tasolle {s['tutkintotaso']}: {bad}")
    t = s.get("tarkistusarviointi")
    if t and set(t["arvosanaMuuttui"]) - set(t["tarkistusarvioidutOsakokeet"]):
        issues.append("arvosanaMuuttui ei ole tarkistettujen osakokeiden osajoukko")
    if t and t["kasittelypaiva"] and t["kasittelypaiva"] < t["saapumispaiva"]:
        issues.append("tarkistusarvioinnin käsittelypäivä on ennen saapumispäivää")
    puuttuvat = [f for f in REQUIRED_HENKILO_FIELDS if not payload["henkilo"][f]]
    if puuttuvat:
        issues.append(f"henkilötietoja puuttuu: {', '.join(puuttuvat)}")
    return issues


def read_text(source):
    if source.startswith("s3://"):
        import boto3  # only needed for S3; keeps local dry-runs dependency-free

        bucket, _, key = source[len("s3://"):].partition("/")
        return boto3.client("s3").get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8")
    with open(source, encoding="utf-8") as f:
        return f.read()


DELIMITER_ALIASES = {"tab": "\t", "\\t": "\t", "comma": ",", "semicolon": ";", "pipe": "|"}
DELIMITER_CANDIDATES = ("\t", ",", ";", "|")


def field_count(line, delimiter):
    return len(next(csv.reader([line], delimiter=delimiter, quotechar='"')))


def resolve_delimiter(sample_line, override):
    """Return the field delimiter: the override (name or literal char), else the
    candidate that splits the sample line into exactly len(COLUMNS) fields."""
    if override:
        return DELIMITER_ALIASES.get(override, override)
    for delimiter in DELIMITER_CANDIDATES:
        if field_count(sample_line, delimiter) == len(COLUMNS):
            return delimiter
    return None


def get_token(token_url, client_id, client_secret):
    data = urllib.parse.urlencode({
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
    }).encode()
    req = urllib.request.Request(
        token_url, data=data,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    with urllib.request.urlopen(req) as r:
        return json.load(r)["access_token"]


class Transient(Exception):
    """A failure that may succeed on a later attempt: an ONR outage, a network error —
    or a request ONR itself rejected, which the haku endpoint also reports as 502."""


class Fatal(Exception):
    """A failure no row will survive: wrong credentials, missing rights, or a response
    shape we do not understand. Retrying the file would only produce noise."""


TRANSIENT_STATUSES = frozenset({429, 500, 502, 503, 504})
TRANSIENT_BACKOFF = (2, 5)
MAX_CONSECUTIVE_TRANSIENT = 10


class ApiSession:
    """Holds the OAuth token and POSTs JSON bodies. Refreshes the token on a 401 (the
    token outlives neither a 76k-row pass nor, in the dev mock, a minute — and
    expires_in cannot be trusted) and retries transient failures with a backoff."""

    def __init__(self, host, token_url, client_id, client_secret, timeout):
        self.host = host
        self.token_url = token_url
        self.client_id = client_id
        self.client_secret = client_secret
        self.timeout = timeout
        self.token = None
        self.refreshes = 0

    def fetch_token(self):
        self.token = get_token(self.token_url, self.client_id, self.client_secret)

    def post_json(self, path, payload):
        """(status, body) for anything the caller can act on. Raises Transient once the
        retries are spent, so the caller can leave the row for a later round."""
        body = json.dumps(payload).encode()
        status, response = None, ""
        for attempt in range(len(TRANSIENT_BACKOFF) + 1):
            if self.token is None:
                self.fetch_token()
            status, response = self._send(path, body)
            if status == 401:
                log("token hylättiin (401), haetaan uusi")
                self.refreshes += 1
                self.fetch_token()
                status, response = self._send(path, body)
                if status == 401:
                    raise Fatal("HTTP 401 myös tokenin uusimisen jälkeen — tarkista client-tunnukset")
            if status == 403:
                raise Fatal(f"HTTP 403 {path}: client-tunnuksilta puuttuu YKI_TALLENNUS-oikeus")
            if status is not None and status not in TRANSIENT_STATUSES:
                return status, response
            if attempt < len(TRANSIENT_BACKOFF):
                delay = TRANSIENT_BACKOFF[attempt]
                log(f"{path}: HTTP {status}, uudelleenyritys {delay}s kuluttua")
                time.sleep(delay)
        raise Transient(f"HTTP {status}: {response[:300]}")

    def _send(self, path, body):
        """(status, body); status is None on a network-level failure."""
        req = urllib.request.Request(
            self.host + path, data=body,
            headers={"Authorization": f"Bearer {self.token}", "Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as r:
                return r.status, r.read().decode()
        except urllib.error.HTTPError as e:
            return e.code, e.read().decode()
        except (urllib.error.URLError, TimeoutError, OSError) as e:
            return None, str(e)


def resolve_oid(session, hetu, etunimet, sukunimi):
    """Resolve a master oppijanumero from hetu + names via kitu's ONR lookup endpoint.
    Returns (oid, reason): the OID with reason None, or None with a permanent reason.
    Raises Transient when the attempt may succeed later."""
    status, response = session.post_json(
        "/yki/api/oppijanumero-haku",
        {"hetu": hetu, "etunimet": etunimet, "sukunimi": sukunimi},
    )
    if 200 <= status < 300:
        oid = json.loads(response).get("oid")
        if not oid:
            raise Fatal(f"haku vastasi {status} ilman oid-kenttää: {response[:200]}")
        return oid, None
    if status == 404:
        return None, "ei löytynyt oppijanumerorekisteristä"
    if status == 400:
        return None, f"haku hylkäsi pyynnön: {response[:200]}"
    raise Transient(f"HTTP {status}: {response[:300]}")


def load_oid_map(path):
    """{solki_id: {"oid": oid_or_None, "reason": reason_or_None}}; a key's presence means
    the row was already attempted with a permanent outcome."""
    oid_map = {}
    if path and os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            for line in f:
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if "solki_id" in rec:
                    oid_map[rec["solki_id"]] = {"oid": rec.get("oid"), "reason": rec.get("reason")}
    return oid_map


def load_leftover_ids(path):
    """solki_ids already written to the unresolved CSV, so re-runs don't duplicate rows."""
    ids = set()
    if path and os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            text = f.read()
        if text.strip():
            delimiter = resolve_delimiter(text.split("\n", 1)[0].rstrip("\r"), None)
            if delimiter is None:
                raise Fatal(
                    f"{path}: erotinta ei tunnistettu, joten jo kirjattuja rivejä ei voi "
                    "tunnistaa ja ajo kirjoittaisi ne uudelleen. Siirrä tiedosto pois tieltä "
                    "tai korjaa sen muoto ennen ajoa."
                )
            for row in csv.reader(io.StringIO(text), delimiter=delimiter, quotechar='"'):
                if len(row) == len(COLUMNS):
                    ids.add(to_null(row[SUORITUS_ID_IDX]))
    return ids


def collapse_ws(value):
    """Collapse runs of whitespace to a single space. to_null only trims the ends, and
    the export's name fields carry stray inner whitespace too."""
    return " ".join(value.split())


def person_key(hetu, etunimet, sukunimi):
    """All of one person's suoritukset come from a single osallistuja row, so this is
    stable across their rows — one ONR lookup per person instead of per suoritus."""
    return (hetu.casefold(), collapse_ws(etunimet).casefold(), collapse_ws(sukunimi).casefold())


def lookup_fields(row, check_hetu):
    """(hetu, etunimet, sukunimi, reason); reason is set when the row cannot be looked
    up at all, and is specific enough for OPH to act on."""
    hetu = to_null(row[HETU_IDX])
    etunimet = to_null(row[ETUNIMET_IDX])
    sukunimi = to_null(row[SUKUNIMI_IDX])
    if hetu is None:
        return hetu, etunimet, sukunimi, "hetu puuttuu"
    if etunimet is None:
        return hetu, etunimet, sukunimi, "etunimet puuttuu"
    if sukunimi is None:
        return hetu, etunimet, sukunimi, "sukunimi puuttuu"
    if check_hetu:
        issue = hetu_issue(hetu)
        if issue:
            return hetu, etunimet, sukunimi, issue
    return hetu, etunimet, sukunimi, None


def backfill_oids(rows, session, map_path, capture_unresolved, limit, max_lookups, cutoff,
                  sleep, check_hetu, classify_only, skip_ids):
    """Resolve OIDs for rows that have none. Permanent outcomes are appended to
    map_path as {solki_id, oid, reason} so a resume skips them; transient ones are
    deliberately left out so a resume retries them. classify_only calls no API at all.
    Returns (counts, aborted)."""
    attempted = load_oid_map(map_path)
    if attempted:
        log(f"backfill resuming: {len(attempted)} rows already attempted will be skipped")
    counts = {"resolved": 0, "unresolved": 0, "cached": 0, "failed": 0, "attemptable": 0,
              "skipped_issue": 0, "skipped_attempted": 0, "excluded": 0, "has_oid": 0,
              "filtered": 0, "lookups": 0}
    aborted = False
    cache = {}
    persons = set()
    consecutive_transient = 0
    out = None if classify_only else open(map_path, "a", encoding="utf-8")
    try:
        for i, row in enumerate(rows):
            if limit is not None and i >= limit:
                break
            if (i + 1) % 1000 == 0:
                log(f"...{i + 1} riviä luettu {counts}")
            if not row or len(row) != len(COLUMNS):
                continue
            if cutoff is not None:
                lm = parse_last_modified(row[LAST_MODIFIED_IDX])
                if lm is None or lm >= cutoff:
                    counts["filtered"] += 1
                    continue
            if to_null(row[SUORITTAJAN_OID_IDX]):
                counts["has_oid"] += 1
                continue
            solki_id = to_null(row[SUORITUS_ID_IDX])
            if solki_id in skip_ids:
                counts["excluded"] += 1
                continue
            if solki_id in attempted:
                counts["skipped_attempted"] += 1
                continue

            # An OID cannot rescue a row the server would 400 anyway, and every lookup
            # is expensive. Not written to the map: that file records OID outcomes only,
            # so a later round still resolves the row if the source data gets fixed.
            if local_issues(build_payload(row)):
                counts["skipped_issue"] += 1
                capture_unresolved(row, solki_id)
                continue

            hetu, etunimet, sukunimi, reason = lookup_fields(row, check_hetu)
            oid = None
            called_api = False

            if reason is None:
                key = person_key(hetu, etunimet, sukunimi)
                persons.add(key)
                if classify_only:
                    counts["attemptable"] += 1
                    continue
                if key in cache:
                    oid, reason = cache[key]
                    counts["cached"] += 1
                else:
                    if max_lookups is not None and counts["lookups"] >= max_lookups:
                        log(f"--max-lookups {max_lookups} saavutettu — lopetetaan")
                        break
                    counts["lookups"] += 1
                    try:
                        oid, reason = resolve_oid(session, hetu, etunimet, sukunimi)
                    except Transient as e:
                        counts["failed"] += 1
                        consecutive_transient += 1
                        log(f"solki_id={solki_id}: ohimenevä virhe, ei kirjata mappiin ({e})")
                        if consecutive_transient >= MAX_CONSECUTIVE_TRANSIENT:
                            log(f"{consecutive_transient} ohimenevää virhettä putkeen — keskeytetään")
                            aborted = True
                            break
                        if sleep:
                            time.sleep(sleep)
                        continue
                    consecutive_transient = 0
                    called_api = True
                    cache[key] = (oid, reason)

            rec = {"solki_id": solki_id, "oid": oid, "reason": reason}
            if out is not None:
                out.write(json.dumps(rec, ensure_ascii=False) + "\n")
                out.flush()
            attempted[solki_id] = rec
            if oid is None:
                capture_unresolved(row, solki_id)
            counts["resolved" if oid else "unresolved"] += 1
            if called_api and sleep:
                time.sleep(sleep)
    finally:
        if out is not None:
            out.close()
    log(f"backfill done: {counts}")
    log(f"eri henkilöitä oidittomilla riveillä: {len(persons)}"
        + ("  <- ONR-kutsujen budjetti" if classify_only else ""))
    return counts, aborted


def load_skip_ids(path):
    """Solki ids to leave out of the run entirely, one per line; '#' starts a comment.
    For rows a human has decided against — e.g. one half of a duplicate pair found by
    the runbook's hetu-level duplicate check — without editing the sensitive source CSV."""
    ids = set()
    if path:
        with open(path, encoding="utf-8") as f:
            for line in f:
                line = line.split("#", 1)[0].strip()
                if line:
                    ids.add(line)
        log(f"skip list: {len(ids)} solki_id(s) will be excluded")
    return ids


def load_done(out_path):
    done = set()
    if out_path and os.path.exists(out_path):
        with open(out_path, encoding="utf-8") as f:
            for line in f:
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    continue
                # Only a real POST counts as done. A dry-run record also carries
                # ok: true, and the runbook reuses one --out across both, so accepting
                # those would make the live run skip the whole file.
                if rec.get("ok") and rec.get("action") == "posted":
                    done.add(rec.get("solki_id"))
    return done


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--source", required=True, help="s3://bucket/key.csv or a local path")
    p.add_argument("--env", choices=sorted(ENV_PRESETS), help="host + token url preset")
    p.add_argument("--host", help="override target host, e.g. https://.../kielitutkinnot")
    p.add_argument("--token-url", help="override OAuth2 token endpoint")
    p.add_argument("--client-id", default=os.environ.get("KITU_CLIENT_ID"))
    p.add_argument("--client-secret", default=os.environ.get("KITU_CLIENT_SECRET"))
    p.add_argument("--dry-run", action="store_true", help="map + check only, POST nothing")
    p.add_argument("--confirm-prod", action="store_true", help="required to POST to prod")
    p.add_argument("--out", default="report.jsonl", help="per-row outcome report (JSONL)")
    p.add_argument("--emit-payloads", help="dry-run: also write mapped payloads here (JSONL)")
    p.add_argument("--limit", type=int, help="process at most N rows")
    p.add_argument("--sleep", type=float, default=0.0, help="seconds to wait between POSTs")
    p.add_argument(
        "--modified-before", metavar="YYYY-MM-DD",
        help="only migrate rows whose last_modified is strictly before this date (UTC)",
    )
    p.add_argument(
        "--delimiter",
        help="field delimiter: 'tab', 'comma', ';', '|', or a literal char (default: auto-detect)",
    )
    p.add_argument(
        "--backfill-oids", action="store_true",
        help="pre-pass: resolve OIDs for rows without one via /yki/api/oppijanumero-haku "
             "into --oid-map, POST no suoritukset. With --dry-run: classify only, no API calls",
    )
    p.add_argument(
        "--oid-map", metavar="PATH",
        help="JSONL map of solki_id → resolved oid; written by --backfill-oids, "
             "read by the normal run to inject OIDs into OID-less rows",
    )
    p.add_argument(
        "--unresolved-out", metavar="PATH",
        help="write rows that still lack a resolvable oppijanumero to this CSV "
             "(same 30-column headerless layout, reusable as --source later)",
    )
    p.add_argument(
        "--no-hetu-check", action="store_true",
        help="skip the local hetu format/checksum check and let ONR judge every hetu",
    )
    p.add_argument(
        "--max-lookups", type=int, metavar="N",
        help="backfill: stop after N actual ONR lookups (--limit counts input rows, "
             "which in an interleaved export may yield no lookups at all)",
    )
    p.add_argument(
        "--skip-solki-ids", metavar="PATH",
        help="file of solki_ids to exclude from the run (one per line, '#' comments); "
             "for rows a human has ruled out, e.g. one half of a duplicate pair",
    )
    p.add_argument(
        "--timeout", type=float, default=180.0, metavar="SECONDS",
        help="per-request socket timeout; generous by default because an unresolvable "
             "lookup fans out to 1+2N ONR calls server-side (default: 180)",
    )
    args = p.parse_args()

    if args.backfill_oids:
        if not args.oid_map:
            p.error("--backfill-oids needs --oid-map")
        if not args.unresolved_out:
            p.error("--backfill-oids needs --unresolved-out, or the rows it cannot "
                    "resolve are not kept for further processing")

    cutoff = None
    if args.modified_before:
        cutoff = datetime.strptime(args.modified_before, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        log(f"filter: migrating only rows with last_modified before {cutoff.date()}")

    host, token_url = args.host, args.token_url
    if args.env:
        preset_host, preset_token = ENV_PRESETS[args.env]
        host = host or preset_host
        token_url = token_url or preset_token

    if not args.dry_run:
        if not host or not token_url:
            p.error("live run needs --env or --host/--token-url")
        if not args.client_id or not args.client_secret:
            p.error("live run needs --client-id/--client-secret (or KITU_CLIENT_ID/SECRET)")
        if args.env == "prod" and not args.confirm_prod:
            p.error("refusing to POST to prod without --confirm-prod")
        if "opintopolku.fi" in host and "untuva" not in host and "testi" not in host:
            log(f"PROD TARGET: host={host} token_url={token_url} — verify these before continuing.")

    skip_ids = load_skip_ids(args.skip_solki_ids)
    done = load_done(args.out) if not (args.dry_run or args.backfill_oids) else set()
    if done:
        log(f"resuming: {len(done)} rows already recorded ok will be skipped")

    session = None
    if not args.dry_run:
        session = ApiSession(host, token_url, args.client_id, args.client_secret, args.timeout)
        log(f"fetching OAuth token from {token_url}")
        session.fetch_token()

    text = read_text(args.source)
    sample = text.split("\n", 1)[0]
    delimiter = resolve_delimiter(sample, args.delimiter)
    if delimiter is None:
        counts_by = {repr(d): field_count(sample, d) for d in DELIMITER_CANDIDATES}
        p.error(f"could not auto-detect a delimiter giving {len(COLUMNS)} columns; "
                f"field counts by delimiter: {counts_by} (pass --delimiter)")
    log(f"delimiter: {delimiter!r}")
    rows = csv.reader(io.StringIO(text), delimiter=delimiter, quotechar='"')

    leftover_fh = open(args.unresolved_out, "a", encoding="utf-8", newline="") if args.unresolved_out else None
    leftover_writer = (
        csv.writer(leftover_fh, delimiter=delimiter, quotechar='"', lineterminator="\n")
        if leftover_fh else None
    )
    leftover_seen = load_leftover_ids(args.unresolved_out) if args.unresolved_out else set()

    def capture_unresolved(row, solki_id):
        if leftover_writer is None or solki_id in leftover_seen:
            return
        leftover_writer.writerow(row)
        leftover_fh.flush()
        leftover_seen.add(solki_id)

    if args.backfill_oids:
        _, aborted = backfill_oids(rows, session, args.oid_map, capture_unresolved, args.limit,
                                   args.max_lookups, cutoff, args.sleep,
                                   not args.no_hetu_check, args.dry_run, skip_ids)
        if leftover_fh:
            leftover_fh.close()
        return 1 if aborted else 0

    oid_map = load_oid_map(args.oid_map) if args.oid_map else {}
    if oid_map:
        resolved = sum(1 for entry in oid_map.values() if entry["oid"])
        log(f"oid map: {resolved} resolved of {len(oid_map)} attempted rows")

    payloads_fh = open(args.emit_payloads, "w", encoding="utf-8") if args.emit_payloads else None
    counts = {"posted": 0, "skipped_issue": 0, "skipped_done": 0, "excluded": 0, "dry": 0,
              "filtered": 0, "unresolved": 0, "malformed_row": 0, "bad_last_modified": 0,
              "post_failed": 0, "transient": 0}
    consecutive_transient = 0
    aborted = False

    with open(args.out, "a", encoding="utf-8") as out:
        for i, row in enumerate(rows):
            if args.limit is not None and i >= args.limit:
                break
            if (i + 1) % 1000 == 0:
                log(f"...{i + 1} riviä luettu {counts}")
            if not row:
                continue
            if len(row) != len(COLUMNS):
                rec = {"row": i, "ok": False, "error": f"odotettiin {len(COLUMNS)} saraketta, saatiin {len(row)}"}
                out.write(json.dumps(rec, ensure_ascii=False) + "\n")
                counts["malformed_row"] += 1
                continue

            if cutoff is not None:
                lm = parse_last_modified(row[LAST_MODIFIED_IDX])
                if lm is None:
                    rec = {"row": i, "ok": False, "error": "last_modified ei jäsenny, ei voi suodattaa"}
                    out.write(json.dumps(rec, ensure_ascii=False) + "\n")
                    counts["bad_last_modified"] += 1
                    continue
                if lm >= cutoff:
                    counts["filtered"] += 1
                    continue

            if to_null(row[SUORITUS_ID_IDX]) in skip_ids:
                counts["excluded"] += 1
                continue

            entry = oid_map.get(to_null(row[SUORITUS_ID_IDX])) if oid_map else None
            if entry and entry["oid"] and not to_null(row[SUORITTAJAN_OID_IDX]):
                row = list(row)
                row[SUORITTAJAN_OID_IDX] = entry["oid"]

            payload = build_payload(row)
            solki_id = payload["suoritus"]["lahdejarjestelmanId"]["id"]
            issues = local_issues(payload)

            if payloads_fh:
                payloads_fh.write(json.dumps(payload, ensure_ascii=False) + "\n")

            if solki_id in done:
                counts["skipped_done"] += 1
                continue

            # Issues first: a row the server would 400 is not "waiting for an OID", and
            # reporting it as such would keep it circulating through backfill rounds.
            if issues:
                capture_unresolved(row, solki_id)
                rec = {"solki_id": solki_id, "ok": False, "action": "skipped", "issues": issues}
                if not payload["henkilo"]["oid"]:
                    rec["oid_missing"] = True
                out.write(json.dumps(rec, ensure_ascii=False) + "\n")
                counts["skipped_issue"] += 1
                continue

            if not payload["henkilo"]["oid"]:
                capture_unresolved(row, solki_id)
                rec = {"solki_id": solki_id, "ok": False, "action": "unresolved",
                       "reason": (entry or {}).get("reason") or "ei oppijanumeroa, ei yritetty"}
                out.write(json.dumps(rec, ensure_ascii=False) + "\n")
                counts["unresolved"] += 1
                continue

            if args.dry_run:
                rec = {"solki_id": solki_id, "action": "dry-run", "ok": True, "dry": True}
                out.write(json.dumps(rec, ensure_ascii=False) + "\n")
                counts["dry"] += 1
            else:
                try:
                    code, resp = session.post_json("/yki/api/suoritus", payload)
                except Transient as e:
                    counts["transient"] += 1
                    consecutive_transient += 1
                    rec = {"solki_id": solki_id, "ok": False, "action": "transient", "error": str(e)}
                    out.write(json.dumps(rec, ensure_ascii=False) + "\n")
                    if consecutive_transient >= MAX_CONSECUTIVE_TRANSIENT:
                        log(f"{consecutive_transient} ohimenevää virhettä putkeen — keskeytetään")
                        aborted = True
                        break
                    continue
                consecutive_transient = 0
                ok = 200 <= code < 300
                try:
                    parsed = json.loads(resp)
                except json.JSONDecodeError:
                    parsed = resp
                rec = {"solki_id": solki_id, "ok": ok, "action": "posted", "http": code, "response": parsed}
                out.write(json.dumps(rec, ensure_ascii=False) + "\n")
                counts["posted" if ok else "post_failed"] += 1
                if args.sleep:
                    time.sleep(args.sleep)

    if payloads_fh:
        payloads_fh.close()
    if leftover_fh:
        leftover_fh.close()
    log(f"done: {counts}")
    diverted = counts["unresolved"] + counts["skipped_issue"]
    if aborted:
        return 1
    if diverted and not args.unresolved_out:
        log(f"VAROITUS: {diverted} riviä ei voitu ladata eikä --unresolved-out ollut "
            "annettu — rivit eivät jääneet talteen jatkokäsittelyä varten. Aja uudelleen "
            "--unresolved-out-valitsimella.")
        return 2
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main() or 0)
    except Fatal as e:
        log(f"VIRHE: {e}")
        sys.exit(1)
