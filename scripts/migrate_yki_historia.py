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
--unresolved-out as a 30-column CSV, directly reusable as --source in a later round:

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
from datetime import date, datetime, timezone

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
        "https://prod.otuva.opintopolku.fi/kayttooikeus-service/oauth2/token",
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
    valid = VALID_ARVOSANA.get(s["tutkintotaso"], set())
    bad = sorted({o["arvosana"] for o in s["osat"] if o["arvosana"] not in valid})
    if bad:
        issues.append(f"virheellinen arvosana tasolle {s['tutkintotaso']}: {bad}")
    t = s.get("tarkistusarviointi")
    if t and set(t["arvosanaMuuttui"]) - set(t["tarkistusarvioidutOsakokeet"]):
        issues.append("arvosanaMuuttui ei ole tarkistettujen osakokeiden osajoukko")
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


def check_credential(name, value):
    """Otuva answers every credential problem with the same opaque
    `invalid_client`, so catch the locally detectable causes first: an unset or
    misspelled env var expands to an empty string, and a value pasted from a
    file or mangled by shell expansion carries whitespace the server never sees
    as part of the credential."""
    if not value:
        return f"{name} is empty (unset or misspelled env var?)"
    if value.strip() != value:
        return f"{name} has leading/trailing whitespace ({len(value)} chars)"
    if any(not c.isprintable() for c in value):
        return f"{name} contains a non-printable character ({len(value)} chars)"
    return None


def get_token(token_url, client_id, client_secret):
    problems = [
        problem for problem in (
            check_credential("client id", client_id),
            check_credential("client secret", client_secret),
        ) if problem
    ]
    if problems:
        raise SystemExit("VIRHE: " + "; ".join(problems))

    data = urllib.parse.urlencode({
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
    }).encode()
    req = urllib.request.Request(
        token_url, data=data,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    try:
        with urllib.request.urlopen(req) as r:
            return json.load(r)["access_token"]
    except urllib.error.HTTPError as e:
        body = e.read().decode(errors="replace")
        log(f"VIRHE: tokenin haku epäonnistui: HTTP {e.code} {token_url}")
        log(f"  vastaus: {body}")
        if "invalid_client" in body:
            log("  invalid_client = otuva ei hyväksy tunnuksia. Tarkista että")
            log("    - tunnukset on luotu SAMAAN ympäristöön kuin --token-url")
            log("    - client id ja secret ovat oikein päin ja kokonaisia")
            log("    - secret ei ole rikkoutunut shellissä: lainausmerkeissä")
            log("      \"...$...\" laajenee, käytä yksinkertaisia lainausmerkkejä")
        raise SystemExit(1)


class TokenSource:
    """Holds the bearer token for a whole run and re-fetches it when the server
    says it is no longer valid. A full pass takes hours and outlives its token;
    the refresh is driven by the 401 rather than by expires_in, which the dev
    mock does not report honestly."""

    def __init__(self, token_url, client_id, client_secret):
        self._credentials = (token_url, client_id, client_secret)
        self.token = get_token(*self._credentials)

    def refresh(self):
        log("token vanhentunut, haetaan uusi")
        self.token = get_token(*self._credentials)


def post_suoritus(host, tokens, payload):
    body = json.dumps(payload).encode()
    for attempt in (1, 2):
        req = urllib.request.Request(
            host + "/yki/api/suoritus", data=body,
            headers={"Authorization": f"Bearer {tokens.token}", "Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(req) as r:
                return r.status, r.read().decode()
        except urllib.error.HTTPError as e:
            response = e.read().decode(errors="replace")
            if e.code == 401 and attempt == 1:
                tokens.refresh()
                continue
            return e.code, response


CENTURY_SIGNS = {"+": 1800, "-": 1900, "Y": 1900, "X": 1900, "W": 1900, "V": 1900,
                 "U": 1900, "A": 2000, "B": 2000, "C": 2000, "D": 2000, "E": 2000, "F": 2000}
HETU_CHECKSUM_CHARS = "0123456789ABCDEFHJKLMNPRSTUVWXY"
HETU_RE = re.compile(r"^(\d{2})(\d{2})(\d{2})([+\-YXWVUABCDEF])(\d{3})([0-9A-Y])$")


def hetu_problem(hetu):
    """Name the defect in a hetu, or None when it is well-formed. ONR answers a
    malformed hetu with a 400, which kitu reports as an opaque 502, so a row that
    cannot possibly resolve is cheaper and clearer to reject here."""
    match = HETU_RE.match(hetu.strip().upper())
    if not match:
        return "muoto"
    dd, mm, yy, sign, individual, checksum = match.groups()
    if HETU_CHECKSUM_CHARS[int(dd + mm + yy + individual) % 31] != checksum:
        return "tarkistusmerkki"
    try:
        date(CENTURY_SIGNS[sign] + int(yy), int(mm), int(dd))
    except ValueError:
        return "päivämäärä"
    return None


class TransientLookupError(Exception):
    """ONR could not answer right now. The row is deliberately NOT written to the
    oid map, so a later run retries it."""


class FatalLookupError(Exception):
    """Credentials or permissions are wrong; every remaining lookup would fail too."""


def person_key(hetu, etunimet, sukunimi):
    """Identity for the lookup cache. Case-folded with inner whitespace
    collapsed, so the same person spelled "Matti  Ilmari" and "matti ilmari"
    costs one ONR call instead of two — and one person's several suoritukset
    cost one call, not one each."""
    return tuple(re.sub(r"\s+", " ", part).strip().casefold()
                 for part in (hetu, etunimet, sukunimi))


def resolve_oid(host, tokens, hetu, etunimet, sukunimi):
    """Resolve an oppijanumero from hetu + names via kitu's ONR lookup endpoint.

    Returns (oid, reason): an OID with reason None, or None with a reason when the
    person is permanently unresolvable. kitu maps every ONR outcome except
    not-found to 502 (YkiApiController), so the exception name in the body is what
    separates "ONR rejected this row's data" from "ONR was unavailable"."""
    body = json.dumps({"hetu": hetu, "etunimet": etunimet, "sukunimi": sukunimi}).encode()
    for attempt in (1, 2):
        req = urllib.request.Request(
            host + "/yki/api/oppijanumero-haku", data=body,
            headers={"Authorization": f"Bearer {tokens.token}", "Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(req) as r:
                return json.load(r)["oid"], None
        except urllib.error.HTTPError as e:
            response = e.read().decode(errors="replace")
            if e.code == 401 and attempt == 1:
                tokens.refresh()
                continue
            if e.code == 404:
                return None, "ei löytynyt oppijanumerorekisteristä"
            if e.code in (401, 403):
                raise FatalLookupError(f"HTTP {e.code}: {response}")
            if e.code == 400 or (e.code == 502 and "(BadRequest)" in response):
                return None, f"ONR hylkäsi pyynnön: {response}"
            raise TransientLookupError(f"HTTP {e.code}: {response}")
        except urllib.error.URLError as e:
            raise TransientLookupError(str(e))


HETULISTAN_ENIMMAISKOKO = 1000


def post_hetulista(host, tokens, hetut):
    """Resolve a chunk of hetus through kitu's name-free batch lookup. Returns
    {HETU: oid} keyed by the upper-cased hetu, since the register echoes back its
    own spelling rather than the one that was sent."""
    body = json.dumps({"hetut": hetut}).encode()
    for attempt in (1, 2):
        req = urllib.request.Request(
            host + "/yki/api/oppijanumero-haku-hetulista", data=body,
            headers={"Authorization": f"Bearer {tokens.token}", "Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(req) as r:
                vastaus = json.load(r)
            return {
                hetu.strip().upper(): oid
                for hetu, oid in (vastaus.get("oppijanumerot") or {}).items()
            }
        except urllib.error.HTTPError as e:
            response = e.read().decode(errors="replace")
            if e.code == 401 and attempt == 1:
                tokens.refresh()
                continue
            if e.code in (401, 403):
                raise FatalLookupError(
                    f"HTTP {e.code}: {response}\n"
                    "  kitu ei päästänyt kutsua läpi. Hetulistahaku on rajattu nimettyihin\n"
                    "  kutsujiin (kitu.yki.hetulistahaku.sallitutKutsujat) — tarkista että\n"
                    "  käyttämäsi client on listalla. Oppijanumerorekisterin oikeusongelmat\n"
                    "  näkyvät 502:na, eivät 403:na.",
                )
            if e.code == 502 and "vastasi HTTP 403" in response:
                raise FatalLookupError(
                    f"HTTP {e.code}: {response}\n"
                    "  oppijanumerorekisteri epäsi haun: vaatii oikeuden 'rekisterinpitäjä read'\n"
                    "  (REKISTERINPITAJA_READ) OPH:n juuriorganisaatioon 1.2.246.562.10.00000000001",
                )
            if e.code == 404:
                raise FatalLookupError(
                    "hetulistahaku puuttuu palvelimelta — onko uusi versio ehtinyt ympäristöön? "
                    f"HTTP 404: {response}",
                )
            raise TransientLookupError(f"HTTP {e.code}: {response}")
        except urllib.error.URLError as e:
            raise TransientLookupError(str(e))


def backfill_oids_hetulista(rows, host, tokens, map_path, capture_unresolved, limit, cutoff,
                            sleep, chunk_size=HETULISTAN_ENIMMAISKOKO):
    """Resolve oppijanumerot for OID-less rows by hetu alone, in batches.

    The name-based lookup answers 409 whenever the register's names differ from the
    ones the source export carries, which is the normal case for decades-old data.
    Hetus that fail the local check are sent too: a batch slot costs nothing, and a
    bad check character simply will not match anything."""
    attempted = load_oid_map(map_path)
    if attempted:
        log(f"backfill resuming: {len(attempted)} rows already attempted will be skipped")
    counts = {"resolved": 0, "unresolved": 0, "skipped_attempted": 0, "has_oid": 0,
              "filtered": 0, "failed": 0, "transient": 0, "ilman_hetua": 0, "erissa": 0}

    kasiteltavat = []
    for i, row in enumerate(rows):
        if limit is not None and i >= limit:
            break
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
        if to_null(row[SUORITUS_ID_IDX]) in attempted:
            counts["skipped_attempted"] += 1
            continue
        kasiteltavat.append(row)

    hetut = sorted({
        to_null(row[HETU_IDX]).strip().upper()
        for row in kasiteltavat
        if to_null(row[HETU_IDX])
    })
    log(f"hetulistahaku: {len(kasiteltavat)} riviä, {len(hetut)} eri henkilötunnusta")

    ratkaistut = {}
    epaonnistuneet = set()
    with open(map_path, "a", encoding="utf-8") as out:
        for alku in range(0, len(hetut), chunk_size):
            era = hetut[alku:alku + chunk_size]
            counts["erissa"] += 1
            try:
                ratkaistut.update(post_hetulista(host, tokens, era))
            except TransientLookupError as e:
                counts["transient"] += len(era)
                epaonnistuneet.update(era)
                log(f"erä {alku}-{alku + len(era)} ei vastannut, yritetään uudelleen resumessa: {e}")
            log(f"...{min(alku + chunk_size, len(hetut))}/{len(hetut)} hetua, {len(ratkaistut)} ratkennut")
            if sleep:
                time.sleep(sleep)

        if hetut and not epaonnistuneet and not ratkaistut:
            log("VAROITUS: yksikään hetu ei ratkennut vaikka haku onnistui. Oikeus "
                "'rekisterinpitäjä read' suodattaa rivit pois, jos se on myönnetty muuhun kuin "
                "OPH:n juuriorganisaatioon 1.2.246.562.10.00000000001 — tarkista oikeus ennen "
                "kuin tulkitset tämän datan ongelmaksi.")

        for row in kasiteltavat:
            solki_id = to_null(row[SUORITUS_ID_IDX])
            hetu = to_null(row[HETU_IDX])
            avain = hetu.strip().upper() if hetu else None
            if avain in epaonnistuneet:
                continue
            if not hetu:
                rec = {"solki_id": solki_id, "oid": None, "reason": "hetu puuttuu"}
                counts["ilman_hetua"] += 1
            else:
                oid = ratkaistut.get(avain)
                rec = {
                    "solki_id": solki_id, "oid": oid,
                    "reason": None if oid else "ei löytynyt oppijanumerorekisteristä (hetulista)",
                }
            out.write(json.dumps(rec, ensure_ascii=False) + "\n")
            out.flush()
            if rec["oid"] is None:
                capture_unresolved(row, solki_id)
            counts["resolved" if rec["oid"] else "unresolved"] += 1
    log(f"backfill done: {counts}")


def load_oid_map(path):
    """{solki_id: oid_or_None}; a key's presence means the row was already attempted."""
    oid_map = {}
    if path and os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            for line in f:
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if "solki_id" in rec:
                    oid_map[rec["solki_id"]] = rec.get("oid")
    return oid_map


def load_leftover_ids(path):
    """solki_ids already written to the unresolved CSV, so re-runs don't duplicate rows."""
    ids = set()
    if path and os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            text = f.read()
        if text.strip():
            delimiter = resolve_delimiter(text.split("\n", 1)[0], None) or "\t"
            for row in csv.reader(io.StringIO(text), delimiter=delimiter, quotechar='"'):
                if len(row) == len(COLUMNS):
                    ids.add(to_null(row[SUORITUS_ID_IDX]))
    return ids


def backfill_oids(rows, host, tokens, map_path, capture_unresolved, limit, cutoff, sleep,
                  check_hetu=True):
    attempted = load_oid_map(map_path)
    if attempted:
        log(f"backfill resuming: {len(attempted)} rows already attempted will be skipped")
    counts = {"resolved": 0, "unresolved": 0, "skipped_attempted": 0, "has_oid": 0,
              "filtered": 0, "failed": 0, "transient": 0, "cache_hits": 0}
    people = {}
    with open(map_path, "a", encoding="utf-8") as out:
        for i, row in enumerate(rows):
            if limit is not None and i >= limit:
                break
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
            if solki_id in attempted:
                counts["skipped_attempted"] += 1
                continue

            hetu = to_null(row[HETU_IDX])
            etunimet = to_null(row[ETUNIMET_IDX])
            sukunimi = to_null(row[SUKUNIMI_IDX])
            problem = hetu_problem(hetu) if (hetu and check_hetu) else None
            if not (hetu and etunimet and sukunimi):
                rec = {"solki_id": solki_id, "oid": None, "reason": "hetu tai nimet puuttuu"}
            elif problem:
                rec = {"solki_id": solki_id, "oid": None, "reason": f"virheellinen hetu ({problem})"}
            else:
                key = person_key(hetu, etunimet, sukunimi)
                if key in people:
                    oid, reason = people[key]
                    counts["cache_hits"] += 1
                else:
                    try:
                        oid, reason = resolve_oid(host, tokens, hetu, etunimet, sukunimi)
                    except TransientLookupError as e:
                        counts["transient"] += 1
                        log(f"ohitetaan tilapäisesti solki_id={solki_id}: {e}")
                        if sleep:
                            time.sleep(sleep)
                        continue
                    people[key] = (oid, reason)
                    if sleep:
                        time.sleep(sleep)
                rec = {"solki_id": solki_id, "oid": oid, "reason": reason}

            out.write(json.dumps(rec, ensure_ascii=False) + "\n")
            out.flush()
            attempted[solki_id] = rec["oid"]
            if rec["oid"] is None:
                capture_unresolved(row, solki_id)
            counts["resolved" if rec["oid"] else "unresolved"] += 1

            attempted_now = counts["resolved"] + counts["unresolved"]
            if attempted_now % 100 == 0:
                log(f"...{attempted_now} oid lookups done {counts}")
    log(f"backfill done: {counts}")


def load_done(out_path):
    """solki_ids that were really POSTed, so a resume skips them. Only
    action=="posted" counts: a dry run writes ok:true records too, and the
    runbook aims both runs at the same report, so accepting those would make a
    live run skip the whole file and migrate nothing."""
    done = set()
    if out_path and os.path.exists(out_path):
        with open(out_path, encoding="utf-8") as f:
            for line in f:
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    continue
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
             "into --oid-map, POST no suoritukset",
    )
    p.add_argument(
        "--oid-map", metavar="PATH",
        help="JSONL map of solki_id → resolved oid; written by --backfill-oids, "
             "read by the normal run to inject OIDs into OID-less rows",
    )
    p.add_argument(
        "--hetu-batch", action="store_true",
        help="backfill: ratkaise oppijanumerot pelkillä hetuilla erissä, ilman nimivertailua",
    )
    p.add_argument(
        "--no-hetu-check", action="store_true",
        help="backfill: skip the local hetu format/checksum/date check before each lookup",
    )
    p.add_argument(
        "--unresolved-out", metavar="PATH",
        help="write rows that still lack a resolvable oppijanumero to this CSV "
             "(same 30-column headerless layout, reusable as --source later)",
    )
    args = p.parse_args()

    if args.backfill_oids:
        if args.dry_run:
            p.error("--backfill-oids calls the API and cannot be combined with --dry-run")
        if not args.oid_map:
            p.error("--backfill-oids needs --oid-map")

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

    done = load_done(args.out) if not (args.dry_run or args.backfill_oids) else set()
    if done:
        log(f"resuming: {len(done)} rows already recorded ok will be skipped")

    tokens = None
    if not args.dry_run:
        log(f"fetching OAuth token from {token_url}")
        tokens = TokenSource(token_url, args.client_id, args.client_secret)

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
    leftover_writer = csv.writer(leftover_fh, delimiter=delimiter, quotechar='"') if leftover_fh else None
    leftover_seen = load_leftover_ids(args.unresolved_out) if args.unresolved_out else set()

    def capture_unresolved(row, solki_id):
        if leftover_writer is None or solki_id in leftover_seen:
            return
        leftover_writer.writerow(row)
        leftover_fh.flush()
        leftover_seen.add(solki_id)

    if args.backfill_oids:
        try:
            if args.hetu_batch:
                backfill_oids_hetulista(rows, host, tokens, args.oid_map, capture_unresolved,
                                        args.limit, cutoff, args.sleep)
            else:
                backfill_oids(rows, host, tokens, args.oid_map, capture_unresolved, args.limit,
                              cutoff, args.sleep, check_hetu=not args.no_hetu_check)
        except FatalLookupError as e:
            log(f"VIRHE: oppijanumerohaku ei ole käytettävissä, keskeytetään: {e}")
            raise SystemExit(1)
        if leftover_fh:
            leftover_fh.close()
        return

    oid_map = load_oid_map(args.oid_map) if args.oid_map else {}
    if oid_map:
        resolved = sum(1 for oid in oid_map.values() if oid)
        log(f"oid map: {resolved} resolved of {len(oid_map)} attempted rows")

    payloads_fh = open(args.emit_payloads, "w", encoding="utf-8") if args.emit_payloads else None
    counts = {"posted": 0, "skipped_issue": 0, "skipped_done": 0, "failed": 0, "dry": 0, "filtered": 0,
              "unresolved": 0}

    with open(args.out, "a", encoding="utf-8") as out:
        for i, row in enumerate(rows):
            if args.limit is not None and i >= args.limit:
                break
            if not row:
                continue
            if len(row) != len(COLUMNS):
                rec = {"row": i, "ok": False, "error": f"odotettiin {len(COLUMNS)} saraketta, saatiin {len(row)}"}
                out.write(json.dumps(rec, ensure_ascii=False) + "\n")
                counts["failed"] += 1
                continue

            if cutoff is not None:
                lm = parse_last_modified(row[LAST_MODIFIED_IDX])
                if lm is None:
                    rec = {"row": i, "ok": False, "error": "last_modified ei jäsenny, ei voi suodattaa"}
                    out.write(json.dumps(rec, ensure_ascii=False) + "\n")
                    counts["failed"] += 1
                    continue
                if lm >= cutoff:
                    counts["filtered"] += 1
                    continue

            if oid_map and not to_null(row[SUORITTAJAN_OID_IDX]):
                backfilled = oid_map.get(to_null(row[SUORITUS_ID_IDX]))
                if backfilled:
                    row = list(row)
                    row[SUORITTAJAN_OID_IDX] = backfilled

            payload = build_payload(row)
            solki_id = payload["suoritus"]["lahdejarjestelmanId"]["id"]
            issues = local_issues(payload)

            if payloads_fh:
                payloads_fh.write(json.dumps(payload, ensure_ascii=False) + "\n")

            if solki_id in done:
                counts["skipped_done"] += 1
                continue

            if not payload["henkilo"]["oid"]:
                capture_unresolved(row, solki_id)
                rec = {"solki_id": solki_id, "ok": False, "action": "unresolved", "reason": "ei oppijanumeroa"}
                out.write(json.dumps(rec, ensure_ascii=False) + "\n")
                counts["unresolved"] += 1
                continue

            if issues:
                rec = {"solki_id": solki_id, "ok": False, "action": "skipped", "issues": issues}
                out.write(json.dumps(rec, ensure_ascii=False) + "\n")
                counts["skipped_issue"] += 1
                continue

            if args.dry_run:
                out.write(json.dumps({"solki_id": solki_id, "action": "dry-run", "ok": True}, ensure_ascii=False) + "\n")
                counts["dry"] += 1
            else:
                code, resp = post_suoritus(host, tokens, payload)
                ok = 200 <= code < 300
                try:
                    parsed = json.loads(resp)
                except json.JSONDecodeError:
                    parsed = resp
                rec = {"solki_id": solki_id, "ok": ok, "action": "posted", "http": code, "response": parsed}
                out.write(json.dumps(rec, ensure_ascii=False) + "\n")
                counts["posted" if ok else "failed"] += 1
                if args.sleep:
                    time.sleep(args.sleep)

            processed = sum(counts.values())
            if processed % 500 == 0:
                log(f"...{processed} rows processed {counts}")

    if payloads_fh:
        payloads_fh.close()
    if leftover_fh:
        leftover_fh.close()
    log(f"done: {counts}")


if __name__ == "__main__":
    main()