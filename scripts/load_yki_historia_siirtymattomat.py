#!/usr/bin/env python3
"""Lataa YKI-historiamigraation siirtymatta jaaneet rivit kitun karanteenitauluun.

Syote on migrate_yki_historia.py:n --failed-out-tiedosto: lahteen 30 saraketta
sellaisenaan + syy 31. sarakkeena. Kohde on taulu yki_historia_siirtymaton (V127),
johon kirjoitetaan Auroran RDS Data API:n kautta - ei tunnelia, ei uutta rajapintaa.

Ajetaan AWS CloudShellista siina tilissa joka omistaa datan: tiedosto sisaltaa
henkilotunnuksia, nimia ja osoitteita eika sen kuulu poistua AWS:sta.

    ./load_yki_historia_siirtymattomat.py --source failed_final.csv \
        --lahdetiedosto yki-historia-2.csv --oid-map oid_map_yhdistetty.jsonl \
        --cluster-arn "$CLUSTER_ARN" --secret-arn "$SECRET_ARN" --confirm-prod

ARN:t loytyvat nain:

    aws rds describe-db-clusters --query "DBClusters[?DatabaseName=='kios'].DBClusterArn" --output text
    aws secretsmanager list-secrets --query "SecretList[?contains(Name,'DbStackSecret')].ARN" --output text

Lataus on toistettavissa: rivit paivitetaan solki_id:n perusteella (ON CONFLICT).

Uusintakierroksella --prune poistaa lopuksi ne saman --lahdetiedoston rivit jotka
puuttuvat uudesta syotteesta, eli onnistuneesti siirtyneet. Ilman sita taulu jaa
nayttamaan siirtymattomina rivit jotka ovat jo rekisterissa. --lahdetiedoston on
oltava sama kuin alkuperaisessa latauksessa, muuten prune ei osu mihinkaan.
"""

import argparse
import collections
import csv
import io
import json
import os
import sys

# Lahdetiedoston sarakkeet YkiSuoritusCsv-jarjestyksessa. Sama jarjestys kuin
# migrate_yki_historia.py:n COLUMNS, mutta suoritus_id on nimetty solki_id:ksi
# kuten kohdetaulussa.
COLUMNS = [
    "suorittajan_oid", "hetu", "sukupuoli", "sukunimi", "etunimet", "kansalaisuus",
    "katuosoite", "postinumero", "postitoimipaikka", "email", "solki_id",
    "last_modified", "tutkintopaiva", "tutkintokieli", "tutkintotaso",
    "jarjestajan_oid", "jarjestajan_nimi", "arviointipaiva", "as_ty", "as_ki",
    "as_rs", "as_py", "as_pu", "as_yl", "tark_saapumis_pvm", "tark_asiatunnus",
    "tark_osakokeet", "arvosana_muuttui", "perustelu", "tark_kasittely_pvm",
]
SOLKI_ID_IDX = COLUMNS.index("solki_id")
EXPECTED_FIELDS = len(COLUMNS) + 1  # 30 lahdesaraketta + syy

DB_COLUMNS = COLUMNS + ["syy", "syyluokka", "oid_haun_syy", "raw_rivi", "lahdetiedosto"]

# Syyluokat vastaavat tietokannan tyyppia yki_historia_siirtymattomyyden_syy.
PAIKALLISEN_TARKISTUKSEN_TUNNISTEET = (
    "ei yhtään osakoetta",
    "virheellinen arvosana tasolle",
    "arvosanaMuuttui ei ole",
)

DELIMITER_ALIASES = {"tab": "\t", "comma": ",", "semicolon": ";", "pipe": "|"}


def log(msg):
    print(msg, file=sys.stderr, flush=True)


def to_null(value):
    """Tyhja, pelkka valilyonti ja MySQL:n \\N-sentinelli ovat kaikki NULL."""
    if value is None:
        return None
    stripped = value.strip().strip("\t")
    if stripped in ("", "\\N"):
        return None
    return stripped


def resolve_delimiter(first_line, override):
    if override:
        return DELIMITER_ALIASES.get(override, override)
    for candidate in ("\t", ",", ";", "|"):
        if len(next(csv.reader([first_line], delimiter=candidate, quotechar='"'))) == EXPECTED_FIELDS:
            return candidate
    return None


def classify(syy):
    """Migraatioskriptin vapaa syyteksti -> syyluokka."""
    s = (syy or "").strip()
    if not s:
        return "MUU"
    if s == "ei oppijanumeroa":
        return "EI_OPPIJANUMEROA"
    if s.startswith("odotettiin ") and "saraketta" in s:
        return "RIKKINAINEN_RIVI"
    if s.startswith("last_modified ei jäsenny"):
        return "LAST_MODIFIED_EI_JASENNY"
    if s.startswith("HTTP "):
        return "API_HYLKASI"
    if any(tunniste in s for tunniste in PAIKALLISEN_TARKISTUKSEN_TUNNISTEET):
        return "PAIKALLINEN_VALIDOINTI"
    return "MUU"


def load_oid_map(path):
    """{solki_id: {"oid": ..., "reason": ...}}; avaimen olemassaolo = hakua yritettiin."""
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


def raw_line(row, delimiter):
    """Rivi takaisin CSV-muotoon, jotta rikkinainen rivi saadaan talteen sellaisenaan."""
    buf = io.StringIO()
    csv.writer(buf, delimiter=delimiter, quotechar='"', lineterminator="").writerow(row)
    return buf.getvalue()


def parse_row(row, lahdetiedosto, oid_map, delimiter):
    """Yksi CSV-rivi -> sarakenimi->arvo-sanakirja. Ei koskaan kaadu rivin muotoon."""
    syy = (row[-1] if row else "") or ""
    values = {column: None for column in DB_COLUMNS}
    values["syy"] = syy.strip()
    values["lahdetiedosto"] = lahdetiedosto

    if len(row) != EXPECTED_FIELDS:
        # Sarakemaara on vaarin, joten paikkoihin ei voi luottaa: rivi talletetaan
        # kokonaisena ja tunnistekentat jaavat tyhjiksi.
        values["syyluokka"] = "RIKKINAINEN_RIVI"
        values["raw_rivi"] = raw_line(row, delimiter)
        return values

    for column, value in zip(COLUMNS, row[: len(COLUMNS)]):
        values[column] = to_null(value)
    values["syyluokka"] = classify(syy)

    kartta = oid_map.get(values["solki_id"])
    if kartta is not None:
        values["oid_haun_syy"] = kartta.get("reason")
    return values


def read_rows(text, delimiter):
    return [row for row in csv.reader(io.StringIO(text), delimiter=delimiter, quotechar='"') if row]


def insert_sql():
    columns = ", ".join(DB_COLUMNS)
    placeholders = ", ".join(
        # Data API ei tunne enum-tyyppeja, joten syyluokka castataan. CAST(:x AS t) on
        # varmempi kuin :x::t - kaksoiskaksoispiste sotkee parametrien jasennyksen.
        f"CAST(:{column} AS yki_historia_siirtymattomyyden_syy)" if column == "syyluokka" else f":{column}"
        for column in DB_COLUMNS
    )
    updates = ", ".join(
        f"{column} = EXCLUDED.{column}" for column in DB_COLUMNS if column != "solki_id"
    )
    return (
        f"INSERT INTO yki_historia_siirtymaton ({columns}) VALUES ({placeholders}) "
        f"ON CONFLICT (solki_id) DO UPDATE SET {updates}, ladattu = now()"
    )


def prune_sql():
    """Poistaa riveja jotka eivat enaa kuulu karanteeniin.

    Jokainen tassa ajossa kirjoitettu rivi saa tuoreen ladattu-arvon (uusi rivi
    sarakkeen oletuksesta, paivitetty ON CONFLICT -lauseesta), joten rajan alle
    jaavat tasan ne jotka puuttuvat uudesta --failed-out-tiedostosta: onnistuneesti
    siirtyneet. Aikaraja on kannan omasta kellosta, ei CloudShellin.
    """
    return (
        "DELETE FROM yki_historia_siirtymaton "
        "WHERE lahdetiedosto = :lahdetiedosto AND ladattu < CAST(:ajon_alku AS timestamptz)"
    )


def run_start_timestamp(client, args):
    vastaus = client.execute_statement(
        resourceArn=args.cluster_arn,
        secretArn=args.secret_arn,
        database=args.database,
        sql="SELECT CAST(now() AS text)",
    )
    return vastaus["records"][0][0]["stringValue"]


def prune_vanhentuneet(client, args, lahdetiedosto, ajon_alku):
    vastaus = client.execute_statement(
        resourceArn=args.cluster_arn,
        secretArn=args.secret_arn,
        database=args.database,
        sql=prune_sql(),
        parameters=[
            {"name": "lahdetiedosto", "value": {"stringValue": lahdetiedosto}},
            {"name": "ajon_alku", "value": {"stringValue": ajon_alku}},
        ],
    )
    return vastaus.get("numberOfRecordsUpdated", 0)


def to_parameter_set(values):
    return [
        {"name": column, "value": {"isNull": True} if values[column] is None else {"stringValue": values[column]}}
        for column in DB_COLUMNS
    ]


def chunked(items, size):
    for start in range(0, len(items), size):
        yield items[start : start + size]


def funnel(parsed, oid_map):
    """Luokkajakauma + oppijanumerohaun tila, jotka ovat koko latauksen tarkistusluvut.

    Kolme oppijanumerolukua erotellaan tarkoituksella: kartasta puuttuva rivi on eri asia
    kuin haettu-ja-hylatty, ja kartassa jo ratkennut rivi tarkoittaa ettei karttaa kaytetty
    migraatioajossa lainkaan.
    """
    counts = collections.Counter(row["syyluokka"] for row in parsed)
    ilman_oppijanumeroa = [row for row in parsed if row["syyluokka"] == "EI_OPPIJANUMEROA"]
    return {
        "rivit": len(parsed),
        **{luokka.lower(): maara for luokka, maara in sorted(counts.items())},
        "ilman_solki_id": sum(1 for row in parsed if not row["solki_id"]),
        "oid_hakua_ei_yritetty": sum(1 for row in ilman_oppijanumeroa if row["solki_id"] not in oid_map),
        "oid_haku_epaonnistui": sum(1 for row in ilman_oppijanumeroa if row["oid_haun_syy"] is not None),
        "oid_ratkennut_kartassa": sum(
            1 for row in ilman_oppijanumeroa if (oid_map.get(row["solki_id"]) or {}).get("oid")
        ),
    }


def read_text(source):
    if source.startswith("s3://"):
        import boto3  # vain S3-lahteelle; paikallinen dry-run ei tarvitse boto3:a

        bucket, _, key = source[len("s3://") :].partition("/")
        return boto3.client("s3").get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8")
    with open(source, encoding="utf-8") as f:
        return f.read()


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--source", required=True, help="migraatioajon --failed-out-tiedosto (tai s3://...)")
    parser.add_argument("--lahdetiedosto", help="alkuperaisen lahdeaineiston nimi; oletus --source-tiedoston nimi")
    parser.add_argument("--oid-map", help="oppijanumerohaun kartta, taydentaa oid_haun_syy-sarakkeen")
    parser.add_argument("--cluster-arn", help="Aurora-klusterin ARN")
    parser.add_argument("--secret-arn", help="klusterin tunnussalaisuuden ARN")
    parser.add_argument("--database", default="kios")
    parser.add_argument("--region", default="eu-west-1")
    parser.add_argument("--batch-size", type=int, default=200)
    parser.add_argument("--limit", type=int, help="lataa vain N ensimmaista rivia (savutesti)")
    parser.add_argument("--delimiter", help="tab|comma|semicolon|pipe tai merkki; oletus tunnistetaan")
    parser.add_argument("--dry-run", action="store_true", help="jasenna ja luokittele, ei kirjoituksia")
    parser.add_argument("--confirm-prod", action="store_true", help="vaaditaan ennen kirjoituksia")
    parser.add_argument(
        "--prune",
        action="store_true",
        help="poista lopuksi taman lahdetiedoston rivit jotka puuttuvat syotteesta (siirtyneet)",
    )
    args = parser.parse_args()

    text = read_text(args.source)
    if not text.strip():
        log("lahde on tyhja, ei ladattavaa")
        if args.prune:
            # Tyhja syote tarkoittaisi "poista kaikki", mika on liian iso seuraus
            # tyhjalle tiedostolle - esim. katkennut migraatioajo tuottaa samanlaisen.
            log(
                "VAROITUS: --prune ohitettiin koska lahde on tyhja. Jos jokainen rivi todella "
                f"siirtyi, poista jaannokset kasin: DELETE FROM yki_historia_siirtymaton WHERE "
                f"lahdetiedosto = '{args.lahdetiedosto or os.path.basename(args.source)}';"
            )
        return 0

    delimiter = resolve_delimiter(text.split("\n", 1)[0], args.delimiter)
    if delimiter is None:
        log(
            f"VIRHE: en tunnistanut erotinta joka antaisi {EXPECTED_FIELDS} saraketta. "
            "Anna --delimiter, tai tarkista ettei tiedosto ole --unresolved-out "
            f"({len(COLUMNS)} saraketta, ei syyta)."
        )
        return 2

    rows = read_rows(text, delimiter)
    if args.limit:
        rows = rows[: args.limit]

    lahdetiedosto = args.lahdetiedosto or os.path.basename(args.source)
    oid_map = load_oid_map(args.oid_map)
    parsed = [parse_row(row, lahdetiedosto, oid_map, delimiter) for row in rows]

    tarkistusluvut = funnel(parsed, oid_map)
    ratkennut_kartassa = tarkistusluvut["oid_ratkennut_kartassa"]

    log(f"lahde: {args.source} (erotin {delimiter!r}), lahdetiedosto: {lahdetiedosto}")
    log(f"funnel: {tarkistusluvut}")
    if ratkennut_kartassa:
        log(
            f"VAROITUS: {ratkennut_kartassa} riville loytyy oppijanumero oid-kartasta, vaikka "
            "migraatioajo kirjasi ne oppijanumerottomiksi. Kartta ei siis ollut kaytossa ajossa - "
            "aja migraatio uudelleen --oid-mapilla ennen kuin nama merkitaan aukoksi."
        )
    if oid_map:
        log(f"oid-kartta: {len(oid_map)} riviä, joista oppijanumero {sum(1 for v in oid_map.values() if v['oid'])}")

    if args.dry_run:
        log(f"dry-run: ei kirjoitettu. Esimerkkirivi: {json.dumps(parsed[0], ensure_ascii=False)}")
        if args.prune:
            log(
                f"dry-run: --prune poistaisi lahdetiedoston {lahdetiedosto} rivit jotka eivat ole "
                f"naiden {len(parsed)} joukossa. Dry-run ei ota yhteytta kantaan, joten maaraa ei "
                "voi nayttaa etukateen."
            )
        return 0

    if not args.confirm_prod:
        log("VIRHE: kirjoitus vaatii --confirm-prod")
        return 2
    if not args.cluster_arn or not args.secret_arn:
        log("VIRHE: --cluster-arn ja --secret-arn vaaditaan")
        return 2

    import boto3

    client = boto3.client("rds-data", region_name=args.region)
    sql = insert_sql()
    ajon_alku = run_start_timestamp(client, args) if args.prune else None

    rikkinaisia = sum(1 for row in parsed if not row["solki_id"])
    if rikkinaisia:
        # Naita ei voi paivittaa ON CONFLICTilla (NULL ei ole ristiriidassa itsensa
        # kanssa), joten saman lahdetiedoston tunnisteettomat rivit poistetaan ensin,
        # jotta uudelleenajo ei monista niita.
        client.execute_statement(
            resourceArn=args.cluster_arn,
            secretArn=args.secret_arn,
            database=args.database,
            sql=(
                "DELETE FROM yki_historia_siirtymaton "
                "WHERE solki_id IS NULL AND lahdetiedosto = :lahdetiedosto"
            ),
            parameters=[{"name": "lahdetiedosto", "value": {"stringValue": lahdetiedosto}}],
        )
        log(f"poistettu aiemmat tunnisteettomat rivit lahteesta {lahdetiedosto} ({rikkinaisia} tulossa)")

    kirjoitettu = 0
    for batch in chunked(parsed, args.batch_size):
        client.batch_execute_statement(
            resourceArn=args.cluster_arn,
            secretArn=args.secret_arn,
            database=args.database,
            sql=sql,
            parameterSets=[to_parameter_set(values) for values in batch],
        )
        kirjoitettu += len(batch)
        log(f"...{kirjoitettu}/{len(parsed)} riviä kirjoitettu")

    if args.prune:
        # Vasta tassa: jokainen era on mennyt lapi, joten rajan alle jaavat rivit ovat
        # tosiaan poissa syotteesta eivatka vain viela kirjoittamatta.
        if kirjoitettu != len(parsed):
            log(f"VIRHE: --prune ohitettiin, kirjoitettuja {kirjoitettu}/{len(parsed)}")
            return 1
        poistettu = prune_vanhentuneet(client, args, lahdetiedosto, ajon_alku)
        log(f"prune: poistettu {poistettu} riviä jotka eivät enää ole siirtymättä")

    log(f"done: {kirjoitettu} riviä tauluun yki_historia_siirtymaton")
    return 0


if __name__ == "__main__":
    sys.exit(main())
