#!/usr/bin/env python3
"""Etsii Solkin YKI-suoritusviennistä rivit, joiden solki_id:tä ei ole kitun kannassa.

Syöte on Solkin 30-sarakkeinen otsikoton vienti (YkiSuoritusCsv-järjestys, sama kuin
duckdb_session.sh:n raw-näkymässä). Suoritus on kannassa, jos sen suoritus_id löytyy
yki_suoritus.solki_id:stä tai karanteenitaulusta yki_historia_siirtymaton. Puuttuvien
suoritusten rivit kirjoitetaan --out-tiedostoon sellaisinaan samalla erottimella, joten
tuloksen voi avata suoraan duckdb_session.sh:lla.

--tutkintopaiva-before rajaa käsittelyn riveihin, joiden tutkintopäivä on ennen annettua
päivää. Raja katsotaan jokaisen rivin omasta tutkintopäivästä; muokkausaikaa
(last_modified) ei käytetä viennin puolella mihinkään. Rajauksen kanssa myös rivit, joiden
tutkintopäivä puuttuu tai ei jäsenny, jäävät pois.

Lopuksi tulostetaan vuosittainen täsmäytys kitun tilastosivun lukuihin. Viennin suoritus
lasketaan sen rajauksen sisälle jäävien rivien myöhäisimmän tutkintopäivän vuodelle. Kitun
luku on sama kuin tilastosivulla: jokaisesta solki_id:stä viimeisin versio (last_modified
DESC, id DESC) ja sen tutkintopäivän vuosi, samalla rajauksella. Taulukon sarakkeille pätee

    ero = puuttuu + karanteenissa + vuosi_eri_kitussa - vuosi_eri_viennissä - vain_kitussa

eli jokainen vuosittainen ero selittyy jollakin näistä. vuosi_eri-sarakkeisiin osuvat myös
suoritukset, jotka ovat toisella puolella rajausta kuin toisessa aineistossa.

Kantaan otetaan yhteys Auroran RDS Data API:n kautta, ja skripti ajaa vain SELECTejä.
Ajetaan AWS CloudShellissä siinä tilissä joka omistaa datan, koska vienti sisältää
henkilötietoja eikä sen kuulu poistua AWS:stä:

    python3 yki_puuttuvat_suoritukset.py --source s3://kitu-yki-historia-upload-prod/<avain>.csv \\
        --tutkintopaiva-before 2026-04-01

Klusterin ja tunnussalaisuuden ARN:t haetaan automaattisesti, ellei niitä anneta
(--cluster-arn, --secret-arn).
"""

import argparse
import collections
import csv
import io
import sys
from datetime import date

COLUMNS = [
    "suorittajan_oid", "hetu", "sukupuoli", "sukunimi", "etunimet", "kansalaisuus",
    "katuosoite", "postinumero", "postitoimipaikka", "email", "suoritus_id",
    "last_modified", "tutkintopaiva", "tutkintokieli", "tutkintotaso",
    "jarjestajan_oid", "jarjestajan_nimi", "arviointipaiva", "as_ty", "as_ki",
    "as_rs", "as_py", "as_pu", "as_yl", "tark_saapumis_pvm", "tark_asiatunnus",
    "tark_osakokeet", "arvosana_muuttui", "perustelu", "tark_kasittely_pvm",
]
OID_IDX = COLUMNS.index("suorittajan_oid")
SOLKI_ID_IDX = COLUMNS.index("suoritus_id")
TUTKINTOPAIVA_IDX = COLUMNS.index("tutkintopaiva")
ARVOSANA_IDXS = [COLUMNS.index(c) for c in ("as_ty", "as_ki", "as_rs", "as_py", "as_pu", "as_yl")]

DELIMITERS = {"tab": "\t", "comma": ",", "semicolon": ";", "pipe": "|"}
ID_OSIOT = 16
ESIMERKKEJA = 5
TUNTEMATON = "????"


def log(msg):
    print(msg, file=sys.stderr, flush=True)


def to_null(value):
    if value is None:
        return None
    value = value.strip()
    return None if value in ("", "\\N") else value


def normalize_id(value):
    value = to_null(value)
    if value is None or not (value.isascii() and value.isdigit()):
        return None
    return str(int(value))


def paiva(value):
    value = to_null(value)
    if value is None:
        return None
    try:
        return date.fromisoformat(value[:10])
    except ValueError:
        return None


def jakso(p, raja):
    if p is None:
        return None if raja else TUNTEMATON
    if raja and p >= raja:
        return None
    return str(p.year)


def read_text(source):
    if source.startswith("s3://"):
        import boto3

        bucket, _, key = source[len("s3://"):].partition("/")
        data = boto3.client("s3").get_object(Bucket=bucket, Key=key)["Body"].read()
    else:
        with open(source, "rb") as f:
            data = f.read()
    return data.decode("utf-8-sig")


def resolve_delimiter(first_line, override):
    if override:
        return DELIMITERS.get(override, override)
    for delimiter in DELIMITERS.values():
        if len(next(csv.reader([first_line], delimiter=delimiter, quotechar='"'))) == len(COLUMNS):
            return delimiter
    raise SystemExit(f"VIRHE: erotinta ei tunnistettu ensimmäisestä rivistä ({len(COLUMNS)} saraketta); anna --delimiter")


def yksi(kuvaus, arvot):
    if len(arvot) != 1:
        raise SystemExit(f"VIRHE: {kuvaus}: odotettiin tasan yksi, löytyi {len(arvot)} ({', '.join(arvot)})")
    return arvot[0]


def resolve_arns(args):
    import boto3

    if not args.cluster_arn:
        rds = boto3.client("rds", region_name=args.region)
        args.cluster_arn = yksi("Aurora-klusteri", [
            c["DBClusterArn"]
            for sivu in rds.get_paginator("describe_db_clusters").paginate()
            for c in sivu["DBClusters"]
            if c.get("DatabaseName") == args.database
        ])
    if not args.secret_arn:
        sm = boto3.client("secretsmanager", region_name=args.region)
        args.secret_arn = yksi("kannan tunnussalaisuus", [
            s["ARN"]
            for sivu in sm.get_paginator("list_secrets").paginate()
            for s in sivu["SecretList"]
            if "DbStackSecret" in s["Name"]
        ])


def data_api(args):
    import boto3

    tili = boto3.client("sts", region_name=args.region).get_caller_identity()["Account"]
    resolve_arns(args)
    log(f"tili: {tili}")
    log(f"klusteri: {args.cluster_arn}")
    client = boto3.client("rds-data", region_name=args.region)

    def kysy(sql):
        vastaus = client.execute_statement(
            resourceArn=args.cluster_arn,
            secretArn=args.secret_arn,
            database=args.database,
            sql=sql,
        )
        kentta = vastaus["records"][0][0]
        if kentta.get("isNull"):
            return None
        return kentta["stringValue"] if "stringValue" in kentta else kentta["longValue"]

    return kysy


def hae_kitun_tutkintopaivat(kysy):
    paivat = {}
    for osa in range(ID_OSIOT):
        arvo = kysy(
            "SELECT string_agg(solki_id::text || ':' || to_char(tutkintopaiva, 'YYYY-MM-DD'), ',') FROM ("
            " SELECT DISTINCT ON (solki_id) solki_id, tutkintopaiva"
            f" FROM yki_suoritus WHERE abs(solki_id % {ID_OSIOT}) = {osa}"
            " ORDER BY solki_id, last_modified DESC, id DESC) s"
        )
        for pari in (arvo or "").split(","):
            if pari:
                solki_id, _, p = pari.partition(":")
                paivat[solki_id] = date.fromisoformat(p)
    odotettu = int(kysy("SELECT count(DISTINCT solki_id) FROM yki_suoritus"))
    if len(paivat) != odotettu:
        raise SystemExit(f"VIRHE: kannasta luettiin {len(paivat)} solki_id:tä, kannassa on {odotettu}")
    return paivat


def hae_karanteeni(kysy):
    arvo = kysy("SELECT string_agg(DISTINCT solki_id, ',') FROM yki_historia_siirtymaton WHERE solki_id IS NOT NULL")
    return {i for i in (normalize_id(a) for a in (arvo or "").split(",")) if i}


def rivit(text, delimiter):
    return enumerate(csv.reader(io.StringIO(text), delimiter=delimiter, quotechar='"'), 1)


def lue_suoritukset(text, delimiter, raja):
    tilasto = collections.Counter()
    sarakemaarat = collections.Counter()
    esimerkit = collections.defaultdict(list)
    nahdyt = set()
    mukana = {}

    for numero, row in rivit(text, delimiter):
        if not row:
            tilasto["tyhjia"] += 1
            continue
        tilasto["rivit"] += 1
        if len(row) != len(COLUMNS):
            sarakemaarat[len(row)] += 1
            esimerkit["sarakemaara"].append(numero)
            continue
        solki_id = normalize_id(row[SOLKI_ID_IDX])
        if solki_id is None:
            tilasto["virheellinen_id"] += 1
            esimerkit["id"].append(numero)
            continue

        if solki_id in nahdyt:
            tilasto["toistuvat"] += 1
        nahdyt.add(solki_id)
        p = paiva(row[TUTKINTOPAIVA_IDX])
        j = jakso(p, raja)
        if j is None:
            tilasto["rajattu_ilman_paivaa" if p is None else "rajattu_rajan_yli"] += 1
            continue
        avain = (p or date.min, numero)
        if solki_id not in mukana or avain >= mukana[solki_id]["avain"]:
            mukana[solki_id] = {
                "avain": avain,
                "jakso": j,
                "tutkintopaiva": p,
                "ei_oppijanumeroa": to_null(row[OID_IDX]) is None,
                "ei_arvosanoja": all(to_null(row[i]) is None for i in ARVOSANA_IDXS),
            }

    return tilasto, sarakemaarat, esimerkit, nahdyt, mukana


def kirjoita_puuttuvat(text, delimiter, raja, puuttuvat, out):
    writer = csv.writer(out, delimiter=delimiter, quotechar='"', lineterminator="\n")
    kirjoitetut = 0
    for _, row in rivit(text, delimiter):
        if (
            len(row) == len(COLUMNS)
            and normalize_id(row[SOLKI_ID_IDX]) in puuttuvat
            and jakso(paiva(row[TUTKINTOPAIVA_IDX]), raja) is not None
        ):
            writer.writerow(row)
            kirjoitetut += 1
    return kirjoitetut


def taulukko(otsikot, rivit):
    leveydet = [max(len(str(x)) for x in sarake) for sarake in zip(otsikot, *rivit)]
    for rivi in [otsikot, ["-" * w for w in leveydet], *rivit]:
        print("  ".join(str(x).rjust(w) if i else str(x).ljust(w) for i, (x, w) in enumerate(zip(rivi, leveydet))))


def yhteensa(rivit, nimi="yht."):
    return [nimi, *(sum(r[i] for r in rivit) for i in range(1, len(rivit[0])))] if rivit else []


def puuttuvien_jako(otsake, ryhma, puuttuvat):
    jako = collections.defaultdict(lambda: [0, 0, 0, 0])
    for x in puuttuvat:
        m = jako[ryhma(x)]
        m[0] += 1
        m[1] += x["ei_oppijanumeroa"]
        m[2] += x["ei_arvosanoja"]
        m[3] += not x["ei_oppijanumeroa"] and not x["ei_arvosanoja"]
    taulu = [[k, *m] for k, m in sorted(jako.items())]
    taulukko([otsake, "puuttuu", "ilman_oppijanumeroa", "ilman_arvosanoja", "muut"], taulu + [yhteensa(taulu)])


def raportoi(args, raja, delimiter, kitu, karanteeni, luku, puuttuvat, kirjoitetut):
    tilasto, sarakemaarat, esimerkit, nahdyt, mukana = luku
    vienti_jakso = {i: (mukana[i]["jakso"] if i in mukana else None) for i in nahdyt}
    kitu_jakso = {i: jakso(p, raja) for i, p in kitu.items()}
    nimi = {d: n for n, d in DELIMITERS.items()}.get(delimiter, repr(delimiter))

    print()
    print(f"Lähde:               {args.source} (erotin {nimi})")
    if raja:
        print(f"Rajaus:              tutkintopäivä ennen {raja}")
        kitu_mukana = sum(1 for j in kitu_jakso.values() if j is not None)
        print(f"Kitun kanta:         {len(kitu)} suoritusta (rajauksen sisällä {kitu_mukana}), karanteenissa {len(karanteeni)}")
    else:
        print(f"Kitun kanta:         {len(kitu)} suoritusta, karanteenissa {len(karanteeni)}")
    print(f"Lähteen rivit:       {tilasto['rivit']}" + (f" (+ {tilasto['tyhjia']} tyhjää)" if tilasto["tyhjia"] else ""))
    if sarakemaarat:
        jakauma = ", ".join(f"{n} saraketta: {k}" for n, k in sorted(sarakemaarat.items()))
        print(f"  väärä sarakemäärä: {sum(sarakemaarat.values())} ({jakauma}; rivit {esimerkit['sarakemaara'][:ESIMERKKEJA]})")
    if tilasto["virheellinen_id"]:
        print(f"  ei solki_id:tä:    {tilasto['virheellinen_id']} (rivit {esimerkit['id'][:ESIMERKKEJA]})")
    if raja:
        rajattu = tilasto["rajattu_rajan_yli"] + tilasto["rajattu_ilman_paivaa"]
        print(f"  rajattu pois:      {rajattu} (tutkintopäivä {raja} tai myöhemmin: {tilasto['rajattu_rajan_yli']}, "
              f"tutkintopäivä puuttuu tai ei jäsenny: {tilasto['rajattu_ilman_paivaa']})")
    print(f"Suorituksia:         {len(nahdyt)} eri solki_id:tä ({tilasto['toistuvat']} riviä toistaa aiempaa solki_id:tä)")
    if raja:
        print(f"  rajauksen sisällä:  {len(mukana)}")
    print(f"  löytyi kitusta:     {sum(1 for i in mukana if i in kitu)}")
    print(f"  vain karanteenissa: {sum(1 for i in mukana if i not in kitu and i in karanteeni)}")
    print(f"  puuttuu kannasta:   {len(puuttuvat)} ({kirjoitetut} riviä → {args.out})")

    vuodet = sorted({x["jakso"] for x in mukana.values()})
    kitun_maarat = collections.Counter(j for j in kitu_jakso.values() if j is not None)
    taulu = []
    for v in vuodet:
        viennissa = [i for i, x in mukana.items() if x["jakso"] == v]
        taulu.append([
            v,
            len(viennissa),
            kitun_maarat[v],
            len(viennissa) - kitun_maarat[v],
            sum(1 for i in viennissa if i in puuttuvat),
            sum(1 for i in viennissa if i not in kitu and i in karanteeni),
            sum(1 for i in viennissa if i in kitu and kitu_jakso[i] != v),
            sum(1 for i, j in kitu_jakso.items() if j == v and i in nahdyt and vienti_jakso[i] != v),
            sum(1 for i, j in kitu_jakso.items() if j == v and i not in nahdyt),
        ])
    print()
    print("Täsmäytys kitun tilastosivuun (eri solki_id:t tutkintovuosittain):")
    taulukko(
        ["vuosi", "viennissä", "kitussa", "ero", "puuttuu", "karanteenissa",
         "vuosi_eri_kitussa", "vuosi_eri_viennissä", "vain_kitussa"],
        taulu + [yhteensa(taulu)],
    )

    if not puuttuvat:
        return
    p = [mukana[i] for i in puuttuvat]
    print()
    print("Puuttuvat tutkintovuosittain (sama rivi voi olla sekä ilman oppijanumeroa että ilman arvosanoja):")
    puuttuvien_jako("vuosi", lambda x: x["jakso"], p)
    print()
    print("Puuttuvat tutkintopäivän kuukauden mukaan:")
    puuttuvien_jako("kuukausi", lambda x: x["tutkintopaiva"].strftime("%Y-%m") if x["tutkintopaiva"] else TUNTEMATON, p)


def main(argv=None, kysy=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--source", required=True, help="Solkin vienti, s3://... tai paikallinen polku")
    parser.add_argument("--out", default="puuttuvat_suoritukset.csv", help="puuttuvien rivien tiedosto")
    parser.add_argument(
        "--tutkintopaiva-before", metavar="YYYY-MM-DD", type=date.fromisoformat,
        help="käsittele vain rivit joiden tutkintopäivä on ennen tätä päivää",
    )
    parser.add_argument("--delimiter", help="tab|comma|semicolon|pipe tai merkki; oletus tunnistetaan")
    parser.add_argument("--cluster-arn", help="Aurora-klusterin ARN; oletus haetaan")
    parser.add_argument("--secret-arn", help="klusterin tunnussalaisuuden ARN; oletus haetaan")
    parser.add_argument("--database", default="kios")
    parser.add_argument("--region", default="eu-west-1")
    args = parser.parse_args(argv)
    raja = args.tutkintopaiva_before

    log(f"luetaan {args.source}")
    text = read_text(args.source)
    delimiter = resolve_delimiter(text.split("\n", 1)[0], args.delimiter)
    luku = lue_suoritukset(text, delimiter, raja)
    mukana = luku[4]

    kysy = kysy or data_api(args)
    log("haetaan kitun solki_id:t")
    kitu = hae_kitun_tutkintopaivat(kysy)
    karanteeni = hae_karanteeni(kysy)

    puuttuvat = {i for i in mukana if i not in kitu and i not in karanteeni}
    log(f"kirjoitetaan puuttuvat → {args.out}")
    with open(args.out, "w", encoding="utf-8", newline="") as out:
        kirjoitetut = kirjoita_puuttuvat(text, delimiter, raja, puuttuvat, out)

    raportoi(args, raja, delimiter, kitu, karanteeni, luku, puuttuvat, kirjoitetut)
    return 0


if __name__ == "__main__":
    sys.exit(main())
