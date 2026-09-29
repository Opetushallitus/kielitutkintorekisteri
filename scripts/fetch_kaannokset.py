#!/usr/bin/env python3
"""Hakee UI-tekstien kaannokset OPH:n lokalisointiproxysta repon lahdetiedostoihin.

Lahdeteksti asuu tiedostossa server/src/main/resources/lokalisointi/fi.json, ei
enaa UiText.kt:n literaaleissa. Tama skripti tuo kaantajien Tolgeessa tekemat
muutokset takaisin repoon, jotta koodi ja kayttoliittyma puhuvat samaa kielta ja
jotta offline-profiilit (test, e2e, local-opintopolku) renderoivat oikeat tekstit.

    python3 scripts/fetch_kaannokset.py              # hae ja kirjoita
    python3 scripts/fetch_kaannokset.py --check      # kerro vanhentuneisuus, ala kirjoita

Oletuslahde on testiopintopolku (QA). Se on ainoa ymparisto johon kitun
Tolgee-synkronointi tyontaa avaimet (TOLGEE_API_KEY injektoidaan vain QA-taskiin),
ja ainoa joka julkaisee ruotsin ja englannin. Untuva palauttaa sv/en-tiedostoihin
404:n, joten sita vasten ajettuna skripti keskeyttaa eika kirjoita mitaan.

Avainjoukon omistaa fi.json: uuden avaimen kirjoittaa kehittaja, ei tama skripti.
Tolgeessa voi olla avaimia joita koodissa ei ole (orpoja, joita poistokatto ei ole
viela siivonnut) - niita ei tuoda, koska UiTextTest vaatii etta fi.json ja UiText
vastaavat toisiaan tasmalleen.

Ei CI:ssa. --check on kehittajan apu; kaantajat muuttavat Tolgeeta jatkuvasti,
joten pakollisena tarkistuksena se punertaisi buildeja PR:aan liittymattomista syista.
"""

import argparse
import json
import pathlib
import sys
import urllib.error
import urllib.request

OLETUS_HOSTNAME = "virkailija.testiopintopolku.fi"
OLETUS_NAMESPACE = "kielitutkintorekisteri"
OLETUS_KIELET = ["fi", "sv", "en"]
OLETUS_HAKEMISTO = "server/src/main/resources/lokalisointi"


def log(viesti):
    print(viesti, file=sys.stderr, flush=True)


def hae_kieli(hostname, namespace, kieli, timeout):
    """Hakee yhden kielen litteana {avain: teksti} -karttana."""
    url = f"https://{hostname}/lokalisointi/tolgee/{namespace}/{kieli}.json"
    try:
        with urllib.request.urlopen(url, timeout=timeout) as vastaus:
            # Proxy tarjoilee application/octet-stream, joten dekoodataan itse.
            data = json.loads(vastaus.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        raise SystemExit(f"VIRHE: {url} vastasi {e.code}. Mitaan ei kirjoitettu.")
    except urllib.error.URLError as e:
        raise SystemExit(f"VIRHE: {url} ei vastannut ({e.reason}). Mitaan ei kirjoitettu.")
    if not isinstance(data, dict) or not all(
        isinstance(k, str) and isinstance(v, str) for k, v in data.items()
    ):
        raise SystemExit(f"VIRHE: {url} ei ole littea merkkijonokartta. Mitaan ei kirjoitettu.")
    return data


def yhdista_fi(paikallinen, tolgee):
    """Tolgee voittaa tuntemansa avaimet; vain paikallisesti olevat sailyvat."""
    return {avain: tolgee.get(avain, arvo) for avain, arvo in paikallinen.items()}


def suodata_kieli(tolgee, avaimet):
    """sv/en tulevat kokonaan Tolgeesta, mutta vain fi.json:n tuntemille avaimille."""
    return {avain: teksti for avain, teksti in tolgee.items() if avain in avaimet}


def serialisoi(kartta):
    """Prettierin kanssa tavulleen yhteensopiva muoto."""
    return json.dumps(kartta, indent=2, ensure_ascii=False, sort_keys=True) + "\n"


def lue(polku):
    if not polku.exists():
        return {}
    return json.loads(polku.read_text(encoding="utf-8"))


def kirjoita(polku, kartta):
    polku.parent.mkdir(parents=True, exist_ok=True)
    with open(polku, "w", encoding="utf-8", newline="\n") as f:
        f.write(serialisoi(kartta))


def yhteenveto(kieli, vanha, uusi, tolgee):
    muuttuneet = sorted(a for a in vanha.keys() & uusi.keys() if vanha[a] != uusi[a])
    lisatyt = sorted(uusi.keys() - vanha.keys())
    poistuneet = sorted(vanha.keys() - uusi.keys())
    vain_tolgeessa = sorted(tolgee.keys() - uusi.keys())
    log(
        f"  {kieli}: {len(uusi)} avainta "
        f"(+{len(lisatyt)} -{len(poistuneet)} ~{len(muuttuneet)})"
    )
    for avain in muuttuneet:
        log(f"      ~ {avain}: {vanha[avain]!r} -> {uusi[avain]!r}")
    for avain in lisatyt:
        log(f"      + {avain}: {uusi[avain]!r}")
    for avain in poistuneet:
        log(f"      - {avain}")
    if vain_tolgeessa:
        log(
            f"      Tolgeessa {len(vain_tolgeessa)} avainta joita fi.json ei tunne, "
            f"ei tuotu: {', '.join(vain_tolgeessa)}"
        )


def main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--hostname", default=OLETUS_HOSTNAME)
    parser.add_argument("--namespace", default=OLETUS_NAMESPACE)
    parser.add_argument("--languages", nargs="+", default=OLETUS_KIELET)
    parser.add_argument("--out-dir", default=None)
    parser.add_argument("--timeout", type=int, default=30)
    parser.add_argument(
        "--check",
        action="store_true",
        help="ala kirjoita; poistu koodilla 1 jos tiedostot olisivat muuttuneet",
    )
    args = parser.parse_args()

    juuri = pathlib.Path(__file__).resolve().parents[1]
    hakemisto = pathlib.Path(args.out_dir) if args.out_dir else juuri / OLETUS_HAKEMISTO

    fi_polku = hakemisto / "fi.json"
    paikallinen_fi = lue(fi_polku)
    if not paikallinen_fi:
        raise SystemExit(
            f"VIRHE: {fi_polku} puuttuu tai on tyhja. Lahdetekstit omistavat avainjoukon, "
            "joten skripti ei voi luoda niita tyhjasta."
        )

    log(f"Haetaan {args.hostname} / {args.namespace}: {', '.join(args.languages)}")
    tolgee = {
        kieli: hae_kieli(args.hostname, args.namespace, kieli, args.timeout)
        for kieli in args.languages
    }

    uudet = {}
    if "fi" in tolgee:
        uudet["fi"] = yhdista_fi(paikallinen_fi, tolgee["fi"])
    avaimet = set(uudet.get("fi", paikallinen_fi))
    for kieli in args.languages:
        if kieli != "fi":
            uudet[kieli] = suodata_kieli(tolgee[kieli], avaimet)

    vanhentunut = False
    for kieli in args.languages:
        polku = hakemisto / f"{kieli}.json"
        vanha = lue(polku)
        yhteenveto(kieli, vanha, uudet[kieli], tolgee[kieli])
        if serialisoi(vanha) != serialisoi(uudet[kieli]):
            vanhentunut = True
            if not args.check:
                kirjoita(polku, uudet[kieli])

    if args.check:
        log("Lahdetiedostot ovat vanhentuneet." if vanhentunut else "Lahdetiedostot ovat ajan tasalla.")
        return 1 if vanhentunut else 0

    log("Valmis." if vanhentunut else "Ei muutoksia.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
