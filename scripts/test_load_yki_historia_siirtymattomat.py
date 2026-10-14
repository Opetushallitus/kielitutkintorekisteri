#!/usr/bin/env python3
"""Yksikkotestit load_yki_historia_siirtymattomat.py:lle.

Ei CI:ssa (kuten ei muillakaan scripts/-hakemiston skripteilla). Aja kasin:

    python3 scripts/test_load_yki_historia_siirtymattomat.py
"""

import json
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import load_yki_historia_siirtymattomat as loader  # noqa: E402


def rivi(**yliajot):
    """31-sarakkeinen rivi: 30 lahdesaraketta + syy."""
    arvot = ["\\N"] * len(loader.COLUMNS)
    arvot[loader.COLUMNS.index("solki_id")] = "12345"
    arvot[loader.COLUMNS.index("sukunimi")] = "Testaaja"
    arvot[loader.COLUMNS.index("tutkintopaiva")] = "2016-05-14"
    for nimi, arvo in yliajot.items():
        if nimi == "syy":
            continue
        arvot[loader.COLUMNS.index(nimi)] = arvo
    return arvot + [yliajot.get("syy", "ei oppijanumeroa")]


class ClassifyTest(unittest.TestCase):
    def test_migraatioskriptin_kaikki_syyt(self):
        self.assertEqual("EI_OPPIJANUMEROA", loader.classify("ei oppijanumeroa"))
        self.assertEqual("RIKKINAINEN_RIVI", loader.classify("odotettiin 30 saraketta, saatiin 28"))
        self.assertEqual(
            "LAST_MODIFIED_EI_JASENNY", loader.classify("last_modified ei jäsenny, ei voi suodattaa")
        )
        self.assertEqual("API_HYLKASI", loader.classify('HTTP 400: {"error":"..."}'))
        self.assertEqual("PAIKALLINEN_VALIDOINTI", loader.classify("ei yhtään osakoetta"))
        self.assertEqual("PAIKALLINEN_VALIDOINTI", loader.classify("virheellinen arvosana tasolle PT: 12"))
        self.assertEqual(
            "PAIKALLINEN_VALIDOINTI",
            loader.classify("arvosanaMuuttui ei ole tarkistettujen osakokeiden osajoukko"),
        )

    def test_useampi_paikallinen_syy_yhdistettyna(self):
        self.assertEqual(
            "PAIKALLINEN_VALIDOINTI",
            loader.classify("ei yhtään osakoetta; virheellinen arvosana tasolle PT: 12"),
        )

    def test_tunnistamaton_ja_tyhja_ovat_muu(self):
        self.assertEqual("MUU", loader.classify(""))
        self.assertEqual("MUU", loader.classify(None))
        self.assertEqual("MUU", loader.classify("jotain ihan muuta"))


class ToNullTest(unittest.TestCase):
    def test_sentinelli_tyhja_ja_valilyonnit(self):
        self.assertIsNone(loader.to_null("\\N"))
        self.assertIsNone(loader.to_null(" \\N "))
        self.assertIsNone(loader.to_null(""))
        self.assertIsNone(loader.to_null("   "))
        self.assertIsNone(loader.to_null(None))

    def test_trimmaa_reunat_mutta_ei_sisusta(self):
        self.assertEqual("Tessa Tellervo", loader.to_null("  Tessa Tellervo\t"))


class ParseRowTest(unittest.TestCase):
    def test_hyva_rivi(self):
        values = loader.parse_row(rivi(), "yki-historia-2.csv", {}, ",")

        self.assertEqual("12345", values["solki_id"])
        self.assertEqual("Testaaja", values["sukunimi"])
        self.assertEqual("2016-05-14", values["tutkintopaiva"])
        self.assertIsNone(values["hetu"])  # \N
        self.assertEqual("ei oppijanumeroa", values["syy"])
        self.assertEqual("EI_OPPIJANUMEROA", values["syyluokka"])
        self.assertEqual("yki-historia-2.csv", values["lahdetiedosto"])
        self.assertIsNone(values["raw_rivi"])
        self.assertEqual(set(loader.DB_COLUMNS), set(values))

    def test_vaara_sarakemaara_talteen_kokonaisena(self):
        values = loader.parse_row(
            ["a", "b", "c", "odotettiin 30 saraketta, saatiin 3"], "lahde.csv", {}, ","
        )

        self.assertEqual("RIKKINAINEN_RIVI", values["syyluokka"])
        # Pilkun sisaltava syy lainausmerkitaan, jotta rivi on luettavissa takaisin CSV:na.
        self.assertEqual('a,b,c,"odotettiin 30 saraketta, saatiin 3"', values["raw_rivi"])
        self.assertIsNone(values["solki_id"])
        self.assertEqual("odotettiin 30 saraketta, saatiin 3", values["syy"])

    def test_liian_leveakin_rivi_on_rikkinainen(self):
        values = loader.parse_row(rivi() + ["ylimaarainen"], "lahde.csv", {}, ",")
        self.assertEqual("RIKKINAINEN_RIVI", values["syyluokka"])
        self.assertIsNotNone(values["raw_rivi"])

    def test_oid_kartta_taydentaa_haun_syyn(self):
        oid_map = {"12345": {"oid": None, "reason": "virheellinen hetu (tarkistusmerkki)"}}
        values = loader.parse_row(rivi(), "lahde.csv", oid_map, ",")
        self.assertEqual("virheellinen hetu (tarkistusmerkki)", values["oid_haun_syy"])

    def test_kartasta_puuttuva_rivi_jaa_tyhjaksi(self):
        values = loader.parse_row(rivi(), "lahde.csv", {"muu": {"oid": None, "reason": "x"}}, ",")
        self.assertIsNone(values["oid_haun_syy"])

    def test_kartassa_ratkennut_ei_saa_keksittya_syyta(self):
        oid_map = {"12345": {"oid": "1.2.246.562.24.1", "reason": None}}
        values = loader.parse_row(rivi(), "lahde.csv", oid_map, ",")
        self.assertIsNone(values["oid_haun_syy"])


class DelimiterTest(unittest.TestCase):
    def test_tunnistaa_pilkun_ja_tabin(self):
        self.assertEqual(",", loader.resolve_delimiter(",".join(rivi()), None))
        self.assertEqual("\t", loader.resolve_delimiter("\t".join(rivi()), None))

    def test_kolmenkymmenen_sarakkeen_tiedostoa_ei_hyvaksyta(self):
        # unresolved_out on 30 saraketta: sita ei saa lukea vahingossa.
        self.assertIsNone(loader.resolve_delimiter(",".join(rivi()[:-1]), None))

    def test_ylikirjoitus_ja_aliakset(self):
        self.assertEqual("\t", loader.resolve_delimiter("mita tahansa", "tab"))
        self.assertEqual(";", loader.resolve_delimiter("mita tahansa", ";"))


class OidMapTest(unittest.TestCase):
    def test_lukee_jsonlin_ja_sivuuttaa_roskan(self):
        with tempfile.NamedTemporaryFile("w", suffix=".jsonl", delete=False, encoding="utf-8") as f:
            f.write(json.dumps({"solki_id": "1", "oid": "1.2.3", "reason": None}) + "\n")
            f.write("ei ole jsonia\n")
            f.write(json.dumps({"solki_id": "2", "oid": None, "reason": "hetu puuttuu"}) + "\n")
            f.write(json.dumps({"ei_solki_idta": True}) + "\n")
            path = f.name
        try:
            oid_map = loader.load_oid_map(path)
        finally:
            os.unlink(path)

        self.assertEqual({"1", "2"}, set(oid_map))
        self.assertEqual("1.2.3", oid_map["1"]["oid"])
        self.assertEqual("hetu puuttuu", oid_map["2"]["reason"])

    def test_puuttuva_tiedosto_on_tyhja_kartta(self):
        self.assertEqual({}, loader.load_oid_map("/ei/ole/olemassa.jsonl"))
        self.assertEqual({}, loader.load_oid_map(None))


class SqlTest(unittest.TestCase):
    def test_syyluokka_castataan_eika_kayteta_kaksoiskaksoispistetta(self):
        sql = loader.insert_sql()
        self.assertIn("CAST(:syyluokka AS yki_historia_siirtymattomyyden_syy)", sql)
        self.assertNotIn("::", sql)

    def test_upsert_solki_idn_perusteella_ei_paivita_avainta(self):
        sql = loader.insert_sql()
        self.assertIn("ON CONFLICT (solki_id) DO UPDATE SET", sql)
        self.assertNotIn("solki_id = EXCLUDED.solki_id", sql)
        self.assertIn("ladattu = now()", sql)

    def test_kaikki_sarakkeet_mukana(self):
        sql = loader.insert_sql()
        for column in loader.DB_COLUMNS:
            self.assertIn(column, sql)


class FakeArgs:
    cluster_arn = "arn:cluster"
    secret_arn = "arn:secret"
    database = "kios"


class FakeClient:
    """Tallentaa kutsut; ei mockkikirjastoa, kuten muuallakin tassa repossa."""

    def __init__(self, records=None, updated=0):
        self.records = records or [[{"stringValue": "2026-09-29 12:00:00+03"}]]
        self.updated = updated
        self.kutsut = []

    def execute_statement(self, **kwargs):
        self.kutsut.append(kwargs)
        return {"records": self.records, "numberOfRecordsUpdated": self.updated}


class PruneSqlTest(unittest.TestCase):
    def test_rajaa_lahdetiedostoon_ja_aikarajaan(self):
        sql = loader.prune_sql()
        self.assertIn("DELETE FROM yki_historia_siirtymaton", sql)
        self.assertIn("lahdetiedosto = :lahdetiedosto", sql)
        self.assertIn("ladattu < CAST(:ajon_alku AS timestamptz)", sql)

    def test_ei_kaksoiskaksoispistetta(self):
        # Sama syy kuin insert_sql:ssa: ::-syntaksi sotkee Data APIn parametrit.
        self.assertNotIn("::", loader.prune_sql())

    def test_ei_poista_ilman_lahdetiedostorajausta(self):
        # Ilman rajausta prune pyyhkisi myos toisen lahdeaineiston rivit.
        self.assertNotIn("DELETE FROM yki_historia_siirtymaton WHERE ladattu", loader.prune_sql())


class RunStartTimestampTest(unittest.TestCase):
    def test_lukee_kannan_kellon_tekstina(self):
        client = FakeClient(records=[[{"stringValue": "2026-09-29 12:00:00+03"}]])

        tulos = loader.run_start_timestamp(client, FakeArgs())

        self.assertEqual("2026-09-29 12:00:00+03", tulos)
        # CloudShellin kello voi olla eri kuin kannan, joten aika on haettava kannasta.
        self.assertEqual("SELECT CAST(now() AS text)", client.kutsut[0]["sql"])


class PruneVanhentuneetTest(unittest.TestCase):
    def test_valittaa_parametrit_ja_palauttaa_poistettujen_maaran(self):
        client = FakeClient(updated=7)

        poistettu = loader.prune_vanhentuneet(client, FakeArgs(), "yki-historia-2.csv", "2026-09-29 12:00:00+03")

        self.assertEqual(7, poistettu)
        params = {p["name"]: p["value"]["stringValue"] for p in client.kutsut[0]["parameters"]}
        self.assertEqual("yki-historia-2.csv", params["lahdetiedosto"])
        self.assertEqual("2026-09-29 12:00:00+03", params["ajon_alku"])

    def test_puuttuva_maara_on_nolla(self):
        class TyhjaVastaus(FakeClient):
            def execute_statement(self, **kwargs):
                return {}

        self.assertEqual(0, loader.prune_vanhentuneet(TyhjaVastaus(), FakeArgs(), "x.csv", "2026-09-29"))


class ParameterSetTest(unittest.TestCase):
    def test_null_ja_arvo(self):
        values = loader.parse_row(rivi(), "lahde.csv", {}, ",")
        params = loader.to_parameter_set(values)

        self.assertEqual([p["name"] for p in params], loader.DB_COLUMNS)
        by_name = {p["name"]: p["value"] for p in params}
        self.assertEqual({"stringValue": "12345"}, by_name["solki_id"])
        self.assertEqual({"isNull": True}, by_name["hetu"])


class FunnelTest(unittest.TestCase):
    def test_laskee_luokat_ja_oppijanumerohaun_tilan(self):
        oid_map = {"12345": {"oid": None, "reason": "hetu puuttuu"}}
        parsed = [
            loader.parse_row(rivi(), "lahde.csv", oid_map, ","),  # haku yritetty
            loader.parse_row(rivi(solki_id="999"), "lahde.csv", oid_map, ","),  # ei yritetty
            loader.parse_row(rivi(solki_id="3", syy="ei yhtään osakoetta"), "lahde.csv", oid_map, ","),
            loader.parse_row(["a", "rikki"], "lahde.csv", oid_map, ","),
        ]

        f = loader.funnel(parsed, oid_map)

        self.assertEqual(4, f["rivit"])
        self.assertEqual(2, f["ei_oppijanumeroa"])
        self.assertEqual(1, f["paikallinen_validointi"])
        self.assertEqual(1, f["rikkinainen_rivi"])
        self.assertEqual(1, f["ilman_solki_id"])
        self.assertEqual(1, f["oid_hakua_ei_yritetty"])
        self.assertEqual(1, f["oid_haku_epaonnistui"])
        self.assertEqual(0, f["oid_ratkennut_kartassa"])

    def test_kartassa_ratkennut_ei_ole_hakua_ei_yritetty(self):
        # Ratkennut rivi tarkoittaa ettei karttaa kaytetty ajossa - oma lukunsa, ei
        # sekoitettuna niihin joille hakua ei yritetty lainkaan.
        oid_map = {"12345": {"oid": "1.2.246.562.24.1", "reason": None}}
        parsed = [loader.parse_row(rivi(), "lahde.csv", oid_map, ",")]

        f = loader.funnel(parsed, oid_map)

        self.assertEqual(1, f["oid_ratkennut_kartassa"])
        self.assertEqual(0, f["oid_hakua_ei_yritetty"])
        self.assertEqual(0, f["oid_haku_epaonnistui"])


class RawLineTest(unittest.TestCase):
    def test_lainausmerkit_ja_erottimet_sailyvat(self):
        self.assertEqual('a,"b,c"', loader.raw_line(["a", "b,c"], ","))
        self.assertEqual("a\tb", loader.raw_line(["a", "b"], "\t"))


class ChunkedTest(unittest.TestCase):
    def test_jakaa_erat(self):
        self.assertEqual([[1, 2], [3, 4], [5]], list(loader.chunked([1, 2, 3, 4, 5], 2)))
        self.assertEqual([], list(loader.chunked([], 2)))


class ReadRowsTest(unittest.TestCase):
    def test_rivinvaihto_lainausmerkkien_sisalla_ei_katkaise_rivia(self):
        row = rivi(perustelu="eka\ntoka")
        text = ",".join('"%s"' % v.replace("\n", "\n") for v in row)
        rows = loader.read_rows(text, ",")
        self.assertEqual(1, len(rows))
        self.assertEqual("eka\ntoka", rows[0][loader.COLUMNS.index("perustelu")])


if __name__ == "__main__":
    unittest.main(verbosity=2)
