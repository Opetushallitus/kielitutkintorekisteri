#!/usr/bin/env python3
"""Unit tests for the pure helpers of migrate_yki_historia.py.

    python3 -m unittest discover -s scripts -p 'test_*.py'

Not wired into CI: the repo has no Python toolchain and check-formatting.sh runs
prettier + ktlint + shellcheck only. These cover the logic that must not be debugged
for the first time against prod — the hetu check that decides whether a person is even
looked up, and the row mapping.
"""

import unittest

from migrate_yki_historia import (
    COLUMNS,
    build_payload,
    load_skip_ids,
    collapse_ws,
    decode_bitmask,
    hetu_issue,
    local_issues,
    lookup_fields,
    parse_last_modified,
    person_key,
    resolve_delimiter,
    to_null,
)

# Every henkilötunnus that appears in the ONR mock fixtures and in YkiApiControllerTest.
VALID_HETUT = [
    "010180-9026", "040265-9985", "010866-9260", "070870X5647", "010180U943H",
    "010116F932M", "010107A329V", "010100A999N", "010100A9846",
]


def row(**overrides):
    """A minimal valid 30-column row; override by column name."""
    values = {name: "\\N" for name in COLUMNS}
    values.update(
        suorittajan_oid="1.2.246.562.24.33342764709",
        hetu="010866-9260",
        sukupuoli="N",
        sukunimi="Sallinen-Testi",
        etunimet="Magdalena Testi",
        kansalaisuus="246",
        katuosoite="Testikatu 1",
        postinumero="00100",
        postitoimipaikka="HELSINKI",
        suoritus_id="12345",
        last_modified="2016-05-04 10:00:00",
        tutkintopaiva="2016-04-01",
        tutkintokieli="fin",
        tutkintotaso="PT",
        jarjestajan_oid="1.2.246.562.10.14893989377",
        jarjestajan_nimi="Testijärjestäjä",
        arviointipaiva="2016-05-01",
        as_ty="2",
    )
    values.update(overrides)
    return [values[name] for name in COLUMNS]


class HetuTest(unittest.TestCase):
    def test_accepts_every_hetu_used_in_the_repo(self):
        for hetu in VALID_HETUT:
            self.assertIsNone(hetu_issue(hetu), hetu)

    def test_rejects_a_wrong_checksum(self):
        # 010180-9026 is valid; any other check character is not.
        self.assertEqual(hetu_issue("010180-902A"), "virheellinen hetun tarkiste")

    def test_rejects_an_impossible_date(self):
        # Passes the checksum but is not a date — the 000000-0000 style sentinel.
        self.assertEqual(hetu_issue("000000-0000"), "virheellinen hetun päivämäärä")
        self.assertEqual(hetu_issue("320177-123X"), "virheellinen hetun päivämäärä")

    def test_rejects_bad_shapes(self):
        for bad in ["", "010866-926", "010866-92600", "010866Q9260", "INVALID_HETU",
                    "WRONG_HETU", "0108669260"]:
            self.assertEqual(hetu_issue(bad), "virheellinen hetun muoto", bad)

    def test_accepts_every_century_separator(self):
        for separator in "+-YXWVUABCDEF":
            hetu = "010180" + separator + "902"
            self.assertIsNone(hetu_issue(hetu + "6"), hetu)


class FieldNormalisationTest(unittest.TestCase):
    def test_to_null_maps_the_mysql_sentinel_and_blanks(self):
        self.assertIsNone(to_null("\\N"))
        self.assertIsNone(to_null("  \\N \t"))
        self.assertIsNone(to_null(""))
        self.assertIsNone(to_null("\t "))
        self.assertIsNone(to_null(None))
        self.assertEqual(to_null("  Testi \t"), "Testi")

    def test_decode_bitmask(self):
        self.assertEqual(decode_bitmask("\\N"), [])
        self.assertEqual(decode_bitmask("0"), [])
        self.assertEqual(decode_bitmask("1"), ["PU"])
        self.assertEqual(decode_bitmask("15"), ["PU", "KI", "TY", "PY"])
        self.assertEqual(decode_bitmask("6"), ["KI", "TY"])

    def test_parse_last_modified_accepts_every_documented_format(self):
        for value in ["2016-05-04T10:00:00", "2016-05-04T10:00:00+0300",
                      "2016-05-04 10:00:00", "2016-05-04 10:00:00+0300", "2016-05-04"]:
            self.assertIsNotNone(parse_last_modified(value), value)
        self.assertIsNone(parse_last_modified("ei päivämäärä"))
        self.assertIsNone(parse_last_modified("\\N"))

    def test_parse_last_modified_assumes_utc_when_naive(self):
        self.assertEqual(parse_last_modified("2016-05-04").utcoffset().total_seconds(), 0)

    def test_resolve_delimiter_finds_the_one_giving_30_columns(self):
        line = ",".join(["x"] * len(COLUMNS))
        self.assertEqual(resolve_delimiter(line, None), ",")
        self.assertEqual(resolve_delimiter(line, "tab"), "\t")
        self.assertIsNone(resolve_delimiter("x,y,z", None))


class PersonKeyTest(unittest.TestCase):
    def test_collapse_ws(self):
        self.assertEqual(collapse_ws("Anna   Maria"), "Anna Maria")
        self.assertEqual(collapse_ws(" Anna\tMaria "), "Anna Maria")

    def test_same_person_written_differently_is_one_lookup(self):
        self.assertEqual(
            person_key("010866-9260", "Magdalena  Testi", "Sallinen-Testi"),
            person_key("010866-9260", "magdalena testi", "SALLINEN-TESTI"),
        )

    def test_different_people_stay_separate(self):
        self.assertNotEqual(
            person_key("010866-9260", "Magdalena", "Sallinen"),
            person_key("010180-9026", "Magdalena", "Sallinen"),
        )


class LookupFieldsTest(unittest.TestCase):
    def test_names_a_specific_missing_field(self):
        self.assertEqual(lookup_fields(row(hetu="\\N"), True)[3], "hetu puuttuu")
        self.assertEqual(lookup_fields(row(etunimet="\\N"), True)[3], "etunimet puuttuu")
        self.assertEqual(lookup_fields(row(sukunimi="\\N"), True)[3], "sukunimi puuttuu")

    def test_reports_a_bad_hetu_only_when_checking(self):
        bad = row(hetu="010180-902A")
        self.assertEqual(lookup_fields(bad, True)[3], "virheellinen hetun tarkiste")
        self.assertIsNone(lookup_fields(bad, False)[3])

    def test_passes_a_complete_row(self):
        hetu, etunimet, sukunimi, reason = lookup_fields(row(), True)
        self.assertIsNone(reason)
        self.assertEqual((hetu, etunimet, sukunimi),
                         ("010866-9260", "Magdalena Testi", "Sallinen-Testi"))


class SkipListTest(unittest.TestCase):
    def test_reads_ids_and_ignores_comments_and_blanks(self):
        import tempfile, os
        with tempfile.NamedTemporaryFile("w", suffix=".txt", delete=False,
                                         encoding="utf-8") as f:
            f.write("# toinen puoli duplikaattiparista\n"
                    "900123\n"
                    "  900124  \n"
                    "\n"
                    "900125  # samana paivana, sama taso\n")
            name = f.name
        try:
            self.assertEqual(load_skip_ids(name), {"900123", "900124", "900125"})
        finally:
            os.unlink(name)

    def test_no_path_means_no_exclusions(self):
        self.assertEqual(load_skip_ids(None), set())


class PayloadTest(unittest.TestCase):
    def test_maps_a_row_the_server_would_accept(self):
        payload = build_payload(row())
        self.assertEqual(local_issues(payload), [])
        self.assertEqual(payload["suoritus"]["osat"], [{"tyyppi": "TY", "arvosana": 2}])
        self.assertEqual(payload["suoritus"]["lahdejarjestelmanId"],
                         {"id": "12345", "lahde": "Solki"})
        self.assertEqual(payload["suoritus"]["arviointitila"], "ARVIOITU")
        self.assertNotIn("tarkistusarviointi", payload["suoritus"])

    def test_arviointitila_without_an_arviointipaiva(self):
        payload = build_payload(row(arviointipaiva="\\N"))
        self.assertEqual(payload["suoritus"]["arviointitila"], "ARVIOITAVA")

    def test_oid_less_row_maps_to_a_null_oid(self):
        self.assertIsNone(build_payload(row(suorittajan_oid="\\N"))["henkilo"]["oid"])

    def test_tarkistusarviointi_decodes_the_bitmasks(self):
        payload = build_payload(row(tark_saapumis_pvm="2016-06-01",
                                    tark_kasittely_pvm="2016-06-20",
                                    tark_osakokeet="6", arvosana_muuttui="2"))
        t = payload["suoritus"]["tarkistusarviointi"]
        self.assertEqual(t["tarkistusarvioidutOsakokeet"], ["KI", "TY"])
        self.assertEqual(t["arvosanaMuuttui"], ["KI"])


class LocalIssuesTest(unittest.TestCase):
    def test_no_osakoe(self):
        self.assertIn("ei yhtään osakoetta", local_issues(build_payload(row(as_ty="\\N"))))

    def test_arvosana_outside_the_taso(self):
        issues = local_issues(build_payload(row(tutkintotaso="PT", as_ty="5")))
        self.assertIn("virheellinen arvosana tasolle PT: [5]", issues)

    def test_unknown_taso_is_named_rather_than_blamed_on_the_arvosana(self):
        issues = local_issues(build_payload(row(tutkintotaso="XX")))
        self.assertEqual(issues, ["tuntematon tutkintotaso: XX"])

    def test_missing_henkilo_fields_are_listed(self):
        issues = local_issues(build_payload(row(kansalaisuus="\\N", postinumero="\\N")))
        self.assertIn("henkilötietoja puuttuu: kansalaisuus, postinumero", issues)

    def test_arvosana_muuttui_must_be_a_subset(self):
        issues = local_issues(build_payload(row(tark_saapumis_pvm="2016-06-01",
                                                tark_osakokeet="2", arvosana_muuttui="4")))
        self.assertIn("arvosanaMuuttui ei ole tarkistettujen osakokeiden osajoukko", issues)

    def test_tarkistus_handled_before_it_arrived(self):
        issues = local_issues(build_payload(row(tark_saapumis_pvm="2016-06-20",
                                                tark_kasittely_pvm="2016-06-01",
                                                tark_osakokeet="2", arvosana_muuttui="2")))
        self.assertIn("tarkistusarvioinnin käsittelypäivä on ennen saapumispäivää", issues)


if __name__ == "__main__":
    unittest.main()
