#!/usr/bin/env python3
"""Yksikkotestit fetch_kaannokset.py:lle.

Ei CI:ssa (kuten ei muillakaan scripts/-hakemiston skripteilla). Aja kasin:

    python3 scripts/test_fetch_kaannokset.py
"""

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import fetch_kaannokset as haku  # noqa: E402


class YhdistaFiTest(unittest.TestCase):
    def test_tolgee_voittaa_tuntemansa_avaimen(self):
        tulos = haku.yhdista_fi({"a": "vanha"}, {"a": "uusi"})
        self.assertEqual({"a": "uusi"}, tulos)

    def test_vain_paikallinen_avain_sailyy(self):
        # Haaralla lisatty avain ei ole viela Tolgeessa; sita ei saa hukata.
        tulos = haku.yhdista_fi({"a": "vanha", "uusi.avain": "teksti"}, {"a": "uusi"})
        self.assertEqual({"a": "uusi", "uusi.avain": "teksti"}, tulos)

    def test_vain_tolgeessa_olevaa_avainta_ei_tuoda(self):
        # Orpo Tolgeessa: fi.json omistaa avainjoukon ja UiTextTest vaatii tasmaavuutta.
        tulos = haku.yhdista_fi({"a": "vanha"}, {"a": "uusi", "orpo": "x"})
        self.assertEqual({"a": "uusi"}, tulos)

    def test_tyhja_tolgee_jattaa_paikalliset_ennalleen(self):
        tulos = haku.yhdista_fi({"a": "vanha"}, {})
        self.assertEqual({"a": "vanha"}, tulos)


class SuodataKieliTest(unittest.TestCase):
    def test_suodattaa_avainjoukkoon(self):
        tulos = haku.suodata_kieli({"a": "A", "orpo": "X"}, {"a", "b"})
        self.assertEqual({"a": "A"}, tulos)

    def test_puuttuvaa_kaannosta_ei_keksita(self):
        tulos = haku.suodata_kieli({"a": "A"}, {"a", "b"})
        self.assertNotIn("b", tulos)


class SerialisoiTest(unittest.TestCase):
    def test_avaimet_jarjestyksessa(self):
        self.assertEqual('{\n  "a": "1",\n  "b": "2"\n}\n', haku.serialisoi({"b": "2", "a": "1"}))

    def test_ei_ascii_escapeja(self):
        # Prettier ei escapaa skandeja; escapattu tiedosto punertaisi check-formatting.sh:n.
        self.assertIn("äö", haku.serialisoi({"a": "äö"}))
        self.assertNotIn("\\u", haku.serialisoi({"a": "äö"}))

    def test_paattyy_rivinvaihtoon(self):
        self.assertTrue(haku.serialisoi({"a": "1"}).endswith("}\n"))

    def test_tyhja_kartta(self):
        self.assertEqual("{}\n", haku.serialisoi({}))


if __name__ == "__main__":
    unittest.main()
