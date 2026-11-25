ALTER TABLE yki_suoritus
    ADD COLUMN koski_siirretty timestamp with time zone;

COMMENT ON COLUMN yki_suoritus.koski_siirretty IS 'Aika jolloin suoritus on siirretty onnistuneesti KOSKI-järjestelmään. Tyhjä ennen 11/2026 tehdyillä siirroilla.';

ALTER TABLE yki_suoritus_lisatieto
    ADD COLUMN arviointitilan_lahetysyritys timestamp with time zone;

COMMENT ON COLUMN yki_suoritus_lisatieto.arviointitilan_lahetysyritys IS 'Aika jolloin arviointitilaa on viimeksi yritetty lähettää ilmoittautumisjärjestelmään (KIOS), onnistuneesti tai epäonnistuneesti';
