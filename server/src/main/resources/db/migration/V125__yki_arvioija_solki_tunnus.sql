-- Solki muodostaa arvioijalle tunnuksen (esim. A00001) ja palauttaa sen POST /oph/arvioija
-- -kutsun vastauksessa. Tunnus yksiloi arvioijan Solkin paassa, joten se talletetaan selvitystyota
-- varten samaan tapaan kuin KOSKI-siirron palauttama opiskeluoikeuden OID.
ALTER TABLE yki_arvioija
    ADD COLUMN solki_tunnus TEXT;

COMMENT ON COLUMN yki_arvioija.solki_tunnus IS 'Solkin arvioijalle muodostama tunnus, esim. A00001, luettuna lahetyksen vastauksesta. Kitun oma kentta: sita ei laheteta Solkille eika se saa nollautua Solkin pushissa, joten tallenna-upsert ei nimea sita SET-listassaan.';
