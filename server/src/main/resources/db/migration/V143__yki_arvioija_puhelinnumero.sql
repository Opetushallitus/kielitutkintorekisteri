ALTER TABLE yki_arvioija
    ADD COLUMN puhelinnumero TEXT;

ALTER TABLE yki_arvioijahakemus_tarkistettava
    ADD COLUMN puhelinnumero TEXT;
