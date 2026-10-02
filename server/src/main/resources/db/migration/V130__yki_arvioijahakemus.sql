CREATE TABLE yki_arvioijahakemus
(
    hakemus_oid      TEXT PRIMARY KEY,
    henkilo_oid      TEXT,
    tila             TEXT        NOT NULL CHECK (tila IN ('KASITELTY', 'HYLATTY', 'EI_TAYTA_EHTOJA')),
    syy              TEXT,
    arvioija_id      INTEGER     REFERENCES yki_arvioija (id) ON DELETE SET NULL,
    kauden_alkupaiva DATE,
    kasitelty        TIMESTAMPTZ NOT NULL
);

COMMENT ON TABLE yki_arvioijahakemus IS
    'Atarun arvioijahakemusten kasittelytila. Rivin olemassaolo estaa hakemuksen uudelleenkasittelyn; hetua tai vastauksia ei talleteta. Pelkka kirjanpito, ei sailytysaikaa: poistettu rivi kasiteltaisiin uudelleen.';
