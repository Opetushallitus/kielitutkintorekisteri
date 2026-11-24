CREATE TABLE yki_arvioija_poisto
(
    id                            INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    arvioija_oid                  henkilo_oid NOT NULL,
    poistettu                     TIMESTAMPTZ NOT NULL DEFAULT now(),
    solki_lahetysvirhe            TEXT,
    solki_lahetysyritykset        INTEGER     NOT NULL DEFAULT 0,
    solki_viimeisin_lahetysyritys TIMESTAMPTZ,
    CONSTRAINT yki_arvioija_poisto_unique_arvioija_oid UNIQUE (arvioija_oid)
);

COMMENT ON TABLE yki_arvioija_poisto IS
    'Solkiin lahetettavien poistoilmoitusten jono. Rivi syntyy, kun arvioija poistetaan sailytysajan umpeuduttua, ja poistuu, kun Solki on vastaanottanut ilmoituksen.';
