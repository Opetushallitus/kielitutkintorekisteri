-- Karanteenitaulu YKI-historiamigraation (2011-2016) riveille jotka eivat siirtyneet
-- rekisteriin. Lahde on migraatioskriptin --failed-out-tiedosto: 30 lahdesaraketta
-- sellaisenaan + syy 31. sarakkeena.
--
-- Taulu EI ole osa rekisteria: taalta ei lahde mitaan KOSKEEN, oppijanumerorekisteriin
-- eika ilmoittautumisjarjestelmaan, eika naita rivia lasketa suorituksiksi. Taulu
-- poistetaan kun aukko on kuitattu tai korjattu.
--
-- Jokainen lahdesarake on TEXT ja talletetaan sellaisenaan. Juuri naissa riveissa on ne
-- arvot joita ei voitu jasentaa - katkaistuja henkilotunnuksia, jasentymattomia
-- aikaleimoja, vaaria sarakemaaria - joten tyypitys hylkaisi juuri sen datan joka on
-- tarkoitus saada talteen. Lahteen paivamaarat ovat ISO-muodossa, joten tekstina
-- lajittelu antaa oikean jarjestyksen. \N ja tyhja arvo talletetaan NULLina.
CREATE TYPE yki_historia_siirtymattomyyden_syy AS ENUM (
    'EI_OPPIJANUMEROA',
    'PAIKALLINEN_VALIDOINTI',
    'API_HYLKASI',
    'RIKKINAINEN_RIVI',
    'LAST_MODIFIED_EI_JASENNY',
    'MUU'
);

CREATE TABLE yki_historia_siirtymaton
(
    id                 INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    -- Lahdesarakkeet YkiSuoritusCsv-jarjestyksessa. solki_id on lahteen suoritus_id.
    suorittajan_oid    TEXT,
    hetu               TEXT,
    sukupuoli          TEXT,
    sukunimi           TEXT,
    etunimet           TEXT,
    kansalaisuus       TEXT,
    katuosoite         TEXT,
    postinumero        TEXT,
    postitoimipaikka   TEXT,
    email              TEXT,
    solki_id           TEXT,
    last_modified      TEXT,
    tutkintopaiva      TEXT,
    tutkintokieli      TEXT,
    tutkintotaso       TEXT,
    jarjestajan_oid    TEXT,
    jarjestajan_nimi   TEXT,
    arviointipaiva     TEXT,
    as_ty              TEXT,
    as_ki              TEXT,
    as_rs              TEXT,
    as_py              TEXT,
    as_pu              TEXT,
    as_yl              TEXT,
    tark_saapumis_pvm  TEXT,
    tark_asiatunnus    TEXT,
    tark_osakokeet     TEXT,
    arvosana_muuttui   TEXT,
    perustelu          TEXT,
    tark_kasittely_pvm TEXT,

    syy                TEXT                               NOT NULL,
    syyluokka          yki_historia_siirtymattomyyden_syy NOT NULL,
    oid_haun_syy       TEXT,
    raw_rivi           TEXT,
    lahdetiedosto      TEXT                               NOT NULL,
    ladattu            TIMESTAMPTZ                        NOT NULL DEFAULT now(),

    -- Uniikki muttei NOT NULL: lataus paivittaa rivin solki_id:n perusteella, mutta
    -- rikkinainen rivi jolta id puuttuu on silti saatava talteen (Postgres sallii
    -- uniikissa indeksissa useita NULLeja).
    CONSTRAINT yki_historia_siirtymaton_solki_id_unique UNIQUE (solki_id)
);

CREATE INDEX yki_historia_siirtymaton_syyluokka_idx ON yki_historia_siirtymaton (syyluokka);

COMMENT ON TABLE yki_historia_siirtymaton IS
    'YKI-historiamigraatiossa siirtymatta jaaneet lahderivit sellaisenaan + syy. Karanteenitaulu, ei osa rekisteria: ei KOSKI-siirtoa, ei oppijanumerorekisterikutsuja, ei ilmoittautumisjarjestelman ilmoituksia. Poistetaan kun aukko on kuitattu.';
COMMENT ON COLUMN yki_historia_siirtymaton.solki_id IS 'Lahteen suoritus_id eli Solki-tunniste. NULL vain rikkinaisella rivilla.';
COMMENT ON COLUMN yki_historia_siirtymaton.syy IS 'Migraatioskriptin kirjaama syy sellaisenaan (--failed-out-tiedoston 31. sarake).';
COMMENT ON COLUMN yki_historia_siirtymaton.syyluokka IS 'Syysta johdettu luokka, jotta ryhmittely ei jasenna vapaata tekstia.';
COMMENT ON COLUMN yki_historia_siirtymaton.oid_haun_syy IS 'Oppijanumerohaun kirjaama syy oid-kartasta. NULL = hakua ei ole yritetty talle riville.';
COMMENT ON COLUMN yki_historia_siirtymaton.raw_rivi IS 'Rivi sellaisenaan silloin kun sarakemaara oli vaarin; muuten NULL.';
