CREATE TABLE yki_arvioijahakemus_virhe
(
    hakemus_oid        TEXT PRIMARY KEY,
    henkilo_oid        TEXT,
    virheet            JSONB       NOT NULL CHECK (jsonb_typeof(virheet) = 'array' AND virheet <> '[]'::jsonb),
    ensimmainen_yritys TIMESTAMPTZ NOT NULL,
    viimeisin_yritys   TIMESTAMPTZ NOT NULL,
    yrityksia          INTEGER     NOT NULL CHECK (yrityksia > 0)
);

COMMENT ON TABLE yki_arvioijahakemus_virhe IS
    'Atarun arvioijahakemukset, joiden kasittely epaonnistui muusta kuin ohimenevasta syysta. Hakemus haetaan joka ajolla uudelleen, koska sille ei ole yki_arvioijahakemus-rivia. virheet = [{tyyppi, kentta, arvo, viesti}]: arvo on kartoitetun vastauksen arvo sellaisenaan, siirron rakennevirheesta talletetaan vain polku; hetua tai siirron person-tietoja ei talleteta. Sailytys: rivi poistetaan kun hakemus kirjataan yki_arvioijahakemus-tauluun tai kun se ei enaa ole atarun listauksessa (tyhja listaus ei poista mitaan).';

-- HYLATTY- ja EI_TAYTA_EHTOJA-riveille ei ole luotu arvioijaa, joten poisto vain kasittelee ne uudelleen.
DELETE FROM yki_arvioijahakemus WHERE tila IN ('HYLATTY', 'EI_TAYTA_EHTOJA');

ALTER TABLE yki_arvioijahakemus
    DROP CONSTRAINT yki_arvioijahakemus_tila_check,
    ADD CONSTRAINT yki_arvioijahakemus_tila_check CHECK (tila IN ('KASITELTY', 'ODOTTAA_YKSILOINTIA', 'KASITTELYSSA'));
