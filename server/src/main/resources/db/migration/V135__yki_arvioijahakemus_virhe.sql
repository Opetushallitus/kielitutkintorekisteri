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
