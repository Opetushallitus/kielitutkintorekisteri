CREATE TABLE yki_arvioijahakemus_tarkistettava
(
    hakemus_oid       TEXT PRIMARY KEY,
    henkilo_oid       TEXT        NOT NULL,
    arvioija_oid      TEXT        NOT NULL,
    sukunimi          TEXT        NOT NULL,
    etunimet          TEXT        NOT NULL,
    arviointioikeudet JSONB       NOT NULL CHECK (jsonb_typeof(arviointioikeudet) = 'array' AND arviointioikeudet <> '[]'::jsonb),
    sahkopostiosoite  TEXT,
    katuosoite        TEXT,
    postinumero       TEXT,
    postitoimipaikka  TEXT,
    ensimmainen_haku  TIMESTAMPTZ NOT NULL,
    viimeisin_haku    TIMESTAMPTZ NOT NULL
);

COMMENT ON TABLE yki_arvioijahakemus_tarkistettava IS
    'Atarusta haetut arvioijahakemukset, jotka odottavat virkailijan tarkistusta ja vahvistusta ennen rekisteriin tuontia. Rivi on viimeisimman haun nakyma ja paivitetaan joka haulla, joten poisto on aina turvallinen. henkilo_oid = atarun personOid, arvioija_oid = ONR:n oppijanumero haun hetkella; nimet ONR:sta listausta varten; yhteystiedot vain hakemukselta (ONR:n tietoja ei talleteta). Hetua tai muita siirron vastauksia ei talleteta. Sailytys: rivi poistetaan kun hakemus vahvistetaan (yki_arvioijahakemus), siirtyy virheisiin tai yksilointia odottamaan, tai ei enaa ole atarun listauksessa (tyhja listaus ei poista mitaan).';

COMMENT ON TABLE yki_arvioijahakemus IS
    'Atarun arvioijahakemusten kirjanpito. Rivin olemassaolo estaa hakemuksen uudelleenhaun (paitsi ODOTTAA_YKSILOINTIA, joka haetaan joka ajolla uudelleen). KASITTELYSSA = virkailijan vahvistus on kesken tai keskeytyi arvioijan tallennuksen jalkeen; KASITELTY = virkailija on vahvistanut hakemuksen rekisteriin. Hetua tai vastauksia ei talleteta. Pelkka kirjanpito, ei sailytysaikaa: poistettu KASITTELYSSA- tai KASITELTY-rivi kasiteltaisiin uudelleen.';

-- Puuttuva osoite ei ole enaa virhe: virkailija syottaa sen tarkistuslomakkeelle.
DELETE FROM yki_arvioijahakemus_virhe WHERE virheet @> '[{"tyyppi": "OSOITE_PUUTTUU"}]'::jsonb;

-- Haun tehtavanimi vaihtui, ja db-schedulerin tehtavanimi on pysyva tunniste.
DELETE FROM scheduled_tasks WHERE task_name = 'Tuo YKI-arvioijahakemukset atarusta';
