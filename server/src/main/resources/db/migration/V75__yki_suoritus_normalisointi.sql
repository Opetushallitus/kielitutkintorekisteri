CREATE TYPE yki_osakoetyyppi AS enum (
    'PU',
    'KI',
    'TY',
    'PY',
    'RS',
    'YL'
    );

CREATE TABLE yki_osakoe
(
    id             integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    suoritus_id    integer          NOT NULL REFERENCES yki_suoritus (id) ON DELETE CASCADE ON UPDATE CASCADE,
    tyyppi         yki_osakoetyyppi NOT NULL,
    arviointipaiva date,
    arvosana       integer
);

CREATE UNIQUE INDEX yki_osakoe_suoritus_id_tyyppi_idx ON yki_osakoe (suoritus_id int4_ops, tyyppi enum_ops);

INSERT INTO yki_osakoe (suoritus_id, tyyppi, arviointipaiva, arvosana)
SELECT id,
       'PU',
       arviointipaiva,
       puhuminen
FROM yki_suoritus
WHERE puhuminen IS NOT NULL;

INSERT INTO yki_osakoe (suoritus_id, tyyppi, arviointipaiva, arvosana)
SELECT id,
       'KI',
       arviointipaiva,
       kirjoittaminen
FROM yki_suoritus
WHERE kirjoittaminen IS NOT NULL;

INSERT INTO yki_osakoe(suoritus_id, tyyppi, arviointipaiva, arvosana)
SELECT id,
       'TY',
       arviointipaiva,
       tekstin_ymmartaminen
FROM yki_suoritus
WHERE tekstin_ymmartaminen IS NOT NULL;

INSERT INTO yki_osakoe (suoritus_id, tyyppi, arviointipaiva, arvosana)
SELECT id,
       'PY',
       arviointipaiva,
       puheen_ymmartaminen
FROM yki_suoritus
WHERE puheen_ymmartaminen IS NOT NULL;

INSERT INTO yki_osakoe (suoritus_id, tyyppi, arviointipaiva, arvosana)
SELECT id,
       'RS',
       arviointipaiva,
       rakenteet_ja_sanasto
FROM yki_suoritus
WHERE rakenteet_ja_sanasto IS NOT NULL;

INSERT INTO yki_osakoe (suoritus_id, tyyppi, arviointipaiva, arvosana)
SELECT id,
       'YL',
       arviointipaiva,
       yleisarvosana
FROM yki_suoritus
WHERE yleisarvosana IS NOT NULL;

CREATE TABLE yki_tarkistusarviointi
(
    id             integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    saapumispaiva  date NOT NULL,
    kasittelypaiva date,
    asiatunnus     text NOT NULL,
    perustelu      text
);

CREATE TABLE yki_osakoe_tarkistusarviointi
(
    osakoe_id             integer NOT NULL REFERENCES yki_osakoe (id) ON DELETE CASCADE ON UPDATE CASCADE,
    tarkistusarviointi_id integer NOT NULL REFERENCES yki_tarkistusarviointi (id) ON DELETE CASCADE ON UPDATE CASCADE,
    arvosana_muuttui      boolean
);

WITH tarkistusarviointi_row AS (
    INSERT INTO yki_tarkistusarviointi (saapumispaiva, kasittelypaiva, asiatunnus, perustelu)
        SELECT tarkistusarvioinnin_saapumis_pvm,
               tarkistusarvioinnin_kasittely_pvm,
               tarkistusarvioinnin_asiatunnus,
               perustelu
        FROM yki_suoritus
        WHERE tarkistusarvioinnin_asiatunnus IS NOT NULL
        RETURNING
            id AS tarkistusarviointi_id,
            asiatunnus)
INSERT
INTO yki_osakoe_tarkistusarviointi (osakoe_id, tarkistusarviointi_id, arvosana_muuttui)
SELECT yki_osakoe.id,
       tarkistusarviointi_id,
       yki_osakoe.tyyppi::text = ANY (arvosana_muuttui)
FROM yki_suoritus
         JOIN tarkistusarviointi_row ON tarkistusarvioinnin_asiatunnus = tarkistusarviointi_row.asiatunnus
         JOIN yki_osakoe ON yki_suoritus.id = yki_osakoe.suoritus_id
WHERE yki_osakoe.tyyppi::text = ANY (tarkistusarvioidut_osakokeet);

ALTER TABLE "yki_suoritus"
    DROP COLUMN "arviointipaiva",
    DROP COLUMN "tekstin_ymmartaminen",
    DROP COLUMN "kirjoittaminen",
    DROP COLUMN "rakenteet_ja_sanasto",
    DROP COLUMN "puheen_ymmartaminen",
    DROP COLUMN "puhuminen",
    DROP COLUMN "yleisarvosana",
    DROP COLUMN "tarkistusarvioinnin_saapumis_pvm",
    DROP COLUMN "tarkistusarvioinnin_asiatunnus",
    DROP COLUMN "perustelu",
    DROP COLUMN "tarkistusarvioinnin_kasittely_pvm",
    DROP COLUMN "tarkistusarvioidut_osakokeet",
    DROP COLUMN "arvosana_muuttui";
