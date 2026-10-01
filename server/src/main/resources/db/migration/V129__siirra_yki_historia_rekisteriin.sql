-- Siirtää karanteenitaulun yki_historia_siirtymaton (V127) rivit rekisteriin. Rivit ovat
-- YKI-historiamigraation (2011-2016) lähderivejä, jotka eivät aikanaan siirtyneet; yleisin syy
-- oli puuttuva oppijanumero, jonka V128 sallii.
--
-- Onnistuneesti siirtynyt rivi POISTETAAN karanteenitaulusta. Rivi jota ei voi muuntaa jää
-- paikalleen, ja sen syy/syyluokka kirjoitetaan uudelleen: muutoksen jälkeen EI_OPPIJANUMEROA ei
-- enää ole syy jäädä karanteeniin, joten vanha syy olisi harhaanjohtava.
--
-- MUUNNETTAVUUS TARKISTETAAN pg_input_is_valid-funktiolla, ei casteilla. Flyway ajaa migraation
-- yhdessä transaktiossa, joten yksikin kaatuva cast kaataisi koko siirron - ja juuri näissä
-- riveissä ovat ne arvot joita ei voitu jäsentää. pg_input_is_valid ei koskaan kaadu, ja se
-- tuntee myös domainit ja enumit, joten esim. iso_oid-domainin regexp-CHECKiä ei tarvitse
-- kirjoittaa tähän uudelleen. VAATII POSTGRES 16+ (tuotanto on Aurora PostgreSQL 16.4).
--
-- Valiaikaistaulut luodaan ilman ON COMMIT DROP -maaretta ja siivotaan lopussa itse, jotta
-- tiedosto on ajettavissa myos ilman yhta kaarivaa transaktiota - esim. psql -f ajaa
-- jokaisen lauseen omana transaktionaan, jolloin ON COMMIT DROP tuhoaisi taulun heti.
-- Flyway ajaa migraation yhdessa transaktiossa, joten siella siivous tapahtuu joka tapauksessa.
--
-- HUOM pg_input_is_valid(NULL, ...) on NULL eikä NULL ole tosi: paljas kutsu WHERE-lauseessa
-- pudottaisi hiljaa NULL-rivit. Siksi pakollinen kenttä tarkistetaan muodossa
-- "col IS NOT NULL AND pg_input_is_valid(...)" ja valinnainen muodossa
-- "col IS NULL OR pg_input_is_valid(...)". Tämä on olennaista, koska suorittajan_oid on näillä
-- riveillä odotetusti NULL.

-- 1. Muunnettavissa olevat rivit. MATERIALIZED on optimointiaita: se takaa että kelpoisuusehdot
--    arvioidaan ennen kuin ulompi SELECT castaa arvot.
DROP TABLE IF EXISTS yki_historia_siirrettava;
CREATE TEMP TABLE yki_historia_siirrettava AS
WITH kelpaava AS MATERIALIZED (
    SELECT *
    FROM yki_historia_siirtymaton h
    WHERE
        -- pakolliset kentät, joiden kohdetyyppi on tiukka
        h.solki_id IS NOT NULL AND pg_input_is_valid(h.solki_id, 'integer')
    AND h.tutkintopaiva IS NOT NULL AND pg_input_is_valid(h.tutkintopaiva, 'date')
    AND h.tutkintokieli IS NOT NULL AND pg_input_is_valid(upper(h.tutkintokieli), 'yki_tutkintokieli')
    AND h.tutkintotaso IS NOT NULL AND pg_input_is_valid(h.tutkintotaso, 'yki_tutkintotaso')
    AND h.sukupuoli IS NOT NULL AND pg_input_is_valid(h.sukupuoli, 'yki_sukupuoli')
    AND h.jarjestajan_oid IS NOT NULL AND pg_input_is_valid(h.jarjestajan_oid, 'organisaatio_oid')
        -- pakolliset tekstikentät
    AND h.sukunimi IS NOT NULL
    AND h.etunimet IS NOT NULL
    AND h.kansalaisuus IS NOT NULL
    AND h.katuosoite IS NOT NULL
    AND h.postinumero IS NOT NULL
    AND h.postitoimipaikka IS NOT NULL
        -- jarjestajan_nimi on kannassa nullable mutta YkiSuoritusEntity.jarjestajanNimi ei ole,
        -- ja buildEntity lukee sen rs.getString()-kutsulla tarkistamatta nullia, joten NULL
        -- päätyisi hiljaa ei-null-kenttään ja kaatuisi vasta myöhemmin näkymässä tai CSV:ssä
    AND h.jarjestajan_nimi IS NOT NULL
        -- valinnaiset kentät: puuttuva kelpaa, virheellinen ei
    AND (h.suorittajan_oid IS NULL OR pg_input_is_valid(h.suorittajan_oid, 'henkilo_oid'))
    AND (h.arviointipaiva IS NULL OR pg_input_is_valid(h.arviointipaiva, 'date'))
        -- vähintään yksi osakoe (= YkiSuoritusValidation.validateOsakokeitaOnAtLeastOne)
    AND num_nonnulls(h.as_ty, h.as_ki, h.as_rs, h.as_py, h.as_pu, h.as_yl) > 0
        -- annetut arvosanat ovat kokonaislukuja ja tutkintotasolle sallittuja
        -- (= YkiSuoritusValidation.validateArvosanat, Koodisto.YkiArvosana.validIntegersFor).
        -- CASE antaa kelpaamattomalle arvolle sentinelin -1, jota ei ole yhdessakaan sallittujen
        -- joukossa, joten yksi ehto kattaa sekä jäsentymättömän että kielletyn arvosanan.
    AND NOT EXISTS (
            SELECT 1
            FROM (VALUES (h.as_ty), (h.as_ki), (h.as_rs), (h.as_py), (h.as_pu), (h.as_yl)) a(v)
            WHERE a.v IS NOT NULL
              AND (CASE WHEN pg_input_is_valid(a.v, 'integer') THEN a.v::int ELSE -1 END)
                  <> ALL (
                      CASE upper(h.tutkintotaso)
                          WHEN 'PT' THEN ARRAY[0, 1, 2, 9, 10, 11, 12]
                          WHEN 'KT' THEN ARRAY[0, 1, 2, 3, 4, 9, 10, 11, 12]
                          WHEN 'YT' THEN ARRAY[0, 1, 2, 3, 4, 5, 6, 9, 10, 11, 12]
                          ELSE ARRAY[]::int[]
                      END
                  )
        )
        -- bittimaskit ovat kokonaislukuja, jos ne on annettu
    AND (h.tark_osakokeet IS NULL OR pg_input_is_valid(h.tark_osakokeet, 'integer'))
    AND (h.arvosana_muuttui IS NULL OR pg_input_is_valid(h.arvosana_muuttui, 'integer'))
        -- tarkistusarviointi on joko kokonaan tai ei lainkaan, ja sen asiatunnus on vapaa
        -- (yki_tarkistusarviointi.asiatunnus on UNIQUE)
    AND (
            (h.tark_saapumis_pvm IS NULL AND h.tark_asiatunnus IS NULL)
         OR (
                h.tark_saapumis_pvm IS NOT NULL AND pg_input_is_valid(h.tark_saapumis_pvm, 'date')
            AND h.tark_asiatunnus IS NOT NULL
            AND (h.tark_kasittely_pvm IS NULL OR pg_input_is_valid(h.tark_kasittely_pvm, 'date'))
            AND NOT EXISTS (
                    SELECT 1 FROM yki_tarkistusarviointi t WHERE t.asiatunnus = h.tark_asiatunnus
                )
            )
        )
)
SELECT
    k.id                                            AS lahde_id,
    k.solki_id::int                                 AS solki_id,
    k.suorittajan_oid::henkilo_oid                  AS suorittajan_oid,
    -- Hetua ei saa tallentaa rajapäivänä tai sen jälkeen järjestetylle tutkinnolle, tulipa
    -- suoritus mitä kirjoituspolkua tahansa pitkin (vrt. V112, YkiSuoritusRepository.withoutHetu)
    CASE WHEN k.tutkintopaiva::date >= '2026-01-01' THEN NULL ELSE k.hetu END AS hetu,
    k.sukupuoli::yki_sukupuoli                      AS sukupuoli,
    k.sukunimi,
    k.etunimet,
    k.kansalaisuus,
    k.katuosoite,
    k.postinumero,
    k.postitoimipaikka,
    k.email,
    k.tutkintopaiva::date                           AS tutkintopaiva,
    upper(k.tutkintokieli)::yki_tutkintokieli       AS tutkintokieli,
    k.tutkintotaso::yki_tutkintotaso                AS tutkintotaso,
    k.jarjestajan_oid::organisaatio_oid             AS jarjestajan_tunnus_oid,
    k.jarjestajan_nimi,
    k.arviointipaiva::date                          AS arviointipaiva,
    k.as_ty::int                                    AS as_ty,
    k.as_ki::int                                    AS as_ki,
    k.as_rs::int                                    AS as_rs,
    k.as_py::int                                    AS as_py,
    k.as_pu::int                                    AS as_pu,
    k.as_yl::int                                    AS as_yl,
    k.tark_saapumis_pvm::date                       AS tark_saapumis_pvm,
    k.tark_kasittely_pvm::date                      AS tark_kasittely_pvm,
    k.tark_asiatunnus,
    k.perustelu,
    -- Bittimaskit: PU=1, KI=2, TY=4, PY=8. CASE takaa ettei castia ajeta kelpaamattomalle
    -- arvolle; AND:in oikosulkuun ei voi luottaa, koska Postgres ei takaa evaluointijärjestystä.
    CASE WHEN k.tark_osakokeet IS NOT NULL AND pg_input_is_valid(k.tark_osakokeet, 'integer')
         THEN k.tark_osakokeet::int ELSE 0 END      AS tark_mask,
    CASE WHEN k.arvosana_muuttui IS NOT NULL AND pg_input_is_valid(k.arvosana_muuttui, 'integer')
         THEN k.arvosana_muuttui::int ELSE 0 END    AS muuttui_mask,
    -- Maskissa on bitti vain osakokeille PU/KI/TY/PY, joten RS ja YL eivät voi olla
    -- tarkistusarvioituja. Tämä kertoo mitkä niistä tällä suorituksella tosiasiassa on.
    (CASE WHEN k.as_pu IS NOT NULL THEN 1 ELSE 0 END
   + CASE WHEN k.as_ki IS NOT NULL THEN 2 ELSE 0 END
   + CASE WHEN k.as_ty IS NOT NULL THEN 4 ELSE 0 END
   + CASE WHEN k.as_py IS NOT NULL THEN 8 ELSE 0 END) AS osakoe_bitit
FROM kelpaava k;

-- 2. Jo rekisterissä oleva solki_id on vanhentunut duplikaatti: sitä ei siirretä eikä poisteta,
--    vaan se jää näkyviin karanteeniin. Tehdään vasta tässä, jotta vertailu on int-int eikä
--    vaadi castia kelpoisuusehtojen rinnalla.
DELETE FROM yki_historia_siirrettava s
WHERE EXISTS (SELECT 1 FROM yki_suoritus ys WHERE ys.solki_id = s.solki_id);

-- 3. Jos kaksi siirrettävää riviä jakaa saman tarkistusarvioinnin asiatunnuksen, kumpaakaan ei
--    voi siirtää: asiatunnus on UNIQUE.
DELETE FROM yki_historia_siirrettava s
WHERE s.tark_asiatunnus IS NOT NULL
  AND s.tark_asiatunnus IN (
      SELECT tark_asiatunnus
      FROM yki_historia_siirrettava
      WHERE tark_asiatunnus IS NOT NULL
      GROUP BY tark_asiatunnus
      HAVING count(*) > 1
  );

-- 4. Suoritukset. last_modified ja received_at otetaan now()-hetkestä (transaktion alku, sama
--    kaikille): API-polku YkiSuoritusEntity.from asettaa molemmat Instant.now()-hetkeen, joten jo
--    aiemmin siirtyneet historiarivit ovat samalla tavalla. Lähteen last_modified ei kelpaa:
--    se on osa unique_suoritus-rajoitetta, eikä se jäsenny osalla riveistä lainkaan.
--    arviointitila johdetaan kuten laskeArviointitila (yki/Arviointitila.kt) sen tekee, kun
--    kitu.yki.convertLegacyArviointitila.enabled on päällä (= tuotanto). Koska osakokeiksi
--    tulevat vain ei-NULL arvosanat, arvosanaPuuttuu on aina 0. Arvosanat 9-12 ovat
--    Keskeytetty/Vilppi/Ei suoritusta, joten "oikea arvosana" on < 9.
--    koski_opiskeluoikeus, todistuskieli ja maa jäävät NULLiksi (lähteessä ei ole näitä
--    sarakkeita) ja koski_siirto_kasitelty oletusarvoonsa false.
DROP TABLE IF EXISTS yki_historia_siirretty;
CREATE TEMP TABLE yki_historia_siirretty AS
WITH lisatty AS (
    INSERT INTO yki_suoritus (
        suorittajan_oid, hetu, sukupuoli, sukunimi, etunimet, kansalaisuus, katuosoite,
        postinumero, postitoimipaikka, email, solki_id, last_modified, received_at,
        tutkintopaiva, tutkintokieli, tutkintotaso, jarjestajan_tunnus_oid, jarjestajan_nimi,
        arviointitila, lahdejarjestelmantunnus
    )
    SELECT
        s.suorittajan_oid, s.hetu, s.sukupuoli, s.sukunimi, s.etunimet, s.kansalaisuus,
        s.katuosoite, s.postinumero, s.postitoimipaikka, s.email, s.solki_id, now(), now(),
        s.tutkintopaiva, s.tutkintokieli, s.tutkintotaso, s.jarjestajan_tunnus_oid,
        s.jarjestajan_nimi,
        CASE
            -- onTarkistusarviointi vastaa lähteen tarkistusarvioinnin olemassaoloa, ei sitä
            -- tallentuuko se: niin laskeArviointitila sen näkee (validointi ajetaan ennen kuin
            -- YkiSuoritusRepository karsii osakkeettoman tarkistusarvioinnin pois)
            WHEN s.tark_saapumis_pvm IS NOT NULL AND s.tark_kasittely_pvm IS NOT NULL
                THEN 'TARKISTUSARVIOITU'
            WHEN s.tark_saapumis_pvm IS NOT NULL
                THEN 'TARKISTUSARVIOITAVA'
            WHEN NOT EXISTS (
                     SELECT 1
                     FROM (VALUES (s.as_ty), (s.as_ki), (s.as_rs), (s.as_py), (s.as_pu),
                                  (s.as_yl)) a(v)
                     WHERE a.v IS NOT NULL AND a.v < 9
                 )
                THEN 'EI_SUORITUSTA'
            ELSE 'ARVIOITU'
        END::yki_arviointitila,
        -- = LahdejarjestelmanTunniste.toTunnus(), lahde Solki
        'yki.' || s.solki_id
    FROM yki_historia_siirrettava s
    RETURNING id, solki_id
)
SELECT l.id AS suoritus_id, s.*
FROM lisatty l
    JOIN yki_historia_siirrettava s ON s.solki_id = l.solki_id;

-- 5. Osakokeet: yksi rivi per annettu arvosana, kaikille sama arviointipäivä.
INSERT INTO yki_osakoe (suoritus_id, tyyppi, arviointipaiva, arvosana)
SELECT s.suoritus_id, a.tyyppi, s.arviointipaiva, a.arvosana
FROM yki_historia_siirretty s
    CROSS JOIN LATERAL (
        VALUES ('TY'::yki_osakoetyyppi, s.as_ty), ('KI', s.as_ki), ('RS', s.as_rs),
               ('PY', s.as_py), ('PU', s.as_pu), ('YL', s.as_yl)
    ) a(tyyppi, arvosana)
WHERE a.arvosana IS NOT NULL;

-- 6. Tarkistusarvioinnit. Tarkistusarviointi on tavoitettavissa vain
--    yki_osakoe_tarkistusarviointi-liitoksen kautta, joten sellaista jonka maski ei osu yhteenkään
--    tosiasiassa tallennettuun osakokeeseen ei kirjoiteta lainkaan (sama karsinta jonka
--    YkiSuoritusRepository.withTallennettavaTarkistusarviointi tekee).
DROP TABLE IF EXISTS yki_historia_siirretty_tark;
CREATE TEMP TABLE yki_historia_siirretty_tark AS
WITH lisatty AS (
    INSERT INTO yki_tarkistusarviointi (saapumispaiva, kasittelypaiva, asiatunnus, perustelu)
    SELECT s.tark_saapumis_pvm, s.tark_kasittely_pvm, s.tark_asiatunnus, s.perustelu
    FROM yki_historia_siirretty s
    WHERE s.tark_saapumis_pvm IS NOT NULL
      AND (s.tark_mask & s.osakoe_bitit) <> 0
    RETURNING id, asiatunnus
)
SELECT l.id AS tarkistusarviointi_id, s.suoritus_id, s.tark_mask, s.muuttui_mask
FROM lisatty l
    JOIN yki_historia_siirretty s ON s.tark_asiatunnus = l.asiatunnus;

INSERT INTO yki_osakoe_tarkistusarviointi (osakoe_id, tarkistusarviointi_id, arvosana_muuttui)
SELECT o.id, t.tarkistusarviointi_id, (t.muuttui_mask & b.bitti) <> 0
FROM yki_historia_siirretty_tark t
    JOIN (VALUES ('PU'::yki_osakoetyyppi, 1), ('KI', 2), ('TY', 4), ('PY', 8)) b(tyyppi, bitti)
        ON (t.tark_mask & b.bitti) <> 0
    JOIN yki_osakoe o ON o.suoritus_id = t.suoritus_id AND o.tyyppi = b.tyyppi;

-- 7. Siirtyneet pois karanteenista.
DELETE FROM yki_historia_siirtymaton h
WHERE EXISTS (SELECT 1 FROM yki_historia_siirretty s WHERE s.lahde_id = h.id);

-- 8. Jäljelle jääneiden syy uudelleen. Syy kirjoitetaan kokonaan uudeksi eikä vanhaan lisätä
--    etuliitettä, jotta uudelleenajo ei kasvata tekstiä. Alkuperäinen oppijanumerohaun syy
--    säilyy omassa sarakkeessaan oid_haun_syy.
UPDATE yki_historia_siirtymaton h
SET syy = CASE
        WHEN h.raw_rivi IS NOT NULL
            THEN 'rikkinäinen lähderivi, väärä sarakemäärä'
        WHEN h.solki_id IS NULL OR NOT pg_input_is_valid(h.solki_id, 'integer')
            THEN 'solki_id puuttuu tai ei ole kokonaisluku'
        WHEN EXISTS (SELECT 1 FROM yki_suoritus ys WHERE ys.solki_id::text = h.solki_id)
            THEN 'suoritus on jo rekisterissä'
        WHEN h.tutkintopaiva IS NULL OR NOT pg_input_is_valid(h.tutkintopaiva, 'date')
            THEN 'tutkintopäivä puuttuu tai ei jäsenny päivämääräksi'
        WHEN h.tutkintokieli IS NULL OR NOT pg_input_is_valid(upper(h.tutkintokieli), 'yki_tutkintokieli')
            THEN 'tutkintokieli puuttuu tai on tuntematon'
        WHEN h.tutkintotaso IS NULL OR NOT pg_input_is_valid(h.tutkintotaso, 'yki_tutkintotaso')
            THEN 'tutkintotaso puuttuu tai on tuntematon'
        WHEN h.sukupuoli IS NULL OR NOT pg_input_is_valid(h.sukupuoli, 'yki_sukupuoli')
            THEN 'sukupuoli puuttuu tai on tuntematon'
        WHEN h.jarjestajan_oid IS NULL OR NOT pg_input_is_valid(h.jarjestajan_oid, 'organisaatio_oid')
            THEN 'järjestäjän oid puuttuu tai ei ole kelvollinen oid'
        WHEN h.jarjestajan_nimi IS NULL
            THEN 'järjestäjän nimi puuttuu'
        WHEN h.sukunimi IS NULL OR h.etunimet IS NULL OR h.kansalaisuus IS NULL
          OR h.katuosoite IS NULL OR h.postinumero IS NULL OR h.postitoimipaikka IS NULL
            THEN 'pakollinen henkilötieto puuttuu'
        WHEN h.suorittajan_oid IS NOT NULL AND NOT pg_input_is_valid(h.suorittajan_oid, 'henkilo_oid')
            THEN 'oppijanumero ei ole kelvollinen oid'
        WHEN h.arviointipaiva IS NOT NULL AND NOT pg_input_is_valid(h.arviointipaiva, 'date')
            THEN 'arviointipäivä ei jäsenny päivämääräksi'
        WHEN num_nonnulls(h.as_ty, h.as_ki, h.as_rs, h.as_py, h.as_pu, h.as_yl) = 0
            THEN 'ei yhtään osakoetta'
        WHEN EXISTS (
                SELECT 1
                FROM (VALUES (h.as_ty), (h.as_ki), (h.as_rs), (h.as_py), (h.as_pu), (h.as_yl)) a(v)
                WHERE a.v IS NOT NULL AND NOT pg_input_is_valid(a.v, 'integer')
            )
            THEN 'arvosana ei ole kokonaisluku'
        WHEN EXISTS (
                SELECT 1
                FROM (VALUES (h.as_ty), (h.as_ki), (h.as_rs), (h.as_py), (h.as_pu), (h.as_yl)) a(v)
                WHERE a.v IS NOT NULL
                  AND (CASE WHEN pg_input_is_valid(a.v, 'integer') THEN a.v::int ELSE -1 END)
                      <> ALL (
                          CASE upper(h.tutkintotaso)
                              WHEN 'PT' THEN ARRAY[0, 1, 2, 9, 10, 11, 12]
                              WHEN 'KT' THEN ARRAY[0, 1, 2, 3, 4, 9, 10, 11, 12]
                              WHEN 'YT' THEN ARRAY[0, 1, 2, 3, 4, 5, 6, 9, 10, 11, 12]
                              ELSE ARRAY[]::int[]
                          END
                      )
            )
            THEN 'arvosana ei ole sallittu tutkintotasolle ' || h.tutkintotaso
        ELSE 'tarkistusarviointia ei voi tallentaa (asiatunnus puuttuu, on jo käytössä tai päivämäärä ei jäsenny)'
    END,
    syyluokka = CASE
        WHEN h.raw_rivi IS NOT NULL THEN 'RIKKINAINEN_RIVI'
        ELSE 'PAIKALLINEN_VALIDOINTI'
    END::yki_historia_siirtymattomyyden_syy;

DROP TABLE IF EXISTS yki_historia_siirretty_tark;
DROP TABLE IF EXISTS yki_historia_siirretty;
DROP TABLE IF EXISTS yki_historia_siirrettava;
