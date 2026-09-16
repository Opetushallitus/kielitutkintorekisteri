-- Arviointikauden paattymispaiva on inklusiivinen: se on kauden viimeinen voimassaolopaiva.
-- Laskenta tuotti silti alkupaiva + 5 v samana paivana, jolloin kausi oli 5 v + 1 pv ja
-- vuosipaivana alkava jatkokausi hylattiin paallekkaisena. Laskenta on korjattu koodissa
-- (Arviointikausi.paattymispaiva); tama migraatio korjaa vanhalla saannolla syntyneet rivit.
--
-- Ehdoksi kirjoitetaan tasmalleen vanha saanto eika "siirra kaikkia": muut paivaparit ovat joko
-- kesken kauden katkaistuja tai Solkista tuotuja poikkeamia. Yhden paivan siirto on hiljainen
-- muutos hallintopaatoksen sisaltoon, joten se tehdaan vain riveille jotka kone on itse laskenut.
--
-- Postgresin interval-aritmetiikka leikkaa karkauspaivan samoin kuin java.timen plusYears
-- (2024-02-29 + 5 v = 2029-02-28), joten ehto osuu tasan niihin riveihin jotka vanha laskenta
-- tuotti.
--
-- Master ja projektio on korjattava samalla ehdolla. Jos ne jaisivat eri linjoille, yollinen
-- paivitaArvioijaProjektiot kirjoittaisi projektion ja kutsuisi merkitseMuuttuneeksi, jolloin
-- korjaus valuisi Solki-lahetysjonoon rivi kerrallaan.

-- Master. Passivoitu kausi on katkaistu tarkoituksella: sen paattymispaiva on passivointipaiva,
-- ei laskettu arvo, eika sita saa siirtaa.
UPDATE yki_arvioija_arviointikausi
SET paattymispaiva = paattymispaiva - 1
WHERE passivoitu IS NULL
  AND paattymispaiva = (alkupaiva + INTERVAL '5 years')::date;

-- Projektio. Vanhentuneet kielet eivat kuulu kausiin lainkaan vaan ovat jaadytettyja tuontirivien
-- jaanteita, joilla voi olla sama alkupaiva kuin hallitulla kaudella - siksi ne on suljettava pois
-- erikseen. EXISTS rajaa lopun niihin riveihin jotka tosiasiassa projisoivat hallittua kautta.
UPDATE yki_arviointioikeus oikeus
SET kauden_paattymispaiva = oikeus.kauden_paattymispaiva - 1
WHERE oikeus.kauden_paattymispaiva = (oikeus.kauden_alkupaiva + INTERVAL '5 years')::date
  AND oikeus.kieli::text <> ALL (ARRAY ['SWE10', 'ENG11', 'ENG12'])
  AND EXISTS (SELECT 1
              FROM yki_arvioija_arviointikausi kausi
              WHERE kausi.arvioija_id = oikeus.arvioija_id
                AND kausi.alkupaiva = oikeus.kauden_alkupaiva
                AND kausi.passivoitu IS NULL);

-- yki_arvioija_kausi on append-only muutosloki: rivi kertoo mita kirjattiin silloin kun se
-- kirjattiin, joten sita ei kirjoiteta uudelleen.
--
-- yki_arvioija.muokattu jatetaan myos koskematta, jolloin korjaus ei aja koko rekisteria
-- Solki-lahetysjonoon. Solkin kopiossa paattymispaiva on se, jonka Solki itse on aikanaan
-- antanut; yhden paivan ero korjaantuu kunkin arvioijan seuraavan muokkauksen yhteydessa.
-- Jos rivit halutaan lahettaa heti, se on oma paatoksensa ja tehdaan erikseen.
