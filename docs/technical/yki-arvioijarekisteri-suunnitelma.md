# YKI-arvioijarekisterin siirto Solkilta Kituun

> **Tila 17.9.2026: suunnitelma on toteutettu.** Vaiheet 1–10 ja 12 ovat mainissa (migraatiot
> V116–V126), ja arvioijarekisterin muokkaus on avattu untuvaan ja QA:han; tuotannossa
> muokkaus- ja integraatiokytkimet ovat yhä pois päältä.
>
> Dokumentti on säilytetty suunnittelun perustelujen vuoksi, ja sitä on ylläpidetty pykälä
> kerrallaan sitä mukaa kun toteutus on edennyt. Siksi **myöhemmät pykälät kumoavat aiemmat**, ja
> **ristiriitatilanteessa koodi voittaa dokumentin**. Merkittävimmät kumoutuneet kohdat on
> merkitty "Päivitys"-laatikoilla.

## Context

Tähän asti YKI-arvioijarekisteriä on hallinnoitu Jyväskylän yliopiston Solki-järjestelmässä, ja
Kielitutkintorekisteri (kitu) on ollut vain vastaanottava osapuoli: Solki työntää arvioijatiedot
kituun `POST /yki/api/arvioija` -rajapinnalla, ja kitu näyttää ne read-only-listana osoitteessa
`/yki/arvioijat`.

Vaatimusmäärittelyn (OPH:n käyttötapauskuvaus, `709673053_…-200826-1322-246.pdf`) mukaan hallinta
siirtyy kituun: **kitu on jatkossa arvioijadatan master**, OPH-virkailija ylläpitää rekisteriä kitun
syöttökäyttöliittymällä, ja kitu välittää tiedot rajapinnan yli Solkin YKI-sovellukseen.

Kolme käyttötapausta:

1. **Uuden arvioijan merkintä** — virkailija lisää arvioijan hallintopäätöksen perusteella. Järjestelmä
   syöttää arvioijan oppijanumeron, jolla henkilötiedot haetaan ONR:stä esitäytöksi,
   laskee 5 vuoden voimassaoloajan automaattisesti ja lähettää tiedot Solkille.
2. **Merkinnän muokkaus / uusi kausi** — virkailija syöttää uuden kauden alkupäivän, järjestelmä laskee
   päättymispäivän; myös yhteystietoja ja arviointioikeuksia voi muokata. Tiedot lähetetään Solkille.
3. **Passivointi** — järjestelmä passivoi arvioijan automaattisesti kauden päättymispäivän jälkeen;
   virkailija voi passivoida myös manuaalisesti kesken kauden.

Virhetilanteissa Solkille lähetys yritetään **3 kertaa** ja sen jälkeen **säännöllisesti uudestaan
(joka yö)**; virkailija näkee epäonnistuneet lähetykset kitun virhenäkymästä syineen.

Vaatimusmäärittelyn alustava työmääräarvio: syöttökäyttöliittymä ~5 vk (tietokanta 1 vk, lisäys 1 vk,
uusi hlö ONR:ään 1 vk, muokkaus + passivointi 2 vk) + Solki-integraatio 1 vk.

### Lähtötilanne koodissa (tarkistettu)

- Solkin **CSV-tuontiputki on jo purettu** (commitit `279bd81f`, `e0ff5d3f`, `d160c1f1`, `ff82bc5a`).
  Ainoa elävä sisääntulo on `POST /yki/api/arvioija`. `CLAUDE.md` ja
  `docs/technical/arkkitehtuuri.md` väittivät yhä CSV-putken olevan olemassa — **korjattu tässä PR:ssä**.
- Taulu `yki_arvioija_error` ja koko `yki/arvioijat/error/`-paketti on **kuollutta koodia**:
  kirjoituspolkua ei ole enää, joten `/yki/arvioijat/virheet` ja dashboardin laskuri ovat pysyvästi nollia.
- `dev/YkiController.kt` `GET /dev/yki/import/arvioijat` on kuollut stubi.
- Arvioijatiedoissa **ei ole koskaan ollut puhelinnumeroa**.
- `yki_arvioija`-rivit ovat jo kitussa, joten erillistä datan tuontia Solkilta ei tarvita — vain skeeman
  täydennys.
- Lomakekuvioita on olemassa (`vkt/html/VktErinomaisenArviointiPage.kt`), mutta **luontilomaketta eikä
  kenttäkohtaista validointivirheiden renderöintiä ei ole missään** — ne on rakennettava.
- Serverimoduulissa **ei ole JS-buildia** CLAUDE.md:n esbuild-väitteestä huolimatta; ainoa selainlogiikan
  mekanismi on `html/Common.kt`:n inline `javascript(code)`.

### Päätökset (sovittu)

| Kysymys             | Päätös                                                                                                                                                                                     |
| ------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Solkin rajapinta    | Ei ole vielä määritelty — **kitu ehdottaa sopimuksen**, sovitaan Jyväskylän kanssa                                                                                                         |
| Olemassa oleva data | Laajennetaan nykyisiä tauluja, `POST /yki/api/arvioija` poistetaan käytöstä                                                                                                                |
| Laajuus             | Koko PDF:n sisältö                                                                                                                                                                         |
| Käyttöoikeus        | Uusi `Authority`-arvo                                                                                                                                                                      |
| Kausi/tila-taso     | ~~Pysyy arviointioikeuskohtaisena~~ → **kumottu V122:ssa**: kausi on arvioijakohtainen (`yki_arvioija_arviointikausi`), ja `yki_arviointioikeus` on siitä johdettu projektio. Ks. §1.6     |
| **Kausihistoria**   | Talletetaan kantaan. ~~`yki_arvioija_kausi` = kausitaulu~~ → **kumottu V122/V123:ssa**: kausimaster on `yki_arvioija_arviointikausi`, ja `yki_arvioija_kausi` on enää muutosloki. Ks. §1.6 |
| Puhelinnumero       | **Ei toteuteta toistaiseksi** — puuttuu tavoitetilan tietotaulukosta (§11 kys. 16)                                                                                                         |
| Listanäkymä         | Täysi käsittely: suodatus, sivutus, CSV-vienti                                                                                                                                             |

### OPH:n vastaukset avoimiin kysymyksiin (21.8.2026)

Kaikki 14 OPH:lle esitettyä kysymystä on vastattu 21.8.2026. Vastaukset on viety suunnitelmaan;
**kuusi niistä muutti aiempaa ratkaisua** (merkitty ✱; kys. 4 on sittemmin kumottu). Loput avoimet asiat ovat §11:ssä.

| #    | Kysymys                        | Päätös                                                                                                                            |
| ---- | ------------------------------ | --------------------------------------------------------------------------------------------------------------------------------- |
| 1    | Kauden päättymispäivä          | **Korjattu 16.9.2026:** `alkupäivä + 5 v − 1 pv`, koska päättymispäivä on inklusiivinen (kys. 2)                                  |
| 2    | Päättymispäivän inklusiivisuus | Inklusiivinen — passivointi vasta päivän jälkeen                                                                                  |
| 3 ✱  | Jatkokausi                     | **Järjestelmä päättelee automaattisesti** kauden alkupäivästä (§2.8); ylikirjoitus poistettiin 31.8.2026                          |
| 4 ✱  | Yksilöimätön henkilö           | ~~Merkintä saa tallentua keskeneräisenä~~ **Kumottu 28.8.2026**: arvioijalla on aina jo oppijanumero, joten yksilöimätön hylätään |
| 5 ✱  | Turvakielto                    | **Ei rajoituksia tietoihin** — käyttöliittymässä näytetään varoitus virkailijalle                                                 |
| 6    | Merkinnän poisto               | Ei toteuteta; passivointi riittää                                                                                                 |
| 7    | Lukuoikeus                     | Säilyy kaikilla kitu-virkailijoilla                                                                                               |
| 8    | Hetu-sarake                    | **Säilyy pysyvästi** — ennen 2026 alkaneiden kausien hetut on säilytettävä lain nojalla                                           |
| 9    | Kausihistorian oikeusperuste   | Osa rekisterimerkinnän elinkaarta, sama 5 v säilytysaika                                                                          |
| 10 ✱ | Säilytysajan alkuhetki         | **Passivointihetkestä**, ei kauden päättymispäivästä — umpeutuneella merkinnällä se on kauden päättymispäivä (§6.2)               |
| 11 ✱ | Puhelinnumero                  | **Ei toteuteta** — tavoitetilan tietotaulukko pätee                                                                               |
| 12 ✱ | Hallintopäätöksen ASHA-numero  | **Toteutetaan** vapaana tekstikenttänä, ei muotovalidointia                                                                       |
| 13   | Käyttöoikeus Otuvaan           | OPH perustaa; pyyntö tehdään heti, jotta läpimenoaika ei estä julkaisua                                                           |
| 14   | Cutover                        | Syöttökäyttöliittymä julkaistaan ensin, Solkin kirjoitukset katkaistaan vasta sen jälkeen                                         |

Yksi vastaus laajentaa toteutusta merkittävästi:

- **Säilytysaika passivointihetkestä (kys. 10)** vaatii uuden `passivoitu`-aikaleiman, jota kannassa ei
  tällä hetkellä ole (§1.1, §6.2).

> **Muutos 28.8.2026 (kys. 4 kumottu).** Arvioijalla on aina jo oppijanumero ONR:ssä, kun hänet
> lisätään rekisteriin. Hetuhaku ja koko keskeneräisen yksilöinnin käsittely on siksi poistettu:
> lisäyslomake ottaa vastaan vain oppijanumeron, yksilöimätön henkilö hylätään kenttävirheellä
> (`OppijanumeroService.getOppijanumero`, joka ei putoa takaisin henkilö-OIDiin), eikä
> `yksilointi_kesken`-saraketta ole (V120 pudotti sen).

### Muut lähdedokumentit: prosessikuvaukset ja tallennettavat tiedot

Käyttötapauskuvauksen lisäksi käytössä on kaksi OPH:n prosessidokumenttia:

| Dokumentti                        | Sisältö                                                                                    |
| --------------------------------- | ------------------------------------------------------------------------------------------ |
| `699059123_…-210826-0946-254.pdf` | _Nykytilan ja uuden prosessin kuvaus_ — nykyprosessi + "miten prosessia halutaan uudistaa" |
| `721421207_…-210826-1009-258.pdf` | _Tavoitetila_ — sama prosessi tavoitetilassa + **tavoitetilan tietotaulukko**              |

Molempien lopussa on taulukko **"Arvioijarekisteriin tallennettavat tiedot"** (oikeusperuste, tietolähde,
säilytyspaikka, säilytysaika). **Tavoitetilan taulukko on suunnittelun kannalta ratkaiseva** — nykytilan
taulukkoa luetaan vain lähtötilanteen kuvauksena (sen rivi _"Rekisteriintuontiaika (kun tiedot on siirretty
Solkilta kielitutkintorekisteriin)"_ kuvaa nykyistä Solki→kitu-suuntaa).

Tavoitetilan taulukon mukaan kitussa säilytetään: oppijanumero, hetu vain ennen 2026 tehdyille merkinnöille,
sukunimi, etunimet, sähköpostiosoite, osoite, tila, tutkinnot (kieli ja taso), viimeisimmän
arviointikauden alku- ja päättymispäivä, jatkorekisteröinti, rekisteröintiaika, rekisteriintuontiaika ja
**hallintopäätöksen ASHA-numero**. Kaikilla säilytysaika **5 vuotta**.

Dokumenttien vahvistamat asiat:

- **Siirto Solkille tehdään arvioijan OID-tunnisteella:** _"Kielitutkintorekisteristä siirtyy uudet
  merkinnät Solkin YKI-kantaan arvioijan OID-tunnisteella."_ Vahvistaa §5.1:n avaimennusratkaisun.
- **Solki säilyttää omat lisätietonsa:** _"Solkilla on kopio arvioijarekisteristä (Solki voi täydentää
  rekisterimerkintöjä omilla tiedoillaan, esim. kauden arviointikerrat)."_ → §5.1:n täyden tilan PUT saa
  korvata **vain OPH:n omistamat kentät**, ei Solkin omia (arviointikerrat, huomautukset, lisätiedot,
  liitteet, puhelinnumerot, postinumero).
- **Kaikilla kielillä on sama kausi:** _"Pidetään edelleen käytäntönä, että henkilöllä ei ole päällekkäisiä
  arviointikausia vaan kaikilla sama kausi."_ → validointisääntö: kaikkien arviointioikeuksien kauden on
  oltava identtinen (§2.4). Vahvistaa käyttöliittymäratkaisun.
- **70 vuoden ikärajaa ei toteuteta:** _"Arviointikauden kesto on aina 5 vuotta, ei oteta teknisesti
  käyttöön 70v rajoitusta."_ (Solkin nykyjärjestelmässä kausi ei voi jatkua sen vuoden jälkeen, kun
  arvioija täyttää 70 — kituun tätä ei tule.)
- **Liitteet ja hallintopäätös eivät tule kituun.** Hallintopäätös laaditaan Ashassa ja tallennetaan
  liitteeksi Solkin järjestelmään; taulukossa _Liitteet_ on Solki-only. Kituun ei siis toteuteta
  liitteiden käsittelyä.
- **Säilytysaika kielitutkintorekisterissä on 5 vuotta** kaikille kitussa säilytettäville kentille.
  → **Uusi vaatimus, jota käyttötapauskuvauksessa ei ollut** (§6.2).
- **Uusi kieli tai taso liittyy olemassa olevaan kauteen:** _"Henkilön kaikilla kielillä ja eri
  tutkintotasoilla on yhtä pitkä arviointikausi. Jos henkilölle tulee uusi arvioitava kieli tai taitotaso,
  niin arviointikausi kaikille alkaa ja päättyy samaan aikaan."_ → kun virkailija lisää kielen kesken
  kauden, **järjestelmä kopioi voimassa olevan kauden päivämäärät uudelle arviointioikeudelle** eikä kysy
  uutta alkupäivää.
- **Arvioija on jo olemassa Solkissa ennen kitun lähetystä:** _"Solki tarvitsee arvioijille käyttäjätunnuksen
  ja viisinumeroisen arvioijatunnuksen YKI-sovellukseen, mutta arvioijarekisterimerkinnän tiedot voidaan
  lisätä arvioijan tietoihin kielitutkintorekisteristä OID-tunnisteella."_ Solki luo arvioijan tunnukset jo
  koulutuksen yhteydessä → kitun lähetys on käytännössä **päivitys olemassa olevaan Solki-riviin**, ei
  luonti (§11 JYU-kysymys 6).
- **Uusi kenttä: hallintopäätöksen ASHA-numero.** Tavoitetilan taulukossa on rivi _"Hallintopäätöksen
  ASHA-numero — Esittelijä kirjaa — Kielitutkintorekisteri — 5 vuotta"_, varauksella _"Tälle tiedolle ei ole
  välttämättä tarvetta. Katsotaan prosessivastaavan kanssa syksyllä."_ → varaudutaan valinnaiseen
  tekstikenttään, toteutus vasta kun tarve on vahvistettu (päätöstaulukko kys. 12).

**Kaksi tavoitetilakuvauksen kohtaa muuttaa aiempia suunnitelmapäätöksiä** — molemmat on merkitty
suunnitelmaan ja avoimiin kysymyksiin:

1. **Solki→kitu-muutosrajapintaa tarvitaan yhä.** Tavoitetilassa arvioijan omasta toiveesta tapahtuva
   kesken kauden passivointi kulkee _Solkista kituun_: _"Solki tekee merkinnän YKI-sovellukseen ja tieto
   siirretään muutosrajapintaa pitkin kielitutkintorekisteriin."_ Sisääntulevaa rajapintaa ei siis voi
   poistaa kokonaan (§4.2, §11 JYU-kysymys 5).
2. **Puhelinnumero puuttuu tavoitetilan tietotaulukosta.** Käyttötapauskuvaus vaatii sen lomakkeelle,
   mutta tavoitetilan taulukossa ei ole työpuhelinta, kotipuhelinta eikä puhelinnumeroa lainkaan
   (nykytilan taulukossa ne ovat Solki-only). Sarake jätetään toteuttamatta kunnes asia on ratkaistu
   (§1.1, ks. päätöstaulukko kys. 11).

Muut **ristiriidat ja tarkennukset** ovat avoimina kysymyksinä §11:ssä (päätöstaulukko ja §11).

---

## 1. Tietokanta

Suunnitteluhetkellä korkein käytössä oleva migraatio oli V115. **Toteutuksessa numerot menivät
toisin kuin alla on otsikoitu**, ja mainissa korkein on nyt **V126**. Toteutuneet numerot:

| Pykälä | Otsikossa | Todellinen tiedosto                    |
| ------ | --------- | -------------------------------------- |
| §1.1   | V116      | `V117__yki_arvioija_master.sql`        |
| §1.2   | V117      | `V118__yki_arvioija_kausihistoria.sql` |
| §1.3   | V118      | `V116__drop_yki_arvioija_error.sql`    |

Myöhemmät pykälät viittaavat migraatioihin oikeilla numeroilla. Jatka aina suurimmasta käytössä
olevasta eteenpäin, älä käytä numeroaukkoja.

### 1.1 `V117__yki_arvioija_master.sql` (suunniteltiin nimellä V116)

```sql
-- Kitu on YKI-arvioijarekisterin master. Arvioijatason lisäkentät, muokkausjäljet
-- ja Solki-lähetyksen outbox.
ALTER TABLE yki_arvioija
    -- HUOM. puhelinnumeroa EI lisätä: tavoitetilan tietotaulukossa ei ole puhelinnumeroa
    -- lainkaan, vaikka käyttötapauskuvaus pyytää sen lomakkeelle. Ks. päätöstaulukko kys. 11.
    ADD COLUMN luotu                        TIMESTAMPTZ NOT NULL DEFAULT now(),
    ADD COLUMN luoja_oid                    henkilo_oid,
    ADD COLUMN muokattu                     TIMESTAMPTZ NOT NULL DEFAULT now(),
    ADD COLUMN muokkaaja_oid                henkilo_oid,
    ADD COLUMN solkiin_lahetetty            TIMESTAMPTZ,
    ADD COLUMN solki_lahetysvirhe           TEXT,
    ADD COLUMN solki_lahetysyritykset       INTEGER     NOT NULL DEFAULT 0,
    ADD COLUMN solki_viimeisin_lahetysyritys TIMESTAMPTZ,
    -- Hallintopaatoksen ASHA-numero: vapaa tekstikentta, ei muotovalidointia (OPH kys. 12)
    ADD COLUMN asha_numero                  TEXT,
    -- Sailytysaika lasketaan passivointihetkesta, joten se on tallennettava (OPH kys. 10)
    ADD COLUMN passivoitu                   TIMESTAMPTZ;
-- Toteutunut V117 lisäsi tähän vielä sarakkeen yksilointi_kesken (OPH kys. 4);
-- se poistettiin V120:ssä, kun ONR-yksilöinnin odotuksesta luovuttiin (ks. 28.8.2026 yllä).

-- tila on Kotlinissa non-null mutta kannassa nullable
UPDATE yki_arviointioikeus SET tila = 'AKTIIVINEN' WHERE tila IS NULL;
ALTER TABLE yki_arviointioikeus
    ALTER COLUMN tila SET DEFAULT 'AKTIIVINEN',
    ALTER COLUMN tila SET NOT NULL;
-- HUOM: V121 kumosi nämä kaksi riviä (DROP NOT NULL, DROP DEFAULT). Sarake on
-- nykyään vanhentunut ja tila lasketaan kauden päivistä, ks. §1.5.

-- KRIITTINEN: ennen kituun siirtoa data on peräisin Solkista, joten sitä ei työnnetä takaisin.
-- Ilman tätä ensimmäinen yöajo lähettäisi koko historiallisen rekisterin Solkiin.
UPDATE yki_arvioija a
SET luotu             = COALESCE(t.viimeisin, now()),
    muokattu          = COALESCE(t.viimeisin, now()),
    solkiin_lahetetty = COALESCE(t.viimeisin, now())
FROM (SELECT arvioija_id, max(rekisteriintuontiaika) AS viimeisin
      FROM yki_arviointioikeus GROUP BY arvioija_id) t
WHERE a.id = t.arvioija_id;

UPDATE yki_arvioija
SET solkiin_lahetetty = COALESCE(solkiin_lahetetty, now());

-- Sailytysaika lasketaan passivointihetkesta. Jo passivoiduille vanhoille riveille hetki
-- taytetaan kauden paattymispaivasta, jotta niiden sailytysaika kuluu historiallisella
-- aikajanalla eika ala vasta kayttoonotosta.
UPDATE yki_arvioija a
SET passivoitu = t.paattyi
FROM (
    SELECT arvioija_id,
           max(kauden_paattymispaiva)::timestamptz AS paattyi
    FROM yki_arviointioikeus
    GROUP BY arvioija_id
    HAVING bool_and(tila = 'PASSIVOITU')
       AND max(kauden_paattymispaiva) IS NOT NULL
) t
WHERE a.id = t.arvioija_id;

CREATE INDEX yki_arvioija_solki_lahettamattomat_idx
    ON yki_arvioija (muokattu)
    WHERE solkiin_lahetetty IS NULL OR solkiin_lahetetty < muokattu;

CREATE INDEX yki_arviointioikeus_passivointi_idx
    ON yki_arviointioikeus (kauden_paattymispaiva)
    WHERE tila = 'AKTIIVINEN';

COMMENT ON COLUMN yki_arvioija.asha_numero IS 'Rekisterimerkintaa koskevan hallintopaatoksen ASHA-numero, esittelijan kirjaama';
COMMENT ON COLUMN yki_arvioija.passivoitu IS 'Hetki jolloin merkinta passivoitiin; sailytysajan (5 v) laskennan alkupiste';
COMMENT ON COLUMN yki_arvioija.solkiin_lahetetty IS 'Milloin rivi on viimeksi onnistuneesti lähetetty Solkiin; NULL tai < muokattu = lähetys kesken';
COMMENT ON COLUMN yki_arvioija.solki_lahetysvirhe IS 'Viimeisimmän epäonnistuneen Solki-lähetyksen virheteksti';
COMMENT ON COLUMN yki_arvioija.solki_lahetysyritykset IS 'Peräkkäisten epäonnistuneiden lähetysyritysten määrä; nollataan onnistuneessa lähetyksessä';
```

Ei triggereitä (projektissa ei ole yhtään) — `muokattu` asetetaan aina eksplisiittisesti UPDATE-lauseissa.

### 1.2 `V118__yki_arvioija_kausihistoria.sql` (suunniteltiin nimellä V117)

Kausi ja tila **pysyvät** `yki_arviointioikeus`-rivillä (= nykyinen, voimassa oleva kausi). Sen rinnalle
tulee append-only-historiataulu, johon kirjataan jokainen kausi. Tällä mallilla kaikki nykyiset kyselyt,
Solki-payload ja passivointi toimivat ennallaan, ja historia on erillinen luettava aikajana.

```sql
CREATE TABLE yki_arvioija_kausi (
    id                     INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    arvioija_id            INTEGER            NOT NULL REFERENCES yki_arvioija (id) ON DELETE CASCADE ON UPDATE CASCADE,
    kieli                  yki_tutkintokieli  NOT NULL,
    tasot                  TEXT[]             NOT NULL,
    tila                   yki_arvioija_tila  NOT NULL,
    kauden_alkupaiva       DATE,
    kauden_paattymispaiva  DATE,
    jatkorekisterointi     BOOLEAN            NOT NULL DEFAULT FALSE,
    kirjattu               TIMESTAMPTZ        NOT NULL DEFAULT now(),
    kirjaaja_oid           henkilo_oid,
    CONSTRAINT yki_arvioija_kausi_unique UNIQUE NULLS NOT DISTINCT
        (arvioija_id, kieli, tasot, kauden_alkupaiva, kauden_paattymispaiva, tila, jatkorekisterointi)
);

CREATE INDEX yki_arvioija_kausi_arvioija_idx ON yki_arvioija_kausi (arvioija_id, kirjattu DESC);

-- Nykytila historian ensimmäiseksi riviksi
INSERT INTO yki_arvioija_kausi
    (arvioija_id, kieli, tasot, tila, kauden_alkupaiva, kauden_paattymispaiva, jatkorekisterointi, kirjattu)
SELECT arvioija_id, kieli, tasot, tila, kauden_alkupaiva, kauden_paattymispaiva,
       jatkorekisterointi, rekisteriintuontiaika
FROM yki_arviointioikeus;

COMMENT ON TABLE yki_arvioija_kausi IS 'Arvioijarekisterimerkintöjen kausihistoria: yksi rivi jokaisesta kirjatusta arviointikaudesta kielikohtaisesti. Voimassa oleva kausi elää yki_arviointioikeus-taulussa.';
```

**Kirjoitussääntö:** jokainen tallennus vertaa uutta kautta voimassa olevaan, ja lisää historiarivin vain
kun jokin kausikentistä muuttuu (`ON CONFLICT DO NOTHING` uniikkiehdon turvin). Näin pelkkä yhteystiedon
korjaus ei kasvata historiaa.

### 1.3 `V116__drop_yki_arvioija_error.sql` (suunniteltiin nimellä V118)

```sql
-- Taulun kirjoituspolku poistui CSV-tuonnin mukana (commitit 279bd81f, e0ff5d3f,
-- d160c1f1, ff82bc5a). Solki-lähetysten virheet elävät nyt yki_arvioija-rivillä.
DROP TABLE IF EXISTS yki_arvioija_error;
```

### 1.4 Entiteetit

`yki/arvioijat/YkiArvioijaEntity.kt`:

- `YkiArvioijaEntity` += `luotu`, `luojaOid`, `muokattu`, `muokkaajaOid`,
  `solkiinLahetetty`, `solkiLahetysvirhe`, `solkiLahetysyritykset`, `solkiViimeisinLahetysyritys`.
  `RowMapper`-companion päivitetään vastaavasti.
- `YkiArviointioikeusEntity` säilyy sellaisenaan.
- Uusi `YkiArvioijaKausiEntity` (`@Table("yki_arvioija_kausi")`) + `fromRow`.
- `YkiArvioijaArviointioikeus`-liitosprojektio (`YkiArvioijaRepository.kt`) laajenee uusilla kentillä.
- `henkilotunnus` jää sarakkeeksi mutta **kirjoituspolku ei enää koskaan aseta sitä** (2026 lainmuutos).
  Sarake **säilyy pysyvästi** — §11 kys. 10 ratkaistiin 28.8.2026 niin, ettei poistoa suunnitella.

### 1.5 `V121__deprecate_yki_arvioija_tila.sql` — tila lasketaan kauden päivistä

Tallennettu `tila` osoittautui käytössä kestämättömäksi: lomake ei kanna sitä, joten tallennus peri sen
vanhalta riviltä ja passivoitu arvioija jäi passiiviseksi vaikka hänelle kirjattiin uusi kausi. Sarake on
siksi **vanhentunut** (nullable, ei defaultia) ja tila lasketaan `Rekisterointitila.laske`ssa:

| ehto                                         | tila            |
| -------------------------------------------- | --------------- |
| tallennettu `tila` = PASSIVOITU              | PASSIVOITU      |
| `kauden_paattymispaiva` ei-tyhjä ja < tänään | PASSIVOITU      |
| `kauden_alkupaiva` ei-tyhjä ja > tänään      | TULEVAISUUDESSA |
| muuten                                       | AKTIIVINEN      |

Kitu kirjoittaa sarakkeeseen aina NULLin; ainoa ei-tyhjän arvon kirjoittaja on Solkin push. Tallennettu
`AKTIIVINEN` sivuutetaan, koska V117 täytti sarakkeen sillä eikä se siten kanna tietoa — näin laskenta
pätee koko rekisteriin ilman datamigraatiota.

**Päivämäärät voittavat myös siirtymän aikana (päätös 1.9.2026).** Sivuuttaminen ei koske vain V117:n
täytearvoja: myös Solkin juuri lähettämä `AKTIIVINEN` jää huomiotta, jos kausi on päivien perusteella
päättynyt, ja merkintä näkyy passiivisena. Tämä on tietoinen valinta — kausi on se, mihin
arviointioikeus perustuu — ja se on ainoa kohta, jossa laskenta ohittaa Solkin nimenomaisen väitteen.
Vastakkainen suunta säilyy: tallennettu `PASSIVOITU` ohittaa voimassa olevatkin päivät, joten Solki voi
yhä passivoida kesken kauden niin kauan kuin se on master. Sama sääntö on kirjoitettu myös SQL:ksi
(`Rekisterointitila.SQL`) listanäkymän suodatusta ja lajittelua varten, ja `YkiArvioijaTilaSqlTest`
vartioi että toteutukset pysyvät yhtenevinä. Tarkasteluhetki sidotaan parametrina `TimeService`ltä, ei
`CURRENT_DATE`:na, jotta kiinnitetty testikello ohjaa myös SQL:ää.

**Passivointihetki on manuaalisen passivoinnin leima, ei tilan johdannainen.** Kun kausi umpeutuu
itsestään, tila muuttuu PASSIVOITUksi mutta `passivoitu` jää NULLiksi — juuri se erottaa
hallintopäätöksellä passivoidun merkinnän luonnollisesti päättyneestä. Säilytysajan laskenta lukee
molempia, ks. §6.2.

---

### 1.6 `V122__yki_arvioija_arviointikausi.sql` ja `V123__yki_arvioija_kausi_muutosloki.sql` — kausi masteriksi

Arviointikausi ei ollut hallittava olio: voimassa oleva kausi eli `yki_arviointioikeus`-rivillä ja
`yki_arvioija_kausi` oli append-only-loki. Kummallakaan ei ole pysyvää tunnistetta, jolla yksittäisen
kauden voisi muokata tai poistaa, joten tietosivulla ei ollut mitään kausikohtaista toimintoa.

**V122** lisää masteriksi `yki_arvioija_arviointikausi`-taulun (`alkupaiva`, `paattymispaiva`,
`passivoitu`, luonti- ja muokkausleimat) ja sen kielirivit `yki_arvioija_arviointikausi_oikeus`.
Kausi on **arvioijakohtainen**, ei kielikohtainen. `ensimmainen_rekisterointipaiva` siirtyy
arvioijatasolle sarakkeeksi `yki_arvioija.arvioijan_ensimmainen_rekisterointipaiva`: se on
arvioijakohtainen tieto ja V63:n Solki-tuonnissa vanhempi kuin yksikään rekonstruoitava kausi, joten
sitä ei saa laskea uudelleen kausista. Nimi poikkeaa tarkoituksella `yki_arviointioikeus`-sarakkeesta,
koska `SELECT yki_arvioija.*` liitoksessa samannimiset sarakkeet osuisivat päällekkäin.

`yki_arviointioikeus` **jää paikalleen johdettuna projektiona**, jotta `Rekisterointitila.SQL`,
listanäkymän kysely ja Solki-lähetys jatkavat ennallaan.

Siirto **deduplikoi lokin ensin** tasolla `(arvioija_id, alkupäivä, kieli)`: passivointi katkaisee
päättymispäivän, jolloin V119:n uniikkiehto päästää läpi toisen rivin samasta kaudesta, ja pelkkä
päiväpariin ryhmittely tekisi yhdestä kaudesta kaksi päällekkäistä. Alkupäivättömiä rivejä ei siirretä,
eikä vanhentuneita kieliä (`SWE10`, `ENG11`, `ENG12`), joita ei voi myöntää eikä perua. Projektion
kirjoitus on no-op arvioijalle jolla ei ole yhtään kautta, joten kumpikin ryhmä säilyy koskemattomana.

`yki_arvioija_arviointikausi` ei rajoita päivien järjestystä kannassa: tuodussa datassa on rivejä
joilla päättymispäivä edeltää alkupäivää, ja kovaehto tekisi niistä korjauskelvottomia. Järjestys
tarkistetaan validoinnissa uusille ja muokatuille kausille.

**V124** siirtää hallintopäätöksen **ASHA-numeron arvioijalta kaudelle**. Numero yksilöi yhden
hallintopäätöksen ja jokainen arviointikausi on oma päätöksensä, joten arvioijatasolla jatkokausi olisi
ylikirjoittanut edellisen kauden päätösviitteen. Sarake tulee myös projektioon ja muutoslokiin, jotta
listanäkymän haku, lajittelu ja CSV-vienti jatkavat ennallaan ja viitteen korjaus näkyy historiassa.
Siirrossa nykyinen arvo annetaan **viimeisimmälle** kaudelle; vanhemmille sitä ei monisteta, koska ne
ovat eri päätöksiä joiden viitettä ei tiedetä.

**V123** tekee `yki_arvioija_kaudesta` pelkän muutoslokin: `toimenpide`- ja `kausi_id`-sarakkeet ja
uniikkiehdon poisto. Taulun **nimi säilyy**, koska uudelleennimeäminen rikkoisi rinnakkain ajossa
olevan vanhan sovellusversion tietosivun rullaavan julkaisun ajaksi. Uniikkiehto oli olemassa vain
vaimentamaan muuttumattoman kauden kirjaus; nyt rivi kirjataan vain kun kausi tosiasiassa muuttuu, ja
saman kauden uudelleenlisäys poiston jälkeen on aito tapahtuma jonka on näyttävä lokissa.

**Säilytysajan suojaus.** Projektio osoittaa yhteen kauteen ja riippuu kuluvasta päivästä, joten se voi
osoittaa päättyneeseen kauteen vaikka arvioijalla on uudempi voimassa oleva. `Sailytysaika.ALKUHETKI_SQL`
ottaa siksi `GREATEST`in projektiosta ja masterista, ja `poistaSailytysajanYlittaneet` tarkistaa
erikseen, ettei masterissa ole voimassa olevaa kautta. Ilman näitä vanhentunut projektio poistaisi
peruuttamattomasti yhä voimassa olevan merkinnän.

### 1.7 `V126__korjaa_arviointikauden_paattymispaiva.sql` — vanhat rivit inklusiiviseen sääntöön

Laskenta korjattiin `+5 v − 1 pv`:ksi (§2.1), mutta vanhalla säännöllä syntyneet rivit jäivät
kantaan. V126 siirtää ne päivää aikaisemmiksi.

Ehdoksi kirjoitetaan **täsmälleen vanha sääntö** (`paattymispaiva = alkupaiva + 5 v`) eikä "siirrä
kaikkia": muut päiväparit ovat joko kesken kauden katkaistuja tai Solkista tuotuja poikkeamia, ja
yhden päivän siirto on hiljainen muutos hallintopäätöksen sisältöön. Postgresin
interval-aritmetiikka leikkaa karkauspäivän samoin kuin `java.time`n `plusYears`
(`2024-02-29 + 5 v = 2029-02-28`), joten ehto osuu tasan niihin riveihin jotka vanha laskenta
tuotti.

**Master ja projektio on korjattava samalla ehdolla.** Jos ne jäisivät eri linjoille, yöllinen
`paivitaArvioijaProjektiot` kirjoittaisi projektion ja kutsuisi `merkitseMuuttuneeksi`, jolloin
korjaus valuisi Solki-lähetysjonoon rivi kerrallaan. Ulkopuolelle jäävät siksi passivoidut kaudet
(päättymispäivä on passivointipäivä, ei laskettu arvo) ja jäädytetyt vanhentuneiden kielten rivit,
joilla ei ole kautta jota seurata — niillä voi olla sama alkupäivä kuin hallitulla kaudella, joten
ne on suljettava pois erikseen.

`yki_arvioija_kausi` on append-only muutosloki eikä sitä kirjoiteta uudelleen.
`yki_arvioija.muokattu` jätetään myös koskematta, jolloin korjaus **ei** aja koko rekisteriä
Solki-lähetysjonoon: Solkin kopiossa päättymispäivä on se jonka Solki itse on aikanaan antanut, ja
yhden päivän ero korjaantuu kunkin arvioijan seuraavan muokkauksen yhteydessä. Rivien lähettäminen
heti on oma päätöksensä ja tehdään erikseen.

`YkiArvioijaKausimigraatioTest` lukee migraation luokkapolusta ja ajaa sen uudelleen, jolloin
kumpikin osumaehto on kiinnitetty testiin.

---

## 2. Palvelukerros

Uudet tiedostot pakettiin `server/src/main/kotlin/fi/oph/kitu/yki/arvioijat/`. Kaikki riippuvuudet
konkreettiselle `@Service`-luokalle — **ei abstraktia kantaluokkaa**, koska `@WithSpan` + CGLIB lukee
kantaluokan injektoidut kentät `null`iksi (CLAUDE.md).

### 2.1 `Arviointikausi.kt` — 5 vuoden laskenta yhdessä paikassa

```kotlin
object Arviointikausi {
    const val KAUDEN_PITUUS_VUOSINA = 5L

    /** Päättymispäivä on inklusiivinen, joten viisi vuotta päättyy vuosipäivää edeltävänä päivänä. */
    fun paattymispaiva(alkupaiva: LocalDate): LocalDate =
        alkupaiva.plusYears(KAUDEN_PITUUS_VUOSINA).minusDays(1)
}
```

**Korjattu 16.9.2026: `plusYears(5).minusDays(1)`.** Päättymispäivä on kauden viimeinen
voimassaolopäivä (kys. 2), joten `plusYears(5)` teki kaudesta 5 v + 1 pv ja esti jatkokauden
alkamisen vuosipäivänä. Tuotu Solki-data käyttää yhä +5 v samaa päivää (`2015-12-07 → 2020-12-07`);
sitä ei lasketa uudelleen, vaan sääntö koskee kitussa kirjattavia kausia.
Puhdas `object`, ei `@Service` — ei CGLIB-proxya, testattavissa ilman Spring-kontekstia.

### 2.2 `YkiArvioijaCommand.kt` — sisäinen komento, korvaa poistuvan API-DTO:n

```kotlin
data class TallennaArvioija(
    val arvioijaOid: Oid,
    val sukunimi: String,
    val etunimet: String,
    val sahkopostiosoite: String?,
    val katuosoite: String,
    val postinumero: String,
    val postitoimipaikka: String,
    val kaudenAlkupaiva: LocalDate,
    val ashaNumero: String?,
    val arviointioikeudet: List<Arviointioikeus>,
) {
    /** Sama kausi kaikille valituille kielille — vaatimus: "5 vuotta kaikille tutkintokielille ja tasoille". */
    val kaudenPaattymispaiva: LocalDate get() = Arviointikausi.paattymispaiva(kaudenAlkupaiva)

    data class Arviointioikeus(val kieli: Tutkintokieli, val tasot: Set<Tutkintotaso>)
}
```

**Ei `henkilotunnus`-kenttää** — hetua ei käsitellä missään vaiheessa; avain on oppijanumero.

### 2.3 `YkiArvioijaError.kt` — Either-vasemman puolen tyyppi

```kotlin
sealed interface YkiArvioijaError {
    data class Validointivirheet(val virheet: NonEmptyList<ValidationError>) : YkiArvioijaError
    data class OppijanumeroaEiSaatu(val syy: OppijanumeroException) : YkiArvioijaError
    data class OppijaaEiYksiloity(val oid: Oid?) : YkiArvioijaError
    data object ArvioijaaEiLoydy : YkiArvioijaError
}
```

### 2.4 `YkiArvioijaValidation.kt` — kirjoitetaan uusiksi tyypille `TallennaArvioija`

Säilytetään nykyinen rakenne (`accumulate { accumulating { … } }`, `util/validation/Validation.kt`) ja
ONR-olemassaolotarkistus (`OppijanumeroValidation.validateOppijanumeroInOnr`). Lisättävät säännöt:
pakolliset kentät, postinumeron muoto (5 numeroa), sähköpostin muoto, vähintään yksi arviointioikeus,
ei tyhjiä tasojoukkoja, ei duplikaattikieliä, kauden alkupäivä ei yli vuotta tulevaisuudessa.

> **Päivitys (V122).** Kaksi alla kuvattua sääntöä **eivät ole enää validointisääntöjä vaan
> rakenteellisia**, ks. §1.6. Arviointikausi on oma rivinsä, jolla on yksi päiväpari ja N
> kielikohtaista oikeusriviä, joten kielet eivät voi olla eri kausilla eikä kielen lisääminen voi
> aloittaa uutta kautta. `ensure(...)`-tarkistus olisi kuollutta koodia. Tilalle tuli **päällekkäisten
> kausien kielto** (`TallennaKausiValidation.validateEiPaallekkaisyytta`), jota ei aiemmin ollut.

Tavoitetilakuvauksesta johdettu lisäsääntö: **kaikkien arviointioikeuksien kauden on oltava identtinen**
(_"Henkilön kaikilla kielillä ja eri tutkintotasoilla on yhtä pitkä arviointikausi"_).

Kun virkailija **lisää kielen tai tason kesken kauden**, uusi arviointioikeus saa voimassa olevan kauden
päivämäärät (_"Jos henkilölle tulee uusi arvioitava kieli tai taitotaso, niin arviointikausi kaikille alkaa
ja päättyy samaan aikaan"_) — lomake ei kysy uutta alkupäivää.

`ValidationError.path` täytetään lomakkeen kenttänimillä (`listOf("postinumero")`) — se on §3.1:n
`FormErrors`-kartan avain. Hetun siirtokielto (`kitu.validaatiot.yki.hetunSiirronRajapaiva`) poistuu tästä
validaattorista, koska hetua ei enää oteta vastaan lainkaan; property jää `YkiSuoritusValidation`in käyttöön.

### 2.5 `YkiArvioijaService.kt`

```kotlin
@Service
class YkiArvioijaService(
    private val repository: YkiArvioijaRepository,
    private val validationService: ValidationService,
    private val solki: SolkiArvioijaService,
    private val oppijanumeroHaku: OppijanumeroHakuService,
    private val oppijanumeroService: OppijanumeroService,
    private val auditLogger: AuditLogger,
    private val timeService: TimeService,
) {
    @WithSpan fun haeArvioija(id: Int): YkiArvioijaEntity?                       // + YkiArvioijaViewed
    @WithSpan fun haeKausihistoria(arvioijaId: Int): List<YkiArvioijaKausiEntity>
    @WithSpan fun haeSivullinen(params: YkiArvioijaParams): List<YkiArvioijaListRow>
    @WithSpan fun laske(params: YkiArvioijaParams): Long

    @WithSpan fun haeHenkilotiedot(haku: OnrHaku): Either<YkiArvioijaError, ArvioijanEsitaytto>
    @WithSpan fun luoArvioija(k: TallennaArvioija, tekija: Oid?): Either<YkiArvioijaError, YkiArvioijaEntity>
    @WithSpan fun paivitaArvioija(id: Int, k: TallennaArvioija, tekija: Oid?): Either<YkiArvioijaError, YkiArvioijaEntity>
    @WithSpan fun passivoiArvioija(id: Int, tekija: Oid?): Either<YkiArvioijaError, YkiArvioijaEntity>

    /** Ajastettu (§6.2). */ @WithSpan fun poistaSailytysajanYlittaneet(): Int
}
```

`luoArvioija` / `paivitaArvioija` kulku:

1. `validationService.validateAndEnrich(komento)` → `Left(Validointivirheet)` epäonnistuessa.
   **Ei `getOrThrow()`** — lomakepolku tarvitsee kenttäkohtaiset virheet, ei 400-JSONia.
2. `repository.tallenna(...)` yhdessä transaktiossa: upsert `yki_arvioija`, korvaa `yki_arviointioikeus`
   -rivit (myös poistot), kirjaa muuttuneet kaudet `yki_arvioija_kausi`-tauluun.
   Samalla `muokattu = now()`, `muokkaaja_oid`, `solkiin_lahetetty = NULL`, `solki_lahetysvirhe = NULL`,
   `solki_lahetysyritykset = 0`.
3. `auditLogger.log(YkiArvioijaCreated | YkiArvioijaUpdated, komento.arvioijaOid)`.
4. `solki.lahetaArvioija(id)` — **yksi synkroninen yritys**, jotta virkailija näkee tuloksen heti.
   Epäonnistuminen ei kaada tallennusta; rivi jää outboxiin.
5. `Right(entity)` — controller lukee `solkiLahetysvirhe`-kentän flash-viestiä varten.

**ONR-haku:** virkailija syöttää oppijanumeron, ja `OppijanumeroService.getOppijanumero(oid)`
ratkaisee sen ONR:n master-oppijanumeroksi. Metodi on `getMasterOid`in tiukka rinnakkaismuoto: se
**ei** palauta henkilö-OIDia korvikkeena, joten yksilöimätön henkilö päätyy
`OppijaNotIdentifiedException`in kautta `OppijaaEiYksiloity`-virheeksi, jonka lomake renderöi
`oppijanumero`-kentän virheeksi. `getMasterOid`in fallbackia ei saa kiristää — siitä riippuvat
virkailijan asiointikielen haku ja yksilöimättömien oppijoiden suoritusnäkymät.
Esitäyttö `getHenkiloByMasterOid(oid)`:n `etunimet`/`kutsumanimi`/`sukunimi` +
`yhteystiedotRyhma[].yhteystieto[]`-arvoista (sähköposti, osoite).

### 2.8 Jatkokauden päättely (OPH kys. 3)

`jatkorekisterointi` **johdetaan palvelimella** eikä tule lomakkeelta: se on `true`, kun tallennettava
kausi alkaa myöhemmin kuin merkinnän `ensimmainen_rekisterointipaiva`. Johdanto on idempotentti, joten
pelkkä yhteystiedon korjaus ei käännä lippua eikä siten kasvata kausihistoriaa. Lomakkeen valintaruutu
poistettiin: virkailijan ylikirjoitus saattoi tuottaa historian kanssa ristiriitaisen arvon.

### 2.6 `YkiArvioijaRepository.kt`

Poistetaan `saveAllNewEntities` (kuoli CSV-tuonnin mukana). `upsert` korvataan `tallenna`-metodilla.

```kotlin
interface CustomYkiArvioijaRepository {
    @Transactional fun tallenna(arvioija: YkiArvioijaEntity, tekija: Oid?): Int
    fun findByArvioijaOid(oid: Oid): YkiArvioijaEntity?
    fun findById(id: Int): YkiArvioijaEntity?
    fun findKausihistoria(arvioijaId: Int): List<YkiArvioijaKausiEntity>
    fun findForListView(params: YkiArvioijaParams): List<YkiArvioijaListRow>
    fun count(params: YkiArvioijaParams): Long

    // Solki-outbox
    fun findLahetettavat(maxYritykset: Int?): List<YkiArvioijaEntity>
    fun merkitseLahetetyksi(id: Int)
    fun merkitseLahetysvirhe(id: Int, virhe: String)

    // Säilytysajan valvonta (§6.2)
    @Transactional fun poistaSailytysajanYlittaneet(tanaan: LocalDate, raja: LocalDate): List<Oid>
}
```

Keskeiset SQL:t:

```sql
-- tallenna(lahde = KITU): arviointioikeuksien TÄYSI korvaus. Nykyinen upsert ei poista koskaan
-- mitään, mikä masterina on virhe: peruttu kielioikeus jäisi roikkumaan ja lähtisi Solkiin.
-- HUOM. sisääntuleva POST /yki/api/arvioija kutsuu tätä lähteellä SOLKI, ks. §4.2.
DELETE FROM yki_arviointioikeus
WHERE arvioija_id = :id AND kieli <> ALL (:kielet::yki_tutkintokieli[]);

-- tallenna(): kausihistorian kirjaus (vain aidosti muuttuneet). Uniikkiehto sisaltaa myos tasot
-- (V119), koska saman kauden sisalla myonnetty uusi tutkintotaso on hallintopaatoksen tulos ja
-- kuuluu historiaan. Tasot normalisoidaan kirjoitettaessa aakkosjarjestykseen, koska taulukon
-- jarjestys on osa sen identiteettia.
INSERT INTO yki_arvioija_kausi
    (arvioija_id, kieli, tasot, tila, kauden_alkupaiva, kauden_paattymispaiva, jatkorekisterointi, kirjaaja_oid)
VALUES (:id, :kieli, :tasot, :tila, :alku, :loppu, :jatko, :tekija)
ON CONFLICT ON CONSTRAINT yki_arvioija_kausi_unique DO NOTHING;

-- findLahetettavat(3)    -> nopeat uusinnat
-- findLahetettavat(null) -> yöllinen kaikkien läpikäynti
SELECT * FROM yki_arvioija
WHERE (solkiin_lahetetty IS NULL OR solkiin_lahetetty < muokattu)
  -- AND solki_lahetysyritykset < :maxYritykset   (vain kun maxYritykset != null)
ORDER BY muokattu;

-- poistaSailytysajanYlittaneet(): merkinnät joiden säilytysaika on umpeutunut.
-- Passiivisuus luetaan samasta lausekkeesta kuin näkymissä (Rekisterointitila.SQL) ja
-- alkuhetki on COALESCE(passivoitu, max(kauden_paattymispaiva)), ks. §6.2.
DELETE FROM yki_arvioija
WHERE EXISTS (SELECT 1 FROM yki_arviointioikeus WHERE arvioija_id = yki_arvioija.id)
  AND NOT EXISTS (
        SELECT 1 FROM yki_arviointioikeus
        WHERE yki_arviointioikeus.arvioija_id = yki_arvioija.id
          AND <Rekisterointitila.SQL> <> 'PASSIVOITU'
      )
  AND COALESCE(
        yki_arvioija.passivoitu::date,
        (SELECT max(kauden_paattymispaiva) FROM yki_arviointioikeus
         WHERE yki_arviointioikeus.arvioija_id = yki_arvioija.id)
      ) < :raja
RETURNING arvioija_oid;
```

Päättymispäivä on **inklusiivinen**: arvioija on aktiivinen vielä päättymispäivänä ja passiivinen
seuraavasta päivästä (§1.5). Manuaalinen passivointi päättää kauden kuluvaan päivään ja kirjaa uuden
rivin `yki_arvioija_kausi`-tauluun.

Litteä listaprojektio (vrt. `VktSuoritusFlat`) `YkiArvioijaListRow` rakennetaan **kahdella kyselyllä**
(arvioijat sivullisittain + arviointioikeudet `WHERE arvioija_id IN (…)`) ja zipataan repositoryssa —
luettavampi kuin `json_agg`, ja taulu on satojen rivien kokoluokkaa.

---

## 3. Käyttöliittymä

### 3.1 Uudelleenkäytettävä lomakevirhekehys (uusi — sellaista ei ole)

`server/src/main/kotlin/fi/oph/kitu/html/FormErrors.kt`:

```kotlin
/** Kenttäpolkuun sidotut validointivirheet lomakerenderöintiä varten. */
class FormErrors private constructor(private val byPath: Map<String, List<String>>) {
    operator fun get(name: String): List<String> = byPath[name].orEmpty()
    val yleiset: List<String> get() = this[""]
    fun isEmpty() = byPath.isEmpty()

    companion object {
        val EMPTY = FormErrors(emptyMap())
        fun of(errors: Iterable<ValidationError>) =
            FormErrors(errors.groupBy({ it.path.joinToString(".") }, { it.message }))
    }
}

/** Pico.css-yhteensopiva kenttäkääre: label, syöte, aria-invalid ja virheteksti. */
fun FlowContent.formField(
    label: LocalizedString,
    name: String,
    errors: FormErrors = FormErrors.EMPTY,
    testId: String? = null,
    input: FlowContent.(invalid: Boolean) -> Unit,
)

fun FlowContent.formErrorSummary(errors: FormErrors)   // errorMessage(...) yleisistä virheistä
```

Ei uutta CSS:ää eikä JS:ää — Pico tyylittää `aria-invalid`in valmiiksi, ja `error-text`-luokka on jo
`style.css`:ssä (`ViewMessageType.ERROR`).

**Round-trip:** POST-käsittelijä palauttaa aina `ResponseEntity<String>`:

- virhe → `ResponseEntity.ok(sivu.render(form, FormErrors.of(virheet)))` — HTTP 200, syötteet säilyvät,
  ei session-tilaa
- onnistuminen → `303 See Other` + `Location: Links.Yki.arvioija(id)` + `ViewMessage`-flash

Yksi paluutyyppi, ei `RedirectView`/`@ResponseBody`-sekamelskaa.

### 3.2 Reitit — uusi `yki/arvioijat/YkiArvioijaViewController.kt`

`arvioijatView` ja `arvioijatVirheetView` **siirretään pois** paisuvasta `YkiViewController`:sta.

| Metodi | Polku                                | Tarkoitus                                   |
| ------ | ------------------------------------ | ------------------------------------------- |
| GET    | `/yki/arvioijat`                     | lista (suodatus, järjestys, sivutus)        |
| GET    | `/yki/arvioijat/uusi`                | lisäyslomake: oppijanumeron syöttö          |
| POST   | `/yki/arvioijat/uusi/haku`           | ONR-haku → vaihe 2 esitäytettynä (200 HTML) |
| POST   | `/yki/arvioijat/uusi`                | luonti → 303 `/yki/arvioijat/{id}`          |
| GET    | `/yki/arvioijat/{id}`                | tiedot, kausihistoria + muokkauslomake      |
| POST   | `/yki/arvioijat/{id}`                | tallennus → 303 self                        |
| POST   | `/yki/arvioijat/{id}/passivoi`       | manuaalinen passivointi → 303 self          |
| POST   | `/yki/arvioijat/{id}/laheta-solkiin` | manuaalinen uudelleenlähetys → 303 takaisin |
| GET    | `/yki/arvioijat/virheet`             | Solki-lähetysvirheiden näkymä               |

`webmvc/Links.kt`-lisäykset: `arvioija(id)`, `uusiArvioija()`, `arvioijaHaku()`, `passivoiArvioija(id)`,
`lahetaArvioijaSolkiin(id)`, `arvioijatCsv()`. HATEOAS-sääntö: `linkTo(methodOn(C::class.java).method(...))`,
lambdan **paluuarvo** ratkaisee (`(C) -> Any`, ei `C.() -> Unit`).

### 3.3 Listanäkymä (`YkiArvioijaPage.kt` uusiksi)

`YkiArvioijaColumn` annotoidaan `@ColumnTags(...)` (nyt annotaatioita ei ole lainkaan, joten enum on
näkymätön tagipohjaiselle `DisplayTableColumn.of`:lle) ja siirtyy tyypille `YkiArvioijaListRow`:

| Sarake                                                                  | Tagit                                   |
| ----------------------------------------------------------------------- | --------------------------------------- |
| `Toiminto` (linkki "Näytä")                                             | `LIST_VIEW`                             |
| `Oppijanumero`, `Sukunimi`, `Etunimet`                                  | `LIST_VIEW, CSV_EXPORT, PERSONAL_DATA`  |
| `Sahkoposti`                                                            | `CSV_EXPORT, PERSONAL_DATA`             |
| `Osoite`                                                                | `LIST_VIEW, CSV_EXPORT, PERSONAL_DATA`  |
| `Kieli`, `Tasot`                                                        | `LIST_VIEW, CSV_EXPORT`                 |
| `Tila`, `KaudenAlkupaiva`, `KaudenPaattymispaiva`, `Jatkorekisterointi` | `LIST_VIEW, CSV_EXPORT`                 |
| `EnsimmainenRekisterointipaiva`, `Muokattu`, `AshaNumero`               | `CSV_EXPORT`                            |
| `SolkiTila` (lähetetty / odottaa / virhe)                               | `LIST_VIEW, CSV_EXPORT`                 |
| `Henkilotunnus`                                                         | **poistuu** — hetuja ei enää tallenneta |

- Korjataan `Katuosoite.getValue` (koko osoite) ja sivun renderöinti (pelkkä katuosoite) vastaamaan
  toisiaan — ero merkitsee heti kun CSV-vienti otetaan käyttöön.
- Uusi `YkiArvioijaParams.kt` (mallina `YkiSuorituksetParams`, `@ModelAttribute`-sidottu):
  `search`, `tila`, `kieli`, `taso`, `kausiPaattyyEnnen`, `vainSolkiVirheet`, `piilotaHenkilotiedot`,
  `sortColumn`, `sortDirection`, `pageNumber`, `pageSize` + `toFilter()`, `toOrder()`, `excludeTags()`,
  `csvFileName()`, `filterDescriptions()` (`jdbc/SqlFilterBuilder`, `jdbc/PaginatedSortOrder`).
- `YkiArvioijaColumn` rekisteröidään `webmvc/EnumFromUrlParamsParsingConfig.kt`:iin — muuten
  `?sortColumn=` ei sidostu.
- Suodatindialogi `html/table/TableFilterDialog.kt`:n `tableFilterDialog` + `enumFilter`/`dateFilter`/
  `toggleFilter`.
- **Sivutus tehdään arvioijittain**, ei arviointioikeusriveittäin: `rowspan`-renderöinti (henkilösarakkeet
  ulottuvat useamman kielirivin yli) rikkoutuisi muuten. `LIMIT/OFFSET` kohdistuu `yki_arvioija`-tasolle.
- Yläreunaan `csvDownloadButton(Links.Yki.arvioijatCsv() + httpParams(params.toMap()))` ja — vain
  kirjoitusoikeudella — `a(href = Links.Yki.uusiArvioija()) { role = "button"; +UiText.Yki.lisaaArvioija }`.

### 3.4 Lisäys- ja muokkauslomake

> **Päivitys (V122–V124).** Muokkauslomakkeelta poistuivat kauden alkupäivä, päättymispäivän esikatselu,
> ASHA-numero ja arviointioikeusmatriisi: kausi hallitaan tietosivun kausitaulukosta. Ilman tätä alkupäivän muutos
> lomakkeella loisi uuden kauden vanhan rinnalle sen sijaan että muokkaisi sitä. Lisäyslomake kantaa
> ne yhä, koska arvioijan luonti perustaa myös ensimmäisen kauden. Muokkaus kulkee omalla komennollaan
> `PaivitaArvioijanTiedot`, jolloin tallennus ei voi koskea arviointioikeuksiin lainkaan.

Syötetty OID ratkaistaan ONR:n **master-OIDiksi**: rekisteri avaimennetaan master-OIDilla, joten duplikaatti-OID loisi muuten samalle
henkilölle toisen merkinnän eikä löytäisi olemassa olevaa.

**Vaihe 1 (`GET /yki/arvioijat/uusi`)** — pieni kortti, jossa on yksi kenttä: `oppijanumero`. Nappi
"Hae henkilön tiedot" (`formPost(Links.Yki.arvioijaHaku())`).

**Vaihe 2** — sama sivufunktio, `esitaytto != null`: koko lomake esitäytettynä ONR:n tiedoilla.
`arvioijaOid` piilokenttänä.

**Jos henkilö on jo rekisterissä** (jatkokausi, §2.8), lomake esitäytetään ONR:n henkilötietojen
_lisäksi_ hänen nykyisellä merkinnällään: arviointioikeusmatriisi, ASHA-numero ja kausi tulevat
`ArvioijanEsitaytto.olemassaolevaMerkinta`sta, ja lomakkeen yläreunassa näkyy varoitus "Arvioija on jo
rekisterissä…". Ilman tätä tallennus pyyhkisi muiden kielten arviointioikeudet ja ASHA-numeron, koska
`tallenna` korvaa merkinnän kokonaan. Tyhjä ONR-kenttä ei ylikirjoita rekisterin arvoa. Samasta syystä
`luoArvioija` säilyttää olemassa olevan `ensimmainen_rekisterointipaiva`n (sama logiikka kuin
`paivitaArvioija`ssa) ja kirjaa auditlokiin `YkiArvioijaUpdated`in `YkiArvioijaCreated`in sijaan, kun
merkintä oli jo olemassa.

Puhdas palvelimen round-trip kahdella lomakkeella — ei fetchiä, ei uutta JS-buildia.

**Lomakedata** (yksi koodattu monivalinta rinnakkaislistojen sijaan):

```kotlin
data class ArvioijaFormData(
    val arvioijaOid: String? = null,
    val sukunimi: String? = null,
    val etunimet: String? = null,
    val sahkopostiosoite: String? = null,
    val katuosoite: String? = null,
    val postinumero: String? = null,
    val postitoimipaikka: String? = null,
    val kaudenAlkupaiva: LocalDate? = null,
    val ashaNumero: String? = null,
    /** Yksi monivalinta, arvot muotoa "FIN:PT" — kieli×taso-matriisin valintaruudut. */
    val arviointioikeus: List<String>? = null,
) {
    fun laskettuPaattymispaiva(): LocalDate? = kaudenAlkupaiva?.let(Arviointikausi::paattymispaiva)
    fun arviointioikeudet(): List<TallennaArvioija.Arviointioikeus>
    fun toCommand(oid: Oid): TallennaArvioija
}
```

**Elinkaarikentät eivät kulje lomakkeen kautta.** Lomake tuntee vain virkailijan syöttämät kentät.
Arviointioikeuden `tila` **lasketaan kauden päivistä** (ks. §1.5), joten sitä ei poimita eikä
tallenneta lainkaan, ja `passivoitu` säilyy tallennuksessa niin kauan kuin merkintä pysyy
passiivisena. Ilman jälkimmäistä pelkkä yhteystiedon korjaus nollaisi säilytysajan alkuhetken. Vastaavasti `arvioijaOid`-kentän validointivirheet nostetaan `formErrorSummary`n
`piilokentat`-listalla näkyviin — kenttä renderöityy vain piilokenttänä, joten ilman tätä ONR-virhe
palauttaisi lomakkeen ilman mitään palautetta.

**Arviointioikeusmatriisi** (`FlowContent.arviointioikeusMatriisi(valitut, errors)`): taulukko, rivit =
ei-legacy `Tutkintokieli`t (`@HideInTableFilter` suodattaa `SWE10`/`ENG11`/`ENG12` pois — sama annotaatio
kuin `enumFilter`issa), sarakkeet = `PT`/`KT`/`YT`, solut
`input type=checkbox name="arviointioikeus" value="FIN:PT"`. Nolla JS:ää, yksi kenttänimi, ja
"sallitaan useita" toteutuu kirjaimellisesti. Jo tallennetut legacy-kielet näytetään read-only-rivinä (`disabled`), ja
`poistaPuuttuvatArviointioikeudet` ohittaa legacy-kielet: koska matriisi ei renderöi niitä, ne
puuttuvat payloadista eikä niitä ole tarkoitus poistaa.

**Optimistinen lukitus.** Lomake on täyden tilan tilannekuva, joten kahdesta rinnakkaisesta
muokkauksesta jälkimmäinen ylikirjoittaisi ensimmäisen hiljaisesti — ja yhteystietojen osalta
menetystä ei voisi jäljittää, koska kausihistoria kattaa vain kaudet eikä auditlokiin talleteta
kenttien arvoja. Lomake kantaa siksi piilokentässä rivin `muokattu`-leiman, ja `tallenna`n
`odotettuMuokkaushetki` tekee päivityksestä compare-and-setin (`ON CONFLICT … DO UPDATE … WHERE
yki_arvioija.muokattu = ?`). Kun ehto ei täsmää, mitään ei kirjoiteta, repository heittää
`OptimisticLockingFailureException`in ja palvelu palauttaa `MuokattuSamanaikaisesti`-virheen, joka
renderöityy lomakkeelle ohjeena ladata sivu uudelleen. Ilman tunnistetta (Solkin push, dev-työkalut)
tarkistusta ei tehdä, joten vanha käytös säilyy. Jäljelle jää kapea rako: kaksi virkailijaa, jotka
luovat saman **uuden** arvioijan yhtä aikaa, eivät kumpikaan kanna tunnistetta.

**Turvakielto** kysytään ONR:stä joka renderöinnissä eikä sitä talleteta kituun. Kysely voi myös
epäonnistua, ja se on eri asia kuin "ei turvakieltoa": `Turvakieltotieto`-enum erottaa `ON`/`EI`/
`EI_TIEDOSSA`, ja viimeisestä renderöityy oma varoitus. Ilman erottelua ONR-katko näyttäisi
yhteystiedot ilman varoitusta — juuri silloin kun kukaan ei tiedä tarkistuksen epäonnistuneen.

**Kauden päättymispäivä** on read-only (`disabled` input + selite "Järjestelmä laskee 5 vuotta
alkupäivästä"). Lisätään `html/Common.kt`:n `javascript(...)`-tyylinen ~5 rivin inline-snippet, joka
päivittää esikatseluarvon `change`-tapahtumassa — sama kuvio kuin `Forms.kt`:n `submitButton`.

### 3.5 Tietosivu (`GET /yki/arvioijat/{id}`)

`Page.renderHtml`: `h1 { kokoNimi }`, `viewMessage(flash)`, sitten

- **Henkilötiedot** — `infoTable` (oppijanumero ONR-linkkinä, nimi, yhteystiedot).
  **Turvakielto** (OPH kys. 5): jos ONR palauttaa `turvakielto = true`, sivun yläreunassa näytetään
  `warningMessage(...)` virkailijalle. Tietoihin ei kohdisteta rajoituksia — osoite näkyy, viedään
  CSV:hen ja lähetetään Solkille normaalisti. Turvakieltoa **ei tallenneta kituun**, vaan se luetaan
  ONR:stä näyttöhetkellä.
- **Rekisterimerkintä** — §3.4:n muokkauslomake ilman vaihetta 1. ~~Sisältää hallintopäätöksen
  ASHA-numeron~~ → **kumottu V124:ssä**: ASHA-numero kuuluu kaudelle, ei arvioijalle, ja se
  syötetään kausilomakkeelta (ks. seuraava kohta ja §1.6). `PaivitaArvioijanTiedot`-komennossa ei
  ole `ashaNumero`-kenttää. Jatkokausi johdetaan palvelimella eikä ole lomakkeella (§2.8)
- **Arviointikaudet** — `displayTable` `yki_arvioija_arviointikausi`-riveistä (tila, alku, loppu,
  arviointioikeudet, toiminnot). Rivikohtaiset napit **Muokkaa**, **Passivoi** (vain aktiiviselle) ja
  **Poista** (ei viimeiselle kaudelle), ja taulukon yllä **Lisää arviointikausi**. Kauden rivillä on myös
  sen **hallintopäätöksen ASHA-numero** (§1.6). Vierekkäiset napit
  kääritään Picon nappiryhmään (`buttonGroup`, `<div role="group">`, https://picocss.com/docs/group);
  `<dialog>`-elementit jäävät ryhmän ulkopuolelle, koska ne eivät ole ryhmän lapsia. Poisto ja
  passivointi vahvistetaan `<dialog>`illa, jonka id on rivikohtainen (`poistaKausiDialog-{id}`);
  dialogit renderöidään `overflow`-kortin **ulkopuolelle**, jottei natiivi dialogi leikkaudu.
  Vanha erillinen Arviointioikeudet-taulukko poistui: sen sisältö on nyt kausirivin sarake.
- **Muutoshistoria** — `yki_arvioija_kausi` -lokirivit (toimenpide, kieli, tasot, tila, alku, loppu,
  jatkokausi, kirjattu, kirjaaja) `<details>`-elementin takana, oletuksena kiinni
- **Vanhentuneet arviointioikeudet** — vain luettava taulukko, näytetään vain jos rivejä on.
  Vanhentuneet kielet eivät kuulu kausiin, joten ilman tätä ne katoaisivat sivulta kokonaan
- **Integraatiot** — Solki-lähetyksen tila (`solkiinLahetetty` / `solkiLahetysvirhe` /
  `solkiLahetysyritykset`) + nappi "Lähetä uudelleen Solkiin"
- **Toiminnot** — "Merkitse passiiviseksi" `<dialog>`-varmistuksella (`html/Modal.kt`,
  `modalCommandButton` — natiivi `command`/`commandfor`, ei JS:ää)

Kirjoitusnapit renderöidään vain jos `CurrentUser.hasAuthority(Authority.YKI_ARVIOIJAREKISTERI)`.

### 3.6 Virhenäkymä (`GET /yki/arvioijat/virheet`)

Ei uutta taulua eikä `errorTablePage`-kutsua: sama `YkiArvioijaColumn`-taulukko suodattimella
`vainSolkiVirheet = true`, lisättynä sarakkeilla `SolkiVirhe` ja `SolkiLahetysyritykset` sekä rivikohtaisella
"Lähetä uudelleen" -napilla. Näin virhe ja rivi eivät voi ajautua epäsynkroniin.
`webmvc/DashboardService.kt`:n `arvioijaImportErrorCount` → `arvioijaSolkiVirheCount`, ja
`webmvc/HomePage.kt`:n rivin teksti vastaavasti.

### 3.7 Navigaatio ja käännökset

`html/Navigation.kt`: "Arvioijat" osoittaa jo `Links.Yki.arvioijat()`:iin — ei muutosta. Muista että
`mainNavigation` **on pysyttävä laskettuna `val … get()`**:nä (Tolgee-jäätymisansa).

`i18n/UiText.kt` → `object Yki` uudet avaimet (`tr("...", fi = "...")`):
`lisaaArvioija`, `muokkaaArvioijaa`, `arvioijanTiedot`, `rekisterimerkinta`, `arviointikausi`,
`arviointikaudet`, `kausiLasketaanAutomaattisesti`, `haeHenkilonTiedot`, `arvioitavatTutkinnot`,
`merkitsePassiiviseksi`, `passivointiVarmistus`, `arvioijaTallennettu`, `arvioijaPassivoitu`,
`solkiLahetysOnnistui`, `solkiLahetysEpaonnistui`, `lahetaUudelleenSolkiin`, `solkiLahetystenVirheet`,
`odottaaLahetysta`, `eiYksiloity`.
`object Sarake` uudet: `ensimmainenRekisterointipaiva`, `ashaNumero`, `solkiTila`, `solkiVirhe`,
`lahetysyritykset`, `muokattu`, `kirjattu`.
Poistuu: `arvioijienTuonninVirheet`.

Muista `import fi.oph.kitu.i18n.unaryPlus` jokaiseen uuteen renderöintitiedostoon.

---

## 4. Sisäinen rajapinta

### 4.1 Uusi `yki/arvioijat/YkiArvioijaApiController.kt`

```kotlin
@RestController
@RequestMapping("/yki/api")
@Tag(name = "Yleinen kielitutkinto")
class YkiArvioijaApiController(private val service: YkiArvioijaService) {

    /** Virkailijan CSV-lataus. */
    @GetMapping("/arvioijat", produces = ["text/csv"])
    fun getArvioijatAsCsv(@ModelAttribute params: YkiArvioijaParams): ResponseEntity<StreamingResponseBody> =
        csvAttachmentResponse<YkiArvioijaColumn, _>(
            filename = params.csvFileName(),
            data = service.haeKaikki(params),
            excludeTags = params.excludeTags(),
        )

    /** Solkin täsmäytysluku. */
    @GetMapping("/arvioijat", produces = ["application/json"])
    @Tag(name = "oauth2")
    fun getArvioijat(
        @RequestParam tila: YkiArvioijaTila? = null,
        @RequestParam muuttunutJalkeen: OffsetDateTime? = null,
        @RequestParam(defaultValue = "0") sivu: Int,
        @RequestParam(defaultValue = "200") koko: Int,
    ): ResponseEntity<ArvioijatResponse>
}
```

JSON-vastaus on **sama dokumentti kuin push-payload** (§5.1), jolloin Solki voi käyttää samaa
deserialisointia sekä pushille että täsmäytyspullille. Endpointin on oltava `…/api/…`-polun alla, jotta
se näkyy `springdoc.pathsToMatch=/**/api/**/*` -suodattimen läpi `/api-docs`issa.

> **Ei toteutettu (17.9.2026).** `YkiApiController` tarjoaa toistaiseksi vain
> `GET /yki/api/arvioijat`-reitin muodossa `produces = ["text/csv"]`. Tässä kuvattua
> JSON-varianttia ei ole rakennettu, eikä Solki ole sitä toistaiseksi pyytänyt.

### 4.2 Sisääntuleva `POST /yki/api/arvioija` — kavennetaan, ei poisteta

> **Passivointi tapahtuu aina kitun käyttöliittymästä (päätös 1.9.2026).** Suunnitelma varautui
> aiemmin siihen, että arvioijan omasta toiveesta tapahtuva passivointi tehtäisiin Solkin
> YKI-sovelluksessa ja siirtyisi muutosrajapintaa pitkin kituun. Näin ei tehdä: virkailija passivoi
> merkinnän kitussa myös silloin, kun pyyntö tulee arvioijalta itseltään. Tämä oli ainoa tapaus, joka
> olisi edellyttänyt Solki→kitu-rajapinnalta rekisterimerkinnän kirjoitusoikeutta, joten **kavennettu
> passivointi-endpoint jää tekemättä.**

Tavoiterakenne:

| Suunta       | Tapaus                                                 | Rajapinta                           |
| ------------ | ------------------------------------------------------ | ----------------------------------- |
| kitu → Solki | Uusi merkintä, muokkaus, uusi kausi                    | §5.1 `POST /arvioija`               |
| kitu → Solki | Passivointi kauden päätyttyä (laskettu, ei toimintoa)  | sama                                |
| kitu → Solki | Passivointi laiminlyönnin takia (OPH kuulee arvioijaa) | sama                                |
| kitu → Solki | **Passivointi arvioijan omasta toiveesta**             | sama — virkailija tekee sen kitussa |
| Solki → kitu | **Yhteystietojen päivitys**                            | nykyinen `POST /yki/api/arvioija`   |

**Yhteys pysyy kaksisuuntaisena:** Solki lähettää jatkossakin yhteystietojen päivityksiä, joten
sisääntulevaa rajapintaa ei poisteta. Rekisterimerkinnän osalta suunta on kuitenkin yksi: kitu
kirjoittaa merkinnän, Solki lukee sen ja kirjoittaa takaisin vain yhteystiedot.

#### Varautuminen: yhteystiedot ja vain ne

Solkilla on nyt §5.1:n dokumenttimuoto, joten se voi lähettää saman dokumentin takaisin. Kavennus ei
siis tarvitse uutta endpointia eikä uutta DTO:ta — riittää että vastaanotto **soveltaa payloadista
vain yhteystietokentät**:

| Kenttä                                                              | Sisääntulevassa           | Perustelu                                           |
| ------------------------------------------------------------------- | ------------------------- | --------------------------------------------------- |
| `sahkopostiosoite`, `katuosoite`, `postinumero`, `postitoimipaikka` | **päivitetään**           | Solki on näiden lähde: arvioija asioi Solkin kanssa |
| `sukunimi`, `etunimet`                                              | ohitetaan                 | ONR on nimien master (§2.5), ei Solki eikä kitu     |
| `arviointioikeudet` (kieli, tasot, kausi, `tila`)                   | ohitetaan                 | Kitu on rekisterin master vaiheesta 11              |
| tuntematon `arvioijanOppijanumero`                                  | virhe, **ei luoda riviä** | Kitu päättää kuka rekisterissä on                   |

Kaikuvaaraa vastaan ei tarvita uutta koneistoa: `tallenna(..., lahde = Tallennuslahde.SOLKI)` leimaa
rivin jo nyt lähetetyksi (ellei se ollut jo jonossa), joten Solkin oma yhteystietomuutos ei palaa
sille takaisin.

**Tallennettu `tila` säilyy siirtymän ajan.** Vaikka yhteystietopäivitys ei kanna tilaa, sarake ja
§1.5:n sääntö "vain tallennettu PASSIVOITU ohittaa laskennan" pidetään ennallaan niin kauan kuin
`kitu.yki.arvioijarekisteri.integraatio.enabled` on pois päältä ja Solki on rekisterin master: siihen asti
Solkin pushin kirjaama tila on oikea tieto, eikä sitä saa hukata. Sarakkeen poistoa voi harkita vasta
kun kytkin on poistettu ja kitu toimii masterina (vaihe 11 jälkeen), ja silloinkin erillisenä
muutoksena — laskenta ei sitä edellytä.

#### Hylätty vaihtoehto: kavennettu passivointi-endpoint

Aiempi suositus oli korvata nykyinen rajapinta kapealla passivointi-endpointilla, joka ottaisi vastaan
vain oppijanumeron, passivoinnin syyn ja päivämäärän. Se **raukeaa** yllä olevan päätöksen myötä:
passivointi ei tule Solkista lainkaan, joten endpointille ei jää käyttötapausta. Sama master-perustelu
(Solki ei kirjoita nimiä, kausia eikä tiloja) toteutuu nyt kavennuksella yhteystietoihin.

**Kunnes kaventaminen tehdään, sisääntuleva push ei saa poistaa mitään eikä kaikua takaisin.**
Vaihe 2 vaihtoi endpointin tallennuksen `upsert`ista `tallenna`an, jolloin se alkoi (a) poistaa
payloadista puuttuvat arviointioikeudet ja (b) nollata `solkiin_lahetetty`n — `origin/main`in
`upsert`issa ei ole `DELETE`ä eikä lähetyskenttiä lainkaan. Solki on yhä master eikä sen payloadin
kattavuudesta ole sopimusta (§11.1), joten osittainen push pyyhkisi muut kielet, ja nollattu
lähetysleima laittaisi Solkin oman datan §5:n lähetysjonoon — sama kaikuvaara jota V117:n
`KRIITTINEN`-backfill torjuu historiariveillä.

Tallennuksen lähde on siksi eksplisiittinen: `tallenna(..., lahde = Tallennuslahde.SOLKI)` jättää
puuttuvat oikeudet rauhaan, ei kirjoita kitun omia kenttiä (`asha_numero`, `passivoitu`,
— Solkin payload ei kanna niitä, joten `EXCLUDED`-arvo pyyhkisi ne) ja leimaa
rivin lähetetyksi kannan omalla `now()`-arvolla (sama arvo kuin `muokattu`, jotta osittainen indeksi
ei poimi riviä). Leimaa **ei** anneta, jos rivi oli jo lähetysjonossa: muuten Solkin push nielaisisi
kitussa tehdyn, vielä lähettämättömän muutoksen. Kitun oma syöttökäyttöliittymä käyttää oletusta
`Tallennuslahde.KITU`: master-semantiikka ja rivi lähetysjonoon. Vaiheen 9 lähetin ei siis tarvitse
erillistä suodatinta sisääntulleille riveille. Katettu testeillä `YkiArvioijaRepositoryTest`
(`Sisaantulevassa pushissa puuttuvia arviointioikeuksia ei poisteta`, `Solkin push ei jata rivia
lahetysjonoon`, `Kitun oma tallennus jaa lahetysjonoon`) ja `YkiApiControllerTest` (`Solkin push ei
poista arviointioikeuksia jotka puuttuvat payloadista`).

Endpointia **ei siis poisteta eikä 410:ta tehdä.** Aiempi suunnitelma eteni kaksivaiheisesti kohti
`410 Gone` -vastausta; se raukeaa, koska Solki jatkaa yhteystietojen lähettämistä samaa reittiä.

Kavennus tehdään vastaanoton puolella: DTO ja security-sääntö säilyvät, mutta tallennus soveltaa vain
yhteystietokentät. `YkiArvioija`/`YkiArviointioikeus`-DTO:t ja schema-esimerkit jäävät paikoilleen.

**Kavennus on sidottu integraatiokytkimeen** `kitu.yki.arvioijarekisteri.integraatio.enabled` (§7.5), joka
kertoo onko kitu master. Kytkin on untuvassa/QA:ssa/prodissa `false` vaiheeseen 11 asti, joten
siirtymän ajan Solkin koko payload otetaan yhä vastaan ja tallennettu `tila` kirjoitetaan — muuten
Solki menettäisi kirjoitusoikeuden rekisteriin ennen kuin kitu on ottanut sen vastuun. Kytkimen
kääntyessä sisääntulo kaventuu samassa hetkessä kuin kitun oma kirjoitus avautuu, ilman erillistä
propertya.

Kavennettuna: tuntematonta arvioijaa **ei luoda** (kitu päättää kuka rekisterissä on, vastaus 400),
nimet ohitetaan (ONR on master) ja arviointioikeudet ohitetaan kokonaan — myös `tila`, joka jää
kitun riveillä NULLiksi. Kaikuvaara hoituu ennallaan `Tallennuslahde.SOLKI`lla: rivi leimataan
lähetetyksi, paitsi jos siinä oli jo lähettämätön kitun oma muutos.

Testit: `YkiArvioijaKavennettuApiTest` (kytkin päällä) ja `YkiArvioijaSiirtymaApiTest` (kytkin pois)
ajavat molemmat regiimit erikseen.

---

## 5. Solki-integraatio (lähtevä)

Uusi paketti `server/src/main/kotlin/fi/oph/kitu/yki/arvioijat/solki/`.

### 5.1 REST-sopimus (JYU hyväksynyt 1.9.2026, korjattu 16.9.2026 julkaistun toteutuksen mukaiseksi)

> **Korjaus 16.9.2026.** JYU julkaisi oman rajapintakuvauksensa
> (`oikeatoliot.fi/yki/doc/OPH-integraatio.html`, 14.9.2026), eikä se vastaa alla 1.9.2026
> hyväksyttyä sopimusta: endpoint on `POST /oph/arvioija` eikä `PUT /oph/arvioijat/{oid}`,
> avainkenttä on `arvioijaOid`, syntymäaika `syntymapaiva`,
> `ensimmainenRekisterointipaiva` on dokumentin juuressa ja tilasanastossa on vain kaksi arvoa.
> Lähetys päätyi siksi 404:ään. Tämä luku kuvaa nyt **julkaistun** rajapinnan, jota koodi
> noudattaa. `versio` ja `Idempotency-Key` eivät esiinny julkaistussa kuvauksessa lainkaan —
> ne lähetetään yhä, mutta niiden kohtalo on varmistettava JYU:lta.
>
> **JYU on hyväksynyt tämän sopimuksen sellaisenaan 1.9.2026**, eli endpoint, payload, kenttäjoukko,
> enkoodaukset ja statuskoodit ovat sovitut. Vaiheen 9 toteutus voi alkaa. Jäljellä on vain
> operatiivisia asioita (§5.1.1) — ne eivät muuta sopimusta.
>
> **Täydennys 4.9.2026:** JYU pyysi payloadiin `syntymaaika`-kentän. Se on ainoa muutos
> 1.9.2026 hyväksyttyyn kenttäjoukkoon, ks. §5.1.1.

**Lähtökohta: samat kentät kuin poistuneessa CSV-tuonnissa.** Solki tuotti aiemmin kitulle
arvioijarivit CSV:nä, joten kentät ovat jo olemassa Solkin päässä. Lähetetään ne takaisin
samannimisinä, jolloin JYU:lle ei synny kartoitustyötä — vain siirtotapa muuttuu. Poikkeukset on
merkitty alle.

`{base}` on olemassa oleva `kitu.yki.baseUrl` (huomaa paattava kautta):
untuva ja QA `https://yki-test.cc.jyu.fi/oph/`, tuotanto `https://yki.jyu.fi/oph/`.

```
POST {base}arvioija
Authorization:   Basic <Solkin kitulle myöntämät tunnukset>
Content-Type:    application/json; charset=utf-8
Idempotency-Key: {arvioijaOid}:{versio}
```

```json
{
  "arvioijaOid": "1.2.246.562.24.59267607404",
  "versio": "2026-08-21T09:12:33.512Z",
  "sukunimi": "Kivinen-Testi",
  "etunimet": "Petro Testi",
  "syntymapaiva": "2016-01-01",
  "sahkopostiosoite": "petro.kivinen@example.com",
  "katuosoite": "Testikatu 1 A 2",
  "postinumero": "00100",
  "postitoimipaikka": "Helsinki",
  "ensimmainenRekisterointipaiva": "2005-01-21",
  "arviointioikeudet": [
    {
      "kieli": "fin",
      "tasot": ["PT", "KT", "YT"],
      "kaudenAlkupaiva": "2026-01-01",
      "kaudenPaattymispaiva": "2030-12-31",
      "jatkorekisterointi": false
    }
  ]
}
```

#### Vastaus: Solkin palauttama arvioijatunnus

Solki palauttaa onnistuneesta lähetyksestä arvioijatunnuksensa. Kitu lukee sen
(`SolkiArvioijaClient`) ja tallentaa sarakkeeseen `yki_arvioija.solki_tunnus` (**V125**); tunnus
näkyy arvioijan tietosivulla. Päivitys tehdään muodossa `COALESCE(?, solki_tunnus)`, joten tyhjä
tai puuttuva tunnus vastauksessa ei pyyhi aiemmin saatua arvoa.

#### Kenttävastaavuus poistuneeseen CSV:hen

CSV-sarakkeet ovat commitista `d160c1f1^` (`SolkiArvioijaResponse`). Rivitaso oli
arvioija × kieli; JSON ryhmittelee saman datan arvioijakohtaiseksi dokumentiksi.

| CSV-sarake                                      | JSON                                      | Muutos                                                                                                              |
| ----------------------------------------------- | ----------------------------------------- | ------------------------------------------------------------------------------------------------------------------- |
| `arvioijanOppijanumero`                         | `arvioijaOid`, dokumentin juuressa        | **Nimi korjattu 16.9.2026** julkaistun kuvauksen mukaiseksi                                                         |
| `henkilotunnus`                                 | **ei mukana**                             | 1.1.2026 lainmuutos; kitu ei ota vastaan eikä lähetä hetua                                                          |
| `sukunimi`, `etunimet`                          | samat                                     | —                                                                                                                   |
| — (ei CSV-saraketta)                            | `syntymapaiva`, dokumentin juuressa       | **Lisätty 4.9.2026**, nimi korjattu 16.9.2026. Solki johti syntymäajan hetusta; hetun poisto vei siltä pohjan       |
| `sahkopostiosoite`                              | sama                                      | —                                                                                                                   |
| `katuosoite`, `postinumero`, `postitoimipaikka` | samat, dokumentin juuressa                | —                                                                                                                   |
| `ensimmainenRekisterointipaiva`                 | sama, **dokumentin juuressa**             | **Siirretty 16.9.2026** arviointioikeudelta juureen: tieto on arvioijakohtainen                                     |
| `kaudenAlkupaiva`                               | sama                                      | —                                                                                                                   |
| `kaudenPaattymispaiva`                          | sama                                      | —                                                                                                                   |
| `jatkorekisterointi`                            | sama, `true`/`false`                      | CSV:ssä `"0"`/`"1"`                                                                                                 |
| `tila`                                          | **ei mukana**                             | **Poistettu 16.9.2026**, ks. huomio alla: tila vanhenee vastaanottajan kopiossa, joten se johdetaan kauden päivistä |
| `kieli`                                         | sama, `Tutkintokieli.solkiCode` (`"fin"`) | CSV:n vanhat numerokoodit `10`/`11`/`12` = `SWE10`/`ENG11`/`ENG12`                                                  |
| `tasot`                                         | sama, JSON-taulukko `["PT","KT","YT"]`    | CSV:ssä `"PT+KT+YT"`                                                                                                |

Enkoodausten muutokset (`0`/`1` → boolean, `"PT+KT+YT"` → taulukko) ovat ehdotus: JSON-natiivit
tyypit ovat luettavampia, mutta jos CSV-identtinen esitys on JYU:lle halvempi, se käy yhtä hyvin.
Kenttien **nimet ja merkitykset** on tarkoitus pitää ennallaan joka tapauksessa.

> **`tila` poistettiin lähetettävistä kentistä 16.9.2026.** Kitu ei tallenna tilaa vaan laskee sen
> kauden päivistä (§1.5). Lähetetty arvo olisi tosi vain lähetyshetkellä ja vanhenisi vastaanottajan
> kopiossa, kun kausi umpeutuu ilman että kumpikaan pää kirjoittaa mitään — juuri se vika, jonka
> takia kitu luopui tallennetusta tilasta. Solki johtaa tilan samoista päivistä: merkintä on
> voimassa `kaudenAlkupaiva`n ja `kaudenPaattymispaiva`n välillä, **päättymispäivä mukaan lukien**.
> Manuaalinen passivointi näkyy päivissä, koska se päättää kauden kuluvaan päivään.

| Päätös                                                                  | Perustelu                                                                                                                                                                                                                     |
| ----------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **POST + koko dokumentti, avaimena `arvioijaOid`**                      | Uudelleenyritys on triviaalisti idempotentti; arviointioikeuden poisto ilmaistavissa (kieli katoaa taulukosta); ei erillisiä create/update/delete-verbejä                                                                     |
| **Arvioijakohtainen dokumentti, ei CSV:n riviesitystä**                 | Ainoa rakenteellinen ero CSV:hen. Riviesityksessä ei voi ilmaista kielen poistoa ilman sopimusta koko joukon korvaamisesta; dokumentissa se on kielen puuttuminen                                                             |
| **Kausi arviointioikeuskohtaisena, tila ei lainkaan**                   | Kausi vastaa kitun tietomallia (sovittu päätös) ja poistuneen CSV:n riviesitystä; tila johdetaan kauden päivistä, ks. huomio yllä                                                                                             |
| **Ei henkilötunnusta**                                                  | 1.1.2026 lainmuutos — Solkin on avaimennettava oppijanumerolla                                                                                                                                                                |
| **Syntymäaika mukana, haettuna ONR:stä**                                | Korvaa hetusta johdetun syntymäajan (JYU:n pyyntö 4.9.2026). Kitu **ei säilytä** kenttää — se ei ole tavoitetilan tietotaulukossa (§1) — vaan hakee sen lähetyshetkellä. Jätetään pois kokonaan jos ONR:ssäkään ei ole tietoa |
| **Ei puhelinnumeroa**                                                   | OPH vahvisti (kys. 11), ettei puhelinnumeroa säilytetä kitussa lainkaan — se on kokonaan Solkin omaa tietoa                                                                                                                   |
| **Ei ASHA-numeroa**                                                     | Hallintopäätöksen viite tallennetaan kituun (kys. 12) mutta se on OPH:n sisäinen hallinnollinen tieto; varmistetaan JYU:lta, onko sille Solkissa käyttöä                                                                      |
| `kieli` = `Tutkintokieli.solkiCode` (`"fin"`), `tasot` = `PT`/`KT`/`YT` | Sama lankamuoto kuin poistuneessa CSV:ssä → ei muunnostyötä JYU:n päässä                                                                                                                                                      |
| `versio` = kitun `muokattu`                                             | Solki voi hylätä vanhemman version, jolloin epäjärjestyksessä saapuva uusinta ei palauta vanhaa tilaa                                                                                                                         |
| **Ei DELETE-operaatiota**                                               | Kitu poistaa oman kopionsa säilytysajan umpeuduttua (§6.2), mutta poistoa **ei** välitetä Solkille: Solkilla on oma säilytysaikansa ja oma rekisterinsä. Käytännössä kitu lakkaa lähettämästä kyseistä henkilöä               |
| **Lähetys korvaa vain OPH:n omistamat kentät**                          | Solki täydentää merkintöjä omilla tiedoillaan (arviointikerrat, huomautukset, lisätiedot, liitteet, puhelinnumerot, postinumero) — lähetys ei saa tyhjentää niitä                                                             |

| Status                    | Merkitys                                                                      | Kitu tekee                                                          |
| ------------------------- | ----------------------------------------------------------------------------- | ------------------------------------------------------------------- |
| `200` / `204`             | Hyväksytty                                                                    | `merkitseLahetetyksi`                                               |
| `409 Conflict`            | Sähköpostiosoite on jo toisella Solkin käyttäjällä, **mitään ei tallennettu** | `merkitseLahetysvirhe`; uusinta ei auta ennen kuin osoite korjataan |
| `400` + `{"virheet":[…]}` | Pysyvä virhe                                                                  | `merkitseLahetysvirhe`; ei uusintaa ennen kuin rivi muokataan       |
| `401` / `403`             | Konfiguraatio-ongelma                                                         | virhe, uusinta aikataulun mukaan                                    |
| `5xx` / yhteysvirhe       | Ohimenevä                                                                     | virhe + yrityslaskuri, uusinta                                      |

### 5.1.1 Sovittu ja avoinna

**Sovittu 1.9.2026 (sopimus hyväksytty sellaisenaan):** endpoint ja verbi, kenttäjoukko ja
CSV-vastaavuus, JSON-natiivit tyypit, passivoinnin ilmaisu päättymispäivällä (`tila`-kenttä
poistettiin 16.9.2026, ks. huomio yllä), idempotenssi (`Idempotency-Key` + `versio`) ja
statuskoodit, ei ASHA-numeroa eikä henkilötunnusta.

**Payload lukittiin 1.9.2026 ja avattiin kerran 4.9.2026.** JYU vahvisti 1.9.2026, ettei Solki
tarvitse passivoinnin syytä, henkilötunnusta eikä ASHA-numeroa. Sen jälkeen JYU pyysi
`syntymaaika`-kentän: Solki johti syntymäajan aiemmin CSV:n `henkilotunnus`-sarakkeesta, jota
kitu ei enää lähetä (1.1.2026 lainmuutos), eikä CSV:ssä ollut erillistä syntymäaikasaraketta.
Syntymäaika on hetun pienin korvaaja: se ei yksilöi henkilöä. Muita avoimia lisäyksiä ei ole.

**Syntymäaikaa ei säilytetä kitussa.** Se haetaan ONR:stä lähetyshetkellä samaan tapaan kuin
turvakielto (§3.4), koska kenttä ei ole tavoitetilan tietotaulukossa (§1). Jos ONR ei vastaa,
riviä **ei lähetetä** vaan se jää lähetysjonoon: vajaana lähtenyt rivi merkittäisiin lähetetyksi,
eikä mikään lähettäisi sitä uudelleen ennen seuraavaa muokkausta. Kentättömänä lähetetään vain
silloin, kun ONR vastaa eikä syntymäaikaa ole (yksilöimätön henkilö).

**Tunnistautuminen (sovittu 1.9.2026):** käytetään **samoja Basic-tunnuksia** kuin nykyisessä
`solkiRestClient`issa (`kitu.yki.username`/`password` = `YKI_API_USER`/`YKI_API_PASSWORD`) saman
`kitu.yki.baseUrl`-osoitteen alla. Uutta tunnusten vaihtoa ei siis tarvita, eikä lähetyksen
käyttöönotto odota muuta kuin JYU:n vastaanottopään valmiutta.

**Avoinna, ei estä vaihetta 9:**

1. **JYU:n testiympäristö ja aikataulu:** milloin untuvan lähetys voidaan ajaa oikeaa vastaanottajaa
   vasten, ja tehdäänkö IP-rajaus JYU:n vai OPH:n päässä.
2. **Sisääntulevan rajapinnan kohtalo (§4.2, vaihe 10).** Nyt sovittiin vain suunta kitu → Solki.
   Kitusta tulee master vaiheessa 11, jolloin `POST /yki/api/arvioija` kavennetaan tai poistetaan —
   sovittava erikseen, jääkö Solkille kirjoitusoikeutta johonkin kenttään.

Dokumentoidaan `docs/technical/integraatiot.md`:hen omana lukunaan (Solki-luku muuttuu kaksisuuntaiseksi).

### 5.2 Luokat (malli 1:1 `ilmoittautumisjarjestelma/`-paketista)

Olemassa oleva `solkiRestClient` (`SolkiRestClientConfig`) osoittaa jo oikeaan baseen ja asettaa
Basic-tunnistautumisen, joten se on lähtökohtaisesti uudelleenkäytettävissä. Harkittava kuitenkin oma
bean, jos lähetykselle halutaan eri timeoutit tai uudelleenyrityskäytäntö kuin debug-haulle.

| Tiedosto                         | Sisältö                                                                                                                                                  |
| -------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `SolkiArvioijaRequest.kt`        | Payload-dataluokat + `fun of(entity): SolkiArvioijaRequest`                                                                                              |
| `SolkiArvioijaException.kt`      | Sealed: `BadRequest`, `Unauthorized`, `Conflict`, `UnexpectedError`, `NullResponse`, `MalformedResponse` + `debugString()`                               |
| `SolkiArvioijaClient.kt`         | Interface + **ehdoton** impl (`SolkiArvioijaClientImpl`, ei `@Conditional*`); `retrieveEntitySafely(String::class.java)`; palauttaa `Either`             |
| `SolkiArvioijaService.kt`        | Interface + **ehdoton** impl (`SolkiArvioijaServiceImpl`). ~~`@ConditionalOnBean` + `@ConditionalOnMissingBean`-mock~~ → kumottu: mock poistui, ks. §7.5 |
| `SolkiArvioijaConfig.kt`         | `RestClient`-konfiguraatio ja osoiteasetukset                                                                                                            |
| `SolkiArvioijaScheduledTasks.kt` | Kaksi `tracer.recurringTask(...)`-beania (§5.3)                                                                                                          |

**`debugString()` ei saa serialisoida pyyntörunkoa** (poikkeama KIOS-mallista): osoite ja sähköposti ovat
henkilötietoa, joka päätyisi lokeihin ja virhesarakkeeseen. Mukaan vain oppijanumero, statuskoodi ja
vastausrunko.

**`SolkiRestClientConfig` on täydennettävä `.withLenientStringConverter()`-kutsulla** — ilman sitä
`retrieveEntitySafely(String::class.java)` kaatuu Jacksonin sisällä, koska Spring 7:n oletusarvoinen
`StringHttpMessageConverter` mainostaa vain `text/*` (CLAUDE.md). Uusi kytkinproperty
`kitu.yki.arvioijarekisteri.integraatio.enabled` erikseen `kitu.yki.baseUrl`ista, koska osoite on asetettu joka ympäristössä
(myös local/e2e dev-stubiin).

### 5.3 Uudelleenyritys: "3 kertaa, sitten yöllisesti"

> **Tärkeä havainto:** `@RetryOutboundIntegration` uusii vain `ResourceAccessException`in ja
> `HttpServerErrorException`in. `retrieveEntitySafely` **ei heitä** kumpaakaan 5xx:llä vaan palauttaa
> `Either.Left` — eli KIOS-palvelun `@RetryOutboundIntegration` ei tosiasiassa uusi HTTP 500:aa.
> **Älä kopioi tätä.** (Kannattaa harkita erillistä korjausta myös `IlmoittautumisjarjestelmaService`en.)

Käytetään eksplisiittistä, kantaan tallennettua yrityslaskuria (`solki_lahetysyritykset`), joka näkyy myös
virhenäkymässä:

| Kerros                                 | Toteutus                                                                          |
| -------------------------------------- | --------------------------------------------------------------------------------- |
| Välitön palaute virkailijalle          | 1 synkroninen yritys tallennuksen jälkeen                                         |
| "3 kertaa"                             | `FIXED_DELAY\|900s` -ajastus, poimii rivit `solki_lahetysyritykset < 3`           |
| "sen jälkeen säännöllisesti (joka yö)" | `DAILY\|02:15` -ajastus, poimii **kaikki** lähettämättömät riippumatta laskurista |

```properties
# Osoite ja tunnukset ovat jo olemassa (kitu.yki.baseUrl, kitu.yki.username/password, §5.1.1);
# vain lähetyksen kytkin ja ajastukset ovat uusia.
kitu.yki.arvioijarekisteri.integraatio.enabled=false
kitu.yki.scheduling.lahetaArvioijatSolkiin.schedule=FIXED_DELAY|900s
kitu.yki.scheduling.lahetaEpaonnistuneetArvioijatSolkiin.schedule=DAILY|02:15
```

untuva/qa/prod: `enabled=true` (JYU:n valmistuttua). Uusia salaisuuksia ei tarvita: `YKI_API_USER` ja
`YKI_API_PASSWORD` ovat jo ympäristöissä.
local: `enabled=true`, ajastukset `-`.
**e2e: ajastukset `0 0 0 29 2 ?`** (karkausvuoden 29.2.) eikä `-` — CLAUDE.md:n mukaan `-` tekee tehtävästä
kokonaan näkymättömän db-scheduler-UI:ssa, jolloin sitä ei voi laukaista testistä käsin.

Paikallinen Solki-stubi `dev/YkiController.kt`:iin (kuollut `GET /dev/yki/import/arvioijat` poistetaan
tieltä): `POST /dev/yki/import/arvioija`, joka palauttaa `200` ja Solkin vastausrungon tai
`?failWith=500`-parametrilla
halutun virheen — e2e-testit ohjaavat sillä virhepolkua.

---

## 6. Ajastetut tehtävät

### 6.1 Automaattinen passivointi (UC3) — ei tarvita

**Vaihe 7 on rauennut.** UC3 toteutui ilman ajastettua tehtävää, kun tila alettiin laskea kauden
päivistä (§1.5): kauden päättymispäivän jälkeen merkintä on PASSIVOITU joka lukukerralla, ilman että
mikään ajo on käynyt kirjoittamassa sitä. Aiemmin tähän kohtaan suunniteltu
`passivoiPaattyneetArvioijat`-tehtävä olisi vain kirjoittanut kantaan tilan, jonka laskenta jo tuottaa.

Harkittiin myös, että tehtävä leimaisi `passivoitu`-aikaleiman umpeutuneille merkinnöille säilytysajan
alkuhetkeksi. Sekin hylättiin: leima tarkoittaa nimenomaan **manuaalista** passivointia, ja sen
kirjoittaminen umpeutumisen perusteella hävittäisi tiedon siitä, päättyikö merkintä hallintopäätöksellä
vai kauden umpeutumiseen. Säilytysaika saadaan ilman leimaa, ks. §6.2.

Sivuvaikutus käyttöliittymään: passivointinappi estetään aina kun merkintä on jo passiivinen — myös
umpeutumisen takia — koska passivointihetki on säilytysajan alkuhetki ja klikkaus siirtäisi umpeutuneen
merkinnän säilytysaikaa eteenpäin.

### 6.2 Säilytysajan valvonta (5 vuotta)

Prosessikuvauksen tietotaulukko määrittelee **säilytysajaksi kielitutkintorekisterissä 5 vuotta**
kaikille kitussa säilytettäville arvioijakentille. Käyttötapauskuvauksessa tätä ei mainittu, eikä
nykyisessä toteutuksessa ole minkäänlaista arvioijatietojen poistoa — **tämä on uusi vaatimus.**

OPH on vahvistanut, että **5 vuotta lasketaan passivointihetkestä** (ei kauden päättymispäivästä).
Tämä on syy `passivoitu`-aikaleimalle (§1.1): pelkkä kauden päättymispäivä ei riitä, koska kesken kauden
passivoidun merkinnän säilytysaika alkaa aiemmin kuin kausi olisi päättynyt.

Koska leima kirjataan vain manuaalisesta passivoinnista (§1.5), **alkuhetki on
`COALESCE(passivoitu, max(kauden_paattymispaiva))`**: hallintopäätöksellä passivoidulla se on leima,
umpeutuneella kauden viimeinen voimassaolopäivä. Molemmissa se on hetki, jona merkintä muuttui
passiiviseksi, eli täsmälleen se mitä OPH vastasi. Reunaehdot: merkintä jonka `kauden_paattymispaiva`
on NULL ei vanhene koskaan, eikä Solkin `PASSIVOITU`-rivi jonka kausi on vielä voimassa (leima
puuttuu, päättymispäivä tulevaisuudessa) — jälkimmäinen ratkeaa itsestään ensimmäisessä kitun
tallennuksessa, joka kääntää Solkin kannanoton kauden päivämääriin.

Poisto on peruuttamaton, joten tehtävä pidetään **oletuksena pois päältä** ja otetaan käyttöön vasta kun
sen toiminta on todennettu untuvassa.

Toteutus samaan `YkiArvioijaScheduledTasks`-luokkaan:

```kotlin
@Value($$"${kitu.yki.scheduling.poistaVanhentuneetArvioijat.schedule}")
lateinit var sailytysaikaSchedule: String

@WithSpan @Bean
fun poistaVanhentuneetArvioijat(service: YkiArvioijaService): Task<Void> =
    tracer.recurringTask("Poista säilytysajan ylittäneet YKI-arvioijamerkinnät", sailytysaikaSchedule) {
        service.poistaSailytysajanYlittaneet()
    }
```

Legacy-riveiltä passivointihetki puuttuu, eikä sitä täytetä: umpeutuneen merkinnän alkuhetki luetaan
kauden päättymispäivästä (§1.5). Näin säilytysaika kuluu historiallisella aikajanalla eikä ala vasta
käyttöönotosta, ilman että kantaan kirjoitetaan passivointihetkiä joita kukaan ei ole päättänyt.
Manuaalinen passivointi kesken kauden merkitsee `passivoitu = now()` ja päättää kauden samaan päivään,
joten `passivoitu` vastaa aina hetkeä, jolloin henkilö lakkasi olemasta arvioija.

```sql
-- Poistaa merkinnän, jonka säilytysaika on umpeutunut. yki_arviointioikeus ja
-- yki_arvioija_kausi poistuvat ON DELETE CASCADE -säännöllä. :tanaan ja :raja sidotaan
-- TimeServicelta (raja = tanaan - 5 v), jotta kiinnitetty testikello ohjaa myös poistoa.
DELETE FROM yki_arvioija
WHERE EXISTS (SELECT 1 FROM yki_arviointioikeus WHERE arvioija_id = yki_arvioija.id)
  AND NOT EXISTS (
        SELECT 1 FROM yki_arviointioikeus
        WHERE yki_arviointioikeus.arvioija_id = yki_arvioija.id
          AND <Rekisterointitila.SQL> <> 'PASSIVOITU'
      )
  AND COALESCE(
        yki_arvioija.passivoitu::date,
        (SELECT max(kauden_paattymispaiva) FROM yki_arviointioikeus
         WHERE yki_arviointioikeus.arvioija_id = yki_arvioija.id)
      ) < :raja
RETURNING arvioija_oid;
```

Huomioita:

- **Passiivisuus luetaan samasta lausekkeesta kuin näkymissä** (`Rekisterointitila.SQL`), jottei
  "päättynyt" tarkoita poistossa eri asiaa kuin käyttöliittymässä. Yksikin ei-passiivinen
  arviointioikeus suojaa poistolta, vaikka jokin toinen kausi olisi vanhentunut aikoja sitten.
- **Tuntematon alkuhetki suojaa poistolta.** Jos merkinnällä ei ole passivointihetkeä eikä yhtään
  kauden päättymispäivää, COALESCE on NULL ja vertailu epätosi. Vanhin ja epäluotettavin data on
  näin suojassa ilman erillistä ehtoa.
- **Arviointioikeudeton merkintä suojaa poistolta.** Ilman `EXISTS`-ehtoa `NOT EXISTS` olisi tyhjälle
  joukolle tosi ja merkintä poistuisi vahingossa.
- **Poisto on takautuva.** Koska passivointihetki täytetään historiallisesta päivämäärästä, käyttöönoton
  jälkeen kannassa on heti rivejä, joiden säilytysaika on jo umpeutunut. Tämä on tarkoitus, mutta se
  tarkoittaa myös, että **ensimmäinen ajo poistaa kerralla suuren joukon rivejä** — ajo on tehtävä
  ensin untuvassa ja tarkistettava poistuvien määrä (`SELECT count(*)` samalla ehdolla) ennen
  tuotantoon ottamista.
- Poisto lokitetaan `logAllInternalOnly`lla (ajastetussa tehtävässä ei ole `AuditContext`ia, §8).
- Poisto **ei** lähde Solkille: Solkilla on oma säilytysaikansa ja oma kopionsa rekisteristä.
- Propertyn oletusarvo on **pois päältä** kunnes sääntö on vahvistettu:
  `kitu.yki.scheduling.poistaVanhentuneetArvioijat.schedule=0 0 0 29 2 ?` (karkausvuoden 29.2.,
  eli käytännössä ei koskaan, mutta tehtävä näkyy db-scheduler-UI:ssa ja on ajettavissa käsin).
- Testattava erikseen kaikki kolme suojaa (ei-passiivinen oikeus, tuntematon alkuhetki,
  arviointioikeudeton merkintä) sekä se, että manuaalinen leima voittaa kauden päättymispäivän.

---

## 7. Käyttöoikeudet

### 7.1 `security/Authority.kt`

```kotlin
YKI_ARVIOIJAREKISTERI("YKI_ARVIOIJAREKISTERI_KIRJOITUS"),
```

→ `ROLE_APP_KIELITUTKINTOREKISTERI_YKI_ARVIOIJAREKISTERI_KIRJOITUS`. OPH liittää tämän Otuvassa
käyttöoikeusryhmään **"Kielitutkintorekisteri-oph-pääkäyttäjä"**. Luku jää `VIRKAILIJA`lle (lista on jo
nyt kaikkien virkailijoiden nähtävissä; erillinen lukuoikeus vaatisi turhaan uuden Otuva-määrittelyn).

> **VÄLIAIKAINEN POIKKEAMA (15.9.2026):** `YKI_ARVIOIJAREKISTERI` osoittaa toistaiseksi avaimeen
> `YKI_TALLENNUS`, koska oikean käyttöoikeuden tilaaminen Otuvaan (kortti **KTR-B**) on kesken eikä
> untuvassa/QA:ssa muuten pääse testaamaan syöttökäyttöliittymää. Seuraus: arvioijarekisterin
> kirjoitus on kaikilla joilla on YKI-tallennusoikeus, ja e2e:n käyttöoikeusmatriisissa Solki-
> käyttäjälle on väliaikaiset 200/404-odotukset. **Palauta arvo `YKI_ARVIOIJAREKISTERI_KIRJOITUS`
> heti kun käyttöoikeus on Otuvassa** — muuten prod-julkaisu menee liian väljillä oikeuksilla.

### 7.2 `security/WebSecurityConfig.kt`

Kirjoitussäännöt **vain CAS-ketjuun** (`casSecurityFilterChain`in `authorizeHttpRequests`-lohkoon),
ei jaettuun `configureCommonAuthorizations`-funktioon — muuten sama sääntö avautuisi myös
OAuth2-bearer-ketjuun, jonka ei ole tarkoitus päästä syöttökäyttöliittymään:

```kotlin
authorize(GET,  "/yki/arvioijat/uusi", hasAuthority(Authority.YKI_ARVIOIJAREKISTERI.role()))
authorize(POST, "/yki/arvioijat/**",   hasAuthority(Authority.YKI_ARVIOIJAREKISTERI.role()))
```

Jaettuun `configureCommonAuthorizations`iin (molemmat ketjut) uusi luku-API:

```kotlin
authorize(GET, "/yki/api/arvioijat/**", hasAnyAuthority(
    *Authority.YKI_ARVIOIJAREKISTERI.authStrings(), *Authority.YKI_TALLENNUS.authStrings()))
```

> **Huom (vaihe 4):** luku-API:n sääntöä **ei** vielä lisätty. Vaihe 3 vei listanäkymän CSV-viennin
> polkuun `GET /yki/api/arvioijat`, ja §7.1:n mukaan luvun pitää säilyä kaikilla virkailijoilla —
> sääntö veisi CSV-viennin pelkiltä virkailijoilta. Ratkaistaan vaiheessa 5, kun §4.1:n JSON-luku-API
> saa oman polkunsa.

`GET /yki/arvioijat` ja `/yki/arvioijat/{id}` osuvat CAS-ketjun loppusääntöön `hasAuthority(VIRKAILIJA)`.
CSRF: `/yki/arvioijat/**` **ei** ole `ignoringRequestMatchers`-listalla → lomakkeiden **on** käytettävä
`formPost(...)`ia, joka injektoi tokenin; suora `form { }` epäonnistuu 403:lla.

### 7.3 Uusi `security/CurrentUser.kt`

Plain `object` (**ei** `@Service` — vrt. `i18n/CurrentLanguage`, välttää `@WithSpan`-CGLIB-ansan):

```kotlin
object CurrentUser {
    fun hasAuthority(authority: Authority): Boolean
    fun oid(): Oid?      // CasUserDetails.oid — myös luoja_oid/muokkaaja_oid -arvojen lähde
}
```

### 7.4 `dev/MockLoginController.kt`

`MockUser.DEFAULT` ja `MockUser.ROOT` saavat `Authority.YKI_ARVIOIJAREKISTERI`. Uusi
`MockUser.VIRKAILIJA` (pelkkä `Authority.VIRKAILIJA`) e2e-oikeusmatriisiin todistamaan 200 luvulle /
403 kirjoitukselle.

### 7.5 Ominaisuuskytkimet: muokkaus ja integraatio

Syöttökäyttöliittymä valmistuu vaiheittain, mutta `main` deployautuu suoraan prodiin asti. Molemmat
kytkimet ovat siksi oletuksena **pois päältä** (`ArvioijarekisteriAsetukset`, oletus `false`), eli
ilman propertyjä rekisteri toimii vain lukutilassa eikä integraatiota ole.

| Kytkin                                           | Mitä ohjaa                                                                                                                                                                                                              |
| ------------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `kitu.yki.arvioijarekisteri.muokkaus.enabled`    | Virkailijan kirjoitusreitit (`WebSecurityConfig`, `denyAll` vs. `hasAuthority`) ja käyttöliittymän napit                                                                                                                |
| `kitu.yki.arvioijarekisteri.integraatio.enabled` | **Molemmat suunnat Solkiin**: sisääntulon kavennus (§4.2), lähtevän **automaattisen** lähetyksen toteutus ja ajastukset, sekä projektion yöllinen päivitys. Ei koske virkailijan käsin käynnistämää lähetystä, ks. alla |

Kytkimet ovat erillisiä, koska ne vastaavat eri kysymyksiin: _saako virkailija muokata_ ja _onko kitu
rekisterin master_. Käyttöönotossa nämä tapahtuvat eri hetkinä (§10 vaihe 11, §11 kysymys 9).

| muokkaus | integraatio | Tila                                                                |
| -------- | ----------- | ------------------------------------------------------------------- |
| pois     | pois        | Lähtötila: vain luku, Solki on master ja kirjoittaa koko payloadin  |
| päällä   | pois        | **Siirtymän työtila**: virkailija syöttää kitussa, Solki yhä master |
| päällä   | päällä      | Tavoitetila: kitu on master                                         |
| pois     | päällä      | Ei käytössä; kytkinjärjestys on muokkaus ensin                      |

Muokkauskytkimen kaksi vaikutusta ovat ennallaan: palvelin torjuu kirjoituksen (403 myös suoralla
URL:lla) ja napit renderöidään estettyinä selitteen kanssa. Sääntö valitaan kerran käynnistyksessä,
joten kytkimen kääntäminen vaatii uudelleenkäynnistyksen.

**Käsin lähetys on integraatiokytkimen ulkopuolella.** Tietosivun "Lähetä uudelleen Solkiin"
-painike kutsuu `SolkiArvioijaService.lahetaArvioijaKasin`ia, joka lähettää myös kytkimen ollessa
pois; kytkin estää vain automaattisen liikenteen (tallennuksen jälkeisen yrityksen, eräajon ja
ajastukset). Ilman tätä integraatiota ei pääsisi kokeilemaan hallitusti yksi rivi kerrallaan ennen
kuin koko lähetysjono avataan. Painike pysyy muokkauskytkimen takana, koska POST-reitti on.
Seuraus toteutukseen: `SolkiArvioijaClientImpl` ja `SolkiArvioijaServiceImpl` ovat ehdottomia
beaneja, ja aiempi `SolkiArvioijaServiceMock` poistui — kytkin luetaan palvelun sisällä.

**Siirtymän työtilan (muokkaus päällä, integraatio pois) haitta on syytä tuntea.** Sisääntuleva
täysi push kirjoittaa `yki_arviointioikeus`-projektion koskematta `yki_arvioija_arviointikausi`
-masteriin, joten taulut ajautuvat väistämättä eri linjoille. Solkin push voi siis yliajaa
projektion, jonka virkailija juuri johti kitun kausista. Korjaus on uusi kausimuutos kyseiselle
arvioijalle — **ei** `synkronoiArvioijaKaudet`, joka rakentaisi kaudet oikeuksista ja toisi Solkin
yliajon masteriin. Juuri tästä syystä yöllinen `paivitaArvioijaProjektiot` on kytketty
integraatiokytkimeen: se on turvaton täsmälleen silloin kun täysi sisääntulo on päällä, ja yliajaisi
koko rekisterin vanhentuneesta masterista sekä työntäisi sen Solki-lähetysjonoon.

E2E ajaa yhdellä profiililla, joten kytkimet pois päältä -tilat katetaan backend-testeillä
`YkiArvioijaMuokkauskytkinTest`, `YkiArvioijaIntegraatiokytkin{Pois,Paalla}Test` ja
`YkiArvioijaProjektiokytkinTest` (`@SpringBootTest(properties = [...])`).

---

## 8. Auditlokit

`auditlogs/AuditLogEntry.kt`:n `AuditLogOperation` on suljettu enum, jossa on tällä hetkellä vain
`*Viewed`-operaatioita (ja käyttämätön `YkiSuoritusPatched`). Lisätään:

```kotlin
YkiArvioijaViewed("YkiArvioijaViewed"),
YkiArvioijaCreated("YkiArvioijaCreated"),
YkiArvioijaUpdated("YkiArvioijaUpdated"),
YkiArvioijaPassivated("YkiArvioijaPassivated"),
```

| Operaatio               | Kutsupaikka (`YkiArvioijaService`, `target = arvioijaOid`)    |
| ----------------------- | ------------------------------------------------------------- |
| `YkiArvioijaViewed`     | `haeArvioija(id)` (tietosivu)                                 |
| `YkiArvioijaCreated`    | `luoArvioija`, onnistuneen INSERTin jälkeen                   |
| `YkiArvioijaUpdated`    | `paivitaArvioija`, onnistuneen UPDATEn jälkeen                |
| `YkiArvioijaPassivated` | `passivoiArvioija` (manuaalinen)                              |
| —                       | säilytysajan poisto ja listan massaluku: `logAllInternalOnly` |

Tämä on projektin **ensimmäinen** luonti/muokkaus-audit-operaatio.

---

## 9. Testaus

### 9.1 Backend (`server/src/test/kotlin/fi/oph/kitu/yki/arvioijat/`)

Uudet:

- `ArviointikausiTest.kt` — +5 v, karkauspäivä 29.2. → 28.2.
- `YkiArvioijaValidationTest.kt` — pakolliset kentät, postinumeromuoto, duplikaattikieli, tyhjät tasot.
- `ArvioijaFormDataTest.kt` — `"FIN:PT"`-koodauksen purku, viallisten arvojen sieto, päättymispäivä.
- `YkiArvioijaColumnTest.kt` — tagikattavuus, CSV-otsikot, `PERSONAL_DATA`-poissulku
  (mallina `YkiSuoritusColumnTest`).
- `YkiArvioijaServiceTest.kt` — `@SpringBootTest` + Testcontainers + `MockOppijanumeroService`:
  luonti oppijanumerolla, päivitys laskee päättymispäivän uusiksi, kielen poisto,
  **kausihistoriarivin synty vain kun kausi muuttuu**, manuaalinen passivointi.
- `SolkiArvioijaClientTest.kt` — `MockRestServiceServer`. **Muista laiskan `RestClient`in ansa:**
  `@TestInstance(PER_CLASS)`, yksi `reset()`-palvelin, kaikki client-mock-testit samassa luokassa.
- `SolkiArvioijaServiceTest.kt` — outbox-tilakone: onnistuminen nollaa virheen ja laskurin, 400 on pysyvä,
  500 kasvattaa laskuria, `findLahetettavat(3)` vs `findLahetettavat(null)`.
- `YkiArvioijaViewControllerTest.kt` — MockMvc: 403 ilman oikeutta, 200 sen kanssa, virheellinen POST →
  200 + `aria-invalid`, onnistunut POST → 303.

Muutettavat: `YkiArvioijaRepositoryTest.kt` (uusi `tallenna`-semantiikka, arviointioikeuksien korvaus,
outbox-kentät, kausihistoria, `poistaSailytysajanYlittaneet`), `YkiApiControllerTest.kt` (arvioija-testit jäävät;
lisätään testit siitä, että push päivittää vain yhteystiedot), `webmvc/DashboardServiceTest.kt`. Poistetaan `yki/YkiArvioijaErrorTests.kt`.

### 9.2 E2E (`e2e/`)

- `fixtures/ykiArvioija.ts` — uudet sarakkeet; variantit `insertPaattynytKausi`, `insertSolkiVirheella`,
  `insertLahettamaton`.
- `models/yki/YkiArvioijatPage.ts` (päivitys: suodatin, sivutus, "Lisää arvioija", rivilinkki) sekä uudet
  `YkiArvioijaPage.ts`, `YkiArvioijaLomake.ts`, `YkiArvioijatFilterDialog.ts`.
- `tests/yki/yki-arvioijat.spec.ts` — päivitetyt sarakeodotukset (hetu pois, Solki-tila mukaan),
  suodatus, sivutus, CSV.
- Uusi `tests/yki/yki-arvioija-lisays.spec.ts` — ONR-haku → esitäyttö → tallennus → flash → rivi listalla;
  yksilöimätön oppijanumero → kenttävirhe; puuttuva pakollinen kenttä → lomake re-renderöityy
  `aria-invalid`illa eivätkä syötteet häviä.
- Uusi `tests/yki/yki-arvioija-muokkaus.spec.ts` — uusi kauden alkupäivä laskee päättymispäivän ja
  **synnyttää kausihistoriarivin**, kielen lisäys/poisto, manuaalinen passivointi dialogin kautta.
- Uusi `tests/yki/yki-arvioija-solki.spec.ts` — dev-stubi 500 → virhenäkymässä rivi + syy + laskuri;
  "Lähetä uudelleen" (stubi 204) → virhe katoaa.
- `tests/security/securityconfig.spec.ts` — uudet reitit ja uusi `MockUser.VIRKAILIJA`-lohko;
  `POST /yki/api/arvioija` säilyy ennallaan.

---

## 10. Vaiheistus (commitit yhdessä PR:ssä)

> **Tila 17.9.2026.** Vaiheet 1–6, 8, 9, 10 ja 12 on mergetty mainiin ja vaihe 7 raukesi (§6.1).
> Vaihe 11 on osittain tehty: muokkauskytkin on avattu untuvaan ja QA:han (`210c4b8e`), mutta
> tuotannossa muokkaus ja Solki-integraatio ovat yhä pois päältä.
> Merkinnät: ✅ mergetty, ◐ osittain, ⊘ rauennut.

Toteutus tehdään **yhdessä haarassa `yki-arvioija-laajenna-taulut-masteriksi` ja yhdessä PR:ssä**
([#3324](https://github.com/Opetushallitus/kielitutkintorekisteri/pull/3324)): kukin alla oleva askel on
yksi tai useampi commit samassa PR:ssä, ei omaa PR:ää. Askeleet pysyvät silti itsenäisinä ja
järjestyksessä katselmoitavina kokonaisuuksina — pidä commitit ehjinä, jotta PR:n voi lukea askel
kerrallaan. Askeleet 1–8 eivät riipu Solkin rajapintasopimuksesta, joten työ ei jää odottamaan
Jyväskylää. Askel 11 tehdään vasta tämän PR:n mergen jälkeen omana muutoksenaan, kun JYU on
vahvistanut rajapinnan.

| #     | Vaihe                                                    | Koko | Riippuu                      | Tila     |
| ----- | -------------------------------------------------------- | ---- | ---------------------------- | -------- |
| 1     | Poista kuollut arvioijien virhetuontikoneisto            | S    | —                            | ✅ #3323 |
| 2     | Laajenna arvioijataulut masteriksi (V116–V118)           | L    | 1                            | ✅ #3324 |
| 3     | Uudista arvioijalistanäkymä                              | L    | 2                            | ✅ #3325 |
| 4     | Lomakevirhekehys ja arvioijarekisterin käyttöoikeus      | M    | —                            | ✅ #3324 |
| 5     | Uuden arvioijan tallennus + ONR-haku (UC1)               | L    | 2, 4                         | ✅ #3324 |
| 6     | Muokkaus, kausihistoria ja manuaalinen passivointi (UC2) | L    | 5                            | ✅ #3353 |
| ~~7~~ | ~~Automaattinen passivointi (UC3)~~ — rauennut, ks. §6.1 | –    | —                            | ⊘        |
| 8     | Säilytysajan valvonta (5 v)                              | M    | 6                            | ✅ #3372 |
| 9     | YKI-arvioijien Solki-lähetys                             | L    | 6 (sopimus sovittu 1.9.2026) | ✅ #3378 |
| 10    | Kavenna sisääntuleva rajapinta yhteystietoihin (§4.2)    | M    | 9                            | ✅ #3378 |
| 11    | Käyttöönotto ja kytkimet                                 | S    | 9, 10                        | ◐        |
| 12    | Arviointikausien hallinta (V122–V124)                    | L    | 6                            | ✅       |

1. **`Poista kuollut arvioijien virhetuontikoneisto`** — `yki/arvioijat/error/`, `V118` DROP TABLE,
   `dev/YkiController` kuollut stubi, `DashboardService`/`HomePage`/`EnumFromUrlParamsParsingConfig`
   -viittaukset, `YkiArvioijaErrorTests`. (Vanhentuneet CSV-tuontiväitteet `CLAUDE.md`:ssä ja
   `arkkitehtuuri.md`:ssä on jo korjattu tämän suunnitelman PR:ssä.)
2. **`Laajenna arvioijataulut masteriksi`** — `V116` (uudet sarakkeet, outbox, `passivoitu`-backfill),
   `V117` (kausihistoria), entiteetit + `RowMapper`it, `YkiArvioijaRepository.tallenna`
   (arviointioikeuksien korvaus + kausihistorian kirjaus), repository-testit.
3. **`Uudista arvioijalistanäkymä`** — `@ColumnTags`, `YkiArvioijaParams`, sivutus arvioijittain,
   suodatindialogi, CSV-vienti, uusi `YkiArvioijaViewController` + `Links`-lisäykset. e2e-päivitys.
4. **`Lisää lomakevirhekehys ja arvioijarekisterin käyttöoikeus`** — `html/FormErrors.kt`,
   `security/CurrentUser.kt`, `Authority.YKI_ARVIOIJAREKISTERI`, security-säännöt, `MockUser`-päivitykset,
   e2e-oikeusmatriisi.
5. **`Lisää uuden YKI-arvioijan tallennus (UC1)`** — `Arviointikausi`, `TallennaArvioija`,
   `YkiArvioijaError`, uusi `YkiArvioijaValidation` (sama kausi kaikille kielille), jatkokauden päättely,
   `YkiArvioijaService.luoArvioija`, oppijanumerohaku + esitäytetty lomake,
   ASHA-numerokenttä, turvakieltovaroitus, audit-operaatiot, UiText. e2e.
6. **`Lisää arvioijan muokkaus, kausihistoria ja manuaalinen passivointi (UC2)`** — tietosivu +
   kausihistoriataulukko + POST, `passivoiArvioija`, kielen lisäys perii voimassa olevan kauden. e2e.
7. _(rauennut)_ **`Lisää arvioijien automaattinen passivointi (UC3)`** — toteutui ilman ajoa, kun tila
   alettiin laskea kauden päivistä (§1.5, §6.1).
8. **`Lisää säilytysajan valvonta`** — §6.2:n poistotehtävä (alkuhetki
   `COALESCE(passivoitu, max(kauden_paattymispaiva))`), oletuksena pois päältä, testit suojaehdoille.
   Ajettava ensin untuvassa ja tarkistettava poistuvien määrä. Tämä on **seuraava vaihe**.
9. **`Lisää YKI-arvioijien Solki-lähetys`** — `solki/`-paketti, outbox-kirjoitukset, dev-stubi, propertyt
   (`enabled=false`), virhenäkymä + "Lähetä uudelleen" + dashboard-laskuri,
   `docs/technical/integraatiot.md`. Testit + e2e.
10. **`Kavenna sisääntuleva arvioijarajapinta`** — endpoint **jää käyttöön**, koska Solki lähettää
    yhteystietojen päivityksiä (§4.2). Kavennus tarkoittaa, että payloadista sovelletaan vain
    yhteystietokentät; nimet, arviointioikeudet ja tila ohitetaan, eikä tuntematonta arvioijaa luoda.
    Tallennettu `tila` säilyy kannassa siirtymän ajan.
11. **Käyttöönotto kahdessa askeleessa** (§7.5), koska kytkimet ovat erilliset:

    a. `kitu.yki.arvioijarekisteri.muokkaus.enabled=true` untuvaan/QA:han/prodiin. Käyttöliittymä
    avautuu, Solki on yhä master ja saa kirjoittaa koko merkinnän. Tämä on §11:n kysymyksen 9
    kuvaama jakso, jolla molemmat voivat kirjoittaa; §7.5 kertoo mitä siitä seuraa.

    b. _(JYU:n vahvistuksen jälkeen)_ `kitu.yki.arvioijarekisteri.integraatio.enabled=true`.
    Sisääntulo kaventuu, lähetys Solkiin alkaa ja projektion yöllinen päivitys käynnistyy.
    **Ennen tätä on ajettava `synkronoiArvioijaKaudet` käsin db-scheduler-UI:sta:** kausimasteri
    on ollut vanhentunut siitä asti kun täysi sisääntulo alkoi kirjoittaa pelkkää projektiota,
    ja yöllinen ajo lähtee siitä hetkestä masterin varaan.

    Sitten säilytysajan poisto päälle. Vasta tämän jälkeen voi harkita tallennetun `tila`-sarakkeen
    poistoa (§4.2). `henkilotunnus`-saraketta **ei** poisteta: ennen 2026 alkaneiden kausien hetut on
    säilytettävä lain nojalla.

12. **`Hallitse arviointikausia arvioijan sivulta`** — §1.6:n taulut ja siirto, `Kausiprojektio`,
    `YkiArvioijaKausiRepository`/`-Service`/`-ViewController`, kausilomake, tietosivun uusi
    kausitaulukko + muutoshistoria `<details>`in takana, muokkauslomakkeen kausikenttien poisto (§3.4),
    päällekkäisyysvalidointi, säilytysajan suojaus, yöllinen projektion päivitys ja käsin ajettava
    kausisynkronointi. Testit + e2e.

Muista jokaisen askeleen lopuksi `./scripts/format.sh` (ktlint + prettier). Pidä pitkäikäinen haara
ajan tasalla komennolla `git pull --rebase origin main` ja pushaa `--force-with-lease`illa. Älä avaa
vaihekohtaisia haaroja tai pinottuja PR:iä. Infraan ei tule muutoksia, joten `infra/README.md` pysyy
koskemattomana.

---

## 11. Avoimet kysymykset

**Jyväskylän yliopisto / Solki**

1. ~~Koko §5.1:n REST-sopimus on ehdotus. Vahvistettava: polku, verbi, autentikointi, virhekoodit,
   `versio`-semantiikka, `Idempotency-Key`, per-ympäristö base-URL ja tunnukset.~~
   **Ratkaistu:** JYU hyväksyi sopimuksen 1.9.2026 ja julkaisi toteutuksensa 14.9.2026; §5.1 on
   korjattu sitä vastaavaksi 16.9.2026.
2. ~~Voiko Solki avaimentaa pelkällä oppijanumerolla?~~ **Vastattu OPH:n tavoitetilakuvauksessa:**
   _"arvioijarekisterimerkinnän tiedot voidaan lisätä arvioijan tietoihin kielitutkintorekisteristä
   OID-tunnisteella."_ Varmistetaan JYU:lta enää tekninen toteutettavuus ja aikataulu.
3. ~~Autentikointi: HTTP Basic vai Otuva OAuth2 client credentials?~~ **Ratkaistu §5.1.1:ssä:** HTTP Basic.
4. Mitä Solki tekee, kun kieli katoaa payloadista — poistaako oikeuden vai säilyttääkö historian?
5. ~~**Solki→kitu-muutosrajapinta**~~ — ratkaistu 1.9.2026: passivointi tehdään aina kitun
   käyttöliittymästä, ja sisääntuleva suunta kavennetaan yhteystietoihin (§4.2).

6. **Onko arvioija aina jo olemassa Solkissa, kun kitu lähettää merkinnän?** Tavoitetilakuvauksen mukaan
   Solki luo arvioijalle käyttäjätunnuksen ja viisinumeroisen arvioijatunnuksen jo koulutuksen yhteydessä,
   joten kitun lähetys (julkaistussa rajapinnassa `POST /oph/arvioija`, ei enää `PUT`) olisi päivitys
   olemassa olevaan riviin. Mitä Solki tekee, jos OID:ta ei tunneta — luodaanko rivi vai palautetaanko
   virhe? Käytännössä 409 käsitellään lähetysvirheenä (`ac31d84a`).

**Jyväskylän yliopisto / Solki — OPH:n päätöksistä seuranneet uudet kysymykset**

7. ~~**Merkintä voi viivästyä, jos ONR-yksilöinti on kesken.**~~ **Rauennut 28.8.2026:** arvioijalla on
   aina jo oppijanumero ONR:ssä, joten keskeneräistä yksilöintiä ei enää synny eikä lähetys viivästy.
8. **Kitu poistaa merkinnät 5 vuoden kuluttua passivoinnista** (§6.2). Poistoa **ei** ole tarkoitus välittää
   Solkille — oletus on, että Solkilla on oma säilytysaikansa ja oma kopionsa rekisteristä. Pitääkö tämä
   paikkansa? Huomattava myös, ettei kitu poiston jälkeen pysty vastaamaan kyseistä arvioijaa koskeviin
   kyselyihin, mikäli Solki käyttää täsmäytykseen kitun luku-rajapintaa (§4.1).
9. **Cutover-päivä ja päällekkäisen kirjoituksen välttäminen.** OPH:n linjaus (kys. 14) on julkaista
   syöttökäyttöliittymä ensin ja katkaista Solkin kirjoitukset vasta sen jälkeen, jolloin väliin jää jakso,
   jolla molemmat voisivat kirjoittaa. Sovitaanko jaksolle menettely — esim. että Solki lopettaa
   arvioijamerkintöjen tekemisen sovittuna päivänä, vaikka rajapinta olisi teknisesti auki? Kuka tarkistaa
   datan täsmäävyyden ennen kytkintä? **Tämä on ainoa kysymys, jolla on kalenteririippuvuus.**

**OPH — avoimet**

OPH on vastannut kaikkiin 14 kysymykseen (ks. suunnitelman alun päätöstaulukko). Viimeinenkin avoin
kohta on nyt ratkaistu:

10. ~~**Henkilötunnussarakkeen kohtalo.**~~ **Ratkaistu 28.8.2026: sarake säilyy pysyvästi.** Ennen 2026
    alkaneiden kausien hetut on säilytettävä lain nojalla, ja V100 tyhjensi hetun vain niiltä, joiden
    arviointioikeus alkaa 1.1.2026 tai myöhemmin. Sarakkeen poistoa ei siis suunnitella. Huomaa, että
    upsert kirjoittaa `henkilotunnus = EXCLUDED.henkilotunnus`: kumpikaan kirjoittaja ei kanna hetua,
    joten kosketun rivin hetu tyhjenee — tämä on tarkoituksellista eikä sitä muuteta.

**OPH — vastauksista seuraavat tarkennettavat kohdat**

Nämä eivät estä toteutusta, mutta on syytä varmistaa ennen kyseisen vaiheen koodausta:

11. ~~**Kahteen kertaan lisätty henkilö.**~~ **Rauennut 28.8.2026:** merkintä avaimennetaan aina ONR:n
    master-oppijanumerolla, joten keskeneräisestä yksilöinnistä johtuvaa kaksoisriviä ei voi syntyä.
12. **ASHA-numeron muoto.** Toteutetaan vapaana tekstikenttänä ilman validointia. Jos numerolla on vakiintunut
    muoto (diaarinumero), validointi kannattaa lisätä myöhemmin.

---

## 12. Verifiointi

```shell
# Muotoilu (aina ennen valmiiksi raportointia)
./scripts/format.sh && ./scripts/check-formatting.sh

# Backend-testit (Docker päällä — Testcontainers)
cd server && ./mvnw test -Dtest='YkiArvioija*'   # sis. YkiArvioijaMuokkauskytkinTest
cd server && ./mvnw test -Dtest='SolkiArvioija*'
cd server && ./mvnw package            # koko sarja

# E2E
cd e2e && TEST_WORKERS=4 npx playwright test yki-arvioija

# Paikallinen ajo
./scripts/start_local_env.sh
```

Manuaalinen läpiajo paikallisesti (`http://localhost:8080/kielitutkinnot`):

1. `/dev/mocklogin/ROOT` → kirjaudu.
2. `/yki/arvioijat` → lista renderöityy, suodatin ja sivutus toimivat, CSV latautuu ja sarakkeet
   vastaavat `CSV_EXPORT`-tageja.
3. "Lisää arvioija" → syötä oppijanumero → ONR-haku (`e2e`-profiilissa `MockOppijanumeroService`) →
   lomake esitäyttyy → tallenna tyhjällä pakollisella kentällä → **kenttäkohtainen virhe näkyy eikä
   syötteitä häviä** → korjaa → tallennus onnistuu, flash-viesti näkyy.
   Tarkista: `select henkilotunnus from yki_arvioija where arvioija_oid = …` → **NULL** (uusi
   merkintä ei koskaan tallenna hetua).
4. Kauden päättymispäivä on täsmälleen 5 v alkupäivästä ja sama kaikilla valituilla kielillä.
5. Muokkaa kauden alkupäivä → `select * from yki_arvioija_kausi where arvioija_id = …` sisältää nyt
   **kaksi** riviä; pelkkä sähköpostin muutos ei lisää kolmatta.
6. Poista yksi kieli arviointioikeusmatriisista → rivi katoaa myös `yki_arviointioikeus`-taulusta.
7. Passivoi manuaalisesti → tila päivittyy, `solkiin_lahetetty` nollautuu, historiaan tulee rivi.
8. Solki-stubi päälle → tallennus lähettää heti; stubi palauttamaan 500 → `/yki/arvioijat/virheet`
   näyttää rivin syineen ja yrityslaskureineen, ja "Lähetä uudelleen" tyhjentää virheen.
9. db-scheduler-UI: kaikki viisi uutta tehtävää näkyvät ja ovat käsin ajettavissa: säilytysajan
   poisto, arviointioikeusprojektion päivitys, kausien synkronointi arviointioikeuksista sekä
   Solki-lähetyksen pikauusinta ja yöajo. (Automaattinen passivointi peruttiin, ks. §6.1; kaksi
   Solki-ajoa näkyy vain, kun `…integraatio.enabled=true`.)
10. Auditlokit: konsolista löytyvät `YkiArvioijaCreated` ja `YkiArvioijaUpdated`.
11. Ominaisuuskytkin (§7.5): käynnistä
    `SPRING_APPLICATION_JSON='{"kitu":{"yki":{"arvioijarekisteri":{"muokkaus":{"enabled":false}}}}}'
→ "Lisää arvioija" ja "Muokkaa" näkyvät harmaina eivätkä avaa mitään, ja `/yki/arvioijat/uusi`
    vastaa 403:lla.

---

## Liite A: Kanban-kortit

Suunnitelman §10 vaiheistus korttimuodossa. Koot ovat suhteellisia: **S** ≈ 1–2 pv, **M** ≈ 3–5 pv,
**L** ≈ 1–2 vk. Kokonaisuus on jonkin verran käyttötapauskuvauksen ~6 viikon arviota suurempi, koska
laajuus kasvoi säilytysajalla ja listanäkymän uudistuksella.

### Ei-tekniset kortit

Nämä kannattaa aloittaa ensimmäisenä: kummassakin on ulkoisen tahon läpimenoaikaa, eikä kumpikaan etene
tiimin omalla työllä.

| Kortti        | Sisältö                                                                                                                                                                                                         | Estää                         |
| ------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------- |
| **KTR-A**     | Sovi Solki-rajapintasopimus Jyväskylän kanssa (§5.1, §11 kysymykset 1–9)                                                                                                                                        | KTR-9, KTR-10, KTR-11         |
| **KTR-B**     | Pyydä käyttöoikeus `YKI_ARVIOIJAREKISTERI_KIRJOITUS` Otuvaan, liitettäväksi ryhmään "Kielitutkintorekisteri-oph-pääkäyttäjä" — **kesken 15.9.2026**, siihen asti käytössä väliaikaisesti `YKI_TALLENNUS` (§7.1) | prod-julkaisun (ei kehitystä) |
| ~~**KTR-C**~~ | ~~Tietosuojan päätös `henkilotunnus`-sarakkeesta~~ — ratkaistu: sarake säilyy pysyvästi (§11 kys. 10)                                                                                                           | —                             |

### Kehityskortit

| Kortti    | Otsikko                                                  | Koko | Riippuu       | Tila  |
| --------- | -------------------------------------------------------- | ---- | ------------- | ----- |
| KTR-1     | Poista kuollut arvioijien virhetuontikoneisto            | S    | —             | ✅    |
| KTR-2     | Laajenna arvioijataulut masteriksi                       | L    | KTR-1         | ✅    |
| KTR-3     | Uudista arvioijalistanäkymä                              | L    | KTR-2         | ✅    |
| KTR-4     | Lomakevirhekehys ja arvioijarekisterin käyttöoikeus      | M    | —             | ✅    |
| KTR-5     | Uuden arvioijan tallennus + ONR-haku (UC1)               | L    | KTR-2, KTR-4  | ✅    |
| KTR-6     | Muokkaus, kausihistoria ja manuaalinen passivointi (UC2) | L    | KTR-5         | ✅    |
| ~~KTR-7~~ | ~~Automaattinen passivointi (UC3)~~ — rauennut           | –    | —             | ⊘     |
| KTR-8     | Säilytysajan valvonta (5 v)                              | M    | KTR-6         | ✅    |
| KTR-9     | YKI-arvioijien Solki-lähetys                             | L    | KTR-6, KTR-A  | 🔄 PR |
| KTR-10    | Kavenna sisääntuleva arvioijarajapinta                   | M    | KTR-9         | 🔄 PR |
| KTR-11    | Käyttöönotto ja kytkimet                                 | S    | KTR-9, KTR-10 | —     |

**KTR-4 on rinnakkaistettavissa** — se ei riipu tietokantatyöstä, joten kaksi tekijää voi edetä yhtä
aikaa (KTR-2 → KTR-3 ja KTR-4). Muuten ketju on käytännössä lineaarinen.

**KTR-8 on laajuuden kasvua**, ei alkuperäistä sisältöä: se seurasi OPH:n vastauksesta (säilytysaika
5 vuotta). Se on syytä nostaa esiin, jos
työmääräarviota verrataan käyttötapauskuvauksen alkuperäiseen arvioon.

### Korttien sisältö ja valmiin määritelmä

**KTR-1 · Poista kuollut arvioijien virhetuontikoneisto** — S
Poistetaan `yki/arvioijat/error/`, `V118 DROP TABLE yki_arvioija_error`, `dev/YkiController`in kuollut
stubi sekä viittaukset `DashboardService`/`HomePage`/`EnumFromUrlParamsParsingConfig`.
_Valmis kun:_ `/yki/arvioijat/virheet` ja dashboard-laskuri on poistettu tai ohjattu uudelleen,
`YkiArvioijaErrorTests` poistettu, build vihreä.

**KTR-2 · Laajenna arvioijataulut masteriksi** — L
`V116` (uudet sarakkeet, `asha_numero`, `passivoitu` + backfill kauden päättymispäivästä,
outbox-sarakkeet, `solkiin_lahetetty`-backfill) ja `V117` (`yki_arvioija_kausi`).
Entiteetit, `RowMapper`it ja `YkiArvioijaRepository.tallenna`.
_Valmis kun:_ arviointioikeuksien poisto toimii upsertissä, kausihistoriarivi syntyy vain kauden
muuttuessa, migraatio ei laukaise massalähetystä Solkiin, repository-testit läpi.

**KTR-3 · Uudista arvioijalistanäkymä** — L
`@ColumnTags`, `YkiArvioijaParams`, sivutus arvioijittain, suodatindialogi, CSV-vienti, uusi
`YkiArvioijaViewController` + `Links`-lisäykset.
_Valmis kun:_ suodatus, järjestys ja sivutus toimivat, CSV vastaa `CSV_EXPORT`-tageja, "piilota
henkilötiedot" toimii, e2e päivitetty.

**KTR-4 · Lomakevirhekehys ja käyttöoikeus** — M
`html/FormErrors.kt` + `formField`, `security/CurrentUser.kt`, `Authority.YKI_ARVIOIJAREKISTERI`,
security-säännöt vain CAS-ketjuun, `MockUser`-päivitykset.
_Valmis kun:_ virheellinen lomake renderöityy uudelleen `aria-invalid`illa eivätkä syötteet häviä;
e2e-oikeusmatriisi kattaa sekä 403:n että 200:n.

**KTR-5 · Uuden arvioijan tallennus + ONR (UC1)** — L
`Arviointikausi`, `TallennaArvioija`, `YkiArvioijaValidation`, jatkokauden päättely,
oppijanumerohaku, esitäytetty lomake, arviointioikeusmatriisi, ASHA-numerokenttä,
turvakieltovaroitus, auditlokit, UiText.
_Valmis kun:_ kausi on alkupäivä + 5 v − 1 pv ja sama kaikilla kielillä, **yksilöimätön oppijanumero hylätään**, e2e vihreä.

**KTR-6 · Muokkaus, kausihistoria ja manuaalinen passivointi (UC2)** — L
Tietosivu + kausihistoriataulukko + POST, `passivoiArvioija`.
_Valmis kun:_ uusi kauden alkupäivä synnyttää historiarivin mutta pelkkä yhteystiedon muutos ei; kielen
lisäys kesken kauden perii voimassa olevan kauden.

**KTR-7 · Automaattinen passivointi (UC3)** — ~~M~~ **rauennut**
Tila lasketaan kauden päivistä (§1.5), joten päättynyt kausi näkyy passiivisena ilman ajoa. Ks. §6.1.

**KTR-8 · Säilytysajan valvonta (5 v)** — M
§6.2:n poistotehtävä, oletuksena pois päältä.
_Valmis kun:_ kaikki kolme suojaa on testattu (ei-passiivinen oikeus, tuntematon alkuhetki,
arviointioikeudeton merkintä), ja tehtävä on **ajettu untuvassa ja poistuvien rivien määrä
tarkistettu** ennen kuin tuotantoon ottamista harkitaan.

**KTR-9 · YKI-arvioijien Solki-lähetys** — L
`solki/`-paketti, outbox-kirjoitukset, dev-stubi, propertyt (`enabled=false`), virhenäkymä +
"Lähetä uudelleen" + dashboard-laskuri, `docs/technical/integraatiot.md`.
_Valmis kun:_ 3 yritystä ja yöajo toimivat, virhe näkyy syineen,
`.withLenientStringConverter()` on mukana ja `debugString()` ei vuoda henkilötietoja lokiin.

**KTR-10 · Kavenna sisääntuleva arvioijarajapinta** — M
`POST /yki/api/arvioija` jää käyttöön mutta soveltaa vain yhteystietokentät (§4.2).
_Valmis kun:_ Solki voi päivittää yhteystiedot muttei nimiä, kausia eikä tilaa, tuntematonta arvioijaa
ei luoda, ja **yhteystietopäivitys ei laukaise kaikuvaa PUT-lähetystä takaisin Solkiin**.

**KTR-11 · Käyttöönotto ja kytkimet** — S
`enabled=true` untuvaan, QA:han ja prodiin, cutover sovitusti.
_Valmis kun:_ cutover-päivä on sovittu JYU:n kanssa ja datan täsmäävyys tarkistettu ennen kytkintä.
