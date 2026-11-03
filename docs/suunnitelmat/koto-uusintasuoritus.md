# Kotoutumiskoulutuksen uusintasuoritus: suunnitelma

**Tila 3.11.2026:** ei toteutettu. Toteutus odottaa Mediamaisterin ja OPH:n vastauksia. Kun toteutus valmistuu, poista tämä tiedosto ja siirrä pysyvä sisältö tiedostoihin `docs/technical/integraatiot.md` ja `CLAUDE.md`.

## Tausta

On päätetty, että kotoutumiskoulutuksen päättötestin saa uusia, joten uusintasuorituksia voi alkaa tulla Koealustalta (Moodle, Mediamaisteri). Niitä ei vielä ole.

**Oletettu muoto (muistikuva, vahvistamatta):** uusintasuorituksella on sama `courseid` ja `userid` kuin alkuperäisellä, ja päivä vaihtuu uusinnan päiväksi.

## Mitä alkuperäiselle suoritukselle tapahtuu

Kitu säilyttää alkuperäisen, oli uutta saraketta tai ei (luettu koodista, ei ajettu):

- `suoritusaika` on osa avainta `findLatestSuoritusVersion`issa, joten uudella päivällä `exists()` on epätosi ja uusinta lisätään uutena rivinä.
- Koton koodissa ei ole UPDATE-lausetta. Ainoa DELETE koskee keskeneräisiä.
- Lista ja CSV käyttävät ehtoa `DISTINCT ON (kurssi_id, oppijanumero, suoritusaika)`, joten molemmat suorituskerrat näkyvät.

Oletetussa muodossa Moodle ei enää raportoi alkuperäistä, joten kitun rivi on ainoa kopio. Mikään testi ei vielä varmista tätä, koska `updated suoritus is saved` käyttää samaa `timecompleted`-arvoa.

| Uusinta saapuu näin                             | Mitä kitu tekee nyt                                                                                                   | Tulos                                           |
| ----------------------------------------------- | --------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------- |
| Sama kurssi, uusi päivä, kaikki neljä arvosanaa | Uusi rivi, alkuperäinen ennallaan                                                                                     | Toimii, mutta uusintaa ei ole merkitty          |
| Sama kurssi, uusi päivä, vain uusitut arvosanat | `MissingGrade`-virheet, mitään ei tallenneta, vesileima pysähtyy                                                      | Näkyy virheenä ja tuodaan, kun koodi sallii sen |
| Sama kurssi, **sama päivä**, uudet arvosanat    | Ei haeta, kun vesileima on ohittanut suorituksen. Jos haetaan, uusi versio piilottaa alkuperäisen listalta ja CSV:stä | **Ainoa muoto, joka vaikuttaa alkuperäiseen**   |
| Päivä on vesileimaa vanhempi                    | Ei haeta, jos `from` rajaa `timecompleted`-arvon mukaan                                                               | **Jää hiljaa pois**                             |
| Sama osakoe kahdesti `results`-listassa         | `.find` ottaa ensimmäisen                                                                                             | **Hiljaa väärä arvosana**                       |
| Ylimääräinen uusintakenttä                      | Jätetään huomiotta (Jackson 3:n oletus, tarkistettu versiosta 3.1.7)                                                  | Toimii, mutta tieto katoaa                      |

Koton tiedot eivät mene KOSKI-järjestelmään eivätkä KIOSiin (`docs/technical/integraatiot.md`). Kitussa ne näkyvät vain listassa, tietosivulla, CSV:ssä ja etusivun määrässä.

### Oletetun muodon seuraukset

1. **Tuomatta jäänyt alkuperäinen katoaa.** Jos alkuperäinen on jumissa ONR- tai validointivirheessä, kun uusinta korvaa sen Moodlessa, kitu ei koskaan saa sen arvosanoja, eikä se näy kokonaismäärässä.
2. **Säilyneet arvosanat näyttävät uusilta.** Jos `results` säilyttää uusimattomat arvosanat, uusintarivillä on kaikki neljä uusinnan päivällä. Uusittu osakoe selviää vain vertaamalla aiempaan riviin.
3. **Mikä tahansa päivän muutos tekee toisen rivin.** Opettajan korjaus `timecompleted`-arvoon näyttäisi uusinnalta.
4. **Ennen toteutusta tulevat uusinnat lasketaan tavallisiksi suorituksiksi.** Täysi uusinta tuodaan jo nyt, mutta ilman merkintää, eikä vesileiman ohittanutta riviä haeta uudelleen, joten myöhemmin lisätty kenttä ei tavoita sitä. Jos uusintoja ehtii tulla ensin, ne merkitään jälkikäteen migraatiolla alla kuvatun aiemman rivin säännön mukaan.

## Päätökset

- **Virkailijanäkymä:** jokainen suorituskerta on oma rivinsä, ja uusinta on merkitty. Kitu ei päätä, mikä arvosana on voimassa.
- **Määrät (OPH:n vaatimus):** OPH näkee suoritusten kokonaismäärän ja sen, kuinka moni niistä on uusintoja. Tulkinta, vahvistamatta: kokonaismäärään sisältyvät uusinnat, ja uusinnat lasketaan suorituskertoina, ei henkilöinä.
- **Uusintaoikeus (oletus, vahvistettava OPH:lta):** Koealusta päättää, kuka saa uusia, eikä kitu valvo sitä.

## Avoimet kysymykset

### Mediamaisterille

1. Onko uusintasuorituksella sama `courseid` ja `userid` kuin alkuperäisellä suorituksella?
2. Vaihtuuko `timecompleted` aina uusinnan päiväksi? Voiko se muuttua muustakin syystä, esimerkiksi opettajan korjauksen vuoksi, tai olla taaksepäin kirjattu?
3. Häviääkö alkuperäinen suoritus viennistä uusinnan alkaessa vai vasta sen valmistuessa?
4. Mitä `results`-listassa on uusinnan jälkeen: kaikki neljä osakoetta (uusimattomat vanhoilla arvosanoilla) vai vain uusitut? Voiko sama osakoe esiintyä listassa useasti?
5. Voiko vientiin lisätä kentän, joka kertoo uusintasuorituksesta? Mikä sen nimi ja arvot olisivat?
6. Mihin `from`-parametri kohdistuu: `timecompleted`-arvoon vai muokkausaikaan? Sisältyykö rajahetki hakuun?
7. Näkyykö uusintaan osallistuva rajapinnassa `local_completion_export_get_incomplete_course_participants`, ja mikä ilmoittautumisaika hänelle tulee `enrolledfrom`-rajausta varten?
8. Milloin ensimmäiset uusinnat tehdään, ja saammeko esimerkkiaineistoa ennen sitä?

### OPH:lle

1. Riittääkö virkailijoille, että jokainen suorituskerta on oma rivinsä ja uusinta on merkitty listassa ja CSV-tiedostossa? Vai pitääkö kitun näyttää voimassa oleva arvosana osakokeittain?
2. Riittääkö, että suoritusten kokonaismäärä ja uusintojen määrä näkyvät suoritusnäkymässä valittujen rajausten mukaan (aika, kieli, hakusana) sekä etusivulla, vai tarvitaanko erillinen tilastonäkymä?
3. Onko uusinnalle aika- tai kertarajoja, joita kitun pitää raportoida? Oletamme, että Koealusta päättää, kuka saa uusia.

### Valinnainen tarkistus tuotannosta

Väitteen "uusintoja ei vielä ole" voi tarkistaa, koska mikään ei nykyään estä toista suoritusta. Kysely palauttaa vain määriä, eikä sitä ole vielä ajettu:

```sql
SELECT count(*) AS henkiloita,
       count(*) FILTER (WHERE kursseja = 1) AS samalla_kurssilla
FROM (
  SELECT oppijanumero, testikieli,
         count(DISTINCT kurssi_id) AS kursseja
  FROM koto_suoritus
  WHERE completed
  GROUP BY oppijanumero, testikieli
  HAVING count(DISTINCT suoritusaika) > 1
) t;
```

Jos samalta kurssilta löytyy toistoja, on päätettävä, lasketaanko ne uusinnoiksi. `DEFAULT false` jättää ne merkitsemättä.

## Suunniteltu toteutus (kun muoto on vahvistettu)

Uusi sarake merkitsee uusintarivin, antaa uusintojen määrän ja tarvittaessa sallii puuttuvat arvosanat. Alkuperäistä se ei suojaa, koska se on jo turvassa.

Ensisijaisesti viennissä on oma uusintakenttä. OPH:n vaatima määrä tekee tästä tärkeämpää, koska luku on vain niin tarkka kuin merkintä. Varakeino: kitu päättelee uusinnan saman kurssin aiemmasta rivistä (ks. taulukko alla), jolloin määrä on likimääräinen.

1. **Jäsennys:** lisää kenttä `Completion`-luokkaan tiedostossa `kotoutumiskoulutus/koealusta/KoealustaSuorituksetResponse.kt`.
2. **Tallennus:** uusi migraatio, joka lisää sarakkeen `koto_suoritus.uusinta BOOLEAN NOT NULL DEFAULT false` samaan tapaan kuin V106:n `completed`.
   - Ei nullable: `exists()` vertaa kaikkia kenttiä, joten tallennettu NULL ja saapuva `false` kirjoittaisivat turhan uuden version.
   - Olemassa olevat rivit jäävät merkitsemättä, joten historian tulkinta ei muutu.
   - Päivitä `KielitestiSuoritus` ja sen `fromRow`.
3. **Validointi** (`KoealustaSuoritusValidator.completionToEntity`), vain jos uusinnoista voi puuttua arvosanoja:
   - Arvosanat saavat puuttua vain uusinnoilta, ja vähintään yksi arvosana vaaditaan silti.
   - Virheellinen arvosana on yhä virhe, ja tavallisilla suorituksilla `MissingGrade`-tarkistus säilyy.
   - Toistuva osakokeen nimi on validointivirhe. Tämä ei riipu muodosta, ja sen voi tehdä milloin tahansa.
4. **Käyttöliittymä ja CSV:**
   - Uusi viimeinen rivi `KielitestiSuoritusColumn`-enumiin tageilla `LIST_VIEW` + `CSV_EXPORT`, jolloin olemassa olevat CSV-sarakkeet eivät siirry.
   - Uusi rivi `KielitestiSuoritusPage.tutkintotiedot`-osioon.
   - Uusi avain `UiText.Koto.Sarake.uusinta` ja sen teksti tiedostoon `lokalisointi/fi.json`.
   - Nykyiset `.orDash()` ja `?: "-"` käsittelevät jo puuttuvat arvosanat.
5. **Määrät:**
   - Lista: uusintojen määrä "Suorituksia yhteensä" -tekstin viereen `KielitestiSuorituksetPage`-sivulla. Se noudattaa voimassa olevia rajauksia, joten ajanjakson ja kielen luvut eivät vaadi muuta.
   - Molemmat luvut samasta kyselystä (`count(*)` + `count(*) FILTER (WHERE uusinta)`) `CustomKielitestiSuoritusRepository`-luokassa, jotta lista ei saa toista laskentakyselyä (mediaani ~97 ms tuotannossa 10/2026). Kysely käyttää samaa `DISTINCT ON` -alikyselyä, joten vanhoja versioita ei lasketa.
   - Etusivu: uusintarivi `kotoRows`-funktioon (`etusivu/HomePage.kt`) ja määrä `KotoStats`-luokkaan (`etusivu/DashboardService.kt`).
   - Ei uutta tilastonäkymää, ellei OPH sitä pyydä. YKI:n näkymä on mallina.
6. **Testidata:** päivitä `dev/mockdata/KielitestiMock.kt`.
7. **Dokumentaatio:** päivitä `docs/technical/integraatiot.md` ja `CLAUDE.md`:n koto-osio. Kaaviot pysyvät ennallaan, koska tietovirrat eivät muutu.

### Jos vastaukset poikkeavat

| Vastaus                                                 | Muutos suunnitelmaan                                                                                                                                                                                                                                                                        |
| ------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Uusintakenttää ei saada                                 | Tuonnissa `uusinta` asetetaan, kun samalla `kurssi_id`:llä ja `oppijanumero`lla on valmis rivi aiemmalla `suoritusaika`-arvolla. Tämä ei tunnista uusintaa, jonka alkuperäinen ei koskaan tullut kituun, ja merkitsee päivän korjauksen uusinnaksi. Uusintojen määrä perii molemmat virheet |
| Kenttää ei saada, ja uusinnoista voi puuttua arvosanoja | Arvosanat saavat puuttua vain, kun aiempi rivi on olemassa. Haku välitetään samaan tapaan kuin nykyinen `tallennetutOppijanumerot`                                                                                                                                                          |
| Päivä pysyy samana                                      | Alkuperäinen piilottuisi, ja vesileima ohittaisi uusinnan. Tarvitaan muokkausaikaan perustuva `from`-rajaus ja kenttä osaksi avainta                                                                                                                                                        |
| `timecompleted` voi olla taaksepäin kirjattu            | Sama vesileimaongelma. Pyydetään muokkausaikaan perustuvaa rajausta tai haetaan limittäisellä ikkunalla                                                                                                                                                                                     |
| Uusinnat ovatkin erillisellä kurssilla                  | Uusi rivi ilman aiempaa riviä samalla kurssilla, joten tarvitaan kenttä tai määritelty lista uusintakurssien `kurssi_id`-arvoista                                                                                                                                                           |
| OPH haluaa voimassa olevan arvosanan osakokeittain      | Vaatii OPH:n säännön ja uuden henkilökohtaisen näkymän. Toteutetaan erillisenä ominaisuutena                                                                                                                                                                                                |

## Varmistus toteutuksen yhteydessä

- `KoealustaServiceTests`:
  - sama kurssi uudella päivällä jättää alkuperäisen rivin ennalleen ja lisää toisen rivin
  - uusinta, josta puuttuu arvosanoja, tallennetaan tyhjin arvosanoin
  - tavallinen suoritus, josta puuttuu arvosanoja, antaa yhä `MissingGrade`-virheen ja pysäyttää vesileiman
  - toistuva osakokeen nimi antaa validointivirheen
  - muuttumaton uudelleen haettu rivi ei kirjoita uutta versiota
- `KielitestiSuoritusRepositoryTest`: molemmat suorituskerrat näkyvät `findSuoritukset`-haussa, ja kokonaismäärä ja uusintojen määrä noudattavat rajauksia ja ohittavat vanhat versiot.
- `KielitestiCsvTest`: uusi sarake on viimeisenä, ja olemassa olevien sarakkeiden järjestys säilyy.
- `e2e/tests/kotoutumiskoulutus/kielitesti-suoritukset.spec.ts`: merkintä ja uusintojen määrä näkyvät listanäkymässä.
- `/verify` testidatalla ja lopuksi `./scripts/format.sh`.
