# Eräajot

Kielitutkintorekisteri käyttää [DB Scheduler](https://github.com/kagkarlsson/db-scheduler)-teknologiaa eräajojen ajastamiseen ja ajamiseen.

Eräajot määritellään koodilla ja niiden aikataulutus tulee tavallisesti properties-tiedostoista.

Osa ajoista on tarkoitettu vain käsin käynnistettäviksi. Niille on annettu harvinainen oikea
cron-lauseke (`0 0 0 29 2 ?` eli karkausvuoden 29.2.) eikä `-`-arvoa: db-scheduler ei luo
`scheduled_tasks`-riviä pois kytketylle ajolle, jolloin tehtävä ei näkyisi tässä
käyttöliittymässä lainkaan eikä sitä voisi käynnistää.

![DB Scheduler UI](./db-scheduler.png)

Eräajot voi käynnistää myös manuaalisesti ja niiden ajohistoriaa tarkastella käyttöliittymästä, joka löytyy seuraavista osoitteista:

- [Untuva](https://virkailija.untuvaopintopolku.fi/kielitutkinnot/db-scheduler)
- [QA](https://virkailija.testiopintopolku.fi/kielitutkinnot/db-scheduler)
- [Tuotanto](https://virkailija.opintopolku.fi/kielitutkinnot/db-scheduler)
- [Paikallinen kehitysympäristö](http://localhost:8080/kielitutkinnot/db-scheduler)

## Yleinen kielitutkinto

### Lähetä YKI-suoritukset KOSKI-palveluun

Lähettää KOSKI-järjestelmään ne yleisen kielitutkinnon suoritukset, joita ei ole aiemmin siirretty onnistuneesti.

### Lähetä YKI-arviointitilat KIOS-palveluun

Lähettää ilmoittautumisjärjestelmään (KIOS) sellaiset yleisen kielitutkinnon arviointitilat, joita ei aiemmin ole
saatu siirrettyä (esim. verkko- tai palvelinvian takia). Tavallisesti arviointitila lähetetään jo siinä yhteydessä
kun Solki lähettää uutta dataa rajapintaan `POST /yki/api/suoritus`. Tämän ajon tarkoitus on varmistaa tiedon
perille pääsy.

## Valtionhallinnon kielitutkinnot

### Lähetä VKT-suoritukset KOSKI-palveluun

Lähettää KOSKI-järjestelmään ne valtionhallinnon kielitutkinnon suoritukset, joita ei ole aiemmin siirretty onnistuneesti.

### Poista merkityt VKT-suoritukset

Poistaa erinomaisen taitotason vkt-suorituksien osakokeet, jotka on merkitty poistettavaksi ja joiden retentioaika on täyttynyt.
Poistoaika vaihtelee ympäristöittäin.

## Kotoutumiskoulutus

### Hae kotoutumiskoulutuksen kielitaidon päättötestit

Hakee koto-koulutukset ja tallentaa ne Kielitutkintorekisteriin.

### Hae kotoutumiskoulutuksen keskeneräiset suoritukset

Hakee Koealustalta ne suoritukset, jotka ovat vielä kesken, jotta virkailija näkee ne rekisterissä
ennen kuin arviointi valmistuu. Ajetaan samalla aikataululla kuin valmiiden suoritusten haku.

### Kotoutumiskoulutuksen kielitaidon tehtäväpankin lataus

Lataa tehtäväpankin varmuuskopion ja tallentaa S3-bucketiin.

## YKI-arvioijarekisteri

Nämä ajot ovat olemassa vain, kun kitu on arvioijarekisterin master. Lähetysajot
(`Laheta ...`) rekisteröidään vain, jos `kitu.yki.arvioijarekisteri.integraatio.enabled=true`;
muuten ne eivät näy käyttöliittymässä lainkaan.

### Laheta YKI-arvioijat Solkiin

Lähettää Solkiin ne arvioijamerkinnät, joita ei ole vielä saatu siirrettyä. Ajetaan 15 minuutin
välein ja uusii vain rivit, joilla on yrityksiä jäljellä (enintään 3).

### Laheta epaonnistuneet YKI-arvioijat Solkiin

Yöllinen ajo (02:15), joka poimii kaikki lähettämättömät rivit yrityslaskurista välittämättä.

### Poista sailytysajan ylittaneet YKI-arvioijamerkinnat

Poistaa arvioijamerkinnät, joiden säilytysaika on täyttynyt. Ei poista arvioijaa, jolla on
kausimasterissa yhä voimassa oleva kausi.

### Paivita YKI-arvioijien arviointioikeusprojektio

Päivittää `yki_arviointioikeus`-projektion kausimasterista. Tarvitaan, koska projisoitava kausi
riippuu kuluvasta päivästä ja vanhenee itsestään, kun tuleva kausi alkaa. Kirjoittaa vain
tosiasiassa muuttuneet arvot, jottei koko rekisteri leimaudu muokatuksi.

### Synkronoi YKI-arvioijien kaudet arviointioikeuksista

Rakentaa kausimasterin olemassa olevista arviointioikeuksista. **Tarkoitettu ajettavaksi käsin
tästä käyttöliittymästä** ennen kuin Solki-integraation kytkin avataan.

## Läpileikkaavat

### Siivoa vanhentuneet CAS-session-kuvaukset

Poistaa `cas_client_session`-taulusta palvelulipun ja Spring Session -istunnon väliset kuvaukset,
joiden istuntoa ei enää ole. Ajetaan päivittäin klo 04:30.

### Päivitä käännökset lokalisointipalvelusta

Hakee sv/en-käännökset OPH:n lokalisointiproxysta Tolgeesta ja päivittää muistinvaraisen
käännösvaraston. Ajo on olemassa vain ympäristöissä, joissa `kitu.lokalisointi.namespace` on
asetettu.
