# Tekninen dokumentaatio

Tämä osio kokoaa Kielitutkintorekisterin (kitu) toteutuksen kannalta keskeiset
suunnitteluratkaisut ja konventiot.

## Sivut

- [Arkkitehtuuri](./arkkitehtuuri.md) — palvelinohjelmiston rakenne, paketit, profiilit, tietokanta
- [Koodikonventiot](./koodikonventiot.md) — virheenkäsittely Eitherillä, validointi, Jackson 3, RestClient
- [Integraatiot](./integraatiot.md) — KOSKI, KIOS, Oppijanumerorekisteri ja muut ulkoiset palvelut, tietovirtakaaviot tutkinnoittain
- [Rekisteristä lähtevät tiedot](./lahtevat-tiedot.md) — mitä tietoja KOSKI-järjestelmään, Solkiin ja ilmoittautumisjärjestelmään lähetetään (tuotettu koodista)
- [Ylläpito ja havainnointi](./yllapito.md) — telemetria, lokit, salaisuudet, deploy-putki

## Liittyvät resurssit

- [Tietokantaskeeman dokumentaatio](https://opetushallitus.github.io/kielitutkintorekisteri/db)
- Beans-komponenttikompositio:
  [Untuva](https://opetushallitus.github.io/kielitutkintorekisteri/uml/untuva) ·
  [QA](https://opetushallitus.github.io/kielitutkintorekisteri/uml/qa) ·
  [Tuotanto](https://opetushallitus.github.io/kielitutkintorekisteri/uml/prod)
- [Eräajot](../db-scheduler) — db-scheduler-pohjaisten ajastettujen tehtävien dokumentaatio
- [Perehdytys](../perehdytys/) — tutoriaalit, jotka käyvät yhden osa-alueen läpi toimialasta koodiin
- [Ulkoiset rajapintakuvaukset](https://virkailija.testiopintopolku.fi/kielitutkinnot/api-docs)
