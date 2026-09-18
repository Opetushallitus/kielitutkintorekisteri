# Tekninen dokumentaatio

Tämä osio kokoaa Kielitutkintorekisterin (kitu) toteutuksen kannalta keskeiset
suunnitteluratkaisut ja konventiot.

## Sivut

- [Arkkitehtuuri](./arkkitehtuuri.md) — palvelinohjelmiston rakenne, paketit, profiilit, tietokanta
- [Koodikonventiot](./koodikonventiot.md) — virheenkäsittely Eitherillä, validointi, Jackson 3, RestClient
- [Integraatiot](./integraatiot.md) — KOSKI, KIOS, Oppijanumerorekisteri ja muut ulkoiset palvelut
- [Ylläpito ja havainnointi](./yllapito.md) — telemetria, lokit, salaisuudet, deploy-putki

## Liittyvät resurssit

- [Tietokantaskeeman dokumentaatio](https://opetushallitus.github.io/kielitutkintorekisteri/db)
- Beans-komponenttikompositio:
  [Untuva](https://opetushallitus.github.io/kielitutkintorekisteri/uml/untuva) ·
  [QA](https://opetushallitus.github.io/kielitutkintorekisteri/uml/qa) ·
  [Tuotanto](https://opetushallitus.github.io/kielitutkintorekisteri/uml/prod)
- [Eräajot](../db-scheduler) — db-scheduler-pohjaisten ajastettujen tehtävien dokumentaatio
- [Ulkoiset rajapintakuvaukset](https://virkailija.testiopintopolku.fi/kielitutkinnot/api-docs)
