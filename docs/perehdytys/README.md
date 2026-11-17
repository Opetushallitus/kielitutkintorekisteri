# Perehdytys

Tämä osio on tarkoitettu kehittäjälle, joka ottaa Kielitutkintorekisterin (kitu) ylläpidon vastuulleen.
Lukijan oletetaan osaavan Kotlinia ja Spring Bootia, mutta ei tuntevan Opetushallituksen
kielitutkintoja eikä Kielitutkintorekisterin toteutusta.

Jokainen sivu on samalla tiedonjakosession runko: pääotsikot ovat session agendakohdat. Sivun voi
lukea myös yksin ilman sessiota.

## Tutoriaalit

Suositeltu lukujärjestys:

1. [YKI-suoritukset](./yki-suoritukset.md) — yleisten kielitutkintojen suoritukset Solkista Kielitutkintorekisteriin
   ja edelleen KOSKI-järjestelmään ja ilmoittautumisjärjestelmään
2. YKI-arvioijarekisteri (tulossa)
3. VKT-rekisteri (tulossa)
4. Kotoutumiskoulutuksen päättötesti ja virheidenkäsittely (tulossa)
5. Kotoutumiskoulutuksen tehtäväpankki (tulossa)
6. Infra ja CI/CD (tulossa)
7. Operations: hälytykset, päivitykset ja muu jatkuva ylläpito (tulossa)

## Tutoriaalin rakenne

Kaikki tutoriaalit noudattavat samaa runkoa, joka etenee toimialasta koodiin ja koodista
ylläpitoon:

1. **Mikä tämä on ja kuka tekee mitä** — toimiala ja osapuolet
2. **Sanasto** — termit, joita koodissa ja keskusteluissa käytetään
3. **Elinkaari** — mitä tiedolle tapahtuu alusta loppuun
4. **Tekninen yleiskuva** — taulut, rajapinnat, eräajot, käyttöoikeudet
5. **Koodipolut** — yksi polku kerrallaan, luokasta luokkaan
6. **Ansat ja päätökset** — mitä ei saa rikkoa ja miksi
7. **Ylläpito** — hälytykset, eräajot ja tavalliset tukipyynnöt
8. **Kokeile itse** — paikallinen ajo ja demo
9. **Lisälukemista**

Luokat ja metodit mainitaan nimellä ilman rivinumeroita, ja paketit suhteessa hakemistoon
`server/src/main/kotlin/fi/oph/kitu/`.

## Muu dokumentaatio

- [Tekninen dokumentaatio](../technical/) — arkkitehtuuri, koodikonventiot, integraatiot, ylläpito
- [Eräajot](../db-scheduler)
- [Ulkoiset rajapintakuvaukset](https://virkailija.testiopintopolku.fi/kielitutkinnot/api-docs)
