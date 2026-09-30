import SQL from "sql-template-strings"
import { TestDB } from "./baseFixture"
import { FixturePerson, peopleFixture } from "./basePeopleFixture"

export interface YkiArvioija {
  arvioijanOppijanumero: string
  henkilotunnus: string | null
  sukunimi: string
  etunimet: string
  sahkopostiosoite: string | null
  katuosoite: string
  postinumero: string
  postitoimipaikka: string
  arviointioikeudet: YkiArviointioikeus[]
}

export interface YkiArviointioikeus {
  kieli: string
  tasot: Set<string>
  /** Vanhentunut sarake: tila lasketaan kauden päivistä, ks. Rekisterointitila. */
  tila: string | null
  kaudenAlkupaiva: string | null
  kaudenPaattymispaiva: string | null
  jatkorekisterointi: boolean
  ensimmainenRekisterointipaiva: string
  rekisteriintuontiaika: string | null
}

const createArvioija = (
  person: FixturePerson,
  {
    rekisteriintuontiaika = null,
    ensimmainenRekisterointipaiva = "2024-09-01",
    kaudenAlkupaiva = "2024-09-01",
    kaudenPaattymispaiva = "2029-09-01",
    jatkorekisterointi = false,
    tila = null,
    kieli = "FIN",
    tasot = new Set(["PT", "KT", "YT"]),
  }: Partial<YkiArviointioikeus>,
) => {
  const p = peopleFixture[person]
  return {
    rekisteriintuontiaika,
    arvioijaOid: p.oppijanumero,
    henkilotunnus: p.hetu,
    sukunimi: p.sukunimi,
    etunimet: p.etunimet,
    sahkopostiosoite: p.email,
    katuosoite: p.osoite.katuosoite,
    postinumero: p.osoite.postinumero,
    postitoimipaikka: p.osoite.postitoimipaikka,
    ensimmainenRekisterointipaiva,
    kaudenAlkupaiva,
    kaudenPaattymispaiva,
    jatkorekisterointi,
    tila,
    kieli,
    tasot,
  }
}

export const fixtureData = {
  ranja: createArvioija("ranja", {}),
  fanni: createArvioija("fanni", {}),
  eino: createArvioija("eino", {}),
  petro: createArvioija("petro", {}),
  pernilla: createArvioija("pernilla", {}),
  kalervo: createArvioija("kalervo", {}),
  toni: createArvioija("toni", {}),
  amalia: createArvioija("amalia", {}),
  topi: createArvioija("topi", {}),
  tobias: createArvioija("tobias", {}),
  silja: createArvioija("silja", {}),
  anniina: createArvioija("anniina", {}),
  magdalena: createArvioija("magdalena", {}),
} as const

export type YkiArvioijaName = keyof typeof fixtureData

export const insert = async (db: TestDB, arvioijaName: YkiArvioijaName) => {
  const arvioija = fixtureData[arvioijaName]

  const arvioijaId = (
    await db.dbClient.query<{ id: number }>(SQL`
      INSERT INTO yki_arvioija (arvioija_oid,
                                henkilotunnus,
                                sukunimi,
                                etunimet,
                                sahkopostiosoite,
                                katuosoite,
                                postinumero,
                                postitoimipaikka)
      VALUES (${arvioija.arvioijaOid},
              ${arvioija.henkilotunnus},
              ${arvioija.sukunimi},
              ${arvioija.etunimet},
              ${arvioija.sahkopostiosoite},
              ${arvioija.katuosoite},
              ${arvioija.postinumero},
              ${arvioija.postitoimipaikka})
      RETURNING id
  `)
  )[0].id

  await db.dbClient.query(SQL`
      INSERT INTO yki_arviointioikeus (arvioija_id,
                                       ensimmainen_rekisterointipaiva,
                                       kauden_alkupaiva,
                                       kauden_paattymispaiva,
                                       jatkorekisterointi,
                                       tila,
                                       kieli,
                                       tasot)
      VALUES (${arvioijaId},
              ${arvioija.ensimmainenRekisterointipaiva},
              ${arvioija.kaudenAlkupaiva},
              ${arvioija.kaudenPaattymispaiva},
              ${arvioija.jatkorekisterointi},
              ${arvioija.tila},
              ${arvioija.kieli},
              ${arvioija.tasot})
  `)

  // Kaudet ovat master ja yki_arviointioikeus niistä johdettu projektio, joten kumpikin
  // on seedattava: pelkkä projektio jättäisi tietosivun kausitaulukon tyhjäksi.
  if (arvioija.kaudenAlkupaiva !== null) {
    const kausiId = (
      await db.dbClient.query(SQL`
        INSERT INTO yki_arvioija_arviointikausi (arvioija_id, alkupaiva, paattymispaiva)
        VALUES (${arvioijaId}, ${arvioija.kaudenAlkupaiva}, ${arvioija.kaudenPaattymispaiva})
        RETURNING id
    `)
    )[0].id

    await db.dbClient.query(SQL`
        INSERT INTO yki_arvioija_arviointikausi_oikeus (kausi_id, kieli, tasot)
        VALUES (${kausiId}, ${arvioija.kieli}, ${arvioija.tasot})
    `)
  }
}
