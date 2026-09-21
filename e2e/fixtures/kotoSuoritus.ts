import SQL from "sql-template-strings"
import { TestDB } from "./baseFixture"
import { Email, FixturePerson, Oid, peopleFixture } from "./basePeopleFixture"

export interface KotoSuoritus {
  etunimet: string
  sukunimi: string
  kutsumanimi: string
  oppijanumero: Oid
  oppilaitosOid: Oid
  email: Email
  suoritusaika: string
  luetunYmmartaminen: string
  kuullunYmmartaminen: string
  puhe: string
  kirjoittaminen: string
  opettajanEmail: string
  kurssiId: number
  kurssi: string
  testikieli: string
  tehtavapaketti: string | null
  completed: boolean
}

type CreateSuoritusArgs = Partial<
  Omit<
    KotoSuoritus,
    "etunimet" | "sukunimi" | "kutsumanimi" | "email" | "oppijanumero"
  >
>

const suoritusRuotsi: CreateSuoritusArgs = {
  oppilaitosOid: "1.2.3.4.5.7",
  luetunYmmartaminen: "A1",
  kuullunYmmartaminen: "B1",
  puhe: "YLIB1",
  kirjoittaminen: "A2",
  opettajanEmail: "opettaja@testi.oph.fi",
  suoritusaika: "2025-01-22 10:30:27",
  kurssiId: 33,
  kurssi: "Integrationstestning",
  testikieli: "SWE",
  tehtavapaketti: "sv_svenska",
}

const createSuoritus = (
  person: FixturePerson,
  {
    oppilaitosOid = "1.2.3.4.5.6",
    luetunYmmartaminen = "A1",
    kuullunYmmartaminen = "B1",
    puhe = "ALLEA1",
    kirjoittaminen = "B1",
    opettajanEmail = "opettaja@testi.oph.fi",
    suoritusaika = "2024-11-22 10:49:49",
    kurssiId = 32,
    kurssi = "Integraatio testaus",
    testikieli = "FIN",
    tehtavapaketti = "fi_suomi",
    completed = true,
  }: CreateSuoritusArgs,
) => {
  const p = peopleFixture[person]
  return {
    etunimet: p.etunimet,
    sukunimi: p.sukunimi,
    kutsumanimi: p.kutsumanimi,
    oppijanumero: p.oppijanumero,
    email: p.email,
    oppilaitosOid: oppilaitosOid,
    suoritusaika: suoritusaika,
    luetunYmmartaminen: luetunYmmartaminen,
    kuullunYmmartaminen: kuullunYmmartaminen,
    kirjoittaminen: kirjoittaminen,
    puhe: puhe,
    opettajanEmail: opettajanEmail,
    kurssiId: kurssiId,
    kurssi: kurssi,
    testikieli: testikieli,
    tehtavapaketti: tehtavapaketti,
    completed: completed,
  }
}

export const fixtureData = {
  ranja: createSuoritus("ranja", {}),
  fanni: createSuoritus("fanni", {}),
  eino: createSuoritus("eino", {}),
  petro: createSuoritus("petro", {}),
  pernilla: createSuoritus("pernilla", {}),
  kalervo: createSuoritus("kalervo", {}),
  toni: createSuoritus("toni", {
    suoritusaika: "2024-11-24 11:36:43",
  }),
  amalia: createSuoritus("amalia", {}),
  topi: createSuoritus("topi", {}),
  tobias: createSuoritus("tobias", suoritusRuotsi),
  silja: createSuoritus("silja", suoritusRuotsi),
  anniina: createSuoritus("anniina", suoritusRuotsi),
  magdalena: createSuoritus("magdalena", suoritusRuotsi),
  fanniRessu: createSuoritus("fanni", {
    oppilaitosOid: "1.2.246.562.10.65693669254",
  }),
  valluKesken: createSuoritus("vallu", {
    completed: false,
    suoritusaika: "2025-02-10 09:15:00",
  }),
} as const

const insertQuery = (suoritus: KotoSuoritus) => SQL`
  INSERT INTO koto_suoritus (etunimet,
                             sukunimi,
                             kutsumanimi,
                             oppijanumero,
                             oppilaitos_oid,
                             email,
                             suoritusaika,
                             luetun_ymmartaminen,
                             kuullun_ymmartaminen,
                             puhe,
                             kirjoittaminen,
                             opettajan_email,
                             kurssi_id,
                             kurssi,
                             testikieli,
                             tehtavapaketti,
                             completed)
  VALUES (${suoritus.etunimet},
          ${suoritus.sukunimi},
          ${suoritus.kutsumanimi},
          ${suoritus.oppijanumero},
          ${suoritus.oppilaitosOid},
          ${suoritus.email},
          ${suoritus.suoritusaika},
          ${suoritus.luetunYmmartaminen},
          ${suoritus.kuullunYmmartaminen},
          ${suoritus.puhe},
          ${suoritus.kirjoittaminen},
          ${suoritus.opettajanEmail},
          ${suoritus.kurssiId},
          ${suoritus.kurssi},
          ${suoritus.testikieli},
          ${suoritus.tehtavapaketti},
          ${suoritus.completed})
`

export type KotoSuorittajaName = keyof typeof fixtureData

export const insert = async (db: TestDB, suoritus: KotoSuorittajaName) =>
  await db.dbClient.query(insertQuery(fixtureData[suoritus]))
