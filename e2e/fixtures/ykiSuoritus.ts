import SQL from "sql-template-strings"
import { expect, TestDB } from "./baseFixture"
import { FixturePerson, peopleFixture } from "./basePeopleFixture"
import { OauthRequestContext } from "./oauthRequestContext"

export interface YkiSuoritus {
  suorittajanOid: string
  hetu: string
  sukupuoli: string
  sukunimi: string
  etunimet: string
  kansalaisuus: string
  katuosoite: string
  postinumero: string
  postitoimipaikka: string
  maa: string | null
  email: string
  suoritusId: number
  lastModified: string
  tutkintopaiva: string
  tutkintokieli: string
  tutkintotaso: string
  jarjestajanTunnusOid: string
  jarjestajanNimi: string
  arviointipaiva: string | null
  tekstinYmmartaminen: number | null
  kirjoittaminen: number | null
  rakenteetJaSanasto: number | null
  puheenYmmartaminen: number | null
  puhuminen: number | null
  yleisarvosana: number | null
  tarkistusarvioinninSaapumisPvm: string | null
  tarkistusarvioinninAsiatunnus: string | null
  tarkistusarvioidutOsakokeet: string[] | null
  arvosanaMuuttui: string[] | null
  perustelu: string | null
  tarkistusarvioinninKasittelyPvm: string | null
  arviointitila: string
  todistuskieli: string
}

type CreateYkiSuoritusArgs = Omit<
  YkiSuoritus,
  | "hetu"
  | "etunimet"
  | "sukunimi"
  | "sukupuoli"
  | "email"
  | "suorittajanOid"
  | "katuosoite"
  | "postinumero"
  | "postitoimipaikka"
  | "maa"
>

const createYkiSuoritus = (
  person: FixturePerson,
  {
    kansalaisuus,
    suoritusId,
    lastModified,
    tutkintopaiva,
    tutkintotaso,
    tutkintokieli,
    jarjestajanTunnusOid,
    jarjestajanNimi,
    arviointipaiva,
    tekstinYmmartaminen,
    kirjoittaminen,
    rakenteetJaSanasto,
    puheenYmmartaminen,
    puhuminen,
    yleisarvosana,
    tarkistusarvioinninSaapumisPvm,
    tarkistusarvioinninAsiatunnus,
    tarkistusarvioidutOsakokeet,
    arvosanaMuuttui,
    perustelu,
    tarkistusarvioinninKasittelyPvm,
    arviointitila,
    todistuskieli,
  }: CreateYkiSuoritusArgs,
): YkiSuoritus => {
  const p = peopleFixture[person]
  return {
    suorittajanOid: p.oppijanumero,
    hetu: p.hetu,
    sukupuoli: p.sukupuoli,
    etunimet: p.etunimet,
    sukunimi: p.sukunimi,
    email: p.email,
    katuosoite: p.osoite.katuosoite,
    postinumero: p.osoite.postinumero,
    postitoimipaikka: p.osoite.postitoimipaikka,
    maa: p.osoite.maa,

    kansalaisuus,

    suoritusId,
    lastModified,
    tutkintopaiva,
    tutkintotaso,
    tutkintokieli,

    jarjestajanTunnusOid,
    jarjestajanNimi,

    arviointipaiva,
    tekstinYmmartaminen,
    kirjoittaminen,
    rakenteetJaSanasto,
    puheenYmmartaminen,
    puhuminen,
    yleisarvosana,

    tarkistusarvioinninSaapumisPvm,
    tarkistusarvioinninAsiatunnus,
    tarkistusarvioidutOsakokeet,
    arvosanaMuuttui,
    perustelu,
    tarkistusarvioinninKasittelyPvm,
    arviointitila,
    todistuskieli,
  }
}

export const fixtureData = {
  ranja: createYkiSuoritus("ranja", {
    kansalaisuus: "EST",
    suoritusId: 183424,
    lastModified: "2024-09-15T13:53:56Z",
    tutkintopaiva: "2024-09-01",
    tutkintokieli: "FIN",
    tutkintotaso: "YT",
    jarjestajanTunnusOid: "1.2.246.562.10.14893989377",
    jarjestajanNimi:
      "Jyväskylän yliopisto, Soveltavan kielentutkimuksen keskus",
    arviointipaiva: "2024-11-14",
    tekstinYmmartaminen: 2,
    kirjoittaminen: 1,
    rakenteetJaSanasto: 1,
    puheenYmmartaminen: 3,
    puhuminen: 3,
    yleisarvosana: 2,
    tarkistusarvioinninSaapumisPvm: null,
    tarkistusarvioinninAsiatunnus: null,
    tarkistusarvioidutOsakokeet: null,
    arvosanaMuuttui: null,
    perustelu: null,
    tarkistusarvioinninKasittelyPvm: null,
    arviointitila: "ARVIOITU",
    todistuskieli: "FIN",
  }),
  ranjaTarkistus: createYkiSuoritus("ranja", {
    kansalaisuus: "EST",
    suoritusId: 183424,
    lastModified: "2024-10-30T13:53:56Z",
    tutkintopaiva: "2024-09-01",
    tutkintokieli: "FIN",
    tutkintotaso: "YT",
    jarjestajanTunnusOid: "1.2.246.562.10.14893989377",
    jarjestajanNimi:
      "Jyväskylän yliopisto, Soveltavan kielentutkimuksen keskus",
    arviointipaiva: "2024-11-14",
    tekstinYmmartaminen: 1,
    kirjoittaminen: 1,
    rakenteetJaSanasto: 1,
    puheenYmmartaminen: 2,
    puhuminen: 3,
    yleisarvosana: 1,
    tarkistusarvioinninSaapumisPvm: "2024-10-01",
    tarkistusarvioinninAsiatunnus: "OPH-14893989377-1",
    tarkistusarvioidutOsakokeet: ["PU"],
    arvosanaMuuttui: ["PU"],
    perustelu: "Tarkistusarvioinnin testi",
    tarkistusarvioinninKasittelyPvm: "2024-10-21",
    arviointitila: "TARKISTUSARVIOITU",
    todistuskieli: "FIN",
  }),
  petro: createYkiSuoritus("petro", {
    kansalaisuus: "EST",
    suoritusId: 123123,
    lastModified: "2024-09-10T14:53:56Z",
    tutkintopaiva: "2024-08-25",
    tutkintokieli: "SWE",
    tutkintotaso: "YT",
    jarjestajanTunnusOid: "1.2.246.562.10.14893989377",
    jarjestajanNimi:
      "Jyväskylän yliopisto, Soveltavan kielentutkimuksen keskus",
    arviointipaiva: "2024-11-14",
    tekstinYmmartaminen: 5,
    kirjoittaminen: 5,
    rakenteetJaSanasto: 9,
    puheenYmmartaminen: 4,
    puhuminen: 11,
    yleisarvosana: 9,
    tarkistusarvioinninSaapumisPvm: null,
    tarkistusarvioinninAsiatunnus: null,
    tarkistusarvioidutOsakokeet: null,
    arvosanaMuuttui: null,
    perustelu: null,
    tarkistusarvioinninKasittelyPvm: null,
    arviointitila: "ARVIOITU",
    todistuskieli: "SWE",
  }),
  magdalena: createYkiSuoritus("magdalena", {
    kansalaisuus: "FIN",
    suoritusId: 172836,
    lastModified: "2025-05-26T11:34:41Z",
    tutkintopaiva: "2025-01-12",
    tutkintokieli: "FIN",
    tutkintotaso: "PT",
    jarjestajanTunnusOid: "1.2.246.562.10.14893989377",
    jarjestajanNimi:
      "Jyväskylän yliopisto, Soveltavan kielentutkimuksen keskus",
    arviointipaiva: "2025-05-04",
    tekstinYmmartaminen: 1,
    kirjoittaminen: 2,
    rakenteetJaSanasto: 12,
    puheenYmmartaminen: 2,
    puhuminen: 10,
    yleisarvosana: 12,
    tarkistusarvioinninSaapumisPvm: null,
    tarkistusarvioinninAsiatunnus: null,
    tarkistusarvioidutOsakokeet: null,
    arvosanaMuuttui: null,
    perustelu: null,
    tarkistusarvioinninKasittelyPvm: null,
    arviointitila: "ARVIOITU",
    todistuskieli: "ENG",
  }),
  magdalenaTarkistettu: createYkiSuoritus("magdalena", {
    kansalaisuus: "FIN",
    suoritusId: 172836,
    lastModified: "2025-05-26T11:34:41Z",
    tutkintopaiva: "2025-01-12",
    tutkintokieli: "FIN",
    tutkintotaso: "PT",
    jarjestajanTunnusOid: "1.2.246.562.10.14893989377",
    jarjestajanNimi:
      "Jyväskylän yliopisto, Soveltavan kielentutkimuksen keskus",
    arviointipaiva: "2025-05-04",
    tekstinYmmartaminen: 2,
    kirjoittaminen: 0,
    rakenteetJaSanasto: 12,
    puheenYmmartaminen: 2,
    puhuminen: 2,
    yleisarvosana: 12,
    tarkistusarvioinninSaapumisPvm: "2025-10-01",
    tarkistusarvioinninAsiatunnus: "OPH-14893989377-2",
    tarkistusarvioidutOsakokeet: ["PU"],
    arvosanaMuuttui: ["PU"],
    perustelu: "Tarkistusarvioinnin testi",
    tarkistusarvioinninKasittelyPvm: "2025-10-22",
    arviointitila: "TARKISTUSARVIOITU",
    todistuskieli: "ENG",
  }),
  einoTarkistettu: createYkiSuoritus("eino", {
    kansalaisuus: "FIN",
    suoritusId: 192836,
    lastModified: "2025-05-26T11:34:41Z",
    tutkintopaiva: "2025-01-12",
    tutkintokieli: "FIN",
    tutkintotaso: "KT",
    jarjestajanTunnusOid: "1.2.246.562.10.14893989377",
    jarjestajanNimi:
      "Jyväskylän yliopisto, Soveltavan kielentutkimuksen keskus",
    arviointipaiva: "2025-05-04",
    tekstinYmmartaminen: 3,
    kirjoittaminen: 4,
    rakenteetJaSanasto: 12,
    puheenYmmartaminen: 1,
    puhuminen: 2,
    yleisarvosana: 12,
    tarkistusarvioinninSaapumisPvm: "2024-10-01",
    tarkistusarvioinninAsiatunnus: "OPH-14893989377-1",
    tarkistusarvioidutOsakokeet: ["PU"],
    arvosanaMuuttui: ["PU"],
    perustelu: "Tarkistusarvioinnin testi",
    tarkistusarvioinninKasittelyPvm: "2024-10-20",
    arviointitila: "TARKISTUSARVIOITU",
    todistuskieli: "FIN",
  }),
  fanniIlmoittautunut: createYkiSuoritus("fanni", {
    kansalaisuus: "FIN",
    suoritusId: 300001,
    lastModified: "2024-09-15T13:53:56Z",
    tutkintopaiva: "2024-09-01",
    tutkintokieli: "FIN",
    tutkintotaso: "YT",
    jarjestajanTunnusOid: "1.2.246.562.10.14893989377",
    jarjestajanNimi:
      "Jyväskylän yliopisto, Soveltavan kielentutkimuksen keskus",
    arviointipaiva: null,
    tekstinYmmartaminen: null,
    kirjoittaminen: null,
    rakenteetJaSanasto: null,
    puheenYmmartaminen: null,
    puhuminen: null,
    yleisarvosana: null,
    tarkistusarvioinninSaapumisPvm: null,
    tarkistusarvioinninAsiatunnus: null,
    tarkistusarvioidutOsakokeet: null,
    arvosanaMuuttui: null,
    perustelu: null,
    tarkistusarvioinninKasittelyPvm: null,
    arviointitila: "ILMOITTAUTUNUT",
    todistuskieli: "FIN",
  }),
  tanjaPeruttu: createYkiSuoritus("tanja", {
    kansalaisuus: "FIN",
    suoritusId: 300002,
    lastModified: "2024-09-15T13:53:56Z",
    tutkintopaiva: "2024-09-01",
    tutkintokieli: "FIN",
    tutkintotaso: "YT",
    jarjestajanTunnusOid: "1.2.246.562.10.14893989377",
    jarjestajanNimi:
      "Jyväskylän yliopisto, Soveltavan kielentutkimuksen keskus",
    arviointipaiva: null,
    tekstinYmmartaminen: null,
    kirjoittaminen: null,
    rakenteetJaSanasto: null,
    puheenYmmartaminen: null,
    puhuminen: null,
    yleisarvosana: null,
    tarkistusarvioinninSaapumisPvm: null,
    tarkistusarvioinninAsiatunnus: null,
    tarkistusarvioidutOsakokeet: null,
    arvosanaMuuttui: null,
    perustelu: null,
    tarkistusarvioinninKasittelyPvm: null,
    arviointitila: "PERUTTU",
    todistuskieli: "FIN",
  }),
  fanniEiSuoritusta: createYkiSuoritus("fanni", {
    kansalaisuus: "FIN",
    suoritusId: 300003,
    lastModified: "2024-09-15T13:53:56Z",
    tutkintopaiva: "2024-09-01",
    tutkintokieli: "FIN",
    tutkintotaso: "YT",
    jarjestajanTunnusOid: "1.2.246.562.10.14893989377",
    jarjestajanNimi:
      "Jyväskylän yliopisto, Soveltavan kielentutkimuksen keskus",
    arviointipaiva: null,
    tekstinYmmartaminen: 12,
    kirjoittaminen: 12,
    rakenteetJaSanasto: 12,
    puheenYmmartaminen: 12,
    puhuminen: 12,
    yleisarvosana: 12,
    tarkistusarvioinninSaapumisPvm: null,
    tarkistusarvioinninAsiatunnus: null,
    tarkistusarvioidutOsakokeet: null,
    arvosanaMuuttui: null,
    perustelu: null,
    tarkistusarvioinninKasittelyPvm: null,
    arviointitila: "EI_SUORITUSTA",
    todistuskieli: "FIN",
  }),
  tanjaKeskeytettyOsakoe: createYkiSuoritus("tanja", {
    kansalaisuus: "FIN",
    suoritusId: 300004,
    lastModified: "2024-09-15T13:53:56Z",
    tutkintopaiva: "2024-09-01",
    tutkintokieli: "FIN",
    tutkintotaso: "YT",
    jarjestajanTunnusOid: "1.2.246.562.10.14893989377",
    jarjestajanNimi:
      "Jyväskylän yliopisto, Soveltavan kielentutkimuksen keskus",
    arviointipaiva: "2024-11-14",
    tekstinYmmartaminen: 2,
    kirjoittaminen: 1,
    rakenteetJaSanasto: 1,
    puheenYmmartaminen: 3,
    puhuminen: 10,
    yleisarvosana: 2,
    tarkistusarvioinninSaapumisPvm: null,
    tarkistusarvioinninAsiatunnus: null,
    tarkistusarvioidutOsakokeet: null,
    arvosanaMuuttui: null,
    perustelu: null,
    tarkistusarvioinninKasittelyPvm: null,
    arviointitila: "ARVIOITU",
    todistuskieli: "FIN",
  }),
  fanniArvioituIlmanOikeitaArvosanoja: createYkiSuoritus("fanni", {
    kansalaisuus: "FIN",
    suoritusId: 300005,
    lastModified: "2024-09-15T13:53:56Z",
    tutkintopaiva: "2024-09-01",
    tutkintokieli: "FIN",
    tutkintotaso: "YT",
    jarjestajanTunnusOid: "1.2.246.562.10.14893989377",
    jarjestajanNimi:
      "Jyväskylän yliopisto, Soveltavan kielentutkimuksen keskus",
    arviointipaiva: "2024-11-14",
    tekstinYmmartaminen: 12,
    kirjoittaminen: 12,
    rakenteetJaSanasto: 12,
    puheenYmmartaminen: 12,
    puhuminen: 12,
    yleisarvosana: 12,
    tarkistusarvioinninSaapumisPvm: null,
    tarkistusarvioinninAsiatunnus: null,
    tarkistusarvioidutOsakokeet: null,
    arvosanaMuuttui: null,
    perustelu: null,
    tarkistusarvioinninKasittelyPvm: null,
    arviointitila: "ARVIOITU",
    todistuskieli: "FIN",
  }),
  valluYksiloimaton: createYkiSuoritus("vallu", {
    kansalaisuus: "FIN",
    suoritusId: 400001,
    lastModified: "2024-09-15T13:53:56Z",
    tutkintopaiva: "2024-09-01",
    tutkintokieli: "FIN",
    tutkintotaso: "YT",
    jarjestajanTunnusOid: "1.2.246.562.10.14893989377",
    jarjestajanNimi:
      "Jyväskylän yliopisto, Soveltavan kielentutkimuksen keskus",
    arviointipaiva: "2024-11-14",
    tekstinYmmartaminen: 2,
    kirjoittaminen: 1,
    rakenteetJaSanasto: 1,
    puheenYmmartaminen: 3,
    puhuminen: 3,
    yleisarvosana: 2,
    tarkistusarvioinninSaapumisPvm: null,
    tarkistusarvioinninAsiatunnus: null,
    tarkistusarvioidutOsakokeet: null,
    arvosanaMuuttui: null,
    perustelu: null,
    tarkistusarvioinninKasittelyPvm: null,
    arviointitila: "ARVIOITU",
    todistuskieli: "FIN",
  }),
  tanjaIlmoittautunutArvosanalla: createYkiSuoritus("tanja", {
    kansalaisuus: "FIN",
    suoritusId: 300006,
    lastModified: "2024-09-15T13:53:56Z",
    tutkintopaiva: "2024-09-01",
    tutkintokieli: "FIN",
    tutkintotaso: "YT",
    jarjestajanTunnusOid: "1.2.246.562.10.14893989377",
    jarjestajanNimi:
      "Jyväskylän yliopisto, Soveltavan kielentutkimuksen keskus",
    arviointipaiva: null,
    tekstinYmmartaminen: null,
    kirjoittaminen: null,
    rakenteetJaSanasto: null,
    puheenYmmartaminen: null,
    puhuminen: 5,
    yleisarvosana: null,
    tarkistusarvioinninSaapumisPvm: null,
    tarkistusarvioinninAsiatunnus: null,
    tarkistusarvioidutOsakokeet: null,
    arvosanaMuuttui: null,
    perustelu: null,
    tarkistusarvioinninKasittelyPvm: null,
    arviointitila: "ILMOITTAUTUNUT",
    todistuskieli: "FIN",
  }),
} as const

export type YkiSuorittajaName = keyof typeof fixtureData

const buildSuoritusBody = (suoritusName: YkiSuorittajaName) => {
  const data = fixtureData[suoritusName]

  const osa = (tyyppi: string, arvosana: number | null) => ({
    tyyppi,
    arvosana,
  })

  return {
    henkilo: {
      oid: data.suorittajanOid,
      etunimet: data.etunimet,
      sukunimi: data.sukunimi,
      hetu: data.hetu,
      sukupuoli: data.sukupuoli,
      kansalaisuus: data.kansalaisuus,
      katuosoite: data.katuosoite,
      postinumero: data.postinumero,
      postitoimipaikka: data.postitoimipaikka,
      maa: data.maa,
      email: data.email,
    },
    suoritus: {
      tyyppi: "yleinenkielitutkinto",
      tutkintotaso: data.tutkintotaso,
      kieli: data.tutkintokieli.toLowerCase(),
      todistuskieli: data.todistuskieli.toLowerCase(),
      jarjestaja: {
        oid: data.jarjestajanTunnusOid,
        nimi: data.jarjestajanNimi,
      },
      tutkintopaiva: data.tutkintopaiva,
      arviointipaiva: data.arviointipaiva,
      osat: [
        osa("PU", data.puhuminen),
        osa("KI", data.kirjoittaminen),
        osa("PY", data.puheenYmmartaminen),
        osa("TY", data.tekstinYmmartaminen),
        osa("RS", data.rakenteetJaSanasto),
        osa("YL", data.yleisarvosana),
      ].filter(Boolean),
      tarkistusarviointi: data.tarkistusarvioinninAsiatunnus
        ? {
            saapumispaiva: data.tarkistusarvioinninSaapumisPvm,
            kasittelypaiva: data.tarkistusarvioinninKasittelyPvm,
            asiatunnus: data.tarkistusarvioinninAsiatunnus,
            tarkistusarvioidutOsakokeet: data.tarkistusarvioidutOsakokeet,
            arvosanaMuuttui: data.arvosanaMuuttui,
            perustelu: data.perustelu,
          }
        : undefined,
      arviointitila: data.arviointitila,
      lahdejarjestelmanId: {
        id: data.suoritusId,
        lahde: "Solki",
      },
    },
  }
}

const postSuoritus = async (
  oauth: OauthRequestContext,
  suoritusName: YkiSuorittajaName,
) => {
  const authHeader = await oauth.getAuthorizationHeader("ROOT")
  return fetch(new URL("/kielitutkinnot/yki/api/suoritus", oauth.baseUrl), {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      ...authHeader,
    },
    body: JSON.stringify(buildSuoritusBody(suoritusName)),
  })
}

export const insert = async (
  oauth: OauthRequestContext,
  suoritusName: YkiSuorittajaName,
) => {
  const response = await postSuoritus(oauth, suoritusName)
  expect(response.status).toBe(200)
}

export const insertExpectingValidationError = async (
  oauth: OauthRequestContext,
  suoritusName: YkiSuorittajaName,
): Promise<string[]> => {
  const response = await postSuoritus(oauth, suoritusName)
  expect(response.status).toBe(400)
  const body = (await response.json()) as { errors: string[] }
  return body.errors
}

// TARKISTUSARVIOINTI_HYVAKSYTTY on sisäinen tila, jota ei tuoda rajapinnan kautta.
export const insertApprovedBeforeFeature = async (
  oauth: OauthRequestContext,
  db: TestDB,
  suoritusName: YkiSuorittajaName,
) => {
  await insert(oauth, suoritusName)
  const data = fixtureData[suoritusName]
  await db.dbClient.query(SQL`
      UPDATE yki_suoritus
      SET arviointitila = 'TARKISTUSARVIOINTI_HYVAKSYTTY'
      WHERE solki_id = ${data.suoritusId}
  `)
}
