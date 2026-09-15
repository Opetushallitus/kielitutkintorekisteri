import { beforeAll, describe, expect, test } from "../../fixtures/baseFixture"
import { APIRequestContext } from "@playwright/test"
import { Config } from "../../config"

type MockUser = "ROOT" | "VIRKAILIJA" | "KIOS" | "SOLKI" | "KOSKI" | "NO_ROLES"
type HttpMethod = "GET" | "POST" | "PUT" | "DELETE"
type ContentType = "application/x-www-form-urlencoded" | "application/json"
type Route = `${HttpMethod} /${string}`

const viewRoutes = [
  // HomeController
  "GET /",

  // YkiViewController
  "GET /yki/suoritukset",
  "GET /yki/suoritukset/virheet",
  "GET /yki/arvioijat",
  "GET /yki/arvioijat/uusi",
  "POST /yki/arvioijat/uusi",
  "GET /yki/arvioijat/1",
  "GET /yki/arvioijat/1/muokkaa",
  "POST /yki/arvioijat/1",
  "GET /yki/koski-virheet",
  "GET /yki/koski-virheet/piilota/1/true",
  "GET /yki/koski-request/1",
  "GET /yki/tarkistusarvioinnit",

  // KielitestiViewController
  "GET /koto-kielitesti/suoritukset",
  "GET /koto-kielitesti/suoritukset/virheet",

  // VktViewController
  "GET /vkt/erinomainen/ilmoittautuneet",
  "GET /vkt/erinomainen/arvioidut",
  "GET /vkt/hyvajatyydyttava/suoritukset",
  "GET /vkt/suoritukset/1.2.246.562.24.00000000856/SWE/Erinomainen",
  "POST /vkt/suoritukset/1.2.246.562.24.00000000856/SWE/Erinomainen",
  "GET /vkt/koski-virheet",
  "GET /vkt/koski-virheet/piilota/1.2.246.562.24.00000000856/SWE/Erinomainen/true",
  "GET /vkt/koski-request/1.2.246.562.24.00000000856/SWE/Erinomainen",
] satisfies Route[]

const apiRoutes = [
  // YkiApiController
  "GET /yki/api/suoritukset",
  "POST /yki/api/suoritus",
  "POST /yki/api/arvioija",
  "GET /yki/api/arvioijat",

  // KielitestiApiController
  "GET /koto-kielitesti/api/suoritukset",
  "GET /koto-kielitesti/api/suoritukset/virheet",

  // VktApiController
  "PUT /api/vkt/kios",
  "GET /api/vkt/suoritus",

  // YhteystiedotApiController
  "GET /yhteystiedot/api/opiskeluoikeus/1.2.246.562.24.00000000001",
] satisfies Route[]

const publicRoutes = [
  "GET /actuator/health",
  "GET /api-docs",
  "GET /swagger-ui/index.html",
  "GET /schema-examples/yki-suoritus.json",
] satisfies Route[]

const allRoutes = [...viewRoutes, ...apiRoutes, ...publicRoutes]

type DefinedRoute = (typeof allRoutes)[0]
type ExpectedStatusCodes<
  T extends DefinedRoute = DefinedRoute,
  N extends number = number,
> = Partial<Record<T, N>>

function expectStatusCodeFor<T extends DefinedRoute, N extends number>(
  routes: T[],
  statusCode: N,
): ExpectedStatusCodes<T, N> {
  return Object.fromEntries(
    routes.map((route) => [route, statusCode]),
  ) as ExpectedStatusCodes<T, N>
}

describe("Käyttöoikeustestit", () => {
  beforeAll(async ({ db, vktSuoritus, config }) => {
    await db.withEmptyDatabase()
    await vktSuoritus.create(config.baseUrl)
  })

  describe("Kirjautumaton", () => {
    const openPublicRoutes = expectStatusCodeFor(publicRoutes, 200)
    allRoutes.forEach((route) => {
      const expectedStatus = openPublicRoutes[route]
      test(testName(route, expectedStatus), async ({ request, config }) => {
        await makeRequest(request, config, route, expectedStatus)
      })
    })
  })

  describe("CAS", () => {
    describe("Käyttäjä, jolla ei ole Kielitutkintopalvelun rooleja", () => {
      defineCasTests("NO_ROLES", expectStatusCodeFor(publicRoutes, 200))
    })

    describe("Pääkäyttäjä", () => {
      defineCasTests("ROOT", {
        ...expectStatusCodeFor(allRoutes, 200),
        "GET /yki/koski-request/1": 404,
        "GET /yki/arvioijat/1": 404,
        "GET /yki/arvioijat/1/muokkaa": 404,
        "POST /yki/arvioijat/1": 404,
        "POST /yki/api/suoritus": 400,
        "POST /yki/api/arvioija": 400,
        "PUT /api/vkt/kios": 400,
        "GET /yhteystiedot/api/opiskeluoikeus/1.2.246.562.24.00000000001": 404,
      })
    })

    describe("Virkailija ilman arvioijarekisterin kirjoitusoikeutta", () => {
      defineCasTests("VIRKAILIJA", {
        ...expectStatusCodeFor(viewRoutes, 200),
        ...expectStatusCodeFor(publicRoutes, 200),
        "GET /yki/koski-request/1": 404,
        "GET /yki/arvioijat/uusi": 403,
        "POST /yki/arvioijat/uusi": 403,
        "GET /yki/arvioijat/1": 404,
        "GET /yki/arvioijat/1/muokkaa": 403,
        "POST /yki/arvioijat/1": 403,
        "GET /yki/api/suoritukset": 200,
        "GET /yki/api/arvioijat": 200,
        "GET /koto-kielitesti/api/suoritukset": 200,
        "GET /koto-kielitesti/api/suoritukset/virheet": 200,
        "GET /api/vkt/suoritus": 200,
      })
    })

    describe("KIOS / Ilmoittautumisjärjestelmä", () => {
      defineCasTests("KIOS", {
        ...expectStatusCodeFor(publicRoutes, 200),
        "PUT /api/vkt/kios": 400,
      })
    })

    describe("Solki", () => {
      defineCasTests("SOLKI", {
        ...expectStatusCodeFor(publicRoutes, 200),
        "POST /yki/api/suoritus": 400,
        "POST /yki/api/arvioija": 400,
        // TODO: valiaikaista. Arvioijarekisterin kirjoitus vaatii tilapaisesti YKI_TALLENNUS-
        // oikeutta (ks. Authority.YKI_ARVIOIJAREKISTERI), joka Solkilla on. Palauta 403-odotukset
        // kun oikea kayttooikeus on kaytossa.
        "GET /yki/arvioijat/uusi": 200,
        "POST /yki/arvioijat/uusi": 200,
        "GET /yki/arvioijat/1/muokkaa": 404,
        "POST /yki/arvioijat/1": 404,
      })
    })

    describe("KOSKI", () => {
      defineCasTests("KOSKI", {
        ...expectStatusCodeFor(publicRoutes, 200),
        "GET /yhteystiedot/api/opiskeluoikeus/1.2.246.562.24.00000000001": 404,
      })
    })
  })

  describe("OAuth2", () => {
    describe("Käyttäjä, jolla ei ole Kielitutkintopalvelun rooleja", () => {
      defineOAuth2Tests("NO_ROLES", expectStatusCodeFor(publicRoutes, 200))
    })

    describe("Pääkäyttäjä", () => {
      defineOAuth2Tests("ROOT", {
        ...expectStatusCodeFor(publicRoutes, 200),
        "POST /yki/api/suoritus": 400,
        "POST /yki/api/arvioija": 400,
        "PUT /api/vkt/kios": 400,
        "GET /yhteystiedot/api/opiskeluoikeus/1.2.246.562.24.00000000001": 404,
      })
    })
    describe("KIOS / Ilmoittautumisjärjestelmä", () => {
      defineOAuth2Tests("KIOS", {
        ...expectStatusCodeFor(publicRoutes, 200),
        "PUT /api/vkt/kios": 400,
      })
    })

    describe("Solki", () => {
      defineOAuth2Tests("SOLKI", {
        ...expectStatusCodeFor(publicRoutes, 200),
        "POST /yki/api/suoritus": 400,
        "POST /yki/api/arvioija": 400,
      })
    })

    describe("KOSKI", () => {
      defineCasTests("KOSKI", {
        ...expectStatusCodeFor(publicRoutes, 200),
        "GET /yhteystiedot/api/opiskeluoikeus/1.2.246.562.24.00000000001": 404,
      })
    })
  })
})

function defineCasTests(
  user: MockUser,
  expectedStatusCodes: ExpectedStatusCodes,
) {
  // Jokaisella workerilla on oma palvelin ja oma sessio, mutta evasteet eivat erotu portin
  // perusteella. Ilman worker-kohtaista tiedostoa workerit ylikirjoittaisivat toistensa tilat.
  const statePath = `browserstate/securitytests-${user}-${process.env.TEST_PARALLEL_INDEX}.json`

  beforeAll(async ({ request, config }) => {
    const response = await request.get(config.baseUrl + `dev/mocklogin/${user}`)
    expect(response.status()).toBe(200)
    await request.storageState({ path: statePath })
  })

  allRoutes.forEach((route) => {
    const expectedStatus = expectedStatusCodes[route]
    test(testName(route, expectedStatus), async ({ browser, config }) => {
      const context = await browser.newContext({ storageState: statePath })
      await makeRequest(context.request, config, route, expectedStatus)
    })
  })
}

function defineOAuth2Tests(
  clientId: MockUser,
  expectedStatusCodes: ExpectedStatusCodes,
) {
  let accessToken: string | null = null

  beforeAll(async ({ request, config }) => {
    const response = await request.post(config.baseUrl + "dev/oauth/token", {
      form: {
        grant_type: "client_credentials",
        client_id: clientId,
        client_secret: "test-secret-key-which-is-long-enough",
      },
    })

    const body = await response.json()
    accessToken = body.access_token
  })

  allRoutes.forEach((route) => {
    const expectedStatus = expectedStatusCodes[route]
    test(testName(route, expectedStatus), async ({ request, config }) => {
      await makeRequest(request, config, route, expectedStatus, accessToken)
    })
  })
}

function testName(route: DefinedRoute, expectedStatus?: number) {
  return `Palauttaa ${expectedStatus || 403}: ${route}`
}

async function makeRequest(
  request: APIRequestContext,
  config: Config,
  route: DefinedRoute,
  expectedStatusCode?: number,
  accessToken?: string,
) {
  const { method, path } = splitRoute(route, config)
  let response = null

  const contentType: ContentType | null =
    method == "POST" || method == "PUT"
      ? route.includes("/api/")
        ? "application/json"
        : "application/x-www-form-urlencoded"
      : null

  const headers = [
    accessToken ? ["Authorization", `Bearer ${accessToken}`] : null,
    contentType ? ["Content-Type", contentType] : null,
  ].filter(Boolean)

  const options = {
    headers: Object.fromEntries(headers),
  }

  switch (method) {
    case "GET":
      response = await request.get(path, options)
      break
    case "POST":
      response = await request.post(path, options)
      break
    case "PUT":
      response = await request.put(path, options)
      break
    case "DELETE":
      response = await request.delete(path, options)
      break
    default:
      throw new Error(`Unsupported method: ${method}`)
  }

  try {
    expect(response.status()).toBe(expectedStatusCode || 403)
  } catch (e) {
    console.log(response)
    console.log((await response.body()).toString())
    throw e
  }
}

function splitRoute(route: Route, config: Config) {
  const [method, path] = route.split(" ")
  return { method: method as HttpMethod, path: config.baseUrl + path.slice(1) }
}
