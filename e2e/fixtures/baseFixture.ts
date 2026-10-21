import { test as baseTest } from "@playwright/test"
import KielitestiSuorituksetPage from "../models/kotoutumiskoulutus/KielitestiSuorituksetPage"
import IndexPage from "../models/IndexPage"
import YkiSuorituksetPage from "../models/yki/YkiSuorituksetPage"
import YkiSuorituksenTiedotPage from "../models/yki/YkiSuorituksenTiedotPage"
import { createTestDatabase } from "../db/database"
import * as kotoSuoritusFixture from "./kotoSuoritus"
import * as ykiSuoritusFixture from "./ykiSuoritus"
import * as kotoSuoritusErrorFixture from "./kotoError"
import * as vktSuoritusFixture from "./vktSuoritus"
import * as ykiArvioijaFixture from "./ykiArvioija"
import BasePage from "../models/BasePage"
import { Config, createConfig } from "../config"
import KielitestiErrorPage from "../models/kotoutumiskoulutus/KielitestiErrorPage"
import VktSuorituksetPage from "../models/vkt/VktSuorituksetPage"
import VktSuorituksenTiedotPage from "../models/vkt/VktSuorituksenTiedotPage"
import VktUusintaoikeudetPage from "../models/vkt/VktUusintaoikeudetPage"
import YkiArvioijatPage from "../models/yki/YkiArvioijatPage"
import YkiArvioijaLomakePage from "../models/yki/YkiArvioijaLomakePage"
import YkiTarkistusarvioinnitPage from "../models/yki/YkiTarkistusarvioinnitPage"
import YkiHyvaksytytTarkistusarvioinnitPage from "../models/yki/YkiHyvaksytytTarkistusarvioinnitPage"
import { OauthRequestContext } from "./oauthRequestContext"

interface Fixtures {
  ykiSuorituksetPage: YkiSuorituksetPage
  ykiSuorituksenTiedotPage: YkiSuorituksenTiedotPage
  ykiArvioijatPage: YkiArvioijatPage
  ykiArvioijaLomakePage: YkiArvioijaLomakePage
  ykiTarkistusarvioinnitPage: YkiTarkistusarvioinnitPage
  ykiHyvaksytytTarkistusarvioinnitPage: YkiHyvaksytytTarkistusarvioinnitPage
  kielitestiSuorituksetPage: KielitestiSuorituksetPage
  kielitestiErrorPage: KielitestiErrorPage
  indexPage: IndexPage
  basePage: BasePage
  kotoSuoritus: typeof kotoSuoritusFixture
  ykiSuoritus: typeof ykiSuoritusFixture
  ykiArvioija: typeof ykiArvioijaFixture
  kotoSuoritusError: typeof kotoSuoritusErrorFixture
  vktSuorituksetPage: VktSuorituksetPage
  vktSuorituksenTiedotPage: VktSuorituksenTiedotPage
  vktUusintaoikeudetPage: VktUusintaoikeudetPage
  vktSuoritus: typeof vktSuoritusFixture
  oauth: OauthRequestContext
}

export type TestDB = ReturnType<typeof createTestDatabase>

interface WorkerArgs {
  db: TestDB
  config: Config
}

export const test = baseTest.extend<Fixtures, WorkerArgs>({
  basePage: async ({ page, config }, use) => {
    const basePage = new BasePage(page, config)
    await use(basePage)
  },
  kielitestiSuorituksetPage: async ({ page, config }, use) => {
    const kielitestiSuorituksetPage = new KielitestiSuorituksetPage(
      page,
      config,
    )
    await use(kielitestiSuorituksetPage)
  },
  kielitestiErrorPage: async ({ page, config }, use) => {
    const kielitestiErrorPage = new KielitestiErrorPage(page, config)
    await use(kielitestiErrorPage)
  },
  indexPage: async ({ page, config }, use) => {
    const indexPage = new IndexPage(page, config)
    await use(indexPage)
  },
  ykiSuorituksetPage: async ({ page, config }, use) => {
    const ykiSuorituksetPage = new YkiSuorituksetPage(page, config)
    await use(ykiSuorituksetPage)
  },
  ykiSuorituksenTiedotPage: async ({ page, config }, use) => {
    const ykiSuorituksenTiedotPage = new YkiSuorituksenTiedotPage(page, config)
    await use(ykiSuorituksenTiedotPage)
  },
  ykiArvioijatPage: async ({ page, config }, use) => {
    const ykiArvioijatPage = new YkiArvioijatPage(page, config)
    await use(ykiArvioijatPage)
  },
  ykiArvioijaLomakePage: async ({ page, config }, use) => {
    const ykiArvioijaLomakePage = new YkiArvioijaLomakePage(page, config)
    await use(ykiArvioijaLomakePage)
  },
  ykiTarkistusarvioinnitPage: async ({ page, config }, use) => {
    const ykiTarkistusarvioinnitPage = new YkiTarkistusarvioinnitPage(
      page,
      config,
    )
    await use(ykiTarkistusarvioinnitPage)
  },
  ykiHyvaksytytTarkistusarvioinnitPage: async ({ page, config }, use) => {
    const ykiHyvaksytytTarkistusarvioinnitPage =
      new YkiHyvaksytytTarkistusarvioinnitPage(page, config)
    await use(ykiHyvaksytytTarkistusarvioinnitPage)
  },
  config: [
    async ({}, use) => {
      const workerIndex = parseInt(process.env.TEST_PARALLEL_INDEX)
      const config = createConfig(workerIndex)
      await use(config)
    },
    { scope: "worker", auto: true },
  ],
  db: [
    async ({ config }, use) => {
      const database = createTestDatabase(config)
      await use({ ...database })
    },
    { scope: "worker", auto: true },
  ],
  kotoSuoritus: async ({}, use) => {
    await use({ ...kotoSuoritusFixture })
  },
  ykiSuoritus: async ({}, use) => {
    await use({ ...ykiSuoritusFixture })
  },
  ykiArvioija: async ({}, use) => {
    await use({ ...ykiArvioijaFixture })
  },
  kotoSuoritusError: async ({}, use) => {
    await use({ ...kotoSuoritusErrorFixture })
  },
  vktSuorituksetPage: async ({ page, config }, use) => {
    await use(new VktSuorituksetPage(page, config))
  },
  vktSuorituksenTiedotPage: async ({ page, config }, use) => {
    await use(new VktSuorituksenTiedotPage(page, config))
  },
  vktUusintaoikeudetPage: async ({ page, config }, use) => {
    await use(new VktUusintaoikeudetPage(page, config))
  },
  vktSuoritus: async ({}, use) => {
    await use({ ...vktSuoritusFixture })
  },
  oauth: async ({ config, request }, use) => {
    await use(new OauthRequestContext(config, request))
  },
})

export const { expect, beforeAll, beforeEach, afterEach, afterAll, describe } =
  test
