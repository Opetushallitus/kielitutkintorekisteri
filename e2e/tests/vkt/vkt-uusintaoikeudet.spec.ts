import * as node_fs from "node:fs"
import { beforeEach, describe, test } from "../../fixtures/baseFixture"
import { expect } from "@playwright/test"
import { expectToHaveTexts } from "../../util/expect"

const fs = node_fs.promises

const RIVI = "1.2.246.562.10.14893989377-FIN-hyvajatyydyttava-kirjoittaminen"

describe("VKT:n uusintaan oikeutetut", () => {
  beforeEach(async ({ db, oauth, vktSuoritus, config }) => {
    await db.withEmptyDatabase()
    await vktSuoritus.createErinomainenIlmoittautuminen(config.baseUrl, oauth)
    await vktSuoritus.createHyvaJaTyydyttavaSuoritus(config.baseUrl, oauth)
  })

  test("raportti näyttää henkilöt ja uusittavat osakokeet taitotasoittain", async ({
    vktUusintaoikeudetPage,
  }) => {
    await vktUusintaoikeudetPage.login()
    await vktUusintaoikeudetPage.openFromNavigation()

    const hyvaJaTyydyttava =
      vktUusintaoikeudetPage.getYhteenveto("hyvajatyydyttava")
    await expect(hyvaJaTyydyttava.henkiloita).toHaveText("1")
    await expect(hyvaJaTyydyttava.osakokeita).toHaveText("1")
    const erinomainen = vktUusintaoikeudetPage.getYhteenveto("erinomainen")
    await expect(erinomainen.henkiloita).toHaveText("0")
    await expect(erinomainen.osakokeita).toHaveText("0")
    await expect(vktUusintaoikeudetPage.getNumberOfRows()).toHaveText("1")

    await expectToHaveTexts(
      vktUusintaoikeudetPage.table.getCellsOfRow(RIVI),
      "1.2.246.562.10.14893989377",
      "Öhman-Testi",
      "Ranja Testi",
      "Suomi",
      "Hyvä ja tyydyttävä",
      "<vktosakoe:kirjoittaminen>",
      "<vktkielitaito:kirjallinen>",
      "12.2.2026",
      "31.12.2028",
    )
  })

  test("CSV sisältää uusittavat osakokeet", async ({
    page,
    vktUusintaoikeudetPage,
  }) => {
    await vktUusintaoikeudetPage.login()
    await vktUusintaoikeudetPage.open()

    const [download] = await Promise.all([
      page.waitForEvent("download"),
      vktUusintaoikeudetPage.getCsvDownloadLink().click(),
    ])
    const path = await download.path()
    expect(path).not.toBeNull()
    const rivit = (await fs.readFile(path!, "utf8"))
      .replace(/^﻿/, "")
      .split("\n")
      .filter((rivi) => rivi.length > 0)

    expect(rivit).toEqual([
      "Oppijanumero;Sukunimi;Etunimet;Tutkintokieli;Taitotaso;Uusittava osakoe;Tutkinnot;Hylkäämispäätös;Oikeus päättyy",
      "1.2.246.562.10.14893989377;Öhman-Testi;Ranja Testi;Suomi;Hyvä ja tyydyttävä;<vktosakoe:kirjoittaminen>;<vktkielitaito:kirjallinen>;12.2.2026;31.12.2028",
    ])
  })
})
