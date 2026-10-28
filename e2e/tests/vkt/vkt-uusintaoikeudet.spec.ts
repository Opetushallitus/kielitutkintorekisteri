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
      "Oppijanumero;Sukunimi;Etunimet;Tutkintokieli;Taitotaso;Uusittava osakoe;Tutkinnot;Hylkäämispäätös;Oikeus päättyy;Puhelinnumero;Sähköposti;Katuosoite;Postinumero;Postitoimipaikka;Maa",
      "1.2.246.562.10.14893989377;Öhman-Testi;Ranja Testi;Suomi;Hyvä ja tyydyttävä;<vktosakoe:kirjoittaminen>;<vktkielitaito:kirjallinen>;12.2.2026;31.12.2028;;;;;;",
    ])
  })

  test("tarkastelupäivän voi valita", async ({ vktUusintaoikeudetPage }) => {
    await vktUusintaoikeudetPage.login()
    await vktUusintaoikeudetPage.open()

    let dialog = await vktUusintaoikeudetPage.openFilterDialog()
    await dialog.setTarkastelupaiva("2026-02-11")
    await dialog.submit()
    await expect(vktUusintaoikeudetPage.getTarkastelupaiva()).toHaveText(
      "11.2.2026",
    )
    await expect(vktUusintaoikeudetPage.getNumberOfRows()).toHaveText("0")

    dialog = await vktUusintaoikeudetPage.openFilterDialog()
    await dialog.setTarkastelupaiva("2028-06-01")
    await dialog.submit()
    await expect(vktUusintaoikeudetPage.getTarkastelupaiva()).toHaveText(
      "1.6.2028",
    )
    await expect(vktUusintaoikeudetPage.getNumberOfRows()).toHaveText("1")
  })

  test("raportin voi rajata taitotason ja kielen mukaan", async ({
    vktUusintaoikeudetPage,
  }) => {
    await vktUusintaoikeudetPage.login()
    await vktUusintaoikeudetPage.open()

    let dialog = await vktUusintaoikeudetPage.openFilterDialog()
    await dialog.setTaitotaso(["Erinomainen"])
    await dialog.submit()
    await expect(
      vktUusintaoikeudetPage
        .getPageContent()
        .getByText("Taitotaso: Erinomainen"),
    ).toBeVisible()
    await expect(vktUusintaoikeudetPage.getNumberOfRows()).toHaveText("0")

    dialog = await vktUusintaoikeudetPage.openFilterDialog()
    await dialog.setTaitotaso([])
    await dialog.setTutkintokieli(["FIN"])
    await dialog.submit()
    await expect(
      vktUusintaoikeudetPage.getPageContent().getByText("Tutkintokieli: Suomi"),
    ).toBeVisible()
    await expect(vktUusintaoikeudetPage.getNumberOfRows()).toHaveText("1")

    dialog = await vktUusintaoikeudetPage.openFilterDialog()
    await dialog.setTutkintokieli([])
    await dialog.setTaitotaso(["Erinomainen", "HyväJaTyydyttävä"])
    await dialog.submit()
    await expect(
      vktUusintaoikeudetPage
        .getPageContent()
        .getByText("Taitotaso: Erinomainen, Hyvä ja tyydyttävä"),
    ).toBeVisible()
    await expect(vktUusintaoikeudetPage.getNumberOfRows()).toHaveText("1")
  })

  test("henkilötiedot voi piilottaa", async ({ vktUusintaoikeudetPage }) => {
    await vktUusintaoikeudetPage.login()
    await vktUusintaoikeudetPage.open()

    const dialog = await vktUusintaoikeudetPage.openFilterDialog()
    await dialog.hideHenkilotiedot(true)
    await dialog.submit()

    await expect(
      vktUusintaoikeudetPage
        .getPageContent()
        .getByText("Henkilötiedot piilotettu"),
    ).toBeVisible()
    await expectToHaveTexts(
      vktUusintaoikeudetPage.table.getCellsOfRow(RIVI),
      "Suomi",
      "Hyvä ja tyydyttävä",
      "<vktosakoe:kirjoittaminen>",
      "<vktkielitaito:kirjallinen>",
      "12.2.2026",
      "31.12.2028",
    )
  })
})
