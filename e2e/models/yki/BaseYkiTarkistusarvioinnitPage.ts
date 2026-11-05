import BasePage from "../BasePage"
import { Locator, Page } from "@playwright/test"
import { Config } from "../../config"
import { expect } from "../../fixtures/baseFixture"
import * as node_fs from "node:fs"
import { YkiTarkistusarvioinnitFilterDialog } from "./YkiTarkistusarvioinnitFilterDialog"

export default abstract class BaseYkiTarkistusarvioinnitPage extends BasePage {
  numberOfRows: Locator

  constructor(page: Page, config: Config) {
    super(page, config)
    this.numberOfRows = this.getPageContent().getByTestId("numberOfRows")
  }

  async downloadCSV(): Promise<string[]> {
    const [download] = await Promise.all([
      this.page.waitForEvent("download"),
      this.getPageContent().getByText("Lataa tiedot CSV:nä").click(),
    ])
    const path = await download.path()
    expect(path).not.toBeNull()
    const csv = await node_fs.promises.readFile(path!, "utf8")
    return csv.replace(/^﻿/, "").trim().split("\n")
  }

  async search(term: string) {
    await this.getPageContent().getByTestId("search").fill(term)
    await this.clickAndWaitForLoad(
      this.getPageContent().getByTestId("search-button"),
    )
  }

  async sortBy(column: string) {
    await this.clickAndWaitForLoad(
      this.getPageContent()
        .getByRole("table")
        .getByRole("link", { name: column }),
    )
  }

  private async clickAndWaitForLoad(locator: Locator) {
    const loaded = this.page.waitForEvent("load")
    await locator.click()
    await loaded
  }

  async openFilterDialog() {
    await this.getPageContent()
      .getByRole("button", { name: "Rajaa näytettävät tiedot" })
      .click()
    return new YkiTarkistusarvioinnitFilterDialog(
      this.getPageContent().getByTestId("table-filter-dialog"),
    )
  }
}
