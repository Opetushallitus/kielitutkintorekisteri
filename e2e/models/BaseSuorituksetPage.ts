import { Locator, Page } from "@playwright/test"
import BasePage from "./BasePage"
import { Config } from "../config"
import { expect } from "../fixtures/baseFixture"
import * as node_fs from "node:fs"

export default abstract class BaseSuorituksetPage extends BasePage {
  constructor(page: Page, config: Config) {
    super(page, config)
  }

  getSuorituksetTable() {
    return this.getPageContent().getByRole("table")
  }

  abstract getSuoritusRow(): Locator

  getSuoritusColumn(rowIndex: number, columnIndex: number) {
    const row = this.getSuoritusRow().nth(rowIndex)
    return row.getByRole("cell").nth(columnIndex)
  }

  getTableColumnHeaderLink(text: string) {
    return this.getSuorituksetTable().getByRole("link", { name: text })
  }

  getErrorLink() {
    return this.getPageContent().locator(".error-text").getByRole("link")
  }

  getCSVDownloadLink() {
    return this.getPageContent().getByRole("link", {
      name: "Lataa tiedot CSV:nä",
    })
  }

  async downloadCSV(): Promise<string> {
    const [download] = await Promise.all([
      this.page.waitForEvent("download"),
      this.getCSVDownloadLink().click(),
    ])
    const path = await download.path()
    expect(path).not.toBeNull()
    return await node_fs.promises.readFile(path!, "utf8")
  }

  protected async openFilterDialogLocator(): Promise<Locator> {
    await this.getPageContent()
      .getByRole("button", { name: "Rajaa näytettävät tiedot" })
      .click()
    return this.getPageContent().getByTestId("table-filter-dialog")
  }
}
