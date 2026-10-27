import BasePage from "../BasePage"
import { Page } from "@playwright/test"
import { Config } from "../../config"

export default class KielitestiErrorPage extends BasePage {
  readonly url = "koto-kielitesti/suoritukset/virheet"

  constructor(page: Page, config: Config) {
    super(page, config)
  }

  async open() {
    await this.goto(this.url)
  }

  getContent() {
    return this.getPageContent()
  }

  getErrorsTable() {
    return this.getContent().getByRole("table")
  }

  getErrorRow() {
    return this.getContent().getByTestId("virhe-summary-row")
  }

  async getErrorTableBody() {
    const errorsTable = this.getErrorsTable()
    return errorsTable.locator(".virheet")
  }

  async getErrorRows() {
    return this.getErrorRow().all()
  }

  getSuoritusColumn(rowIndex: number, columnIndex: number) {
    const row = this.getErrorRow().nth(rowIndex)
    return row.getByRole("cell").nth(columnIndex)
  }

  getTableColumnHeaderLink(text: string) {
    return this.getErrorsTable().getByRole("link", {
      name: new RegExp(`^${text}( [▲▼])?$`),
    })
  }

  getCSVDownloadLink() {
    return this.getPageContent().getByRole("link", {
      name: "Lataa tiedot CSV:nä",
    })
  }
}
