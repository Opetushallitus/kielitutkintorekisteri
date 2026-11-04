import BasePage from "../BasePage"
import { Locator, Page } from "@playwright/test"
import { Config } from "../../config"
import { YkiTarkistusarvioinnitFilterDialog } from "./YkiTarkistusarvioinnitFilterDialog"

export default abstract class BaseYkiTarkistusarvioinnitPage extends BasePage {
  numberOfRows: Locator

  constructor(page: Page, config: Config) {
    super(page, config)
    this.numberOfRows = this.getPageContent().getByTestId("numberOfRows")
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
