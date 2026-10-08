import BaseSuorituksetPage from "../BaseSuorituksetPage"
import { Locator, Page } from "@playwright/test"
import { expect } from "../../fixtures/baseFixture"
import { Config } from "../../config"
import { YkiSuorituksetFilterDialog } from "./YkiSuorituksetFilterDialog"

export default class YkiSuoritusTilastotPage extends BaseSuorituksetPage {
  constructor(page: Page, config: Config) {
    super(page, config)
  }

  async open(query: string = "") {
    await this.goto(`yki/suoritukset/tilastot${query}`)
  }

  async expectContentToBeVisible() {
    await expect(
      this.getPageContent().getByRole("heading", { name: "Tilastot" }),
    ).toBeVisible()
  }

  getSuoritusRow(): Locator {
    return this.getSuorituksetTable().locator(".tilastorivi")
  }

  getYhteensa(): Locator {
    return this.getPageContent().getByTestId("numberOfRows")
  }

  async sortBy(column: string) {
    await this.getTableColumnHeaderLink(column).click()
  }

  async openFilterDialog() {
    return new YkiSuorituksetFilterDialog(await this.openFilterDialogLocator())
  }

  async backToSuoritukset() {
    await this.getPageContent().getByTestId("takaisin-suorituksiin").click()
  }
}
