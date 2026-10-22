import BasePage from "../BasePage"
import { Locator, Page } from "@playwright/test"
import { Config } from "../../config"
import DisplayTable from "../components/DisplayTable"
import { VktUusintaoikeudetFilterDialog } from "./VktUusintaoikeudetFilterDialog"

type Yhteenvetorivi = "erinomainen" | "hyvajatyydyttava" | "yhteensa"

export default class VktUusintaoikeudetPage extends BasePage {
  table: DisplayTable

  constructor(page: Page, config: Config) {
    super(page, config)
    this.table = new DisplayTable(page.getByTestId("uusintaoikeudet"))
  }

  async open() {
    await this.goto("vkt/uusintaoikeudet")
  }

  async openFromNavigation() {
    await this.gotoFromMainNav("Uusintaan oikeutetut")
  }

  getYhteenveto(rivi: Yhteenvetorivi): {
    henkiloita: Locator
    osakokeita: Locator
  } {
    const tr = this.getPageContent().getByTestId(`yhteenveto-${rivi}`)
    return {
      henkiloita: tr.getByTestId("henkiloita"),
      osakokeita: tr.getByTestId("osakokeita"),
    }
  }

  getNumberOfRows(): Locator {
    return this.getPageContent().getByTestId("numberOfRows")
  }

  getCsvDownloadLink(): Locator {
    return this.getPageContent().getByText("Lataa tiedot CSV:nä")
  }

  getTarkastelupaiva(): Locator {
    return this.getPageContent().getByTestId("tarkastelupaiva")
  }

  async openFilterDialog() {
    await this.getPageContent()
      .getByRole("button", { name: "Rajaa näytettävät tiedot" })
      .click()
    return new VktUusintaoikeudetFilterDialog(
      this.getPageContent().getByTestId("table-filter-dialog"),
    )
  }
}
