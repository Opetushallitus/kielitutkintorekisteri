import { Locator, Page } from "@playwright/test"
import { Config } from "../../config"
import DisplayTable from "../components/DisplayTable"
import BaseYkiTarkistusarvioinnitPage from "./BaseYkiTarkistusarvioinnitPage"

export default class YkiTarkistusarvioinnitPage extends BaseYkiTarkistusarvioinnitPage {
  odottaaTable: DisplayTable
  odottaaDate: Locator
  odottaaSubmit: Locator

  hyvaksytytLink: Locator

  constructor(page: Page, config: Config) {
    super(page, config)
    this.odottaaTable = new DisplayTable(
      page.getByTestId("odottaaHyvaksyntaaTable"),
    )
    this.odottaaDate = page.getByTestId("odottaaHyvaksyntaaDate")
    this.odottaaSubmit = page.getByTestId("odottaaHyvaksyntaaSubmit")
    this.hyvaksytytLink = page.getByTestId("hyvaksytytLink")
  }

  async open(query: string = "") {
    await this.goto(`yki/tarkistusarvioinnit${query}`)
  }

  async openHyvaksytyt() {
    await this.hyvaksytytLink.click()
  }
}
