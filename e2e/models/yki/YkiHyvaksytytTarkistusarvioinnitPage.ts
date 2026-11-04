import { Locator, Page } from "@playwright/test"
import { Config } from "../../config"
import DisplayTable from "../components/DisplayTable"
import BaseYkiTarkistusarvioinnitPage from "./BaseYkiTarkistusarvioinnitPage"

export default class YkiHyvaksytytTarkistusarvioinnitPage extends BaseYkiTarkistusarvioinnitPage {
  hyvaksytytTable: DisplayTable
  hyvaksytytDate: Locator
  hyvaksytytSubmit: Locator

  takaisinLink: Locator

  constructor(page: Page, config: Config) {
    super(page, config)
    this.hyvaksytytTable = new DisplayTable(page.getByTestId("hyvaksyttyTable"))
    this.hyvaksytytDate = page.getByTestId("hyvaksyttyDate")
    this.hyvaksytytSubmit = page.getByTestId("hyvaksyttySubmit")
    this.takaisinLink = page.getByTestId("takaisinLink")
  }

  async open() {
    await this.goto("yki/tarkistusarvioinnit/hyvaksytyt")
  }
}
