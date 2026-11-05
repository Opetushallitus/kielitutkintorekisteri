import { Locator, Page } from "@playwright/test"
import { Config } from "../../config"
import BaseYkiTarkistusarvioinnitPage from "./BaseYkiTarkistusarvioinnitPage"

export default class YkiTarkistusarviointiTilastotPage extends BaseYkiTarkistusarvioinnitPage {
  rivit: Locator
  takaisin: Locator

  constructor(page: Page, config: Config) {
    super(page, config)
    this.rivit = this.getPageContent().locator("tr.tilastorivi")
    this.takaisin = this.getPageContent().getByTestId(
      "takaisin-tarkistusarviointeihin",
    )
  }

  async open(query: string = "") {
    await this.goto(`yki/tarkistusarvioinnit/tilastot${query}`)
  }

  rivi(kohde: string): Locator {
    return this.rivit.filter({
      has: this.page.getByTestId("Kohde").getByText(kohde, { exact: true }),
    })
  }

  luvut(kohde: string): Locator {
    return this.rivi(kohde).locator(
      '[data-testid="Yhteensa"], [data-testid="ArvosanaMuuttui"], [data-testid="EiMuuttunut"]',
    )
  }

  async setRyhmittely(
    aikaryhmittely: "Ei" | "Tutkintopaiva" | "Tutkintovuosi",
    sarakkeet: string[],
  ) {
    const lomake = this.getPageContent().getByTestId("ryhmittely-lomake")
    await lomake.getByTestId("aikaryhmittely").selectOption(aikaryhmittely)
    for (const sarake of ["tutkintokieli", "tutkintotaso"]) {
      await lomake
        .getByTestId(`ryhmittely-${sarake}`)
        .setChecked(sarakkeet.includes(sarake))
    }
    await lomake.getByRole("button", { name: "Päivitä" }).click()
  }
}
