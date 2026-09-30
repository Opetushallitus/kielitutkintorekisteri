import BasePage from "../BasePage"
import { Locator, Page } from "@playwright/test"
import { Config } from "../../config"
import DisplayTable from "../components/DisplayTable"
import {
  expectToHaveKoodiviite,
  expectToHaveText,
  testForEach,
} from "../../util/expect"

export default class VktSuorituksenTiedotPage extends BasePage {
  tutkinnot: TutkinnotTable
  osakokeet: DisplayTable

  constructor(page: Page, config: Config) {
    super(page, config)
    this.tutkinnot = new TutkinnotTable(page.getByTestId("tutkinnot"))
    this.osakokeet = new DisplayTable(page.getByTestId("osakokeet"))
  }

  async open(oppijanumero: string, kieli: "FIN" | "SWE", taso: "Erinomainen") {
    await this.goto(
      `/kielitutkinnot/vkt/suoritukset/${oppijanumero}/${kieli}/${taso}`,
    )
  }

  heading() {
    return this.page.getByRole("heading").first()
  }

  async save() {
    await this.page.getByRole("button", { name: "Tallenna" }).click()
  }
}

class TutkinnotTable extends DisplayTable {
  constructor(locator: Locator) {
    super(locator)
  }

  async expectRows(...rows: string[][]) {
    await testForEach(
      this.allCells,
      ...rows.flatMap((row) => [
        expectToHaveKoodiviite("vktkielitaito", row[0]),
        expectToHaveText(row[1]),
        expectToHaveKoodiviite("vktarvosana", row[2] || ""),
      ]),
    )
  }

  async expectRawRows(...rows: string[][]) {
    await testForEach(
      this.allCells,
      ...rows.flatMap((row) => [
        expectToHaveText(row[0]),
        expectToHaveText(row[1]),
        expectToHaveText(row[2]),
      ]),
    )
  }
}
