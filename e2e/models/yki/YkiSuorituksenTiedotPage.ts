import BasePage from "../BasePage"
import { Locator, Page } from "@playwright/test"
import { Config } from "../../config"

export default class YkiSuorituksenTiedotPage extends BasePage {
  constructor(page: Page, config: Config) {
    super(page, config)
  }

  private henkilotiedotTable(): Locator {
    return this.getPageContent().locator("table.info-table").first()
  }

  private exactLabel(label: string): RegExp {
    return new RegExp(`^${label}$`)
  }

  getLabel(label: string): Locator {
    return this.henkilotiedotTable().locator("th", {
      hasText: this.exactLabel(label),
    })
  }

  getValue(label: string): Locator {
    return this.henkilotiedotTable()
      .locator("tr")
      .filter({
        has: this.page.locator("th", { hasText: this.exactLabel(label) }),
      })
      .locator("td")
      .first()
  }

  copyButton(label: string): Locator {
    return this.getValue(label).getByRole("button", {
      name: "Kopioi leikepöydälle",
    })
  }

  yksilointiLink(): Locator {
    return this.getPageContent().getByRole("button", {
      name: "Tee yksilöinti oppijanumerorekisterissä",
    })
  }
}
