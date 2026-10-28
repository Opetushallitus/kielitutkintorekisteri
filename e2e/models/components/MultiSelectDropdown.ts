import { Locator } from "@playwright/test"

export default class MultiSelectDropdown {
  details: Locator
  summary: Locator
  checkboxes: Locator

  constructor(parent: Locator, name: string) {
    this.details = parent.getByTestId(`${name}-valinta`)
    this.summary = this.details.locator("summary")
    this.checkboxes = this.details.locator('input[type="checkbox"]')
  }

  async select(values: string[]) {
    await this.summary.click()
    for (const checkbox of await this.checkboxes.all()) {
      const value = await checkbox.getAttribute("value")
      await checkbox.setChecked(values.includes(value ?? ""))
    }
    await this.summary.click()
  }
}
