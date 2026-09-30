import { Locator } from "@playwright/test"

export default class DisplayTable {
  head: Locator
  labels: Locator
  body: Locator
  rows: Locator
  allCells: Locator

  constructor(locator: Locator) {
    this.head = locator.locator("thead")
    this.labels = this.head.locator("th")
    this.body = locator.locator("tbody")
    this.rows = this.body.locator("tr")
    this.allCells = this.rows.locator("td")
  }

  getCellsOfRow(testId: string) {
    return this.body.getByTestId(testId).locator("td")
  }

  getCellsOfColumn(testId: string) {
    return this.rows.getByTestId(testId)
  }
}
