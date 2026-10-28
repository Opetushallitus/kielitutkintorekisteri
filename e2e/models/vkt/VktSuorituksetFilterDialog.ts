import { Locator } from "@playwright/test"
import MultiSelectDropdown from "../components/MultiSelectDropdown"

export class VktSuorituksetFilterDialog {
  modal: Locator

  constructor(modal: Locator) {
    this.modal = modal
  }

  async setAlkupaiva(date: string) {
    await this.modal.getByLabel("Alkaen").fill(date)
  }

  async setLoppupaiva(date: string) {
    await this.modal.getByLabel("Päättyen").fill(date)
  }

  async setTutkintokieli(values: string[]) {
    await this.setEnum("tutkintokieli", values)
  }

  async setTaitotaso(values: string[]) {
    await this.setEnum("taitotaso", values)
  }

  async setArvioitu(values: string[]) {
    await this.setEnum("arvioitu", values)
  }

  async setMerkittyPoistettavaksi(value: boolean | null) {
    const stringValue = value === null ? "" : value ? "true" : "false"
    await this.modal
      .locator(`input[name="merkittyPoistettavaksi"][value="${stringValue}"]`)
      .check()
  }

  async hideHenkilotiedot(state: boolean) {
    await this.modal
      .getByRole("checkbox", { name: "Piilota henkilötiedot" })
      .setChecked(state)
  }

  async submit() {
    await this.modal.getByRole("button", { name: "Rajaa" }).click()
    await this.modal.page().waitForLoadState("networkidle")
  }

  private async setEnum(name: string, values: string[]) {
    await new MultiSelectDropdown(this.modal, name).select(values)
  }
}
