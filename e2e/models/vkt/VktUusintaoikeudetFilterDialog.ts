import { Locator } from "@playwright/test"

export class VktUusintaoikeudetFilterDialog {
  modal: Locator

  constructor(modal: Locator) {
    this.modal = modal
  }

  async setTarkastelupaiva(date: string) {
    await this.modal.getByLabel("Tarkastelupäivä").fill(date)
  }

  async setTutkintokieli(value: string) {
    await this.setEnum("tutkintokieli", value)
  }

  async setTaitotaso(value: string) {
    await this.setEnum("taitotaso", value)
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

  private async setEnum(name: string, value: string) {
    await this.modal
      .locator(`input[type="radio"][name="${name}"][value="${value}"]`)
      .check()
  }
}
