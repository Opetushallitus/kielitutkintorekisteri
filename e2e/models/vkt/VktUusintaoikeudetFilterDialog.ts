import { Locator } from "@playwright/test"
import MultiSelectDropdown from "../components/MultiSelectDropdown"

export class VktUusintaoikeudetFilterDialog {
  modal: Locator

  constructor(modal: Locator) {
    this.modal = modal
  }

  async setTarkastelupaiva(date: string) {
    await this.modal.getByLabel("Tarkastelupäivä").fill(date)
  }

  async setTutkintokieli(values: string[]) {
    await this.setEnum("tutkintokieli", values)
  }

  async setTaitotaso(values: string[]) {
    await this.setEnum("taitotaso", values)
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
