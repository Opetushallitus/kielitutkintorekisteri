import { Locator } from "@playwright/test"

export class YkiSuorituksetFilterDialog {
  modal: Locator

  constructor(modal: Locator) {
    this.modal = modal
  }

  async setVersionHistory(state: boolean) {
    await this.modal
      .getByRole("checkbox", { name: "Näytä versiohistoria" })
      .setChecked(state)
  }

  async hideHenkilotiedot(state: boolean) {
    await this.modal
      .getByRole("checkbox", { name: "Piilota henkilötiedot" })
      .setChecked(state)
  }

  async submit() {
    await this.modal.getByRole("button", { name: "Rajaa" }).click()
  }

  async cancel() {
    await this.modal.getByTestId("peruutaRajaus").click()
  }
}
