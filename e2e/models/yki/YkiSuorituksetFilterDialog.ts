import { Locator } from "@playwright/test"
import MultiSelectDropdown from "../components/MultiSelectDropdown"

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

  async setTuontiaika(alku: string, loppu: string) {
    await this.modal.getByLabel("Rekisteriintuontiaika alkaen").fill(alku)
    await this.modal.getByLabel("Rekisteriintuontiaika päättyen").fill(loppu)
  }

  tutkintokieli() {
    return new MultiSelectDropdown(this.modal, "tutkintokieli")
  }

  async setTutkintokieli(values: string[]) {
    await this.tutkintokieli().select(values)
  }

  async submit() {
    await this.modal.getByRole("button", { name: "Rajaa" }).click()
  }

  async cancel() {
    await this.modal.getByTestId("peruutaRajaus").click()
  }
}
