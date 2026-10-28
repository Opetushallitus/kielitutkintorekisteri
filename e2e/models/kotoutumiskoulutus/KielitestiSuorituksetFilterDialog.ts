import { Locator } from "@playwright/test"
import MultiSelectDropdown from "../components/MultiSelectDropdown"

export class KielitutkintoSuorituksetFilterDialog {
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

  async setTestikieli(values: string[]) {
    await new MultiSelectDropdown(this.modal, "testikieli").select(values)
  }

  async setNaytettavatSuoritukset(
    value: "VALMIIT" | "KESKENERAISET" | "KAIKKI",
  ) {
    await this.modal
      .locator(
        `input[type="radio"][name="naytettavatSuoritukset"][value="${value}"]`,
      )
      .check()
  }

  async hideHenkilotiedot(state: boolean) {
    await this.modal
      .getByRole("checkbox", { name: "Piilota henkilötiedot" })
      .setChecked(state)
  }

  async submit() {
    await this.modal.getByRole("button", { name: "Rajaa" }).click()
  }
}
