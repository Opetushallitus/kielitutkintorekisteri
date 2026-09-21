import { Locator } from "@playwright/test"

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

  async setTestikieli(value: string) {
    await this.setEnum("testikieli", value)
  }

  async setNaytettavatSuoritukset(
    value: "VALMIIT" | "KESKENERAISET" | "KAIKKI",
  ) {
    await this.setEnum("naytettavatSuoritukset", value)
  }

  async hideHenkilotiedot(state: boolean) {
    await this.modal
      .getByRole("checkbox", { name: "Piilota henkilötiedot" })
      .setChecked(state)
  }

  async submit() {
    await this.modal.getByRole("button", { name: "Rajaa" }).click()
  }

  private async setEnum(name: string, value: string) {
    const radio = this.modal.locator(
      `input[type="radio"][name="${name}"][value="${value}"]`,
    )
    if ((await radio.count()) > 0) {
      await radio.check()
    } else {
      await this.modal.locator(`select[name="${name}"]`).selectOption(value)
    }
  }
}
