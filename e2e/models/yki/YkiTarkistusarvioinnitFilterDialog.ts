import { Locator } from "@playwright/test"
import MultiSelectDropdown from "../components/MultiSelectDropdown"

export class YkiTarkistusarvioinnitFilterDialog {
  modal: Locator

  constructor(modal: Locator) {
    this.modal = modal
  }

  async setTutkintotaso(values: string[]) {
    await new MultiSelectDropdown(this.modal, "tutkintotaso").select(values)
  }

  async setTila(values: string[]) {
    await new MultiSelectDropdown(this.modal, "tila").select(values)
  }

  async setOsa(values: string[]) {
    await new MultiSelectDropdown(this.modal, "osa").select(values)
  }

  async setKasittelyaika(alku: string, loppu: string) {
    await this.modal.getByLabel("Pyyntö käsitelty alkaen").fill(alku)
    await this.modal.getByLabel("Pyyntö käsitelty päättyen").fill(loppu)
  }

  async setHyvaksymisaika(alku: string, loppu: string) {
    await this.modal.getByLabel("Tulos hyväksytty alkaen").fill(alku)
    await this.modal.getByLabel("Tulos hyväksytty päättyen").fill(loppu)
  }

  async submit() {
    const loaded = this.modal.page().waitForEvent("load")
    await this.modal.getByRole("button", { name: "Rajaa" }).click()
    await loaded
  }
}
