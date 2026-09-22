import { Page } from "@playwright/test"
import BasePage from "./BasePage"
import { Config } from "../config"

export default class IndexPage extends BasePage {
  constructor(page: Page, config: Config) {
    super(page, config)
  }

  async open() {
    await this.goto("")
  }

  getYkiSuorituksetLink() {
    const ykiLinkList = this.getPageContent().getByTestId("yki-links")
    return ykiLinkList.getByRole("link", { name: "Suoritukset" })
  }

  getYkiTarkistusarvioinnitLink() {
    const ykiLinkList = this.getPageContent().getByTestId("yki-links")
    return ykiLinkList.getByRole("link", { name: "Tarkistusarvioinnit" })
  }

  getTyhjennaKaannosvalimuistiButton() {
    return this.getPageContent().getByTestId("tyhjenna-kaannosvalimuisti")
  }
}
