import BasePage from "../BasePage"
import { Page } from "@playwright/test"
import { Config } from "../../config"

export default class YkiArvioijahakemuksetPage extends BasePage {
  constructor(page: Page, config: Config) {
    super(page, config)
  }

  async open() {
    await this.goto("yki/arvioijat/hakemukset")
  }

  async haeUudet() {
    await this.page.getByTestId("haeArvioijahakemukset").click()
  }

  get tarkistettavat() {
    return this.page.getByTestId("tarkistettavat-arvioijahakemukset")
  }

  get keskeneraiset() {
    return this.page.getByTestId("arvioijahakemukset")
  }

  get eiTarkistettavia() {
    return this.page.getByTestId("eiTarkistettavia")
  }

  tarkistettava(hakemusOid: string) {
    return this.tarkistettavat.getByTestId(hakemusOid)
  }

  async tarkista(hakemusOid: string) {
    await this.tarkistettava(hakemusOid).getByTestId("tarkista").click()
  }
}
