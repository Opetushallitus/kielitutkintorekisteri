import { Locator, Page } from "@playwright/test"
import { Config } from "../config"
import { expect } from "../fixtures/baseFixture"

type GotoParams = Parameters<Page["goto"]>[1]

export default class BasePage {
  readonly page: Page
  readonly config: Config

  viewMessage: Locator

  constructor(page: Page, config: Config) {
    this.page = page
    this.config = config
    this.viewMessage = page.getByTestId("viewMessage")
  }

  protected async goto(url: string, params?: GotoParams) {
    const fullUrl = new URL(url, this.config.baseUrl)
    return this.page.goto(fullUrl.toString(), params)
  }

  protected async gotoFromMainNav(navLinkText: string, nth: number = 0) {
    const navigation = this.getMainNavigationHeader().getByTestId("main-nav")
    const dropdown = navigation.getByRole("listitem").filter({ hasText: "☰" })
    await dropdown.click()
    await this.getMainNavigationHeader()
      .getByTestId("dropdown-menu")
      .getByRole("listitem")
      .filter({ hasText: navLinkText })
      .nth(nth)
      .click()
  }

  async login(user?: string) {
    const response = await this.goto(
      user ? `dev/mocklogin/${user}` : "dev/mocklogin",
    )
    expect(response?.status()).toBe(200)
  }

  getPageContent() {
    return this.page.getByTestId("page-content")
  }

  getMainNavigationHeader() {
    return this.page.getByTestId("page-main-nav-header")
  }
}
