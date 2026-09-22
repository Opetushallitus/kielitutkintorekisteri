import { beforeEach, describe, expect, test } from "../fixtures/baseFixture"

describe("Etusivun kojelautanäkymä", () => {
  beforeEach(async ({ db, basePage }) => {
    await db.withEmptyDatabase()
    await basePage.login()
  })

  test("näyttää neljä sektion korttia", async ({ indexPage, page }) => {
    await indexPage.open()

    const dashboard = page.getByTestId("dashboard")
    await expect(dashboard.getByTestId("yki-links")).toBeVisible()
    await expect(dashboard.getByTestId("vkt-links")).toBeVisible()
    await expect(dashboard.getByTestId("koto-kielitesti-links")).toBeVisible()
    await expect(dashboard.getByTestId("admin-links")).toBeVisible()

    await expect(
      dashboard.getByRole("heading", { name: "Yleinen kielitutkinto" }),
    ).toBeVisible()
    await expect(
      dashboard.getByRole("heading", {
        name: "Valtionhallinnon kielitutkinto",
      }),
    ).toBeVisible()
    await expect(
      dashboard.getByRole("heading", {
        name: "Kotoutumiskoulutuksen kielitaidon päättötesti",
      }),
    ).toBeVisible()
    await expect(
      dashboard.getByRole("heading", { name: "Ylläpito" }),
    ).toBeVisible()
  })

  test("YKI-kortin Suoritukset-linkki vie YKI:n suoritussivulle", async ({
    indexPage,
    ykiSuorituksetPage,
  }) => {
    await indexPage.open()
    await indexPage.getYkiSuorituksetLink().click()

    await ykiSuorituksetPage.expectContentToBeVisible()
  })

  test("tyhjästä kannasta latest-received näytetään viivana", async ({
    indexPage,
    page,
  }) => {
    await indexPage.open()

    const latest = page.getByTestId("latest-received").first()
    await expect(latest).toHaveText("—")
  })

  test("Ylläpito-kortista voi tyhjentää käännösvälimuistin", async ({
    indexPage,
    page,
  }) => {
    await indexPage.open()
    await indexPage.getTyhjennaKaannosvalimuistiButton().click()

    await expect(page.getByTestId("viewMessage")).toContainText(
      "Käännösvälimuisti tyhjennettiin",
    )
  })

  test("yki/vkt/koto-korttien sisältö ladataan fragmenttina ja korvaa paikkamerkit", async ({
    indexPage,
    page,
  }) => {
    const fragmentRequests = [
      page.waitForResponse((r) => r.url().endsWith("/dashboard/yki") && r.ok()),
      page.waitForResponse((r) => r.url().endsWith("/dashboard/vkt") && r.ok()),
      page.waitForResponse(
        (r) => r.url().endsWith("/dashboard/koto") && r.ok(),
      ),
    ]

    await indexPage.open()
    await Promise.all(fragmentRequests)

    await expect(page.locator('[data-card-content="yki"]')).toHaveCount(0)
    await expect(page.locator('[data-card-content="vkt"]')).toHaveCount(0)
    await expect(page.locator('[data-card-content="koto"]')).toHaveCount(0)
    await expect(
      page
        .getByTestId("yki-links")
        .getByRole("link", { name: "Tarkistusarvioinnit" }),
    ).toBeVisible()
  })
})
