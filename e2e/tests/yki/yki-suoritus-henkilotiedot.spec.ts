import { beforeEach, describe, expect, test } from "../../fixtures/baseFixture"

describe("YKI suorituksen henkilötiedot", () => {
  beforeEach(async ({ db, oauth, ykiSuoritus, basePage }) => {
    await db.withEmptyDatabase()
    await ykiSuoritus.insert(oauth, "ranja")
    await ykiSuoritus.insert(oauth, "valluYksiloimaton")

    await basePage.login()
  })

  test("näyttää oppijanumeron yksilöidylle oppijalle", async ({
    ykiSuorituksetPage,
    ykiSuorituksenTiedotPage,
  }) => {
    await ykiSuorituksetPage.open()
    await ykiSuorituksetPage.setSearchTerm("Ranja")
    await ykiSuorituksetPage.filterSuoritukset()
    await ykiSuorituksetPage.openSuoritusDetails()

    await expect(
      ykiSuorituksenTiedotPage.getLabel("Oppijanumero"),
    ).toBeVisible()
    await expect(ykiSuorituksenTiedotPage.getValue("Oppijanumero")).toHaveText(
      "1.2.246.562.24.20281155246",
    )

    await expect(ykiSuorituksenTiedotPage.getLabel("Henkilö-oid")).toHaveCount(
      0,
    )
    await expect(ykiSuorituksenTiedotPage.yksilointiLink()).toHaveCount(0)
  })

  test("kopioi oppijanumeron leikepöydälle", async ({
    context,
    page,
    ykiSuorituksetPage,
    ykiSuorituksenTiedotPage,
  }) => {
    await context.grantPermissions(["clipboard-read", "clipboard-write"])
    await ykiSuorituksetPage.open()
    await ykiSuorituksetPage.setSearchTerm("Ranja")
    await ykiSuorituksetPage.filterSuoritukset()
    await ykiSuorituksetPage.openSuoritusDetails()

    const copyButton = ykiSuorituksenTiedotPage.copyButton("Oppijanumero")
    await copyButton.click()

    await expect(copyButton).toHaveClass(/copied/)
    expect(await page.evaluate(() => navigator.clipboard.readText())).toBe(
      "1.2.246.562.24.20281155246",
    )
  })

  test("näyttää henkilö-oidin ja yksilöintilinkin yksilöimättömälle oppijalle", async ({
    ykiSuorituksetPage,
    ykiSuorituksenTiedotPage,
  }) => {
    await ykiSuorituksetPage.open()
    await ykiSuorituksetPage.setSearchTerm("Vallu")
    await ykiSuorituksetPage.filterSuoritukset()
    await ykiSuorituksetPage.openSuoritusDetails()

    await expect(ykiSuorituksenTiedotPage.getLabel("Henkilö-oid")).toBeVisible()
    await expect(
      ykiSuorituksenTiedotPage.getValue("Henkilö-oid"),
    ).toContainText("1.2.246.562.24.10691606777")

    await expect(ykiSuorituksenTiedotPage.getLabel("Oppijanumero")).toHaveCount(
      0,
    )

    const yksilointiLink = ykiSuorituksenTiedotPage.yksilointiLink()
    await expect(yksilointiLink).toBeVisible()
    await expect(yksilointiLink).toHaveAttribute(
      "href",
      "/henkilo-ui/oppija/1.2.246.562.24.10691606777",
    )
  })
})
