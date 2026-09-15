import { beforeEach, describe, test } from "../../fixtures/baseFixture"
import { expectToHaveTexts } from "../../util/expect"
import { expect, Locator } from "@playwright/test"

describe("Yleinen kielitutkinto arvioijat page", () => {
  beforeEach(async ({ db, ykiArvioija }) => {
    await db.withEmptyDatabase()
    await ykiArvioija.insert(db, "ranja")
    await ykiArvioija.insert(db, "fanni")
    await ykiArvioija.insert(db, "amalia")
    await ykiArvioija.insert(db, "petro")
  })

  test("yki arvioijat page is navigable from main nav", async ({
    indexPage,
    ykiArvioijatPage,
  }) => {
    await indexPage.login()
    await ykiArvioijatPage.openFromNavigation()
    await ykiArvioijatPage.expectContentToBeVisible()
  })

  test("arvioijat page shows table with content", async ({
    indexPage,
    ykiArvioijatPage,
  }) => {
    await indexPage.login()
    await ykiArvioijatPage.openFromNavigation()
    const table = ykiArvioijatPage.table

    await expectToHaveTexts(
      table.labels,
      // Linkkisarakkeella ei ole otsikkoa
      "",
      "Oppijanumero",
      "Sukunimi ▲",
      "Etunimet",
      "Kieli",
      "Tasot",
      "Tila",
      "Kauden alkupäivä",
      "Kauden päättymispäivä",
    )
    await expect(table.rows).toHaveCount(4)
  })

  test("toolbar buttons share a vertical center", async ({
    indexPage,
    ykiArvioijatPage,
  }) => {
    await indexPage.login()
    await ykiArvioijatPage.openFromNavigation()

    const page = ykiArvioijatPage.page
    const rajaa = page.getByRole("button", {
      name: "Rajaa näytettävät tiedot",
    })
    const lisaa = page.getByTestId("lisaaArvioija")

    const center = async (locator: Locator) => {
      const box = await locator.boundingBox()
      if (!box) throw new Error("nappia ei loytynyt")
      return box.y + box.height / 2
    }

    // Pico antaa nav-linkeille negatiivisen ylamarginaalin, joka nostaisi
    // nappilinkin nappia ylemmas; alle pikselin ero riittaa toleranssiksi.
    expect(
      Math.abs((await center(rajaa)) - (await center(lisaa))),
    ).toBeLessThan(1)
  })

  test("row links to the arvioija details page", async ({
    indexPage,
    ykiArvioijatPage,
  }) => {
    await indexPage.login()
    await ykiArvioijatPage.openFromNavigation()

    await ykiArvioijatPage.search("Kivinen")
    await ykiArvioijatPage.table.rows
      .first()
      .getByTestId("Linkki")
      .getByRole("link")
      .click()

    await expect(
      ykiArvioijatPage
        .getPageContent()
        .getByRole("heading", { name: "Petro Testi Kivinen-Testi" }),
    ).toBeVisible()
    expect(ykiArvioijatPage.page.url()).toMatch(/\/yki\/arvioijat\/\d+$/)
  })

  test("row link stays available when personal data is hidden", async ({
    indexPage,
    ykiArvioijatPage,
  }) => {
    await indexPage.login()
    await ykiArvioijatPage.open()
    await ykiArvioijatPage.page.goto(
      ykiArvioijatPage.page.url() + "?piilotaHenkilotiedot=true",
    )

    await expect(
      ykiArvioijatPage.table.rows.first().getByTestId("Linkki"),
    ).toBeVisible()
    await expect(
      ykiArvioijatPage.table.rows.first().getByTestId("Sukunimi"),
    ).toHaveCount(0)
  })

  test("searching by name filters the list", async ({
    indexPage,
    ykiArvioijatPage,
  }) => {
    await indexPage.login()
    await ykiArvioijatPage.openFromNavigation()
    const table = ykiArvioijatPage.table

    await ykiArvioijatPage.search("Kivinen")
    await expect(table.rows).toHaveCount(1)
    await expect(table.rows.first().getByTestId("Sukunimi")).toHaveText(
      "Kivinen-Testi",
    )

    // Etu- ja sukunimi ovat eri sarakkeissa, joten monisanaisen haun on osuttava molempiin
    await ykiArvioijatPage.search("Petro Kivinen")
    await expect(table.rows).toHaveCount(1)

    await ykiArvioijatPage.search("Kivinen Andersson")
    await expect(table.rows).toHaveCount(0)
  })

  test("search is preserved when sorting", async ({
    indexPage,
    ykiArvioijatPage,
  }) => {
    await indexPage.login()
    await ykiArvioijatPage.openFromNavigation()
    const table = ykiArvioijatPage.table

    await ykiArvioijatPage.search("Testi")
    const rowsBefore = await table.rows.count()

    await table.head.getByTestId("Sukunimi").getByRole("link").click()
    await expect(table.rows).toHaveCount(rowsBefore)
    await expect(
      ykiArvioijatPage.page.getByTestId("arvioijaSearch"),
    ).toHaveValue("Testi")
  })

  test("sorting by sukunimi works", async ({ indexPage, ykiArvioijatPage }) => {
    await indexPage.login()
    await ykiArvioijatPage.openFromNavigation()
    const table = ykiArvioijatPage.table

    // ascending order
    await expect(table.rows.first().getByTestId("Sukunimi")).toHaveText(
      "Andersson-Testi",
    )
    await expect(table.rows.last().getByTestId("Sukunimi")).toHaveText(
      "Öhman-Testi",
    )

    // descending order
    await table.head.getByTestId("Sukunimi").getByRole("link").click()

    await expect(table.rows.first().getByTestId("Sukunimi")).toHaveText(
      "Öhman-Testi",
    )
    await expect(table.rows.last().getByTestId("Sukunimi")).toHaveText(
      "Andersson-Testi",
    )
  })
})
