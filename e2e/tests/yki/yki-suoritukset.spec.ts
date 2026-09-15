import { beforeEach, describe, expect, test } from "../../fixtures/baseFixture"
import { enumerate } from "../../util/arrays"

describe('"YKI Suoritukset" -page', () => {
  beforeEach(async ({ db, oauth, basePage, ykiSuoritus, ykiSuoritusError }) => {
    await db.withEmptyDatabase()
    await ykiSuoritus.insert(oauth, "ranja")
    await ykiSuoritus.insert(oauth, "ranjaTarkistus")
    await ykiSuoritus.insert(oauth, "petro")
    await ykiSuoritus.insert(oauth, "magdalena")
    await ykiSuoritusError.insert(db, "missingOid")

    await basePage.login()
  })

  test("yki suoritukset page is navigable from index page", async ({
    indexPage,
    ykiSuorituksetPage,
  }) => {
    await indexPage.open()
    await indexPage.getYkiSuorituksetLink().click()

    await ykiSuorituksetPage.expectContentToBeVisible()
  })

  test("yki suoritukset page is navigable from main nav", async ({
    indexPage,
    ykiSuorituksetPage,
  }) => {
    await indexPage.open()
    await ykiSuorituksetPage.openFromNavigation()

    await ykiSuorituksetPage.expectContentToBeVisible()
  })

  test("yki suoritus versions are hidden by default", async ({
    indexPage,
    ykiSuorituksetPage,
  }) => {
    await indexPage.open()
    await ykiSuorituksetPage.openFromNavigation()

    const suoritukset = ykiSuorituksetPage.getSuoritusRow()

    await expect(suoritukset).toHaveCount(3)
  })

  test("yki suoritukset with show version history shows all suoritukset", async ({
    indexPage,
    ykiSuorituksetPage,
  }) => {
    await indexPage.open()
    await ykiSuorituksetPage.openFromNavigation()

    let dialog = await ykiSuorituksetPage.openFilterDialog()
    await dialog.setVersionHistory(true)
    await dialog.submit()

    const suoritukset = ykiSuorituksetPage.getSuoritusRow()

    await expect(suoritukset).toHaveCount(4)
  })

  test("filter dialog can be cancelled without applying the filter", async ({
    indexPage,
    ykiSuorituksetPage,
  }) => {
    await indexPage.open()
    await ykiSuorituksetPage.openFromNavigation()

    const dialog = await ykiSuorituksetPage.openFilterDialog()
    await expect(dialog.modal).toBeVisible()
    await dialog.setVersionHistory(true)
    await dialog.cancel()

    // Peruutus sulkee dialogin eikä rajaa listaa: versiohistoria jää pois.
    await expect(dialog.modal).toBeHidden()
    await expect(ykiSuorituksetPage.getSuoritusRow()).toHaveCount(3)
  })

  test("yki suoritukset search", async ({ indexPage, ykiSuorituksetPage }) => {
    await indexPage.open()
    await ykiSuorituksetPage.openFromNavigation()
    await ykiSuorituksetPage.setSearchTerm("ranja")
    await ykiSuorituksetPage.filterSuoritukset()

    const suoritukset = ykiSuorituksetPage.getSuoritusRow()

    await expect(suoritukset).toHaveCount(1)
  })

  test("yki suoritukset search with version history", async ({
    indexPage,
    ykiSuorituksetPage,
  }) => {
    await indexPage.open()
    await ykiSuorituksetPage.openFromNavigation()
    await ykiSuorituksetPage.setSearchTerm("ranja")
    await ykiSuorituksetPage.filterSuoritukset()

    let dialog = await ykiSuorituksetPage.openFilterDialog()
    await dialog.setVersionHistory(true)
    await dialog.submit()

    const suoritukset = ykiSuorituksetPage.getSuoritusRow()

    await expect(suoritukset).toHaveCount(2)
  })

  test("henkilötunnus column is hidden and arviointitila is shown in the list", async ({
    ykiSuorituksetPage,
  }) => {
    await ykiSuorituksetPage.open()

    const table = ykiSuorituksetPage.getSuorituksetTable()
    await expect(
      table.getByRole("columnheader", { name: "Henkilötunnus" }),
    ).toHaveCount(0)
    await expect(
      table.getByRole("columnheader", { name: "Arviointitila" }),
    ).toBeVisible()
  })

  test("should download yki suoritukset CSV and verify its content", async ({
    page,
    ykiSuorituksetPage,
  }) => {
    await ykiSuorituksetPage.open()

    const csvContent = await ykiSuorituksetPage.downloadCSV()
    expect(csvContent).toContain(
      [
        "Oppijanumero",
        "Sukunimi",
        "Etunimi",
        "Sukupuoli",
        "Henkilötunnus",
        "Kansalaisuus",
        "Osoite",
        "Sähköposti",
        "Tutkintopäivä",
        "Tutkintokieli",
        "Tutkintotaso",
        "Järjestäjän OID",
        "Järjestäjän nimi",
        "Arviointitila",
        "Arviointipäivä",
        "Rekisteriintuontiaika",
        "Tekstin ymmärtäminen",
        "Kirjoittaminen",
        "Puheen ymmärtäminen",
        "Puhuminen",
        "Rakenteet ja sanasto",
        "Yleisarvosana",
        "Todistuskieli",
        "Tarkistusarvioinnin saapumispäivä",
        "Tarkistusarvioinnin käsittelypäivä",
        "Tarkistusarviointi hyväksytty",
        "Asiatunnus",
        "Tarkistusarvioidut osakokeet",
        "Osakokeet joiden arvosana muuttui",
        "Perustelu",
        "Tila lähetetty",
        "Opiskeluoikeus-OID",
      ].join(";"),
    ) // Validate headers
  })

  test("should download yki suoritukset without henkilötiedot CSV and verify its content", async ({
    page,
    ykiSuorituksetPage,
  }) => {
    await ykiSuorituksetPage.open()

    const dialog = await ykiSuorituksetPage.openFilterDialog()
    await dialog.hideHenkilotiedot(true)
    await dialog.submit()

    const csvContent = await ykiSuorituksetPage.downloadCSV()

    expect(csvContent).toContain(
      [
        "Sukupuoli",
        "Kansalaisuus",
        "Tutkintopäivä",
        "Tutkintokieli",
        "Tutkintotaso",
        "Järjestäjän OID",
        "Järjestäjän nimi",
        "Arviointitila",
        "Arviointipäivä",
        "Rekisteriintuontiaika",
        "Tekstin ymmärtäminen",
        "Kirjoittaminen",
        "Puheen ymmärtäminen",
        "Puhuminen",
        "Rakenteet ja sanasto",
        "Yleisarvosana",
        "Todistuskieli",
        "Tarkistusarvioinnin saapumispäivä",
        "Tarkistusarvioinnin käsittelypäivä",
        "Tarkistusarviointi hyväksytty",
        "Asiatunnus",
        "Tarkistusarvioidut osakokeet",
        "Osakokeet joiden arvosana muuttui",
        "Perustelu",
        "Tila lähetetty",
      ].join(";"),
    ) // Validate headers
  })

  describe("Version history columns", () => {
    test("Solki-tunniste and Versio columns are not shown by default", async ({
      ykiSuorituksetPage,
    }) => {
      await ykiSuorituksetPage.open()

      const table = ykiSuorituksetPage.getSuorituksetTable()
      await expect(
        table.getByRole("columnheader", { name: "Solki-tunniste" }),
      ).toHaveCount(0)
      await expect(
        table.getByRole("columnheader", { name: "Versio" }),
      ).toHaveCount(0)
    })

    test("Solki-tunniste and Versio columns are shown when versionHistory is enabled", async ({
      ykiSuorituksetPage,
    }) => {
      await ykiSuorituksetPage.open()

      const dialog = await ykiSuorituksetPage.openFilterDialog()
      await dialog.setVersionHistory(true)
      await dialog.submit()

      const table = ykiSuorituksetPage.getSuorituksetTable()
      await expect(
        table.getByRole("columnheader", { name: "Solki-tunniste" }),
      ).toBeVisible()
      await expect(
        table.getByRole("columnheader", { name: "Versio" }),
      ).toBeVisible()
    })
  })

  describe("Sorting", () => {
    const sortTestCases = [
      {
        column: "Oppijanumero",
        tableColumnIndex: 1,
        order: [
          "1.2.246.562.24.59267607404",
          "1.2.246.562.24.33342764709",
          "1.2.246.562.24.20281155246",
        ],
      },
      {
        column: "Sukunimi",
        tableColumnIndex: 2,
        order: ["Öhman-Testi", "Sallinen-Testi", "Kivinen-Testi"],
      },
      {
        column: "Etunimi",
        tableColumnIndex: 3,
        order: ["Ranja Testi", "Petro Testi", "Magdalena Testi"],
      },
      {
        column: "Tutkintopäivä",
        tableColumnIndex: 4,
        order: ["25.8.2024", "1.9.2024", "12.1.2025"],
      },
      {
        column: "Tutkintokieli",
        tableColumnIndex: 5,
        order: ["SWE", "FIN", "FIN"],
      },
      {
        column: "Tutkintotaso",
        tableColumnIndex: 6,
        order: ["YT", "YT", "PT"],
      },
    ] as const

    for (const testCase of sortTestCases) {
      const { column, tableColumnIndex, order } = testCase
      const reverseOrder = [...order].reverse()

      test(`registry data can be sorted by "${column}"`, async ({
        ykiSuorituksetPage: page,
      }) => {
        await page.open()

        const sortByLink = page.getTableColumnHeaderLink(column)
        await sortByLink.click()

        for (const [expected, row] of enumerate(order)) {
          const actualValue = page.getSuoritusColumn(row, tableColumnIndex)
          await expect(actualValue).toHaveText(expected)
        }

        await sortByLink.click()

        for (const [expected, row] of enumerate(reverseOrder)) {
          const actualValue = page.getSuoritusColumn(row, tableColumnIndex)
          await expect(actualValue).toHaveText(expected)
        }
      })
    }
  })

  describe("Sorting with version history", () => {
    const solkiColumnIndex = 9

    // With version history enabled, 4 rows are shown.
    // Solki IDs: petro=123123, magdalena=172836, ranja=183424, ranjaTarkistus=183424
    const solkiSortTestCase = {
      column: "Solki-tunniste",
      tableColumnIndex: solkiColumnIndex,
      // First click inherits current direction (DESC)
      order: ["183424", "183424", "172836", "123123"],
    } as const

    test(`registry data can be sorted by "Solki-tunniste"`, async ({
      ykiSuorituksetPage: page,
    }) => {
      await page.open()

      const dialog = await page.openFilterDialog()
      await dialog.setVersionHistory(true)
      await dialog.submit()

      const { column, tableColumnIndex, order } = solkiSortTestCase
      const reverseOrder = [...order].reverse()

      const sortByLink = page.getTableColumnHeaderLink(column)
      await sortByLink.click()

      for (const [expected, row] of enumerate(order)) {
        const actualValue = page.getSuoritusColumn(row, tableColumnIndex)
        await expect(actualValue).toHaveText(expected)
      }

      await sortByLink.click()

      for (const [expected, row] of enumerate(reverseOrder)) {
        const actualValue = page.getSuoritusColumn(row, tableColumnIndex)
        await expect(actualValue).toHaveText(expected)
      }
    })

    test(`registry data can be sorted by "Versio"`, async ({
      ykiSuorituksetPage: page,
    }) => {
      await page.open()

      const dialog = await page.openFilterDialog()
      await dialog.setVersionHistory(true)
      await dialog.submit()

      // Verify Versio column is sortable by checking the sort link exists and
      // that sorting changes the row order. Fixtures are inserted in order:
      // ranja, ranjaTarkistus, petro, magdalena — so lastModified increases
      // in that order. Verify via the Solki-tunniste column (index 9).
      const sortByLink = page.getTableColumnHeaderLink("Versio")
      await sortByLink.click()

      // First click: DESC (newest first) — magdalena(172836), petro(123123), then ranja pair(183424)
      await expect(page.getSuoritusColumn(0, solkiColumnIndex)).toHaveText(
        "172836",
      )
      await expect(page.getSuoritusColumn(1, solkiColumnIndex)).toHaveText(
        "123123",
      )

      await sortByLink.click()

      // Second click: ASC (oldest first) — ranja pair(183424), then petro(123123), magdalena(172836)
      await expect(page.getSuoritusColumn(2, solkiColumnIndex)).toHaveText(
        "123123",
      )
      await expect(page.getSuoritusColumn(3, solkiColumnIndex)).toHaveText(
        "172836",
      )
    })
  })
})
