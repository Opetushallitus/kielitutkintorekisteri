import * as node_fs from "node:fs"
import { beforeEach, describe, expect, test } from "../../fixtures/baseFixture"
import { enumerate } from "../../util/arrays"
const fs = node_fs.promises

describe("Kotoutumiskoulutuksen kielitesti -page", () => {
  beforeEach(async ({ db, kotoSuoritus, basePage }) => {
    await db.withEmptyDatabase()

    await kotoSuoritus.insert(db, "anniina")
    await kotoSuoritus.insert(db, "eino")
    await kotoSuoritus.insert(db, "magdalena")
    await kotoSuoritus.insert(db, "toni")
    await kotoSuoritus.insert(db, "fanniRessu")
    await kotoSuoritus.insert(db, "valluKesken")

    await basePage.login()
  })

  test("loads and has content visible", async ({
    kielitestiSuorituksetPage,
  }) => {
    await kielitestiSuorituksetPage.open()

    await expect(
      kielitestiSuorituksetPage.getHeader(
        "Kotoutumiskoulutuksen kielitaidon päättötesti",
      ),
    ).toBeVisible()
    await expect(kielitestiSuorituksetPage.getPageContent()).toBeVisible()
  })

  test("can be accessed from the index page", async ({
    kielitestiSuorituksetPage,
    indexPage,
  }) => {
    await indexPage.open()
    await kielitestiSuorituksetPage.openFromNavigation()

    await expect(
      kielitestiSuorituksetPage.getHeader(
        "Kotoutumiskoulutuksen kielitaidon päättötesti",
      ),
    ).toBeVisible()
    await expect(kielitestiSuorituksetPage.getPageContent()).toBeVisible()
  })

  test("registry data is visible", async ({
    kielitestiSuorituksetPage,
    kotoSuoritus,
  }) => {
    await kielitestiSuorituksetPage.open()

    const anniina = kotoSuoritus.fixtureData.anniina
    const toni = kotoSuoritus.fixtureData.toni

    const firstSuoritus = kielitestiSuorituksetPage.getSuoritusRow().nth(0)
    await expect(firstSuoritus).toBeVisible()
    await expect(firstSuoritus).toContainText(anniina.etunimet)

    const thirdSuoritus = kielitestiSuorituksetPage.getSuoritusRow().nth(2)
    await expect(thirdSuoritus).toBeVisible()
    await expect(thirdSuoritus).toContainText(toni.etunimet)
  })

  const sortTestCases = [
    {
      column: "Sukunimi",
      tableColumnIndex: 1,
      order: [
        "Välimaa-Testi",
        "Vesala-Testi",
        "Torvinen-Testi",
        "Sallinen-Testi",
        "Laasonen-Testi",
      ],
    },
    {
      column: "Etunimet",
      tableColumnIndex: 2,
      order: [
        "Toni Testi",
        "Magdalena Testi",
        "Fanni Testi",
        "Eino Testi",
        "Anniina Testi",
      ],
    },
    {
      column: "Testikieli",
      tableColumnIndex: 4,
      order: ["SWE", "SWE", "FIN", "FIN", "FIN"],
    },
  ] as const
  for (const testCase of sortTestCases) {
    const { column, tableColumnIndex, order } = testCase
    const reverseOrder = [...order].reverse()

    test(`registry data can be sorted by "${column}"`, async ({
      kielitestiSuorituksetPage: page,
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

  test("search registry data with first and last name", async ({
    kielitestiSuorituksetPage,
  }) => {
    await kielitestiSuorituksetPage.open()
    await kielitestiSuorituksetPage.search("magdalena sallinen")
    const suoritukset = kielitestiSuorituksetPage.getSuoritusRow()
    await expect(suoritukset).toHaveCount(1)
  })

  test("search registry data with oppijanumero", async ({
    kielitestiSuorituksetPage,
  }) => {
    await kielitestiSuorituksetPage.open()
    await kielitestiSuorituksetPage.search("1.2.246.562.24.33342764709")
    const suoritukset = kielitestiSuorituksetPage.getSuoritusRow()
    await expect(suoritukset).toHaveCount(1)
  })

  test("search by organisaatio name", async ({ kielitestiSuorituksetPage }) => {
    await kielitestiSuorituksetPage.open()
    await kielitestiSuorituksetPage.search("Ressun peruskoulu")
    const suoritukset = kielitestiSuorituksetPage.getSuoritusRow()
    await expect(suoritukset).toHaveCount(1)
  })

  test("search by partial organisaatio name", async ({
    kielitestiSuorituksetPage,
  }) => {
    await kielitestiSuorituksetPage.open()
    await kielitestiSuorituksetPage.search("Ressun")
    const suoritukset = kielitestiSuorituksetPage.getSuoritusRow()
    await expect(suoritukset).toHaveCount(1)
  })

  test("search results can be sorted", async ({
    kielitestiSuorituksetPage,
  }) => {
    await kielitestiSuorituksetPage.open()
    await kielitestiSuorituksetPage.search("devnull-1")
    const suoritukset = kielitestiSuorituksetPage.getSuoritusRow()
    await kielitestiSuorituksetPage.getTableColumnHeaderLink("Sukunimi").click()
    const firstSuoritus = kielitestiSuorituksetPage.getSuoritusColumn(0, 1)
    const lastSuoritus = kielitestiSuorituksetPage.getSuoritusColumn(2, 1)
    await expect(suoritukset).toHaveCount(3)
    await expect(firstSuoritus).toHaveText("Välimaa-Testi")
    await expect(lastSuoritus).toHaveText("Sallinen-Testi")
  })

  test("should download koto-suoritukset CSV and verify its content", async ({
    page,
    kielitestiSuorituksetPage,
  }) => {
    await kielitestiSuorituksetPage.open()

    // Intercept the download
    const [download] = await Promise.all([
      page.waitForEvent("download"),
      kielitestiSuorituksetPage.getCSVDownloadLink().click(),
    ])

    // Save the file to a temporary location
    const path = await download.path()
    expect(path).not.toBeNull()

    const csvContent = await fs.readFile(path!, "utf8")
    let headers =
      "Oppijanumero;Sukunimi;Etunimet;Kutsumanimi;Sähköposti;Kurssin ID;Kurssin nimi;Testikieli;Oppilaitos OID;Oppilaitos;Opettajan sähköposti;Suoritusaika;Luetun ymmärtäminen;Kuullun ymmärtäminen;Puhe;Kirjoittaminen"
    let anniina =
      "1.2.246.562.24.24941612410;Torvinen-Testi;Anniina Testi;Anniina;devnull-12@oph.fi;33;Integrationstestning;SWE;1.2.3.4.5.7;1.2.3.4.5.7;opettaja@testi.oph.fi;2025-01-22T10:30:27Z;A1;B1;Yli B1;A2\n"
    let eino =
      "1.2.246.562.24.67409348034;Välimaa-Testi;Eino Testi;Eino;devnull-10@oph.fi;32;Integraatio testaus;FIN;1.2.3.4.5.6;1.2.3.4.5.6;opettaja@testi.oph.fi;2024-11-22T10:49:49Z;A1;B1;Alle A1;B1\n"
    let magdalena =
      "1.2.246.562.24.33342764709;Sallinen-Testi;Magdalena Testi;Magdalena;devnull-14@oph.fi;33;Integrationstestning;SWE;1.2.3.4.5.7;1.2.3.4.5.7;opettaja@testi.oph.fi;2025-01-22T10:30:27Z;A1;B1;Yli B1;A2\n"
    let toni =
      "1.2.246.562.24.16014275446;Laasonen-Testi;Toni Testi;Toni;devnull-6@oph.fi;32;Integraatio testaus;FIN;1.2.3.4.5.6;1.2.3.4.5.6;opettaja@testi.oph.fi;2024-11-24T11:36:43Z;A1;B1;Alle A1;B1\n"

    expect(csvContent).toContain(headers)
    expect(csvContent).toContain(anniina)
    expect(csvContent).toContain(eino)
    expect(csvContent).toContain(magdalena)
    expect(csvContent).toContain(toni)
  })

  describe("Tietojen rajaaminen", async () => {
    test("Suoritusten rajaaminen alkupäivän mukaan", async ({
      kielitestiSuorituksetPage,
    }) => {
      await kielitestiSuorituksetPage.open()
      const dialog = await kielitestiSuorituksetPage.openFilterDialog()
      await dialog.setAlkupaiva("2025-01-01")
      await dialog.submit()

      const suoritukset = kielitestiSuorituksetPage.getSuoritusRow()
      await expect(suoritukset).toHaveCount(2)
    })

    test("Suoritusten rajaaminen loppupäivän mukaan", async ({
      kielitestiSuorituksetPage,
    }) => {
      await kielitestiSuorituksetPage.open()
      const dialog = await kielitestiSuorituksetPage.openFilterDialog()
      await dialog.setLoppupaiva("2024-11-24")
      await dialog.submit()

      const suoritukset = kielitestiSuorituksetPage.getSuoritusRow()
      await expect(suoritukset).toHaveCount(3)
    })

    test("Suoritusten rajaaminen alku- ja loppupäivän mukaan", async ({
      kielitestiSuorituksetPage,
    }) => {
      await kielitestiSuorituksetPage.open()
      const dialog = await kielitestiSuorituksetPage.openFilterDialog()
      await dialog.setAlkupaiva("2024-11-23")
      await dialog.setLoppupaiva("2024-11-24")
      await dialog.submit()

      const suoritukset = kielitestiSuorituksetPage.getSuoritusRow()
      await expect(suoritukset).toHaveCount(1)
      const suoritus = kielitestiSuorituksetPage.getSuoritusRow().nth(0)
      await expect(suoritus).toContainText("Laasonen-Testi")
    })

    test("Suoritusten rajaaminen testikielen mukaan", async ({
      kielitestiSuorituksetPage,
    }) => {
      await kielitestiSuorituksetPage.open()
      const dialog = await kielitestiSuorituksetPage.openFilterDialog()
      await dialog.setTestikieli("SWE")
      await dialog.submit()

      const suoritukset = kielitestiSuorituksetPage.getSuoritusRow()
      await expect(suoritukset).toHaveCount(2)
    })

    test("Henkilötietojen rajaaminen suorituksilta", async ({
      kielitestiSuorituksetPage,
    }) => {
      await kielitestiSuorituksetPage.open()
      const dialog = await kielitestiSuorituksetPage.openFilterDialog()
      await dialog.hideHenkilotiedot(true)
      await dialog.submit()

      const headers = kielitestiSuorituksetPage.page.getByRole("columnheader")
      await expect(headers.filter({ hasText: "Sukunimi" })).toHaveCount(0)
      await expect(headers.filter({ hasText: "Etunimet" })).toHaveCount(0)
    })

    test("Henkilötietojen rajaaminen suorituksilta rajaa henkilötiedot pois csv:ltä", async ({
      page,
      kielitestiSuorituksetPage,
    }) => {
      await kielitestiSuorituksetPage.open()
      const dialog = await kielitestiSuorituksetPage.openFilterDialog()
      await dialog.hideHenkilotiedot(true)
      await dialog.submit()

      const [download] = await Promise.all([
        page.waitForEvent("download"),
        kielitestiSuorituksetPage.getCSVDownloadLink().click(),
      ])
      const path = await download.path()

      const csvContent = await fs.readFile(path!, "utf8")
      let headers =
        "Kurssin ID;Kurssin nimi;Testikieli;Oppilaitos OID;Oppilaitos;Suoritusaika;Luetun ymmärtäminen;Kuullun ymmärtäminen;Puhe;Kirjoittaminen"
      let anniina =
        "\n33;Integrationstestning;SWE;1.2.3.4.5.7;1.2.3.4.5.7;2025-01-22T10:30:27Z;A1;B1;Yli B1;A2\n"
      let eino =
        "\n32;Integraatio testaus;FIN;1.2.3.4.5.6;1.2.3.4.5.6;2024-11-22T10:49:49Z;A1;B1;Alle A1;B1\n"
      let magdalena =
        "\n33;Integrationstestning;SWE;1.2.3.4.5.7;1.2.3.4.5.7;2025-01-22T10:30:27Z;A1;B1;Yli B1;A2\n"
      let toni =
        "\n32;Integraatio testaus;FIN;1.2.3.4.5.6;1.2.3.4.5.6;2024-11-24T11:36:43Z;A1;B1;Alle A1;B1\n"

      expect(csvContent).toContain(headers)
      expect(csvContent).toContain(anniina)
      expect(csvContent).toContain(eino)
      expect(csvContent).toContain(magdalena)
      expect(csvContent).toContain(toni)

      expect(csvContent).not.toContain("Oppijanumero")
      expect(csvContent).not.toContain("Sukunimi")
      expect(csvContent).not.toContain("Etunimet")
      expect(csvContent).not.toContain("Kutsumanimi")
      expect(csvContent).not.toContain("Sähköposti")
      expect(csvContent).not.toContain("Opettajan sähköposti")
    })

    test("Rajaukset säilyvät kun hakua käytetään", async ({
      kielitestiSuorituksetPage,
    }) => {
      await kielitestiSuorituksetPage.open()

      const dialog = await kielitestiSuorituksetPage.openFilterDialog()
      await dialog.setTestikieli("FIN")
      await dialog.hideHenkilotiedot(true)
      await dialog.submit()

      await kielitestiSuorituksetPage.search("laasonen")

      await expect(
        kielitestiSuorituksetPage.getPageContent().getByText("Testikieli: FIN"),
      ).toBeVisible()
      await expect(
        kielitestiSuorituksetPage
          .getPageContent()
          .getByText("Henkilötiedot piilotettu"),
      ).toBeVisible()
    })

    test("Oletuksena näytetään vain valmiit suoritukset", async ({
      kielitestiSuorituksetPage,
    }) => {
      await kielitestiSuorituksetPage.open()

      const suoritukset = kielitestiSuorituksetPage.getSuoritusRow()
      await expect(suoritukset).toHaveCount(5)
      await expect(
        suoritukset.filter({ hasText: "Vastaanottaja-Testi" }),
      ).toHaveCount(0)
    })

    test("Keskeneräisten suoritusten rajaaminen", async ({
      kielitestiSuorituksetPage,
    }) => {
      await kielitestiSuorituksetPage.open()
      const dialog = await kielitestiSuorituksetPage.openFilterDialog()
      await dialog.setNaytettavatSuoritukset("KESKENERAISET")
      await dialog.submit()

      const suoritukset = kielitestiSuorituksetPage.getSuoritusRow()
      await expect(suoritukset).toHaveCount(1)
      await expect(suoritukset.nth(0)).toContainText("Vastaanottaja-Testi")
      await expect(
        kielitestiSuorituksetPage
          .getPageContent()
          .getByText("Näytettävät suoritukset: Keskeneräiset"),
      ).toBeVisible()
    })

    test("Kaikkien suoritusten näyttäminen", async ({
      kielitestiSuorituksetPage,
    }) => {
      await kielitestiSuorituksetPage.open()
      const dialog = await kielitestiSuorituksetPage.openFilterDialog()
      await dialog.setNaytettavatSuoritukset("KAIKKI")
      await dialog.submit()

      const suoritukset = kielitestiSuorituksetPage.getSuoritusRow()
      await expect(suoritukset).toHaveCount(6)
    })

    test("Suoritusten valmiusrajaus vaikuttaa myös csv:hen", async ({
      page,
      kielitestiSuorituksetPage,
      kotoSuoritus,
    }) => {
      const vallu = kotoSuoritus.fixtureData.valluKesken
      const anniina = kotoSuoritus.fixtureData.anniina

      await kielitestiSuorituksetPage.open()
      const dialog = await kielitestiSuorituksetPage.openFilterDialog()
      await dialog.setNaytettavatSuoritukset("KESKENERAISET")
      await dialog.submit()

      const [download] = await Promise.all([
        page.waitForEvent("download"),
        kielitestiSuorituksetPage.getCSVDownloadLink().click(),
      ])
      const csvContent = await fs.readFile((await download.path())!, "utf8")

      expect(csvContent).toContain(vallu.oppijanumero)
      expect(csvContent).not.toContain(anniina.oppijanumero)
    })

    test("Hakutermi säilyy kun rajauksia lisätään", async ({
      kielitestiSuorituksetPage,
    }) => {
      await kielitestiSuorituksetPage.open()

      await kielitestiSuorituksetPage.search("laasonen")

      const dialog = await kielitestiSuorituksetPage.openFilterDialog()
      await dialog.setTestikieli("SWE")
      await dialog.submit()

      await expect(kielitestiSuorituksetPage.searchField).toHaveValue(
        "laasonen",
      )
    })
  })
})
