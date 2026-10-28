import * as node_fs from "node:fs"
import { beforeEach, describe, test } from "../../fixtures/baseFixture"
import { expect, Page } from "@playwright/test"
import {
  expectToHaveInputValue,
  expectToHaveKoodiviite,
  expectToHaveSelectedValue,
  expectToHaveText,
  expectToHaveTexts,
  testForEach,
  testForEachTestId,
} from "../../util/expect"
import { todayISODate } from "../../util/time"
import { insert as insertKoskiError } from "../../fixtures/koskiError"
import { VktSuorituksetFilterDialog } from "../../models/vkt/VktSuorituksetFilterDialog"
import VktSuorituksetPage from "../../models/vkt/VktSuorituksetPage"

const fs = node_fs.promises

const stripExcelCsvPrelude = (csv: string): string => csv.replace(/^\uFEFF/, "")

describe("Valtionkielitutkinnon suoritukset page", () => {
  beforeEach(async ({ db, vktSuoritus, config }) => {
    await db.withEmptyDatabase()
    await vktSuoritus.create(config.baseUrl)
  })

  test("Ilmoittauneet page shows a table with content", async ({
    vktSuorituksetPage,
  }) => {
    await vktSuorituksetPage.login()
    await vktSuorituksetPage.openErinomainenIlmoittautuneet()

    const table = vktSuorituksetPage.table

    await expectToHaveTexts(
      table.labels,
      "",
      "Sukunimi ▲",
      "Etunimet",
      "Taitotaso",
      "Tutkintokieli",
      "Tutkintopäivä",
    )

    await expect(table.rows).toHaveCount(50)

    await testForEach(
      table.getCellsOfRow("1.2.246.562.24.00000000012-SWE"),
      expectToHaveText("Näytä"),
      expectToHaveText("Halonen"),
      expectToHaveText("Vilho Eero"),
      expectToHaveText("Erinomainen"),
      expectToHaveText("Ruotsi"),
      expectToHaveText("23.11.2006"),
    )
  })

  test("Earliest tutkintopäivä is shown per taitotaso regardless of filters", async ({
    vktSuorituksetPage,
  }) => {
    const odotettu =
      "Rekisterin varhaisin tutkintopäivä: Erinomainen 24.2.2000, Hyvä ja tyydyttävä 4.1.2000"

    await vktSuorituksetPage.login()
    await vktSuorituksetPage.openKaikkiSuoritukset()
    const varhaisin = vktSuorituksetPage
      .getPageContent()
      .getByTestId("varhaisin-paiva")
    await expect(varhaisin).toHaveText(odotettu)

    await vktSuorituksetPage.openHyvaJaTyydyttavaSuoritukset()
    await expect(varhaisin).toHaveText(odotettu)

    const table = vktSuorituksetPage.table
    await table.head.getByTestId("Tutkintopaiva").getByRole("link").click()
    await expect(table.rows.first().getByTestId("Tutkintopaiva")).toHaveText(
      "4.1.2000",
    )
  })

  test("Sorting works", async ({ page, vktSuorituksetPage }) => {
    await vktSuorituksetPage.login()
    await vktSuorituksetPage.openErinomainenIlmoittautuneet()

    const table = vktSuorituksetPage.table
    const firstRow = table.rows.first()
    const lastRow = table.rows.last()

    const testSorting = async (
      columnId: string,
      expectedFirstText: string,
      expectedLastText: string,
    ) => {
      await table.head.getByTestId(columnId).getByRole("link").click()
      await expect(firstRow.getByTestId(columnId)).toHaveText(expectedFirstText)
      await expect(lastRow.getByTestId(columnId)).toHaveText(expectedLastText)
    }

    // Oletussorttaus on sukunimen perusteella, joten järjestys kääntyy päinvastaiseksi
    await testSorting("Sukunimi", "Väänänen", "Salo")

    // Testataan loputkin kentät
    await testSorting("Etunimet", "Aarni Eino", "Eero Hugo")
    await testSorting("Tutkintopaiva", "24.2.2000", "26.10.2002")
  })

  test("Details page shows correct information of hyvä ja tyydyttävä taso", async ({
    vktSuorituksetPage,
    vktSuorituksenTiedotPage,
  }) => {
    // Varmista että ollaan oikeassa fikstuurissa
    await vktSuorituksetPage.login()
    await vktSuorituksetPage.openHyvaJaTyydyttavaSuoritukset()
    await vktSuorituksetPage.followLinkOfRow("1.2.246.562.24.00000000007-SWE")
    await expect(vktSuorituksenTiedotPage.heading()).toHaveText(
      "Eriksson, Fiona Konsta",
    )

    // Tarkista että tutkintotaulukossa on oletetut tiedot
    const tutkinnot = vktSuorituksenTiedotPage.tutkinnot
    await expectToHaveTexts(
      tutkinnot.labels,
      "Tutkinto",
      "Tutkintopäivä",
      "Arvosana",
    )
    await tutkinnot.expectRows(
      ["kirjallinen", "22.12.2007", "tyydyttava"],
      ["suullinen", "22.12.2007", "tyydyttava"],
      ["ymmartaminen", "22.12.2007", "tyydyttava"],
    )

    // Tarkista että osakoetaulukossa on oletetut tiedot
    const osakokeet = vktSuorituksenTiedotPage.osakokeet
    await expectToHaveTexts(
      osakokeet.labels,
      "Osakoe",
      "Tutkintopäivä",
      "Arvosana",
      "Arviointipäivä",
      "Suorituksen vastaanottaja",
      "Suorituspaikkakunta",
    )
    await testForEachTestId(
      osakokeet.rows.getByTestId("puheenymmartaminen-2007-12-22"),
      {
        osakoe: expectToHaveKoodiviite("vktosakoe", "puheenymmartaminen"),
        tutkintopaiva: expectToHaveText("22.12.2007"),
        arvosana: expectToHaveKoodiviite("vktarvosana", "tyydyttava"),
        arviointipaiva: expectToHaveText("20.2.2008"),
      },
    )
  })

  test("Details page shows koski error message for hyvä ja tyydyttävä taso", async ({
    db,
    vktSuorituksetPage,
    vktSuorituksenTiedotPage,
  }) => {
    await insertKoskiError(db, "fionaHT")

    await vktSuorituksetPage.login()
    await vktSuorituksetPage.openHyvaJaTyydyttavaSuoritukset()
    await vktSuorituksetPage.followLinkOfRow("1.2.246.562.24.00000000007-SWE")
    await expect(vktSuorituksenTiedotPage.heading()).toHaveText(
      "Eriksson, Fiona Konsta",
    )
    const errorMessage = vktSuorituksenTiedotPage
      .getPageContent()
      .getByText("KOSKI-siirto on epäonnistunut 24.09.2025 14:51:45:")
    await expect(errorMessage).toBeVisible()
    await expect(
      errorMessage.getByRole("listitem").filter({ hasText: "key" }).last(),
    ).toHaveText("key: notFound.oppijaaEiLöydy")
    await expect(
      errorMessage.getByRole("listitem").filter({ hasText: "message" }).last(),
    ).toHaveText("message: Oppijaa 1.2.246.562.24.00000000007 ei löydy.")
  })

  test("Details page shows correct information of erinomainen taso", async ({
    vktSuorituksetPage,
    vktSuorituksenTiedotPage,
  }) => {
    // Varmista että ollaan oikeassa fikstuurissa
    await vktSuorituksetPage.login()
    await vktSuorituksetPage.openErinomainenArvioidut()
    await vktSuorituksetPage.followLinkOfRow("1.2.246.562.24.00000000063-FIN")
    await expect(vktSuorituksenTiedotPage.heading()).toHaveText(
      "Eriksson, Daniel Ville",
    )

    // Tarkista että taulukossa on oletetut tiedot
    const tutkinnot = vktSuorituksenTiedotPage.tutkinnot
    await expectToHaveTexts(
      tutkinnot.labels,
      "Tutkinto",
      "Tutkintopäivä",
      "Arvosana",
    )
    await tutkinnot.expectRows(
      ["kirjallinen", "9.7.2010", "erinomainen"],
      ["suullinen", "9.7.2010", "erinomainen"],
      ["ymmartaminen", "9.4.2010", "erinomainen"],
    )

    const osakokeet = vktSuorituksenTiedotPage.osakokeet
    await expectToHaveTexts(
      osakokeet.labels,
      "Osakoe",
      "Tutkintopäivä",
      "Arvosana",
      "Arviointipäivä",
      "Suorituspaikkakunta",
    )
    await expect(osakokeet.rows).toHaveCount(10)
    await testForEachTestId(
      osakokeet.rows.getByTestId("puhuminen-2010-07-08"),
      {
        osakoe: expectToHaveKoodiviite("vktosakoe", "puhuminen"),
        tutkintopaiva: expectToHaveText("8.7.2010"),
        arvosana: expectToHaveSelectedValue("Erinomainen"),
        arviointipaiva: expectToHaveInputValue("2010-09-06"),
      },
    )
  })

  test("Details page shows koski error message for erinomainen taso", async ({
    db,
    vktSuorituksetPage,
    vktSuorituksenTiedotPage,
  }) => {
    await insertKoskiError(db, "danielE")

    await vktSuorituksetPage.login()
    await vktSuorituksetPage.openErinomainenArvioidut()
    await vktSuorituksetPage.followLinkOfRow("1.2.246.562.24.00000000063-FIN")
    await expect(vktSuorituksenTiedotPage.heading()).toHaveText(
      "Eriksson, Daniel Ville",
    )

    const errorMessage = vktSuorituksenTiedotPage
      .getPageContent()
      .getByText("KOSKI-siirto on epäonnistunut 24.09.2025 16:12:32:")
    await expect(errorMessage).toBeVisible()
    await expect(
      errorMessage.getByRole("listitem").filter({ hasText: "key" }).last(),
    ).toHaveText("key: notFound.oppijaaEiLöydy")
    await expect(
      errorMessage.getByRole("listitem").filter({ hasText: "message" }).last(),
    ).toHaveText("message: Oppijaa 1.2.246.562.24.00000000063 ei löydy.")
  })

  test("Tutkinto katkeaa, jos ensimmäisen osakokeen suorituksesta on kolme vuotta", async ({
    vktSuorituksenTiedotPage,
  }) => {
    // Varmista että ollaan oikeassa fikstuurissa
    await vktSuorituksenTiedotPage.login()
    await vktSuorituksenTiedotPage.open(
      "1.2.246.562.24.00000000446",
      "FIN",
      "Erinomainen",
    )
    await expect(vktSuorituksenTiedotPage.heading()).toHaveText(
      "Rautio, Lucas Nelli",
    )

    // Tarkista että taulukossa on oletetut tiedot
    const tutkinnot = vktSuorituksenTiedotPage.tutkinnot
    await tutkinnot.expectRawRows(
      [
        "<vktkielitaito:suullinen>",
        "15.1.2009",
        "Osakoe puuttuu: <vktosakoe:puheenymmartaminen>",
      ],
      [
        "<vktkielitaito:suullinen>",
        "15.1.2005",
        "Osakoe puuttuu: <vktosakoe:puheenymmartaminen>",
      ],
      ["<vktkielitaito:suullinen>", "15.1.2002", "<vktarvosana:hylatty>"],
      [
        "<vktkielitaito:kirjallinen>",
        "15.10.2001",
        "<vktarvosana:erinomainen>",
      ],
      [
        "<vktkielitaito:ymmartaminen>",
        "15.10.2001",
        "<vktarvosana:erinomainen>",
      ],
    )
  })

  test("Arvosana can be set", async ({
    vktSuorituksetPage,
    vktSuorituksenTiedotPage,
  }) => {
    // Varmista että ollaan oikeassa fikstuurissa
    await vktSuorituksetPage.login()
    await vktSuorituksetPage.openErinomainenIlmoittautuneet()
    await vktSuorituksetPage.followLinkOfRow("1.2.246.562.24.00000000012-SWE")
    await expect(vktSuorituksenTiedotPage.heading()).toHaveText(
      "Halonen, Vilho Eero",
    )

    // Tutkinnoilla ei pitäisi näkyä vielä mitään arvosanoja
    const tutkinnot = vktSuorituksenTiedotPage.tutkinnot
    const tutkintojenArvosanat = tutkinnot.getCellsOfColumn("arvosana")
    await expectToHaveTexts(
      tutkintojenArvosanat,
      "Arvioinnit puuttuvat: <vktosakoe:kirjoittaminen>, <vktosakoe:tekstinymmartaminen>",
      "Arvioinnit puuttuvat: <vktosakoe:puheenymmartaminen>, <vktosakoe:puhuminen>",
      "Arvioinnit puuttuvat: <vktosakoe:puheenymmartaminen>, <vktosakoe:tekstinymmartaminen>",
    )

    // Ota talteen locatorit suullisen taidon osakokeiden riveille
    const osakokeet = vktSuorituksenTiedotPage.osakokeet
    const puheenYmmartaminen = osakokeet.body.getByTestId(
      "puheenymmartaminen-2006-11-23",
    )
    const puhuminen = osakokeet.body.getByTestId("puhuminen-2006-11-23")

    // Varmista ettei puhumisen riville ole vielä syötetty mitään
    await testForEachTestId(puhuminen, {
      osakoe: expectToHaveKoodiviite("vktosakoe", "puhuminen"),
      tutkintopaiva: expectToHaveText("23.11.2006"),
      arvosana: expectToHaveSelectedValue(""),
      arviointipaiva: expectToHaveInputValue(""),
    })

    // Aseta suullisen taidon osakokeille arvosanat ja tallenna
    await puheenYmmartaminen.getByTestId("arvosana").selectOption("Erinomainen")
    await puhuminen.getByTestId("arvosana").selectOption("Erinomainen")
    await vktSuorituksenTiedotPage.save()

    // Tarkista onko suulliselle taidolle muodostunut arvosana
    await testForEach(
      tutkintojenArvosanat,
      expectToHaveKoodiviite("vktarvosana", "erinomainen"),
      expectToHaveText(
        "Arvioinnit puuttuvat: <vktosakoe:kirjoittaminen>, <vktosakoe:tekstinymmartaminen>",
      ),
      expectToHaveText("Arviointi puuttuu: <vktosakoe:tekstinymmartaminen>"),
    )

    // Tarkista että arvosana on valittuna taulukossa ja arviointipäiväksi on automaattisesti valittu tämä päivä
    await testForEachTestId(puheenYmmartaminen, {
      osakoe: expectToHaveKoodiviite("vktosakoe", "puheenymmartaminen"),
      tutkintopaiva: expectToHaveText("23.11.2006"),
      arvosana: expectToHaveSelectedValue("Erinomainen"),
      arviointipaiva: expectToHaveInputValue(todayISODate()),
    })
  })

  describe("Filter", () => {
    async function testFiltering(
      vktSuorituksetPage: VktSuorituksetPage,
      f: (d: VktSuorituksetFilterDialog) => Promise<void>,
    ) {
      await vktSuorituksetPage.login()
      await vktSuorituksetPage.openKaikkiSuoritukset()

      const countBefore = await vktSuorituksetPage.getNumberOfRows()

      const dialog = await vktSuorituksetPage.openFilterDialog()
      await f(dialog)
      await dialog.submit()

      const countAfter = await vktSuorituksetPage.getNumberOfRows()
      expect(countAfter).toBeLessThan(countBefore)
    }

    test("Date range filter reduces the number of results", async ({
      vktSuorituksetPage,
    }) => {
      await testFiltering(vktSuorituksetPage, async (dialog) => {
        await dialog.setAlkupaiva("2020-01-01")
        await dialog.setLoppupaiva("2022-12-31")
      })
    })

    test("Tutkintokieli filter reduces the number of results", async ({
      vktSuorituksetPage,
    }) => {
      await testFiltering(vktSuorituksetPage, async (dialog) => {
        await dialog.setTutkintokieli(["FIN"])
      })
    })

    test("Taitotaso filter reduces the number of results", async ({
      vktSuorituksetPage,
    }) => {
      await testFiltering(vktSuorituksetPage, async (dialog) => {
        await dialog.setTaitotaso(["Erinomainen"])
      })
    })

    test("Arvioitu filter reduces the number of results", async ({
      vktSuorituksetPage,
    }) => {
      await testFiltering(vktSuorituksetPage, async (dialog) => {
        await dialog.setArvioitu(["ArvioituOsittainTaiKokonaan"])
      })
    })

    test("Merkitty poistettavaksi filter reduces the number of results", async ({
      vktSuorituksetPage,
    }) => {
      await testFiltering(vktSuorituksetPage, async (dialog) => {
        await dialog.setMerkittyPoistettavaksi(true)
      })
    })

    test("Filter settings are preserved when user uses search", async ({
      vktSuorituksetPage,
    }) => {
      await vktSuorituksetPage.login()
      await vktSuorituksetPage.openKaikkiSuoritukset()

      const dialog = await vktSuorituksetPage.openFilterDialog()
      await dialog.setTutkintokieli(["FIN"])
      await dialog.setTaitotaso(["Erinomainen"])
      await dialog.submit()

      await vktSuorituksetPage.search("eriksson")

      await expect(
        vktSuorituksetPage.getPageContent().getByText("Tutkintokieli: Suomi"),
      ).toBeVisible()
      await expect(
        vktSuorituksetPage.getPageContent().getByText("Taitotaso: Erinomainen"),
      ).toBeVisible()
    })

    test("Search query is preserved when user sets new filters", async ({
      vktSuorituksetPage,
    }) => {
      await vktSuorituksetPage.login()
      await vktSuorituksetPage.openKaikkiSuoritukset()

      await vktSuorituksetPage.search("eriksson")

      const dialog = await vktSuorituksetPage.openFilterDialog()
      await dialog.setTaitotaso(["Erinomainen"])
      await dialog.submit()

      await expect(vktSuorituksetPage.searchField).toHaveValue("eriksson")
    })

    test("Filter settings are preserved when user changes the sorting order", async ({
      vktSuorituksetPage,
    }) => {
      await vktSuorituksetPage.login()
      await vktSuorituksetPage.openKaikkiSuoritukset()

      const dialog = await vktSuorituksetPage.openFilterDialog()
      await dialog.setTutkintokieli(["FIN"])
      await dialog.setTaitotaso(["Erinomainen"])
      await dialog.submit()

      await vktSuorituksetPage.table.head
        .getByTestId("Etunimet")
        .getByRole("link")
        .click()

      await expect(
        vktSuorituksetPage.getPageContent().getByText("Tutkintokieli: Suomi"),
      ).toBeVisible()
      await expect(
        vktSuorituksetPage.getPageContent().getByText("Taitotaso: Erinomainen"),
      ).toBeVisible()
    })

    test("Filter settings are preserved when user changes pagination page", async ({
      vktSuorituksetPage,
    }) => {
      await vktSuorituksetPage.login()
      await vktSuorituksetPage.openKaikkiSuoritukset()

      const dialog = await vktSuorituksetPage.openFilterDialog()
      await dialog.setTutkintokieli(["FIN"])
      await dialog.submit()

      await vktSuorituksetPage.pagination.goToNextPage()

      await expect(
        vktSuorituksetPage.getPageContent().getByText("Tutkintokieli: Suomi"),
      ).toBeVisible()
    })

    test("Piilota henkilötiedot filter reduces the number of columns", async ({
      vktSuorituksetPage,
    }) => {
      await vktSuorituksetPage.login()
      await vktSuorituksetPage.openKaikkiSuoritukset()

      const columnsBefore = await vktSuorituksetPage.table.labels.count()

      const dialog = await vktSuorituksetPage.openFilterDialog()
      await dialog.hideHenkilotiedot(true)
      await dialog.submit()

      const columnsAfter = await vktSuorituksetPage.table.labels.count()
      expect(columnsAfter).toBeLessThan(columnsBefore)
    })
  })

  describe("Search", () => {
    test("Search by first name works", async ({ vktSuorituksetPage }) => {
      await vktSuorituksetPage.login()
      await vktSuorituksetPage.openErinomainenIlmoittautuneet()
      await vktSuorituksetPage.search("fiona")

      await expectToHaveTexts(
        vktSuorituksetPage.table.getCellsOfColumn("Etunimet"),
        "Fiona Kerttu",
        "Fiona Roosa",
      )
    })

    test("Search by surname works", async ({ vktSuorituksetPage }) => {
      await vktSuorituksetPage.login()
      await vktSuorituksetPage.openErinomainenIlmoittautuneet()
      await vktSuorituksetPage.search("halonen")

      await expectToHaveTexts(
        vktSuorituksetPage.table.getCellsOfColumn("Sukunimi"),
        "Halonen",
        "Halonen",
      )
    })

    test("Search by oppijanumero works", async ({ vktSuorituksetPage }) => {
      await vktSuorituksetPage.login()
      await vktSuorituksetPage.openErinomainenIlmoittautuneet()
      await vktSuorituksetPage.search("1.2.246.562.24.00000000055")

      await expect(vktSuorituksetPage.table.rows).toHaveCount(1)
      await expectToHaveTexts(
        vktSuorituksetPage.table.getCellsOfRow(
          "1.2.246.562.24.00000000055-SWE",
        ),
        "Näytä",
        "Huhtala",
        "Nella Eveliina",
        "Erinomainen",
        "Ruotsi",
        "19.12.2009",
      )
    })
  })
})

describe("Valtionkielitutkinnon suoritukset csv download", () => {
  beforeEach(async ({ db, oauth, vktSuoritus, config }) => {
    await db.withEmptyDatabase()
    await vktSuoritus.createErinomainenIlmoittautuminen(config.baseUrl, oauth)
    await vktSuoritus.createHyvaJaTyydyttavaSuoritus(config.baseUrl, oauth)
  })

  async function downloadCsv(
    page: Page,
    vktSuorituksetPage: VktSuorituksetPage,
  ): Promise<string> {
    const [download] = await Promise.all([
      page.waitForEvent("download"),
      vktSuorituksetPage
        .getPageContent()
        .getByText("Lataa tiedot CSV:nä")
        .click(),
    ])
    const path = await download.path()
    expect(path).not.toBeNull()
    return stripExcelCsvPrelude(await fs.readFile(path!, "utf8"))
  }

  function parseCsv(csv: string): Record<string, string>[] {
    const [headerLine, ...rest] = csv
      .split("\n")
      .filter((line) => line.length > 0)
    const headers = headerLine.split(";")
    return rest.map((line) => {
      const cols = line.split(";")
      return Object.fromEntries(headers.map((h, i) => [h, cols[i] ?? ""]))
    })
  }

  test("csv download only includes arvioitu suoritus", async ({
    page,
    vktSuorituksetPage,
  }) => {
    await vktSuorituksetPage.login()
    await vktSuorituksetPage.openErinomainenIlmoittautuneet()

    const rows = parseCsv(await downloadCsv(page, vktSuorituksetPage))
    expect(rows).toHaveLength(1)
    expect(rows[0]).toMatchObject({
      "Ilmoittautumisen tunniste": "KIOS:748",
      Sukunimi: "Sallinen-Testi",
      Etunimet: "Magdalena Testi",
      Oppijanumero: "1.2.246.562.24.33342764709",
      Taitotaso: "Erinomainen",
      Tutkintokieli: "Suomi",
      Tutkintopäivä: "10.2.2026",
    })
  })

  test("csv download resolves Suorituksen vastaanottaja OID to a name", async ({
    page,
    vktSuorituksetPage,
  }) => {
    await vktSuorituksetPage.login()
    await vktSuorituksetPage.openKaikkiSuoritukset()

    const rows = parseCsv(await downloadCsv(page, vktSuorituksetPage))
    const row = rows.find(
      (r) =>
        r["Suorituksen vastaanottajan OID"] === "1.2.246.562.24.59267607404",
    )
    expect(row).toBeDefined()
    expect(row!["Suorituksen vastaanottaja"]).toBe("Petro Testi Kivinen-Testi")
  })

  test("csv download resolves Suorituspaikkakunta koodi to a name", async ({
    page,
    vktSuorituksetPage,
  }) => {
    await vktSuorituksetPage.login()
    await vktSuorituksetPage.openKaikkiSuoritukset()

    const rows = parseCsv(await downloadCsv(page, vktSuorituksetPage))
    const rowWithPaikkakunta = rows.find((r) => r["Suorituspaikkakunta"] !== "")
    expect(rowWithPaikkakunta).toBeDefined()
    expect(rowWithPaikkakunta!["Suorituspaikkakunta"]).not.toMatch(/^\d+$/)
  })
})

describe("Valtionkielitutkinnon suoritukset csv download filtering", () => {
  beforeEach(async ({ db, vktSuoritus, config }) => {
    await db.withEmptyDatabase()
    await vktSuoritus.create(config.baseUrl)
  })

  async function downloadCsv(
    page: Page,
    vktSuorituksetPage: VktSuorituksetPage,
  ): Promise<string> {
    const [download] = await Promise.all([
      page.waitForEvent("download"),
      vktSuorituksetPage
        .getPageContent()
        .getByText("Lataa tiedot CSV:nä")
        .click(),
    ])
    const path = await download.path()
    expect(path).not.toBeNull()
    return stripExcelCsvPrelude(await fs.readFile(path!, "utf8"))
  }

  const csvRowCount = (csv: string) =>
    csv.split("\n").filter((line) => line.length > 0).length - 1

  async function testFilteredCsv(
    page: Page,
    vktSuorituksetPage: VktSuorituksetPage,
    applyFilter: (dialog: VktSuorituksetFilterDialog) => Promise<void>,
  ) {
    await vktSuorituksetPage.login()
    await vktSuorituksetPage.openKaikkiSuoritukset()

    const rowsBefore = csvRowCount(await downloadCsv(page, vktSuorituksetPage))
    expect(rowsBefore).toBeGreaterThan(0)

    const dialog = await vktSuorituksetPage.openFilterDialog()
    await applyFilter(dialog)
    await dialog.submit()

    const rowsAfter = csvRowCount(await downloadCsv(page, vktSuorituksetPage))
    expect(rowsAfter).toBeLessThan(rowsBefore)
  }

  test("Date range filter reduces csv row count", async ({
    page,
    vktSuorituksetPage,
  }) => {
    await testFilteredCsv(page, vktSuorituksetPage, async (dialog) => {
      await dialog.setAlkupaiva("2020-01-01")
      await dialog.setLoppupaiva("2022-12-31")
    })
  })

  test("Tutkintokieli filter reduces csv row count", async ({
    page,
    vktSuorituksetPage,
  }) => {
    await testFilteredCsv(page, vktSuorituksetPage, (dialog) =>
      dialog.setTutkintokieli(["FIN"]),
    )
  })

  test("Taitotaso filter reduces csv row count", async ({
    page,
    vktSuorituksetPage,
  }) => {
    await testFilteredCsv(page, vktSuorituksetPage, (dialog) =>
      dialog.setTaitotaso(["Erinomainen"]),
    )
  })

  test("Arvioitu filter reduces csv row count", async ({
    page,
    vktSuorituksetPage,
  }) => {
    await testFilteredCsv(page, vktSuorituksetPage, (dialog) =>
      dialog.setArvioitu(["ArvioituOsittainTaiKokonaan"]),
    )
  })

  test("Merkitty poistettavaksi filter reduces csv row count", async ({
    page,
    vktSuorituksetPage,
  }) => {
    await testFilteredCsv(page, vktSuorituksetPage, (dialog) =>
      dialog.setMerkittyPoistettavaksi(true),
    )
  })

  test("Piilota henkilötiedot filter reduces csv column count", async ({
    page,
    vktSuorituksetPage,
  }) => {
    await vktSuorituksetPage.login()
    await vktSuorituksetPage.openKaikkiSuoritukset()

    const headerBefore = (await downloadCsv(page, vktSuorituksetPage)).split(
      "\n",
    )[0]
    const columnsBefore = headerBefore.split(";").length

    const dialog = await vktSuorituksetPage.openFilterDialog()
    await dialog.hideHenkilotiedot(true)
    await dialog.submit()

    const headerAfter = (await downloadCsv(page, vktSuorituksetPage)).split(
      "\n",
    )[0]
    const columnsAfter = headerAfter.split(";").length

    expect(columnsAfter).toBeLessThan(columnsBefore)
  })
})
