import * as node_fs from "node:fs"
import { beforeEach, describe, expect, test } from "../../fixtures/baseFixture"
import { fixtureData } from "../../fixtures/kotoError"
import { enumerate } from "../../util/arrays"

const fs = node_fs.promises

const toFinnishDateTime = (isoString: string, includeTimezone = false) => {
  const parts = new Intl.DateTimeFormat("fi-FI", {
    timeZone: "Europe/Helsinki",
    day: "2-digit",
    month: "2-digit",
    year: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hourCycle: "h23",
    ...(includeTimezone ? { timeZoneName: "longOffset" as const } : {}),
  }).formatToParts(new Date(isoString))
  const get = (type: Intl.DateTimeFormatPartTypes) =>
    parts.find((p) => p.type === type)!.value
  const base = `${get("day")}.${get("month")}.${get("year")} ${get("hour")}:${get("minute")}:${get("second")}`
  if (!includeTimezone) return base
  const [, sign, hours] = get("timeZoneName").match(/([+-])(\d{1,2})/)!
  return `${base}${sign}${hours.padStart(2, "0")}`
}

describe('"Koto Suoritukset" -page', () => {
  beforeEach(async ({ db, basePage, kotoSuoritusError }) => {
    await db.withEmptyDatabase()

    await kotoSuoritusError.insert(db, "suoritusVirhe")

    await basePage.login()
  })

  test("koto suoritukset error page is navigable via suoritukset - page", async ({
    page,
    kielitestiSuorituksetPage,
    kielitestiErrorPage,
  }) => {
    await kielitestiSuorituksetPage.open()
    await kielitestiSuorituksetPage.getErrorLink().click()

    expect(page.url()).toContain(kielitestiErrorPage.url)

    const errors = await kielitestiErrorPage.getErrorRows()
    expect(errors).toHaveLength(1)
  })

  test("koto suoritus error is displayed properly", async ({
    kielitestiErrorPage,
  }) => {
    await kielitestiErrorPage.open()

    const errors = await kielitestiErrorPage.getErrorTableBody()

    const virheFixture = fixtureData.suoritusVirhe
    const hetuCell = errors.getByRole("cell", { name: virheFixture.hetu })
    const nimiCell = errors.getByTestId("nimi")
    const schoolOidCell = errors.getByText(virheFixture.schoolOid!)
    const teacherEmailCell = errors.getByText(virheFixture.teacherEmail)
    const virheenLuontiaikaCell = errors.getByText(
      toFinnishDateTime(virheFixture.virheenLuontiaika),
    )
    const viestiCell = errors.getByText(virheFixture.viesti!)
    const virheellinenKenttaCell = errors.getByText(
      virheFixture.virheellinenKentta!,
      { exact: true },
    )
    const virheellinenArvoCell = errors.getByText(
      virheFixture.virheellinenArvo!,
    )

    await expect(hetuCell).toHaveAttribute("data-testid", "hetu")
    await expect(nimiCell).toHaveText(
      /Etunimet: Ranja Testi\s*Kutsumanimi: Ranja\s*Sukunimi: Öhman-Testi/,
    )
    await expect(schoolOidCell).toHaveAttribute("data-testid", "schoolOid")
    await expect(teacherEmailCell).toHaveAttribute(
      "data-testid",
      "teacherEmail",
    )
    await expect(virheenLuontiaikaCell).toHaveAttribute(
      "data-testid",
      "virheenLuontiaika",
    )
    await expect(viestiCell).toHaveAttribute("data-testid", "viesti")
    await expect(virheellinenKenttaCell).toHaveAttribute(
      "data-testid",
      "virheellinenKentta",
    )
    await expect(virheellinenArvoCell).toHaveAttribute(
      "data-testid",
      "virheellinenArvo",
    )
  })

  test("ratkaisuehdotus column is displayed properly", async ({
    db,
    kielitestiErrorPage,
    kotoSuoritusError,
  }) => {
    await kotoSuoritusError.insert(db, "virheEino")
    await kielitestiErrorPage.open()

    const errors = await kielitestiErrorPage.getErrorTableBody()
    const ratkaisuehdotusCell = errors
      .getByTestId("onrLisatietoja")
      .filter({ hasText: fixtureData.virheEino.onrLisatietoja! })
    await expect(ratkaisuehdotusCell).toHaveText(
      /Korjaa nimet lähdejärjestelmään näin:\s*Etunimet: Eino Test\s*Kutsumanimi: Eino\s*Sukunimi: Välimaa-Testi/,
    )
  })

  test("virheen lisätiedot avautuvat modaaliin ja sulkeutuvat", async ({
    db,
    page,
    kielitestiErrorPage,
    kotoSuoritusError,
  }) => {
    await kotoSuoritusError.insert(db, "virheLisatiedoilla")
    await kielitestiErrorPage.open()

    const errors = await kielitestiErrorPage.getErrorTableBody()
    const viestiCell = errors
      .getByTestId("viesti")
      .filter({ hasText: fixtureData.virheLisatiedoilla.viesti })
    const modal = page.getByRole("dialog")

    await expect(modal).toBeHidden()
    await viestiCell.getByRole("button", { name: "Näytä lisätiedot" }).click()

    await expect(modal).toBeVisible()
    await expect(modal).toContainText("Virheen lisätiedot")
    await expect(modal).toContainText("source: oppijanumerorekisteri")

    await modal.getByRole("button", { name: "Sulje" }).click()
    await expect(modal).toBeHidden()
  })

  test("eri henkilön nimiä ei näytetä lihavoituna korjausohjeena", async ({
    db,
    kielitestiErrorPage,
    kotoSuoritusError,
  }) => {
    await kotoSuoritusError.insert(db, "virheEino")
    await kotoSuoritusError.insert(db, "virheVieraallaNimella")
    await kielitestiErrorPage.open()

    const errors = await kielitestiErrorPage.getErrorTableBody()
    const vierasNimi = errors
      .getByTestId("onrLisatietoja")
      .filter({ hasText: fixtureData.virheVieraallaNimella.onrLisatietoja! })
    const vierasNimiosio = vierasNimi.locator(".ratkaisuehdotus-nimet")
    await expect(vierasNimiosio).toHaveClass(/ei-vastaa/)
    await expect(vierasNimiosio).toHaveCSS("font-weight", "400")
    await expect(vierasNimiosio).toContainText("Sukunimi: Ahtisaari")

    const korjausehdotus = errors
      .getByTestId("onrLisatietoja")
      .filter({ hasText: fixtureData.virheEino.onrLisatietoja! })
      .locator(".ratkaisuehdotus-nimet")
    await expect(korjausehdotus).not.toHaveClass(/ei-vastaa/)
    await expect(korjausehdotus).toHaveCSS("font-weight", "700")
  })

  test("koto suoritukset error page handles null values in error properly", async ({
    page,
    kielitestiErrorPage,
    db,
    kotoSuoritusError,
  }) => {
    await kotoSuoritusError.insert(db, "withNullValues")
    await kielitestiErrorPage.open()

    expect(page.url()).toContain(kielitestiErrorPage.url)

    const errors = await kielitestiErrorPage.getErrorRows()
    expect(errors).toHaveLength(2)
  })

  describe("Sorting", () => {
    const sortTestCases = [
      {
        column: "Henkilötunnus",
        tableColumnIndex: 0,
        order: ["010866-9260", "010180-9026", "010116A9518"],
      },
      {
        column: "Nimi",
        tableColumnIndex: 1,
        order: [
          "Etunimet: Ranja Testi Kutsumanimi: Ranja Sukunimi: Öhman-Testi",
          "Etunimet: Petro Testi Kutsumanimi: Petro Sukunimi: Kivinen-Testi",
          "Etunimet: Magdalena Testi Kutsumanimi: Magdalena Sukunimi: Sallinen-Testi",
        ],
      },
      {
        column: "Organisaatio",
        tableColumnIndex: 2,
        order: [
          "Vallilan ala-aste\n1.2.246.562.10.59904379811",
          "Jyväskylän yliopisto, Soveltavan kielentutkimuksen keskus\n1.2.246.562.10.14893989377",
          "Jyväskylän yliopisto, Soveltavan kielentutkimuksen keskus\n1.2.246.562.10.14893989377",
        ],
      },
      {
        column: "Opettajan sähköpostiosoite",
        tableColumnIndex: 3,
        order: [
          "yksi-opettajista@testi.oph.fi",
          "toinen-opettaja@testi.oph.fi",
          "opettaja@testi.oph.fi",
        ],
      },
      {
        column: "Virheen luontiaika",
        tableColumnIndex: 4,
        order: [
          toFinnishDateTime("2024-11-22T10:49:49Z", true),
          toFinnishDateTime("2025-05-26T12:34:56Z", true),
          toFinnishDateTime("2042-12-22T22:42:42Z", true),
        ],
      },
      {
        column: "Virheviesti",
        tableColumnIndex: 6,
        order: [
          'Unexpectedly missing quiz grade "puhuminen" on course "Integraatio testaus" for user "1"',
          "testiviesti, ei tekstiviesti",
          'Malformed quiz grade "kirjoittaminen" on course "Integraatio testaus" for user "2"',
        ],
      },
      {
        column: "Ratkaisuehdotus",
        tableColumnIndex: 7,
        order: ["", ""],
      },
      {
        column: "Virheellinen kenttä",
        tableColumnIndex: 8,
        order: ["yksi niistä", "puhuminen", "kirjoittaminen"],
      },
      {
        column: "Virheellinen arvo",
        tableColumnIndex: 9,
        order: ["virheellinen arvosana", "tyhjää täynnä", "en kerro, arvaa!"],
      },
    ] as const
    for (const testCase of sortTestCases) {
      const { column, tableColumnIndex, order } = testCase
      const reverseOrder = [...order].reverse()

      test(`registry data can be sorted by "${column}"`, async ({
        kielitestiErrorPage: page,
        kotoSuoritusError,
        db,
      }) => {
        await kotoSuoritusError.insert(db, "virheMagdalena")
        await kotoSuoritusError.insert(db, "virhePetro")

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

  test("should download koto-errors CSV and verify its content", async ({
    page,
    kielitestiErrorPage,
  }) => {
    await kielitestiErrorPage.open()

    // Intercept the download
    const [download] = await Promise.all([
      page.waitForEvent("download"),
      kielitestiErrorPage.getCSVDownloadLink().click(),
    ])

    // Save the file to a temporary location
    const path = await download.path()
    expect(path).not.toBeNull()

    const csvContent = await fs.readFile(path!, "utf8")
    let headers =
      "virheenLuontiaika;suorittajanOid;hetu;nimi;etunimet;sukunimi;kutsumanimi;schoolOid;teacherEmail;viesti;lisatietoja;onrLisatietoja;onrEtunimet;onrKutsumanimi;onrSukunimi;virheellinenKentta;virheellinenArvo"
    let ranjaError =
      '"2024-11-22T10:49:49Z";"1.2.246.562.24.20281155246";"010180-9026";"Ranja Testi Öhman-Testi";"Ranja Testi";"Öhman-Testi";Ranja;"1.2.246.562.10.14893989377";"opettaja@testi.oph.fi";"Unexpectedly missing quiz grade ""puhuminen"" on course ""Integraatio testaus"" for user ""1""";;;;;;puhuminen;"virheellinen arvosana"'
    expect(csvContent).toContain(headers)
    expect(csvContent).toContain(ranjaError)
  })
})
