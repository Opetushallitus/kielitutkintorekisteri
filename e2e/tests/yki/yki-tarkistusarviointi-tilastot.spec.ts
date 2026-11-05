import { beforeEach, describe, expect, test } from "../../fixtures/baseFixture"
import { expectToHaveTexts } from "../../util/expect"

describe("YKI-tarkistusarviointien tilastot", () => {
  beforeEach(async ({ db, oauth, ykiSuoritus, basePage }) => {
    await db.withEmptyDatabase()
    await ykiSuoritus.insert(oauth, "ranjaTarkistus")
    await ykiSuoritus.insert(oauth, "petro")
    await ykiSuoritus.insert(oauth, "magdalenaTarkistettu")
    await ykiSuoritus.insertApprovedBeforeFeature(oauth, db, "einoTarkistettu")
    await basePage.login()
  })

  test("Hyväksyttyjen sivulta avataan hyväksyttyjen tilastot ja palataan takaisin", async ({
    ykiHyvaksytytTarkistusarvioinnitPage,
    ykiTarkistusarviointiTilastotPage: tilastot,
  }) => {
    await ykiHyvaksytytTarkistusarvioinnitPage.open()
    await ykiHyvaksytytTarkistusarvioinnitPage
      .getPageContent()
      .getByTestId("tilastot-linkki")
      .click()

    await expect(tilastot.numberOfRows).toHaveText("1")
    await expectToHaveTexts(
      tilastot.luvut("Tarkistusarvioinnit"),
      "1",
      "1",
      "0",
    )
    await expectToHaveTexts(tilastot.luvut("Osakokeet yhteensä"), "1", "1", "0")
    await expectToHaveTexts(tilastot.luvut("Puhuminen"), "1", "1", "0")
    await expectToHaveTexts(tilastot.luvut("Kirjoittaminen"), "0", "0", "0")
    await expect(tilastot.getPageContent()).toContainText("Tila: Hyväksytty")

    await tilastot.takaisin.click()
    await expect(
      ykiHyvaksytytTarkistusarvioinnitPage.hyvaksytytTable.getCellsOfColumn(
        "etunimet",
      ),
    ).toHaveText("Eino Testi")
  })

  test("Odottavien sivulta avataan odottavien tilastot", async ({
    ykiTarkistusarvioinnitPage,
    ykiTarkistusarviointiTilastotPage: tilastot,
  }) => {
    await ykiTarkistusarvioinnitPage.open()
    await ykiTarkistusarvioinnitPage
      .getPageContent()
      .getByTestId("tilastot-linkki")
      .click()

    await expect(tilastot.numberOfRows).toHaveText("2")
    await expectToHaveTexts(
      tilastot.luvut("Tarkistusarvioinnit"),
      "2",
      "2",
      "0",
    )
    await expect(tilastot.getPageContent()).toContainText(
      "Tila: Odottaa hyväksyntää",
    )
  })

  test("Tilarajauksen voi vaihtaa dialogissa", async ({
    ykiTarkistusarviointiTilastotPage: tilastot,
  }) => {
    await tilastot.open("?tila=Hyvaksytty")
    await expect(tilastot.numberOfRows).toHaveText("1")

    let dialog = await tilastot.openFilterDialog()
    await dialog.setTila(["OdottaaHyvaksyntaa"])
    await dialog.submit()
    await expect(tilastot.numberOfRows).toHaveText("2")

    dialog = await tilastot.openFilterDialog()
    await dialog.setTila([])
    await dialog.submit()
    await expect(tilastot.numberOfRows).toHaveText("3")
    await expect(tilastot.getPageContent()).not.toContainText("Tila:")
  })

  test("Ryhmittely toistaa rivit ryhmittäin ja säilyy rajattaessa", async ({
    ykiTarkistusarviointiTilastotPage: tilastot,
  }) => {
    await tilastot.open()
    await expect(tilastot.rivit).toHaveCount(6)

    await tilastot.setRyhmittely("Ei", ["tutkintotaso"])
    await expectToHaveTexts(
      tilastot.rivit
        .getByTestId("Tutkintotaso")
        .and(tilastot.page.locator("td"))
        .filter({ hasText: "Ylin taso" }),
      "Ylin taso",
      "Ylin taso",
      "Ylin taso",
      "Ylin taso",
      "Ylin taso",
      "Ylin taso",
    )
    await expect(tilastot.rivit).toHaveCount(18)

    const dialog = await tilastot.openFilterDialog()
    await dialog.setTila(["Hyvaksytty"])
    await dialog.submit()
    await expect(tilastot.rivit).toHaveCount(6)
    await expect(
      tilastot.getPageContent().getByTestId("ryhmittely-tutkintotaso"),
    ).toBeChecked()
  })

  test("Tilastot voi ladata CSV:nä", async ({
    ykiTarkistusarviointiTilastotPage: tilastot,
  }) => {
    await tilastot.open("?tila=Hyvaksytty")

    expect(await tilastot.downloadCSV()).toEqual([
      "Kohde;Yhteensä;Arvosana muuttui;Ei muuttunut",
      "Tarkistusarvioinnit;1;1;0",
      "Osakokeet yhteensä;1;1;0",
      "Puhuminen;1;1;0",
      "Kirjoittaminen;0;0;0",
      "Tekstin ymmärtäminen;0;0;0",
      "Puheen ymmärtäminen;0;0;0",
    ])
  })
})
