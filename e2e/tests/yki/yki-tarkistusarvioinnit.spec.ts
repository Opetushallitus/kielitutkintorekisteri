import { beforeEach, describe, expect, test } from "../../fixtures/baseFixture"
import { expectToHaveTexts } from "../../util/expect"

describe("YKI-tarkastusarvioinnit", () => {
  beforeEach(
    async ({
      db,
      oauth,
      ykiSuoritus,
      basePage,
      ykiTarkistusarvioinnitPage,
    }) => {
      await db.withEmptyDatabase()
      await ykiSuoritus.insert(oauth, "ranjaTarkistus")
      await ykiSuoritus.insert(oauth, "petro")
      await ykiSuoritus.insert(oauth, "magdalenaTarkistettu")
      await ykiSuoritus.insertApprovedBeforeFeature(
        oauth,
        db,
        "einoTarkistettu",
      )
      await basePage.login()
      await ykiTarkistusarvioinnitPage.open()
    },
  )

  test("Tarkistusarviointinäkymä näyttää vain tarkistusarvioitavan henkilön", async ({
    ykiTarkistusarvioinnitPage,
    ykiHyvaksytytTarkistusarvioinnitPage,
  }) => {
    await expectToHaveTexts(
      ykiTarkistusarvioinnitPage.odottaaTable.getCellsOfColumn("etunimet"),
      "Magdalena Testi",
      "Ranja Testi",
    )

    await ykiHyvaksytytTarkistusarvioinnitPage.open()
    await expectToHaveTexts(
      ykiHyvaksytytTarkistusarvioinnitPage.hyvaksytytTable.getCellsOfColumn(
        "etunimet",
      ),
      "Eino Testi",
    )
  })

  test("Oppijanumero näkyy molemmissa taulukoissa", async ({
    ykiTarkistusarvioinnitPage,
    ykiHyvaksytytTarkistusarvioinnitPage,
  }) => {
    await expectToHaveTexts(
      ykiTarkistusarvioinnitPage.odottaaTable.getCellsOfColumn(
        "suorittajanoid",
      ),
      "1.2.246.562.24.33342764709",
      "1.2.246.562.24.20281155246",
    )

    await ykiHyvaksytytTarkistusarvioinnitPage.open()
    await expectToHaveTexts(
      ykiHyvaksytytTarkistusarvioinnitPage.hyvaksytytTable.getCellsOfColumn(
        "suorittajanoid",
      ),
      "1.2.246.562.24.67409348034",
    )
  })

  test("Hyväksytyt-painike vie hyväksyttyjen tarkistusarviointien sivulle", async ({
    ykiTarkistusarvioinnitPage,
    ykiHyvaksytytTarkistusarvioinnitPage,
  }) => {
    await ykiTarkistusarvioinnitPage.openHyvaksytyt()
    await expectToHaveTexts(
      ykiHyvaksytytTarkistusarvioinnitPage.hyvaksytytTable.getCellsOfColumn(
        "etunimet",
      ),
      "Eino Testi",
    )
  })

  test("Tarkistusarvioinnin hyväksyminen siirtää sen seuraavaan taulukkoon", async ({
    ykiTarkistusarvioinnitPage,
    ykiHyvaksytytTarkistusarvioinnitPage,
  }) => {
    await ykiTarkistusarvioinnitPage.odottaaTable.body
      .getByRole("row", { name: "Sallinen-Testi Magdalena" })
      .getByRole("checkbox")
      .setChecked(true)
    await ykiTarkistusarvioinnitPage.odottaaDate.fill("2025-11-11")
    await ykiTarkistusarvioinnitPage.odottaaSubmit.click()
    await expect(ykiTarkistusarvioinnitPage.viewMessage).toHaveText(
      "1 tarkistusarviointi merkitty hyväksytyksi",
    )

    await expectToHaveTexts(
      ykiTarkistusarvioinnitPage.odottaaTable.getCellsOfColumn("etunimet"),
      "Ranja Testi",
    )

    await ykiHyvaksytytTarkistusarvioinnitPage.open()
    await expectToHaveTexts(
      ykiHyvaksytytTarkistusarvioinnitPage.hyvaksytytTable.getCellsOfColumn(
        "etunimet",
      ),
      "Magdalena Testi",
      "Eino Testi",
    )
  })

  test("Liian varhainen hyväksymispäivä aiheuttaa virheilmoituksen", async ({
    ykiTarkistusarvioinnitPage,
  }) => {
    await ykiTarkistusarvioinnitPage.odottaaTable.body
      .getByRole("row", { name: "Sallinen-Testi Magdalena" })
      .getByRole("checkbox")
      .setChecked(true)
    await ykiTarkistusarvioinnitPage.odottaaDate.fill("2025-10-10")
    await ykiTarkistusarvioinnitPage.odottaaSubmit.click()

    await expect(ykiTarkistusarvioinnitPage.viewMessage).toContainText(
      "arkistusarviointi suoritukselle '1.2.246.562.24.33342764709 Sallinen-Testi Magdalena Testi, PT FIN' ei voi hyväksyä päivämäärällä 10.10.2025, koska se on aiemmin kuin käsittelypäivä 22.10.2025.",
    )
  })

  test("Jo hyväksyttyjä tarkistusarvioita voi päivittää", async ({
    ykiHyvaksytytTarkistusarvioinnitPage,
  }) => {
    await ykiHyvaksytytTarkistusarvioinnitPage.open()
    const table = ykiHyvaksytytTarkistusarvioinnitPage.hyvaksytytTable
    await expect(table.getCellsOfColumn("kasitelty").first()).toHaveText(
      "20.10.2024",
    )
    await expect(table.getCellsOfColumn("hyvaksytty").first()).toHaveText("")

    await ykiHyvaksytytTarkistusarvioinnitPage.hyvaksytytTable.body
      .getByRole("row")
      .first()
      .getByRole("checkbox")
      .setChecked(true)
    await ykiHyvaksytytTarkistusarvioinnitPage.hyvaksytytDate.fill("2025-11-20")
    await ykiHyvaksytytTarkistusarvioinnitPage.hyvaksytytSubmit.click()
    await expect(ykiHyvaksytytTarkistusarvioinnitPage.viewMessage).toHaveText(
      "1 tarkistusarviointi merkitty hyväksytyksi",
    )

    await expect(table.getCellsOfColumn("hyvaksytty").first()).toHaveText(
      "20.11.2025",
    )
  })

  test("Mitään ei tapahdu, jos rakseja ei ole valittu ja painaa submit", async ({
    ykiTarkistusarvioinnitPage,
  }) => {
    await expectToHaveTexts(
      ykiTarkistusarvioinnitPage.odottaaTable.getCellsOfColumn("etunimet"),
      "Magdalena Testi",
      "Ranja Testi",
    )

    await ykiTarkistusarvioinnitPage.odottaaSubmit.click()
    await expect(ykiTarkistusarvioinnitPage.viewMessage).not.toBeVisible()

    await expectToHaveTexts(
      ykiTarkistusarvioinnitPage.odottaaTable.getCellsOfColumn("etunimet"),
      "Magdalena Testi",
      "Ranja Testi",
    )
  })

  test("Hakusana rajaa taulukon ja säilyy järjestettäessä", async ({
    ykiTarkistusarvioinnitPage,
  }) => {
    const etunimet =
      ykiTarkistusarvioinnitPage.odottaaTable.getCellsOfColumn("etunimet")

    await ykiTarkistusarvioinnitPage.search("OPH-14893989377-2")
    await expectToHaveTexts(etunimet, "Magdalena Testi")
    await expect(ykiTarkistusarvioinnitPage.numberOfRows).toHaveText("1")

    await ykiTarkistusarvioinnitPage.sortBy("Sukunimi")
    await expectToHaveTexts(etunimet, "Magdalena Testi")
    await expect(
      ykiTarkistusarvioinnitPage.getPageContent().getByTestId("search"),
    ).toHaveValue("OPH-14893989377-2")

    await ykiTarkistusarvioinnitPage.search("Ranja")
    await expectToHaveTexts(etunimet, "Ranja Testi")
  })

  test("Tyhjä hakutulos säilyttää haun ja rajauksen, mutta piilottaa hyväksyntälomakkeen", async ({
    ykiTarkistusarvioinnitPage,
  }) => {
    await ykiTarkistusarvioinnitPage.search("eiloydy")

    await expect(ykiTarkistusarvioinnitPage.numberOfRows).toHaveText("0")
    await expect(ykiTarkistusarvioinnitPage.odottaaSubmit).not.toBeVisible()
    await expect(
      ykiTarkistusarvioinnitPage.getPageContent().getByTestId("search"),
    ).toHaveValue("eiloydy")

    const dialog = await ykiTarkistusarvioinnitPage.openFilterDialog()
    await expect(dialog.modal).toBeVisible()
  })

  test("Rajausdialogi suodattaa tutkintotasolla ja käsittelypäivällä", async ({
    ykiTarkistusarvioinnitPage,
  }) => {
    const etunimet =
      ykiTarkistusarvioinnitPage.odottaaTable.getCellsOfColumn("etunimet")

    let dialog = await ykiTarkistusarvioinnitPage.openFilterDialog()
    await dialog.setTutkintotaso(["YT"])
    await dialog.submit()
    await expectToHaveTexts(etunimet, "Ranja Testi")
    await expect(ykiTarkistusarvioinnitPage.numberOfRows).toHaveText("1")

    dialog = await ykiTarkistusarvioinnitPage.openFilterDialog()
    await dialog.setTutkintotaso([])
    await dialog.setKasittelyaika("2025-10-22", "2025-10-22")
    await dialog.submit()
    await expectToHaveTexts(etunimet, "Magdalena Testi")
  })

  test("Sarakkeen otsikosta järjestetään, ja järjestys säilyy rajattaessa", async ({
    ykiTarkistusarvioinnitPage,
  }) => {
    const etunimet =
      ykiTarkistusarvioinnitPage.odottaaTable.getCellsOfColumn("etunimet")
    await expectToHaveTexts(etunimet, "Magdalena Testi", "Ranja Testi")

    await ykiTarkistusarvioinnitPage.sortBy("Käsitelty")
    await expectToHaveTexts(etunimet, "Ranja Testi", "Magdalena Testi")

    await ykiTarkistusarvioinnitPage.sortBy("Käsitelty")
    await expectToHaveTexts(etunimet, "Magdalena Testi", "Ranja Testi")

    await ykiTarkistusarvioinnitPage.sortBy("Taso")
    await expectToHaveTexts(etunimet, "Magdalena Testi", "Ranja Testi")
    await ykiTarkistusarvioinnitPage.sortBy("Taso")
    await expectToHaveTexts(etunimet, "Ranja Testi", "Magdalena Testi")

    const dialog = await ykiTarkistusarvioinnitPage.openFilterDialog()
    await dialog.setTutkintotaso(["PT", "YT"])
    await dialog.submit()
    await expectToHaveTexts(etunimet, "Ranja Testi", "Magdalena Testi")
  })

  test("Hyväksynnän jälkeen palataan samaan rajaukseen ja järjestykseen", async ({
    ykiTarkistusarvioinnitPage,
  }) => {
    const etunimet =
      ykiTarkistusarvioinnitPage.odottaaTable.getCellsOfColumn("etunimet")

    const dialog = await ykiTarkistusarvioinnitPage.openFilterDialog()
    await dialog.setTutkintotaso(["PT", "YT"])
    await dialog.submit()
    await ykiTarkistusarvioinnitPage.sortBy("Sukunimi")

    await ykiTarkistusarvioinnitPage.odottaaTable.body
      .getByRole("row", { name: "Sallinen-Testi Magdalena" })
      .getByRole("checkbox")
      .setChecked(true)
    await ykiTarkistusarvioinnitPage.odottaaDate.fill("2025-11-11")
    await ykiTarkistusarvioinnitPage.odottaaSubmit.click()

    await expect(ykiTarkistusarvioinnitPage.viewMessage).toHaveText(
      "1 tarkistusarviointi merkitty hyväksytyksi",
    )
    await expectToHaveTexts(etunimet, "Ranja Testi")
    const url = new URL(ykiTarkistusarvioinnitPage.page.url())
    expect(url.searchParams.get("tutkintotaso")).toBe("PT,YT")
    expect(url.searchParams.get("sortColumn")).toBe("sukunimi")
  })

  test("Hyväksyttyjä voi rajata ja järjestää hyväksymispäivällä", async ({
    ykiTarkistusarvioinnitPage,
    ykiHyvaksytytTarkistusarvioinnitPage,
  }) => {
    await ykiTarkistusarvioinnitPage.odottaaTable.body
      .getByRole("row", { name: "Sallinen-Testi Magdalena" })
      .getByRole("checkbox")
      .setChecked(true)
    await ykiTarkistusarvioinnitPage.odottaaDate.fill("2025-11-11")
    await ykiTarkistusarvioinnitPage.odottaaSubmit.click()
    await expect(ykiTarkistusarvioinnitPage.viewMessage).toBeVisible()

    await ykiHyvaksytytTarkistusarvioinnitPage.open()
    const etunimet =
      ykiHyvaksytytTarkistusarvioinnitPage.hyvaksytytTable.getCellsOfColumn(
        "etunimet",
      )
    await ykiHyvaksytytTarkistusarvioinnitPage.sortBy("Hyväksytty")
    await expectToHaveTexts(etunimet, "Magdalena Testi", "Eino Testi")

    const dialog = await ykiHyvaksytytTarkistusarvioinnitPage.openFilterDialog()
    await dialog.setHyvaksymisaika("2025-11-11", "2025-11-11")
    await dialog.submit()
    await expectToHaveTexts(etunimet, "Magdalena Testi")
  })

  test("Osat ja muuttuneet näkyvät omissa sarakkeissaan", async ({
    ykiTarkistusarvioinnitPage,
  }) => {
    const rivi = ykiTarkistusarvioinnitPage.odottaaTable.body.getByRole("row", {
      name: "Sallinen-Testi Magdalena",
    })

    await expectToHaveTexts(
      rivi.getByTestId("osat").getByRole("listitem"),
      "Puhuminen",
    )
    await expectToHaveTexts(
      rivi.getByTestId("muuttuneet").getByRole("listitem"),
      "Puhuminen",
    )
  })

  test("Perustelu avautuu info-painikkeesta eikä lähetä hyväksyntää", async ({
    ykiTarkistusarvioinnitPage,
  }) => {
    const rivi = ykiTarkistusarvioinnitPage.odottaaTable.body.getByRole("row", {
      name: "Sallinen-Testi Magdalena",
    })
    const valinta = rivi.getByRole("checkbox")
    await valinta.setChecked(true)

    await expect(rivi.getByRole("dialog")).not.toBeVisible()
    await rivi.getByRole("button", { name: "Näytä perustelu" }).click()

    const modaali = rivi.getByRole("dialog")
    await expect(modaali).toBeVisible()
    await expect(modaali).toContainText("Perustelu")
    await expect(modaali).toContainText("Tarkistusarvioinnin testi")

    await modaali.getByRole("button", { name: "Sulje" }).click()
    await expect(modaali).not.toBeVisible()

    await expect(valinta).toBeChecked()
    await expect(ykiTarkistusarvioinnitPage.viewMessage).not.toBeVisible()
    await expect(ykiTarkistusarvioinnitPage.numberOfRows).toHaveText("2")
  })
})
