import { beforeEach, describe, test } from "../../fixtures/baseFixture"
import { expect } from "@playwright/test"

const PETRO = "1.2.246.562.24.59267607404"

describe("YKI-arvioijan Solki-lähetys", () => {
  beforeEach(async ({ db }) => {
    await db.withEmptyDatabase()
  })

  test("onnistunut lähetys näkyy tietosivulla", async ({
    indexPage,
    ykiArvioijaLomakePage,
  }) => {
    const page = ykiArvioijaLomakePage.page
    await indexPage.login()

    await ykiArvioijaLomakePage.open()
    await ykiArvioijaLomakePage.haeOppijanumerolla(PETRO)
    await ykiArvioijaLomakePage.asetaKaudenAlkupaiva("2026-06-01")
    await ykiArvioijaLomakePage.valitseArviointioikeus("FIN", "PT")
    await ykiArvioijaLomakePage.tallenna()

    // Dev-stubi vastaa 200, joten tallennuksen synkroninen yritys onnistuu heti.
    await expect(page.getByTestId("lahetaArvioijaSolkiin")).toBeVisible()
    await expect(
      ykiArvioijaLomakePage.getPageContent().getByText("Odottaa lähetystä"),
    ).toHaveCount(0)

    // Stubin palauttama tunnus tallentuu ja näkyy tietosivulla.
    await expect(
      ykiArvioijaLomakePage.getPageContent().getByText("A00001"),
    ).toBeVisible()
  })

  test("lähetysvirhe näkyy syineen ja lähetyksen voi yrittää uudelleen", async ({
    indexPage,
    ykiArvioijaLomakePage,
    ykiArvioijatPage,
    db,
  }) => {
    const page = ykiArvioijaLomakePage.page
    await indexPage.login()

    await ykiArvioijaLomakePage.open()
    await ykiArvioijaLomakePage.haeOppijanumerolla(PETRO)
    await ykiArvioijaLomakePage.asetaKaudenAlkupaiva("2026-06-01")
    await ykiArvioijaLomakePage.valitseArviointioikeus("FIN", "PT")
    await ykiArvioijaLomakePage.tallenna()

    // Virhe kirjataan riville suoraan, koska dev-stubin ohjaus vaatisi oman reitin per arvioija.
    await db.dbClient.query(
      `UPDATE yki_arvioija SET solki_lahetysvirhe = 'Unexpected error; oppijanumero: ${PETRO}',
         solki_lahetysyritykset = 2, solkiin_lahetetty = NULL`,
    )

    await page.reload()
    await expect(
      ykiArvioijaLomakePage.getPageContent().getByText("Unexpected error"),
    ).toBeVisible()

    await ykiArvioijatPage.open()
    await ykiArvioijatPage.table.rows
      .first()
      .getByTestId("Linkki")
      .getByRole("link")
      .click()
    await page.getByTestId("lahetaArvioijaSolkiin").click()

    // Stubi vastaa 200, joten uusinta onnistuu ja virhe katoaa.
    await expect(
      ykiArvioijaLomakePage.getPageContent().getByText("Unexpected error"),
    ).toHaveCount(0)
  })
})
