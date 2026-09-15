import { beforeEach, describe, test } from "../../fixtures/baseFixture"
import { expect } from "@playwright/test"

const PETRO = "1.2.246.562.24.59267607404"

/** Kauden alkupäivä ei saa olla yli vuotta tulevaisuudessa, joten päivät johdetaan tästä päivästä. */
const isoPaiva = (vuosiaSitten: number) => {
  const d = new Date()
  d.setFullYear(d.getFullYear() - vuosiaSitten)
  return d.toISOString().slice(0, 10)
}

const fiPaiva = (iso: string) =>
  new Intl.DateTimeFormat("fi-FI").format(new Date(iso))

describe("Yleisen kielitutkinnon arvioijan muokkaus", () => {
  beforeEach(async ({ db }) => {
    await db.withEmptyDatabase()
  })

  test("arvioijan tiedot avataan muokattavaksi napista ja muutos tallentuu", async ({
    indexPage,
    ykiArvioijaLomakePage,
    ykiArvioijatPage,
  }) => {
    await indexPage.login()

    // Luodaan arvioija, jotta muokattavaa on
    await ykiArvioijaLomakePage.open()
    await ykiArvioijaLomakePage.haeOppijanumerolla(PETRO)
    await ykiArvioijaLomakePage.asetaKaudenAlkupaiva("2025-12-07")
    await ykiArvioijaLomakePage.valitseArviointioikeus("FIN", "PT")
    await ykiArvioijaLomakePage.tallenna()

    // Tietosivulta muokkaustilaan
    await ykiArvioijaLomakePage.page.getByTestId("muokkaaArvioijaa").click()

    await expect(ykiArvioijaLomakePage.field("sukunimi")).toHaveValue(
      "Kivinen-Testi",
    )

    await ykiArvioijaLomakePage.field("postitoimipaikka").fill("TAMPERE")
    await ykiArvioijaLomakePage.tallenna()

    await expect(ykiArvioijaLomakePage.viewMessage).toContainText(
      "päivitettiin",
    )

    await ykiArvioijatPage.open()
    await expect(ykiArvioijatPage.table.rows).toHaveCount(1)

    await ykiArvioijaLomakePage.page.goBack()
    await ykiArvioijaLomakePage.page
      .getByTestId("muokkaaArvioijaa")
      .first()
      .click()
    await expect(ykiArvioijaLomakePage.field("postitoimipaikka")).toHaveValue(
      "TAMPERE",
    )
  })

  test("arviointikausi lisätään omalla lomakkeellaan ja passivointi päättää merkinnän", async ({
    indexPage,
    ykiArvioijaLomakePage,
  }) => {
    const page = ykiArvioijaLomakePage.page
    await indexPage.login()

    // Päättynyt kausi, jotta uusi mahtuu sen perään menemättä päällekkäin.
    await ykiArvioijaLomakePage.open()
    await ykiArvioijaLomakePage.haeOppijanumerolla(PETRO)
    await ykiArvioijaLomakePage.asetaKaudenAlkupaiva(isoPaiva(8))
    await ykiArvioijaLomakePage.valitseArviointioikeus("FIN", "PT")
    await ykiArvioijaLomakePage.tallenna()

    const kaudet = page.getByTestId("arviointikaudet")
    await expect(kaudet).toContainText(fiPaiva(isoPaiva(8)))

    // Uusi kausi lisätään kausilomakkeelta, ei arvioijan muokkauslomakkeelta.
    await page.getByTestId("uusiKausi").click()
    await page.getByTestId("alkupaiva-input").fill(isoPaiva(1))
    await page.getByTestId("arviointioikeus-FIN:PT").check()
    await page.getByTestId("tallennaKausi").click()

    await expect(ykiArvioijaLomakePage.viewMessage).toContainText("lisättiin")
    await expect(kaudet).toContainText(fiPaiva(isoPaiva(8)))
    await expect(kaudet).toContainText(fiPaiva(isoPaiva(1)))

    // Muutosloki on oletuksena kiinni ja avautuu details-elementistä.
    await page.getByTestId("naytaMuutoshistoria").click()
    await expect(page.getByTestId("kausihistoria")).toContainText(
      fiPaiva(isoPaiva(1)),
    )

    // Passivointi vaatii vahvistuksen dialogissa
    await page.getByTestId("passivoiArvioija").click()
    await expect(page.getByTestId("passivoiArvioijaDialog")).toBeVisible()
    await page.getByTestId("vahvistaPassivointi").click()

    await expect(ykiArvioijaLomakePage.viewMessage).toContainText("passiivise")
    // Nappi jää näkyviin estettynä, jotta toiminnon olemassaolo ja eston syy näkyvät sivulta.
    await expect(page.getByTestId("passivoiArvioija")).toHaveAttribute(
      "aria-disabled",
      "true",
    )
    await expect(page.getByTestId("passivoiArvioija")).toHaveAttribute(
      "data-tooltip",
      /merkitty passiiviseksi/,
    )
  })

  test("passivoinnin vahvistusdialogin voi sulkea peruuttamalla", async ({
    indexPage,
    ykiArvioijaLomakePage,
  }) => {
    const page = ykiArvioijaLomakePage.page
    await indexPage.login()

    await ykiArvioijaLomakePage.open()
    await ykiArvioijaLomakePage.haeOppijanumerolla(PETRO)
    await ykiArvioijaLomakePage.asetaKaudenAlkupaiva(isoPaiva(1))
    await ykiArvioijaLomakePage.valitseArviointioikeus("FIN", "PT")
    await ykiArvioijaLomakePage.tallenna()

    const dialogi = page.getByTestId("passivoiArvioijaDialog")
    await page.getByTestId("passivoiArvioija").click()
    await expect(dialogi).toBeVisible()

    await page.getByRole("button", { name: "Peruuta" }).click()

    // Peruutus sulkee dialogin eikä passivoi: nappi jää käytettäväksi.
    await expect(dialogi).toBeHidden()
    await expect(page.getByTestId("passivoiArvioija")).not.toHaveAttribute(
      "aria-disabled",
      "true",
    )
  })

  test("muokkauksen voi peruuttaa palaamatta tallentamiseen", async ({
    indexPage,
    ykiArvioijaLomakePage,
  }) => {
    await indexPage.login()

    await ykiArvioijaLomakePage.open()
    await ykiArvioijaLomakePage.haeOppijanumerolla(PETRO)
    await ykiArvioijaLomakePage.asetaKaudenAlkupaiva("2025-12-07")
    await ykiArvioijaLomakePage.valitseArviointioikeus("FIN", "PT")
    await ykiArvioijaLomakePage.tallenna()

    await ykiArvioijaLomakePage.page.getByTestId("muokkaaArvioijaa").click()
    await ykiArvioijaLomakePage.field("postitoimipaikka").fill("EI TALLENNETA")
    await ykiArvioijaLomakePage.page.getByTestId("peruutaMuokkaus").click()

    await expect(
      ykiArvioijaLomakePage.page.getByTestId("muokkaaArvioijaa"),
    ).toBeVisible()
    await expect(
      ykiArvioijaLomakePage.getPageContent().getByText("EI TALLENNETA"),
    ).toHaveCount(0)
  })

  test("lukuoikeudella ei ole muokkausnappia", async ({
    indexPage,
    ykiArvioijaLomakePage,
    ykiArvioijatPage,
  }) => {
    await indexPage.login()
    await ykiArvioijaLomakePage.open()
    await ykiArvioijaLomakePage.haeOppijanumerolla(PETRO)
    await ykiArvioijaLomakePage.asetaKaudenAlkupaiva("2025-12-07")
    await ykiArvioijaLomakePage.valitseArviointioikeus("FIN", "PT")
    await ykiArvioijaLomakePage.tallenna()

    await indexPage.login("VIRKAILIJA")
    await ykiArvioijatPage.open()
    await ykiArvioijatPage.table.rows
      .first()
      .getByTestId("Linkki")
      .getByRole("link")
      .click()

    await expect(
      ykiArvioijatPage.getPageContent().getByTestId("muokkaaArvioijaa"),
    ).toHaveCount(0)
  })
})
