import { beforeEach, describe, test } from "../../fixtures/baseFixture"
import { expect } from "@playwright/test"
import YkiArvioijaLomakePage from "../../models/yki/YkiArvioijaLomakePage"

const PETRO = "1.2.246.562.24.59267607404"

/**
 * Päivät johdetaan tästä päivästä, koska kauden alkupäivä ei saa olla yli vuotta
 * tulevaisuudessa eikä kausi saa mennä päällekkäin toisen kanssa.
 */
const isoPaiva = (vuosiaSitten: number) => {
  const d = new Date()
  d.setFullYear(d.getFullYear() - vuosiaSitten)
  return d.toISOString().slice(0, 10)
}

const fiPaiva = (iso: string) =>
  new Intl.DateTimeFormat("fi-FI").format(new Date(iso))

/**
 * Päättymispäivä on inklusiivinen, joten viiden vuoden kausi päättyy vuosipäivää
 * edeltävänä päivänä. Laskenta pysyy UTC:ssä, jottei kesäaika siirrä päivää, ja
 * karkauspäivä leikataan kuukauden viimeiseen kuten palvelimen `plusYears`.
 */
const paattymispaiva = (alkupaiva: string) => {
  const [vuosi, kuukausi, paiva] = alkupaiva.split("-").map(Number)
  const kuunViimeinen = new Date(Date.UTC(vuosi + 5, kuukausi, 0)).getUTCDate()
  const d = new Date(
    Date.UTC(vuosi + 5, kuukausi - 1, Math.min(paiva, kuunViimeinen)),
  )
  d.setUTCDate(d.getUTCDate() - 1)
  return d.toISOString().slice(0, 10)
}

const TANAAN = new Intl.DateTimeFormat("fi-FI").format(new Date())

/** Dialogeja on yksi per kausirivi, joten vahvistusnappi haetaan avoimen dialogin sisältä. */
const avoinDialogi = (page: import("@playwright/test").Page) =>
  page.locator("dialog[open]")

describe("Yleisen kielitutkinnon arvioijan arviointikaudet", () => {
  beforeEach(async ({ db }) => {
    await db.withEmptyDatabase()
  })

  const luoArvioija = async (
    ykiArvioijaLomakePage: YkiArvioijaLomakePage,
    alkupaiva: string,
  ) => {
    await ykiArvioijaLomakePage.open()
    await ykiArvioijaLomakePage.haeOppijanumerolla(PETRO)
    await ykiArvioijaLomakePage.asetaKaudenAlkupaiva(alkupaiva)
    await ykiArvioijaLomakePage.valitseArviointioikeus("FIN", "PT")
    await ykiArvioijaLomakePage.tallenna()
  }

  test("kauden alkupäivän muokkaus siirtää myös päättymispäivää", async ({
    indexPage,
    ykiArvioijaLomakePage,
  }) => {
    const page = ykiArvioijaLomakePage.page
    await indexPage.login()
    await luoArvioija(ykiArvioijaLomakePage, isoPaiva(1))

    await page.getByTestId("muokkaaKautta").first().click()
    await page.getByTestId("alkupaiva-input").fill(isoPaiva(2))
    await page.getByTestId("tallennaKausi").click()

    await expect(ykiArvioijaLomakePage.viewMessage).toContainText(
      "päivitettiin",
    )
    const kaudet = page.getByTestId("arviointikaudet")
    await expect(kaudet).toContainText(fiPaiva(isoPaiva(2)))
    // Päättymispäivä lasketaan alkupäivästä, joten se siirtyy mukana.
    await expect(kaudet).toContainText(fiPaiva(paattymispaiva(isoPaiva(2))))
  })

  test("aktiivisen kauden voi passivoida", async ({
    indexPage,
    ykiArvioijaLomakePage,
  }) => {
    const page = ykiArvioijaLomakePage.page
    await indexPage.login()
    await luoArvioija(ykiArvioijaLomakePage, isoPaiva(1))

    await page.getByTestId("passivoiKausi").first().click()
    await avoinDialogi(page).getByTestId("vahvistaKaudenPassivointi").click()

    await expect(ykiArvioijaLomakePage.viewMessage).toContainText(
      "passivoitiin",
    )
    // Kausi päättyy tähän päivään. Päättymispäivä on inklusiivinen, joten merkintä
    // lukee vielä tänään aktiiviseksi ja muuttuu passiiviseksi vasta huomenna.
    await expect(page.getByTestId("arviointikaudet")).toContainText(TANAAN)
  })

  test("väärälle henkilölle kirjattu kausi poistetaan, viimeistä ei voi poistaa", async ({
    indexPage,
    ykiArvioijaLomakePage,
  }) => {
    const page = ykiArvioijaLomakePage.page
    await indexPage.login()
    // Päättynyt kausi, jotta uusi mahtuu sen perään menemättä päällekkäin.
    await luoArvioija(ykiArvioijaLomakePage, isoPaiva(8))

    // Ainoan kauden poistaminen jättäisi arvioijan ilman arviointioikeuksia.
    await expect(page.getByTestId("poistaKausi")).toHaveCount(0)

    await page.getByTestId("uusiKausi").click()
    await page.getByTestId("alkupaiva-input").fill(isoPaiva(1))
    await page.getByTestId("ashaNumero-input").fill("OPH-77-2026")
    await page.getByTestId("arviointioikeus-FIN:PT").check()
    await page.getByTestId("tallennaKausi").click()

    const kaudet = page.getByTestId("arviointikaudet")
    await expect(kaudet).toContainText(fiPaiva(isoPaiva(1)))
    // Hallintopäätöksen viite kuuluu kaudelle, joten se näkyy kauden rivillä.
    await expect(kaudet).toContainText("OPH-77-2026")

    await page.getByTestId("poistaKausi").first().click()
    await avoinDialogi(page).getByTestId("vahvistaKaudenPoisto").click()

    await expect(ykiArvioijaLomakePage.viewMessage).toContainText("poistettiin")
    await expect(kaudet).not.toContainText(fiPaiva(isoPaiva(1)))
    await expect(kaudet).toContainText(fiPaiva(isoPaiva(8)))
  })

  test("päällekkäistä kautta ei tallenneta", async ({
    indexPage,
    ykiArvioijaLomakePage,
  }) => {
    const page = ykiArvioijaLomakePage.page
    await indexPage.login()
    await luoArvioija(ykiArvioijaLomakePage, isoPaiva(1))

    await page.getByTestId("uusiKausi").click()
    await page.getByTestId("alkupaiva-input").fill(isoPaiva(0))
    await page.getByTestId("arviointioikeus-FIN:PT").check()
    await page.getByTestId("tallennaKausi").click()

    // Virhe kohdistuu alkupäivään, joten se näkyy kentän vieressä.
    await expect(page.getByTestId("alkupaiva-error")).toContainText(
      "päällekkäin",
    )
  })

  test("lukuoikeudella ei ole kausitoimintoja", async ({
    indexPage,
    ykiArvioijaLomakePage,
    ykiArvioijatPage,
  }) => {
    await indexPage.login()
    await luoArvioija(ykiArvioijaLomakePage, isoPaiva(1))

    await indexPage.login("VIRKAILIJA")
    await ykiArvioijatPage.open()
    await ykiArvioijatPage.table.rows
      .first()
      .getByTestId("Linkki")
      .getByRole("link")
      .click()

    const sivu = ykiArvioijatPage.getPageContent()
    await expect(sivu.getByTestId("uusiKausi")).toHaveCount(0)
    await expect(sivu.getByTestId("muokkaaKautta")).toHaveCount(0)
    await expect(sivu.getByTestId("arviointikaudet")).toBeVisible()
  })
})
