import { beforeEach, describe, test } from "../../fixtures/baseFixture"
import { expect } from "@playwright/test"
import { todayISODate } from "../../util/time"

/** opintopolku-mocks/ataru/arvioijahakemukset.json */
const PETRON_HAKEMUS = "1.2.246.562.11.00000000000000000001"
const YKSILOIMATON_HAKEMUS = "1.2.246.562.11.00000000000000000002"
const ASHA = "OPH-1234-2026"

/** Fixture-Petron kausi päättyy 2029-09-01; sen jälkeen ehdotus on tämä päivä. */
const PETRON_JATKOKAUDEN_ALKU =
  todayISODate() <= "2029-09-01" ? "2029-09-02" : todayISODate()

describe("Yleisen kielitutkinnon arvioijahakemusten tarkistus", () => {
  beforeEach(async ({ db }) => {
    await db.withEmptyDatabase()
  })

  test("haku tuo hakemukset tarkistettaviksi luomatta arvioijia", async ({
    indexPage,
    ykiArvioijahakemuksetPage,
    ykiArvioijatPage,
  }) => {
    await indexPage.login()
    await ykiArvioijahakemuksetPage.open()
    await expect(ykiArvioijahakemuksetPage.eiTarkistettavia).toBeVisible()

    await ykiArvioijahakemuksetPage.haeUudet()

    await expect(ykiArvioijahakemuksetPage.viewMessage).toContainText(
      "Hakemukset haettu",
    )
    const rivi = ykiArvioijahakemuksetPage.tarkistettava(PETRON_HAKEMUS)
    await expect(rivi).toContainText("Kivinen-Testi, Petro Testi")
    await expect(rivi).toContainText("suomi: PT, KT")
    await expect(rivi).toContainText("ruotsi: YT")
    await expect(rivi.getByTestId("maksunTila")).toHaveText(
      "Maksettu 3.10.2026",
    )
    await expect(
      ykiArvioijahakemuksetPage.keskeneraiset.getByTestId(YKSILOIMATON_HAKEMUS),
    ).toContainText("Odottaa yksilöintiä")
    await expect(
      ykiArvioijahakemuksetPage.page.locator(
        'a[href*="/yki/arvioijat/hakemukset/virheet"]',
      ),
    ).toBeVisible()

    await ykiArvioijatPage.open()
    await expect(ykiArvioijatPage.table.rows).toHaveCount(0)
  })

  test("vahvistus vaatii ASHA-numeron ja tallentaa arvioijan virkailijan alkupäivällä", async ({
    indexPage,
    ykiArvioijahakemuksetPage,
    ykiArvioijaLomakePage,
  }) => {
    await indexPage.login()
    await ykiArvioijahakemuksetPage.open()
    await ykiArvioijahakemuksetPage.haeUudet()
    await ykiArvioijahakemuksetPage.tarkista(PETRON_HAKEMUS)

    await ykiArvioijaLomakePage.expectLomakeVisible()
    await expect(
      ykiArvioijaLomakePage.page.getByTestId("maksunTila"),
    ).toHaveText("Maksettu 3.10.2026")
    await expect(
      ykiArvioijaLomakePage.page.getByText(
        "Päätösmaksua ei ole kirjattu maksetuksi",
      ),
    ).toHaveCount(0)
    await expect(ykiArvioijaLomakePage.field("sahkopostiosoite")).toHaveValue(
      "kivinen-testi@oph.fi",
    )
    await expect(ykiArvioijaLomakePage.field("katuosoite")).toHaveValue(
      "Kivinenkatu 2 A 3",
    )
    await expect(ykiArvioijaLomakePage.field("kaudenAlkupaiva")).toHaveValue(
      todayISODate(),
    )
    await expect(ykiArvioijaLomakePage.field("ashaNumero")).toHaveValue("")
    await expect(
      ykiArvioijaLomakePage.arviointioikeus("FIN", "PT"),
    ).toBeChecked()
    await expect(
      ykiArvioijaLomakePage.arviointioikeus("FIN", "KT"),
    ).toBeChecked()
    await expect(
      ykiArvioijaLomakePage.arviointioikeus("SWE", "YT"),
    ).toBeChecked()

    await ykiArvioijaLomakePage.tallenna()
    await expect(ykiArvioijaLomakePage.fieldError("ashaNumero")).toHaveText(
      "Hallintopäätöksen ASHA-numero on pakollinen tieto",
    )

    await ykiArvioijaLomakePage.asetaKaudenAlkupaiva("2026-10-01")
    await ykiArvioijaLomakePage.field("ashaNumero").fill(ASHA)
    await ykiArvioijaLomakePage.tallenna()

    await expect(
      ykiArvioijaLomakePage
        .getPageContent()
        .getByRole("heading", { name: "Petro Testi Kivinen-Testi" }),
    ).toBeVisible()
    await expect(ykiArvioijaLomakePage.viewMessage).toContainText(
      "tallennettiin",
    )
    const kaudet = ykiArvioijaLomakePage.page.getByTestId("arviointikaudet")
    await expect(kaudet).toContainText("1.10.2026")
    await expect(kaudet).toContainText(ASHA)

    await ykiArvioijahakemuksetPage.open()
    await expect(ykiArvioijahakemuksetPage.eiTarkistettavia).toBeVisible()
    await ykiArvioijahakemuksetPage.haeUudet()
    await expect(ykiArvioijahakemuksetPage.eiTarkistettavia).toBeVisible()
  })

  test("jatkokausi ehdotetaan alkamaan voimassa olevan kauden jälkeen", async ({
    db,
    indexPage,
    ykiArvioija,
    ykiArvioijahakemuksetPage,
    ykiArvioijaLomakePage,
  }) => {
    await ykiArvioija.insert(db, "petro")
    await indexPage.login()
    await ykiArvioijahakemuksetPage.open()
    await ykiArvioijahakemuksetPage.haeUudet()
    await expect(
      ykiArvioijahakemuksetPage.tarkistettava(PETRON_HAKEMUS),
    ).toContainText("Jo rekisterissä")
    await ykiArvioijahakemuksetPage.tarkista(PETRON_HAKEMUS)

    await expect(ykiArvioijaLomakePage.getPageContent()).toContainText(
      "Arvioija on jo rekisterissä",
    )
    await expect(ykiArvioijaLomakePage.field("kaudenAlkupaiva")).toHaveValue(
      PETRON_JATKOKAUDEN_ALKU,
    )

    await ykiArvioijaLomakePage.field("ashaNumero").fill(ASHA)
    await ykiArvioijaLomakePage.tallenna()

    await expect(ykiArvioijaLomakePage.viewMessage).toContainText(
      "tallennettiin",
    )
    const kaudet = ykiArvioijaLomakePage.page.getByTestId("arviointikaudet")
    await expect(kaudet).toContainText("1.9.2024")
    await expect(kaudet).toContainText(
      new Intl.DateTimeFormat("fi-FI").format(
        new Date(PETRON_JATKOKAUDEN_ALKU),
      ),
    )
    await expect(kaudet).toContainText(ASHA)
  })
})
