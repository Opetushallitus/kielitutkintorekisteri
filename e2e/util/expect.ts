import { expect, Locator } from "@playwright/test"

export const expectToHaveTexts = async (
  locator: Locator,
  ...texts: string[]
) => {
  // locator.all() ei odota renderöintiä, joten ilman tätä vertailu voi osua tyhjään DOMiin.
  await expect(locator).toHaveCount(texts.length)

  // Tehdään vertailu parhaiden käytäntöjen mukaan, jotta vältetään väpättävät testit...
  const locators = await locator.all()
  if (locators.length !== texts.length) {
    expect(await locator.allTextContents()).toEqual(texts)
  }
  try {
    const promises = locators.map(async (l, i) => {
      await expect(l).toHaveText(texts[i])
    })
    return await Promise.all(promises)
  } catch (_) {
    // Epäonnistutaan uudelleen hieman eri tavalla, koska tämä antaa mukavemman virhetulosteen.
    // Yksinään käytettynä tämä saattaa aiheuttaa väpättäviä testejä.
    expect(await locator.allTextContents()).toEqual(texts)
  }
}

export const testForEach = async (
  locator: Locator,
  ...tests: Array<(l: Locator) => Promise<void>>
) => {
  const locators = await locator.all()
  if (locators.length !== tests.length) {
    throw Error(`Expected ${tests.length} elements, got ${locators.length}`)
  }
  for (let i = 0; i < locators.length; i++) {
    await tests[i](locators[i])
  }
}

export const testForEachTestId = async (
  locator: Locator,
  tests: Record<string, (l: Locator) => Promise<void>>,
) => {
  const locators = await locator.all()
  for (let locator of locators) {
    for (let [testId, test] of Object.entries(tests)) {
      await test(locator.getByTestId(testId))
    }
  }
}

export const expectToHaveText = (expected: string) => (l: Locator) =>
  expect(l).toHaveText(expected)

export const expectToHaveInputValue = (expected: string) => (l: Locator) =>
  expect(l).toHaveValue(expected)

export const expectToHaveSelectedValue = (expected: string) => (l: Locator) =>
  expect(l).toHaveValue(expected)

export const expectToHaveKoodiviite =
  (koodistoUri: string, koodiarvo: string) => (l: Locator) =>
    expect(l).toHaveText(`<${koodistoUri}:${koodiarvo}>`)
