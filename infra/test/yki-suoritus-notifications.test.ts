import {
  MAX_RIVEJA,
  muotoileAikavali,
  SuoritusRyhma,
  yhteenveto,
} from "../lib/yki-suoritus-notifications/summary"

const ryhma = (
  ryhma: Partial<SuoritusRyhma> & Pick<SuoritusRyhma, "lkm">,
): SuoritusRyhma => ({
  tutkintopaiva: "2026-09-12",
  tutkintokieli: "FIN",
  tutkintotaso: "KT",
  arviointitila: "ARVIOITU",
  alku: new Date("2026-10-06T07:02:00Z"),
  loppu: new Date("2026-10-06T07:15:00Z"),
  ...ryhma,
})

describe("yhteenveto", () => {
  test("ryhmittelee tutkintopäivän alle ja järjestää kielen ja tason mukaan", () => {
    const viesti = yhteenveto([
      ryhma({ tutkintopaiva: "2026-09-19", tutkintokieli: "ENG", lkm: 10 }),
      ryhma({ tutkintotaso: "YT", lkm: 5 }),
      ryhma({
        tutkintokieli: "SWE",
        tutkintotaso: "PT",
        arviointitila: "TARKISTUSARVIOITU",
        lkm: 2,
      }),
      ryhma({ lkm: 20 }),
    ])

    expect(viesti.content.title).toBe(
      "✅ Vastaanotettu 37 arvioitua YKI-suoritusta (ti 6.10. klo 10.02–10.15)",
    )
    expect(viesti.content.description).toBe(
      [
        "*Tutkintopäivä 12.9.2026*",
        "• ruotsi, perustaso, tarkistusarvioitu: 2",
        "• suomi, keskitaso, arvioitu: 20",
        "• suomi, ylin taso, arvioitu: 5",
        "*Tutkintopäivä 19.9.2026*",
        "• englanti, keskitaso, arvioitu: 10",
      ].join("\n"),
    )
  })

  test("käyttää yksikköä yhdelle suoritukselle", () => {
    expect(yhteenveto([ryhma({ lkm: 1 })]).content.title).toMatch(
      /^✅ Vastaanotettu 1 arvioitu YKI-suoritus \(/,
    )
  })

  test("näyttää puuttuvat attribuutit tuntemattomina", () => {
    const viesti = yhteenveto([
      ryhma({
        tutkintopaiva: undefined,
        tutkintokieli: undefined,
        tutkintotaso: undefined,
        arviointitila: undefined,
        lkm: 3,
      }),
    ])
    expect(viesti.content.description).toBe(
      "*Tutkintopäivä tuntematon*\n• tuntematon, tuntematon, tuntematon: 3",
    )
  })

  test("rajaa rivien määrän", () => {
    const ryhmat = Array.from({ length: MAX_RIVEJA + 3 }, (_, i) =>
      ryhma({
        tutkintopaiva: `2026-01-${String(i + 1).padStart(2, "0")}`,
        lkm: 1,
      }),
    )
    const rivit = yhteenveto(ryhmat).content.description!.split("\n")
    expect(rivit.filter((r) => r.startsWith("•"))).toHaveLength(MAX_RIVEJA)
    expect(rivit[rivit.length - 1]).toBe("…ja 3 muuta ryhmää")
  })
})

describe("muotoileAikavali", () => {
  test("näyttää loppupäivän kun aikaväli ylittää keskiyön Helsingin aikaa", () => {
    expect(
      muotoileAikavali(
        new Date("2026-10-06T20:50:00Z"),
        new Date("2026-10-06T21:10:00Z"),
      ),
    ).toBe("ti 6.10. klo 23.50 – ke 7.10. klo 00.10")
  })
})
