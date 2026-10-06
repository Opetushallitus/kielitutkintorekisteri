export interface SuoritusRyhma {
  tutkintopaiva?: string
  tutkintokieli?: string
  tutkintotaso?: string
  arviointitila?: string
  lkm: number
  alku: Date
  loppu: Date
}

export interface ChatbotNotification {
  version: "1.0"
  source: "custom"
  content: {
    textType: "client-markdown"
    title: string
    description?: string
  }
}

export const MAX_RIVEJA = 25

const TUNTEMATON = "tuntematon"

const kielet: Record<string, string> = {
  DEU: "saksa",
  ENG: "englanti",
  FIN: "suomi",
  FRA: "ranska",
  ITA: "italia",
  RUS: "venäjä",
  SME: "pohjoissaame",
  SPA: "espanja",
  SWE: "ruotsi",
}

const tasot: Record<string, string> = {
  PT: "perustaso",
  KT: "keskitaso",
  YT: "ylin taso",
}

const tasojarjestys = ["PT", "KT", "YT"]

const arviointitilat: Record<string, string> = {
  ARVIOITU: "arvioitu",
  TARKISTUSARVIOITU: "tarkistusarvioitu",
}

const nimi = (taulu: Record<string, string>, koodi?: string) =>
  koodi ? (taulu[koodi] ?? koodi) : TUNTEMATON

const helsinki = (options: Intl.DateTimeFormatOptions) =>
  new Intl.DateTimeFormat("fi-FI", { timeZone: "Europe/Helsinki", ...options })

const paivaJaKellonaika = helsinki({
  weekday: "short",
  day: "numeric",
  month: "numeric",
  hour: "2-digit",
  minute: "2-digit",
})
const paiva = helsinki({ day: "numeric", month: "numeric", year: "numeric" })
const kellonaika = helsinki({ hour: "2-digit", minute: "2-digit" })

const samaPaiva = (a: Date, b: Date) => paiva.format(a) === paiva.format(b)

export const muotoileAikavali = (alku: Date, loppu: Date): string => {
  const alkuteksti = paivaJaKellonaika.format(alku)
  return samaPaiva(alku, loppu)
    ? `${alkuteksti}–${kellonaika.format(loppu)}`
    : `${alkuteksti} – ${paivaJaKellonaika.format(loppu)}`
}

const muotoileTutkintopaiva = (iso?: string) => {
  const osat = iso?.match(/^(\d{4})-(\d{2})-(\d{2})$/)
  return osat ? `${Number(osat[3])}.${Number(osat[2])}.${osat[1]}` : TUNTEMATON
}

const vertaa = (a: string, b: string) => (a < b ? -1 : a > b ? 1 : 0)

const tasoindeksi = (koodi?: string) => {
  const i = koodi ? tasojarjestys.indexOf(koodi) : -1
  return i === -1 ? tasojarjestys.length : i
}

const jarjesta = (ryhmat: SuoritusRyhma[]) =>
  [...ryhmat].sort(
    (a, b) =>
      vertaa(a.tutkintopaiva ?? "~", b.tutkintopaiva ?? "~") ||
      vertaa(nimi(kielet, a.tutkintokieli), nimi(kielet, b.tutkintokieli)) ||
      tasoindeksi(a.tutkintotaso) - tasoindeksi(b.tutkintotaso) ||
      vertaa(a.arviointitila ?? "", b.arviointitila ?? ""),
  )

export const suorituksia = (lkm: number) =>
  lkm === 1 ? "1 arvioitu YKI-suoritus" : `${lkm} arvioitua YKI-suoritusta`

export const saapumisilmoitus = (): ChatbotNotification => ({
  version: "1.0",
  source: "custom",
  content: {
    textType: "client-markdown",
    title: "📥 Solkista saapuu arvioituja YKI-suorituksia…",
  },
})

export const yhteenveto = (ryhmat: SuoritusRyhma[]): ChatbotNotification => {
  const yhteensa = ryhmat.reduce((summa, r) => summa + r.lkm, 0)
  const alku = new Date(Math.min(...ryhmat.map((r) => r.alku.getTime())))
  const loppu = new Date(Math.max(...ryhmat.map((r) => r.loppu.getTime())))

  const rivit: string[] = []
  let edellinenPaiva: string | undefined = undefined
  const naytettavat = jarjesta(ryhmat).slice(0, MAX_RIVEJA)
  for (const r of naytettavat) {
    const tutkintopaiva = muotoileTutkintopaiva(r.tutkintopaiva)
    if (tutkintopaiva !== edellinenPaiva) {
      rivit.push(`*Tutkintopäivä ${tutkintopaiva}*`)
      edellinenPaiva = tutkintopaiva
    }
    rivit.push(
      `• ${nimi(kielet, r.tutkintokieli)}, ${nimi(tasot, r.tutkintotaso)}, ${nimi(arviointitilat, r.arviointitila)}: ${r.lkm}`,
    )
  }
  const piilotetut = ryhmat.length - naytettavat.length
  if (piilotetut > 0) {
    rivit.push(`…ja ${piilotetut} muuta ryhmää`)
  }

  return {
    version: "1.0",
    source: "custom",
    content: {
      textType: "client-markdown",
      title: `✅ Vastaanotettu ${suorituksia(yhteensa)} (${muotoileAikavali(alku, loppu)})`,
      description: rivit.join("\n"),
    },
  }
}

export const yhteenvetoIlmanErittelya = (
  alku: Date,
  loppu: Date,
): ChatbotNotification => ({
  version: "1.0",
  source: "custom",
  content: {
    textType: "client-markdown",
    title: `✅ Arvioituja YKI-suorituksia vastaanotettu (${muotoileAikavali(alku, loppu)})`,
    description: "Erittelyä ei saatu haettua.",
  },
})
