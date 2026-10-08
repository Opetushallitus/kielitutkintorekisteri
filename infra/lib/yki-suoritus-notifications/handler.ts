import {
  CloudWatchLogsClient,
  GetQueryResultsCommand,
  ResultField,
  StartQueryCommand,
} from "@aws-sdk/client-cloudwatch-logs"
import {
  CloudWatchClient,
  DescribeAlarmHistoryCommand,
} from "@aws-sdk/client-cloudwatch"
import { PublishCommand, SNSClient } from "@aws-sdk/client-sns"
import {
  ChatbotNotification,
  saapumisilmoitus,
  sahkopostiksi,
  SuoritusRyhma,
  yhteenveto,
  yhteenvetoIlmanErittelya,
} from "./summary"

interface AlarmState {
  value: "OK" | "ALARM" | "INSUFFICIENT_DATA"
  timestamp: string
}

export interface AlarmStateChangeEvent {
  detail: {
    alarmName: string
    state: AlarmState
    previousState: AlarmState
  }
}

const cloudwatch = new CloudWatchClient({})
const logs = new CloudWatchLogsClient({})
const sns = new SNSClient({})

const QUERY = `filter attributes.arvioitu = 1 and status.code != "ERROR"
| stats count(*) as lkm, min(@timestamp) as alku, max(@timestamp) as loppu
    by attributes.tutkintopaiva as tutkintopaiva, attributes.tutkintokieli as tutkintokieli,
       attributes.tutkintotaso as tutkintotaso, attributes.arviointitila as arviointitila`

const ALKUMARGINAALI_MS = 5 * 60 * 1000
const KYSELYN_AIKARAJA_MS = 90 * 1000
const UUSINNAN_VIIVE_MS = 60 * 1000

const odota = (ms: number) => new Promise((r) => setTimeout(r, ms))

const insightsAika = (arvo: string) => new Date(arvo.replace(" ", "T") + "Z")

const ryhmaksi = (rivi: ResultField[]): SuoritusRyhma => {
  const kentta = (nimi: string) =>
    rivi.find((f) => f.field === nimi)?.value || undefined
  return {
    tutkintopaiva: kentta("tutkintopaiva"),
    tutkintokieli: kentta("tutkintokieli"),
    tutkintotaso: kentta("tutkintotaso"),
    arviointitila: kentta("arviointitila"),
    lkm: Number(kentta("lkm") ?? 0),
    alku: insightsAika(kentta("alku")!),
    loppu: insightsAika(kentta("loppu")!),
  }
}

const haeRyhmat = async (alku: Date, loppu: Date) => {
  const { queryId } = await logs.send(
    new StartQueryCommand({
      logGroupName: process.env.LOG_GROUP,
      startTime: Math.floor(alku.getTime() / 1000),
      endTime: Math.ceil(loppu.getTime() / 1000),
      queryString: QUERY,
    }),
  )
  const takaraja = Date.now() + KYSELYN_AIKARAJA_MS
  while (Date.now() < takaraja) {
    await odota(2000)
    const tulos = await logs.send(new GetQueryResultsCommand({ queryId }))
    if (tulos.status === "Complete") {
      return (tulos.results ?? []).map(ryhmaksi)
    }
    if (tulos.status !== "Running" && tulos.status !== "Scheduled") {
      throw new Error(`Insights-kysely päättyi tilaan ${tulos.status}`)
    }
  }
  throw new Error("Insights-kysely ei valmistunut ajoissa")
}

const edellinenPalautuminen = async (alarmName: string, ennen: Date) => {
  const { AlarmHistoryItems } = await cloudwatch.send(
    new DescribeAlarmHistoryCommand({
      AlarmName: alarmName,
      HistoryItemType: "StateUpdate",
      EndDate: ennen,
      StartDate: new Date(ennen.getTime() - ALKUMARGINAALI_MS),
    }),
  )
  const palautumiset = (AlarmHistoryItems ?? [])
    .filter(
      (h) => JSON.parse(h.HistoryData ?? "{}").newState?.stateValue === "OK",
    )
    .map((h) => h.Timestamp!.getTime())
  return palautumiset.length > 0
    ? new Date(Math.max(...palautumiset))
    : undefined
}

const kyselynAlku = async (alarmName: string, halytysAlkoi: Date) => {
  const marginaalinAlku = new Date(halytysAlkoi.getTime() - ALKUMARGINAALI_MS)
  const palautuminen = await edellinenPalautuminen(
    alarmName,
    halytysAlkoi,
  ).catch((e) => {
    console.error("Hälytyshistorian haku epäonnistui", e)
    return undefined
  })
  return palautuminen && palautuminen > marginaalinAlku
    ? palautuminen
    : marginaalinAlku
}

const julkaise = (viesti: ChatbotNotification) =>
  sns.send(
    new PublishCommand({
      TopicArn: process.env.TOPIC_ARN,
      Message: JSON.stringify(viesti),
    }),
  )

const julkaiseSahkopostina = async (viesti: ChatbotNotification) => {
  if (!process.env.EMAIL_TOPIC_ARN) return
  const { otsikko, teksti } = sahkopostiksi(viesti)
  await sns.send(
    new PublishCommand({
      TopicArn: process.env.EMAIL_TOPIC_ARN,
      Subject: otsikko,
      Message: teksti,
    }),
  )
}

export const handler = async (event: AlarmStateChangeEvent) => {
  const { alarmName, state, previousState } = event.detail

  if (state.value === "ALARM") {
    await julkaise(saapumisilmoitus())
    return
  }

  if (state.value === "OK" && previousState.value === "ALARM") {
    const halytysAlkoi = new Date(previousState.timestamp)
    const alku = await kyselynAlku(alarmName, halytysAlkoi)
    const loppu = new Date(state.timestamp)
    const hae = () =>
      haeRyhmat(alku, loppu).catch((e) => {
        console.error("YKI-suoritusten erittelyn haku epäonnistui", e)
        return []
      })
    let ryhmat = await hae()
    if (ryhmat.length === 0) {
      await odota(UUSINNAN_VIIVE_MS)
      ryhmat = await hae()
    }
    const viesti =
      ryhmat.length > 0
        ? yhteenveto(ryhmat)
        : yhteenvetoIlmanErittelya(halytysAlkoi, loppu)
    await Promise.all([
      julkaise(viesti),
      julkaiseSahkopostina(viesti).catch((e) =>
        console.error("Yhteenvedon lähetys sähköpostiin epäonnistui", e),
      ),
    ])
  }
}
