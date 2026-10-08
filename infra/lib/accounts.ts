export interface EnvironmentConfig {
  name: string
  account: string
  region: string
  network: {
    cidr: string
    maxAzs: number
  }
  domainName: string
  databaseName: string
  productionQuality: boolean
  // Käynnistetäänkö hälytyksestä automaattisesti Amazon Q -tutkimus. Tutkimukset
  // ajavat Logs Insights -kyselyitä ja metriikkahakuja, joista CloudWatch
  // laskuttaa, joten tämä pidetään oletuksena pois päältä.
  automaticInvestigations: boolean
  slackWorkspaceId: string
  slackAlarmsChannel: SlackChannel
  slackInfoChannel?: SlackChannel
  // Vastaanotettujen YKI-suoritusten yhteenveto lähetetään myös näihin
  // osoitteisiin. Jokaisen osoitteen on vahvistettava SNS-tilaus ennen kuin
  // viestejä tulee perille.
  ykiSuoritusEmailRecipients: string[]
  koski: {
    region: string
    account: string
  }
}

export type SlackChannel = {
  name: string
  id: string
}

// CIDR allocation strategy:
// Top: 10.15.0.0/16
// VPCs: 10.15.0.0/18, 10.15.64.0/18, 10.15.128.0/18, 10.15.192.0/18 (16382 addresses)
// Subnets: (let AWS calculate these for us)

export type EnvironmentName = "dev" | "test" | "prod"

export const deploymentAccounts: {
  [A in EnvironmentName]: EnvironmentConfig
} = {
  dev: {
    name: "untuva",
    account: "682033502734",
    region: "eu-west-1",
    network: {
      cidr: "10.15.0.0/18",
      maxAzs: 2,
    },
    domainName: "kios.untuvaopintopolku.fi",
    databaseName: "kios",
    productionQuality: false,
    automaticInvestigations: false,
    slackWorkspaceId: "T02C6SZL7KP",
    slackAlarmsChannel: {
      name: "kielitutkintorekisteri-alerts-dev-test",
      id: "C08E14CRZ3J",
    },
    ykiSuoritusEmailRecipients: [],
    koski: {
      region: "eu-west-1",
      account: "500150530292",
    },
  },
  test: {
    name: "qa",
    account: "961341546901",
    region: "eu-west-1",
    network: {
      cidr: "10.15.64.0/18",
      maxAzs: 3,
    },
    domainName: "kios.testiopintopolku.fi",
    databaseName: "kios",
    productionQuality: false,
    automaticInvestigations: false,
    slackWorkspaceId: "T02C6SZL7KP",
    slackAlarmsChannel: {
      name: "kielitutkintorekisteri-alerts-dev-test",
      id: "C08E14CRZ3J",
    },
    ykiSuoritusEmailRecipients: [],
    koski: {
      region: "eu-west-1",
      account: "692437769085",
    },
  },
  prod: {
    name: "prod",
    account: "515966535475",
    region: "eu-west-1",
    network: {
      cidr: "10.15.128.0/18",
      maxAzs: 3,
    },
    domainName: "kios.opintopolku.fi",
    databaseName: "kios",
    productionQuality: true,
    automaticInvestigations: false,
    slackWorkspaceId: "T02C6SZL7KP",
    slackAlarmsChannel: {
      name: "kielitutkintorekisteri-alerts",
      id: "C07QPSYBY7L",
    },
    slackInfoChannel: {
      name: "kielitutkintorekisteri",
      id: "C07KFEA4DEH",
    },
    ykiSuoritusEmailRecipients: ["kielitutkinnot@oph.fi"],
    koski: {
      region: "eu-west-1",
      account: "508832528142",
    },
  },
}

export const utilityAccount = {
  name: "util",
  account: "961341524988",
  region: "eu-west-1",
  slackChannel: {
    name: "kielitutkintorekisteri-alerts",
    id: "C07QPSYBY7L",
  },
  slackWorkspaceId: "T02C6SZL7KP",
}
