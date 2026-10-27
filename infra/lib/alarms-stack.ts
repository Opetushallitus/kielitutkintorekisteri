import * as cdk from "aws-cdk-lib"
import {
  aws_chatbot,
  aws_events,
  aws_events_targets,
  aws_sns,
  PhysicalName,
  StackProps,
} from "aws-cdk-lib"
import { Construct } from "constructs"
import {
  Effect,
  ManagedPolicy,
  PolicyDocument,
  PolicyStatement,
  Role,
  ServicePrincipal,
} from "aws-cdk-lib/aws-iam"
import { CfnInvestigationGroup } from "aws-cdk-lib/aws-aiops"
import {
  AlarmActionConfig,
  IAlarm,
  IAlarmAction,
} from "aws-cdk-lib/aws-cloudwatch"
import { LoggingLevel } from "aws-cdk-lib/aws-chatbot"
import { SlackChannel } from "./accounts"

// scheduledChange-tapahtumat, joihin ei tarvitse reagoida. Fargaten
// patch-retirement korvaa tehtävät itsestään ECS-palvelun kautta, ja jokainen
// mainin deploy vaihtaa ne joka tapauksessa uusiin.
const OHITETTAVAT_HEALTH_TAPAHTUMAT = ["AWS_ECS_TASK_PATCHING_RETIREMENT"]

export interface AlarmsStackSlackProps {
  workspaceId: string
  alarmsChannel: SlackChannel
  infoChannel?: SlackChannel
  // Topics from other AlarmsStacks (e.g. the us-east-1 mirror) that should
  // share this stack's Slack configuration. Needed because Chatbot only allows
  // one SlackChannelConfiguration per Slack channel per account.
  additionalAlarmTopics?: aws_sns.ITopic[]
  additionalInfoTopics?: aws_sns.ITopic[]
}

export interface AlarmsStackProps extends StackProps {
  slack?: AlarmsStackSlackProps
  // Kun tämä on false, tutkimusryhmä luodaan edelleen (tutkimuksen voi
  // käynnistää käsin konsolista) mutta hälytykset eivät käynnistä sitä itse.
  automaticInvestigations: boolean
}

export class AlarmsStack extends cdk.Stack {
  readonly alarmSnsTopic: aws_sns.Topic
  readonly infoSnsTopic: aws_sns.Topic
  readonly investigationGroup: CfnInvestigationGroup
  // Tyhjä lista kun automaattiset tutkimukset on kytketty pois päältä.
  readonly investigationActions: IAlarmAction[]

  constructor(scope: Construct, id: string, props: AlarmsStackProps) {
    super(scope, id, props)

    this.alarmSnsTopic = this.createSnsTopic("AlarmSnsTopic")
    this.infoSnsTopic = this.createSnsTopic("InfoSnsTopic")

    // Ilman tätä Q ei pysty julkaisemaan tutkimustapahtumia
    // chatbotNotificationChannelsin SNS-aiheeseen, joten Slackiin ei tule
    // tutkimusviestejä vaikka tutkimus käynnistyisi onnistuneesti.
    // Katso: https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/Investigations-Integrations.html#Investigations-Integrations-Chat-policy
    this.alarmSnsTopic.grantPublish(
      new ServicePrincipal("aiops.amazonaws.com", {
        conditions: {
          StringEquals: { "aws:SourceAccount": this.account },
        },
      }),
    )

    const alarmsSlack = props.slack
      ? this.createSlackChannelConfiguration(
          "SlackBot",
          props.slack.workspaceId,
          props.slack.alarmsChannel,
          [this.alarmSnsTopic, ...(props.slack.additionalAlarmTopics ?? [])],
        )
      : undefined

    if (props.slack?.infoChannel) {
      this.createSlackChannelConfiguration(
        "InfoSlackBot",
        props.slack.workspaceId,
        props.slack.infoChannel,
        [this.infoSnsTopic, ...(props.slack.additionalInfoTopics ?? [])],
      )
    }

    this.investigationGroup = this.createInvestigationGroup(alarmsSlack)
    this.investigationActions = props.automaticInvestigations
      ? [new InvestigationGroupAlarmAction(this.investigationGroup.attrArn)]
      : []

    // Pidetään tutkimusryhmän ARN:n vienti pysyvästi olemassa, vaikka mikään
    // pino ei viittaisi siihen. Käyttämätön vienti ei maksa mitään, ja näin
    // automaticInvestigations-lipun kääntäminen on kumpaankin suuntaan pelkkä
    // hälytystoiminnon muutos: vientiä ei luoda eikä poisteta. Ilman tätä lipun
    // pois kytkeminen kaataa tuottajapinon rollbackiin, koska CloudFormation ei
    // poista vientiä johon kuluttajapino vielä viittaa ja CDK päivittää
    // tuottajan ensin.
    this.exportValue(this.investigationGroup.attrArn)

    this.createHealthEventRule()
  }

  private createSnsTopic(id: string) {
    const topic = new aws_sns.Topic(this, id, {
      topicName: PhysicalName.GENERATE_IF_NEEDED,
    })

    topic.grantPublish(new ServicePrincipal("cloudwatch.amazonaws.com"))

    return topic
  }

  // AWS Health kertoo etukäteen mm. Lambda-ajonaikojen ja RDS-versioiden
  // elinkaaren päättymisestä. Ilman tätä sääntöä ilmoitukset jäävät AWS Health
  // Dashboardiin, jota kukaan ei lue. Kohteena on alarmSnsTopic eikä
  // infoSnsTopic, koska vain se on varmasti tilattu jokaisessa pinossa:
  // info-kanava on määritelty ainoastaan prodille (accounts.ts), ja us-east-1:n
  // pino saa Slack-yhteyden vain additionalAlarmTopicsin kautta.
  private createHealthEventRule() {
    new aws_events.Rule(this, "AwsHealthScheduledChanges", {
      description:
        "AWS Health scheduled change -tapahtumat (ajonaikojen ja versioiden elinkaari) Slackiin",
      eventPattern: {
        source: ["aws.health"],
        detailType: ["AWS Health Event"],
        // Pelkkä scheduledChange: accountNotification toisi mukanaan mm.
        // Shield-tilauksen uusimismuistutukset ja hukuttaisi signaalin.
        detail: {
          eventTypeCategory: ["scheduledChange"],
          eventTypeCode: aws_events.Match.anythingBut(
            ...OHITETTAVAT_HEALTH_TAPAHTUMAT,
          ),
        },
      },
      targets: [new aws_events_targets.SnsTopic(this.alarmSnsTopic)],
    })
  }

  private createSlackChannelConfiguration(
    id: string,
    slackWorkspaceId: string,
    slackChannel: SlackChannel,
    notificationTopics: aws_sns.ITopic[],
  ) {
    const config = new aws_chatbot.SlackChannelConfiguration(this, id, {
      slackChannelId: slackChannel.id,
      slackWorkspaceId,
      slackChannelConfigurationName: slackChannel.name,
      notificationTopics,
      loggingLevel: LoggingLevel.INFO,
    })

    // Ilman näitä oikeuksia Q ei pysty hakemaan tutkimuksen tietoja, joten
    // Slackiin ei tule tutkimusviestejä vaikka chatConfigurationArns olisi
    // kytketty investigation groupin alle.
    config.role?.addManagedPolicy(
      ManagedPolicy.fromAwsManagedPolicyName("AIOpsAssistantPolicy"),
    )

    return config
  }

  private createInvestigationGroup(
    alarmsSlack: aws_chatbot.SlackChannelConfiguration | undefined,
  ) {
    const role = new Role(this, "InvestigationGroupRole", {
      assumedBy: new ServicePrincipal("aiops.amazonaws.com"),
      managedPolicies: [
        ManagedPolicy.fromAwsManagedPolicyName("AIOpsAssistantPolicy"),
      ],
    })

    // CloudWatch alarms can't start investigations unless the group's resource
    // policy grants aiops.alarms.cloudwatch.amazonaws.com explicit access.
    const alarmsPolicy = new PolicyDocument({
      statements: [
        new PolicyStatement({
          effect: Effect.ALLOW,
          principals: [
            new ServicePrincipal("aiops.alarms.cloudwatch.amazonaws.com"),
          ],
          actions: [
            "aiops:CreateInvestigation",
            "aiops:CreateInvestigationEvent",
          ],
          resources: ["*"],
          conditions: {
            StringEquals: { "aws:SourceAccount": this.account },
            ArnLike: {
              "aws:SourceArn": `arn:${this.partition}:cloudwatch:${this.region}:${this.account}:alarm:*`,
            },
          },
        }),
      ],
    })

    return new CfnInvestigationGroup(this, "InvestigationGroup", {
      name: `kitu-${this.stackName}`,
      roleArn: role.roleArn,
      retentionInDays: 90,
      investigationGroupPolicy: JSON.stringify(alarmsPolicy.toJSON()),
      // When a Slack config exists, route investigation events through
      // Chatbot for Q-curated formatting. Otherwise just publish to SNS and
      // rely on whichever stack owns the Slack config to fan it out.
      chatbotNotificationChannels: [
        {
          snsTopicArn: this.alarmSnsTopic.topicArn,
          ...(alarmsSlack && {
            chatConfigurationArns: [alarmsSlack.slackChannelConfigurationArn],
          }),
        },
      ],
    })
  }
}

// Adding an investigation group's ARN as an alarm action tells CloudWatch to
// auto-start an Amazon Q investigation when the alarm transitions to ALARM.
// There is no built-in IAlarmAction for this in aws-cdk-lib yet.
class InvestigationGroupAlarmAction implements IAlarmAction {
  constructor(private readonly investigationGroupArn: string) {}

  bind(_scope: Construct, _alarm: IAlarm): AlarmActionConfig {
    return { alarmActionArn: this.investigationGroupArn }
  }
}
