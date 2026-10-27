import {
  aws_sns,
  Duration,
  RemovalPolicy,
  Stack,
  StackProps,
} from "aws-cdk-lib"
import {
  ComparisonOperator,
  IAlarmAction,
  Stats,
  TreatMissingData,
} from "aws-cdk-lib/aws-cloudwatch"
import { SnsAction } from "aws-cdk-lib/aws-cloudwatch-actions"
import { AnyPrincipal, ServicePrincipal } from "aws-cdk-lib/aws-iam"
import { Key } from "aws-cdk-lib/aws-kms"
import {
  CfnResourcePolicy,
  CustomDataIdentifier,
  DataProtectionPolicy,
  FilterPattern,
  LogGroup,
  LogRetention,
  RetentionDays,
} from "aws-cdk-lib/aws-logs"
import { CfnTransactionSearchConfig } from "aws-cdk-lib/aws-xray"
import { Construct } from "constructs"
import { LambdaFunction } from "aws-cdk-lib/aws-events-targets"
import { ITopic, Topic } from "aws-cdk-lib/aws-sns"
import { EmailSubscription } from "aws-cdk-lib/aws-sns-subscriptions"
import { Rule } from "aws-cdk-lib/aws-events"
import { NodejsFunction } from "aws-cdk-lib/aws-lambda-nodejs"
import { Runtime } from "aws-cdk-lib/aws-lambda"
import { PolicyStatement } from "aws-cdk-lib/aws-iam"
import path = require("node:path")

const LOG_GROUP_NAMES = {
  service: "KituService",
  serviceAudit: "KituServiceAudit",
  dataProtectionAudit: "KituServiceDataProtectionAudit",
}

export interface LogGroupsStackProps extends StackProps {
  alarmsSnsTopic: aws_sns.ITopic
  infoSnsTopic: aws_sns.ITopic
  investigationActions: IAlarmAction[]
  ykiSuoritusEmailRecipients: string[]
}

export class LogGroupsStack extends Stack {
  readonly serviceLogGroup: LogGroup
  readonly serviceAuditLogGroup: LogGroup

  constructor(scope: Construct, id: string, props: LogGroupsStackProps) {
    super(scope, id, props)

    const encryptionKey = this.createLogsKey()

    const dataProtectionAuditLogGroup = new LogGroup(
      this,
      "DataProtectionAudit",
      {
        logGroupName: LOG_GROUP_NAMES.dataProtectionAudit,
        encryptionKey,
      },
    )

    const FINNISH_SSN_REGEX =
      "((0[1-9]|[12][0-9]|3[01]))((0[1-9]|1[0-2]))(\\d{2})([-AaBbCcDdEeFfXxYyWwVvUu+])(\\d{3})([0-9A-Ya-y])"
    const dataProtectionPolicy = new DataProtectionPolicy({
      name: "KituDataProtectionPolicy",
      description: "Kitu data protection policy",
      identifiers: [new CustomDataIdentifier("Finnish_SSN", FINNISH_SSN_REGEX)],
      logGroupAuditDestination: dataProtectionAuditLogGroup,
    })

    this.serviceLogGroup = new LogGroup(this, "Service", {
      logGroupName: LOG_GROUP_NAMES.service,
      dataProtectionPolicy,
      encryptionKey,
    })
    this.serviceAuditLogGroup = new LogGroup(this, "ServiceAudit", {
      logGroupName: LOG_GROUP_NAMES.serviceAudit,
      encryptionKey,
    })

    this.enableTransactionSearch(
      props.infoSnsTopic,
      props.ykiSuoritusEmailRecipients,
    )

    const errorsAlarm = this.serviceLogGroup
      .addMetricFilter("Errors", {
        metricName: "LogErrors",
        metricNamespace: "Kitu",
        filterPattern: FilterPattern.any(
          FilterPattern.booleanValue("$.success", false),
          FilterPattern.stringValue("$.log.level", "=", "ERROR"),
        ),
      })
      .metric({ statistic: Stats.SAMPLE_COUNT })
      .createAlarm(this, "ErrorsAlarm", {
        threshold: 3,
        evaluationPeriods: 1,
        treatMissingData: TreatMissingData.NOT_BREACHING,
      })

    errorsAlarm.addAlarmAction(
      new SnsAction(props.alarmsSnsTopic),
      ...props.investigationActions,
    )
    errorsAlarm.addOkAction(new SnsAction(props.alarmsSnsTopic))

    const warningsAlarm = this.serviceLogGroup
      .addMetricFilter("Warnings", {
        metricName: "LogWarnings",
        metricNamespace: "Kitu",
        filterPattern: FilterPattern.any(
          FilterPattern.stringValue("$.level", "=", "WARN"),
          FilterPattern.stringValue("$.log.level", "=", "WARN"),
        ),
      })
      .metric({ statistic: Stats.SAMPLE_COUNT })
      .createAlarm(this, "WarningsAlarm", {
        threshold: 5,
        evaluationPeriods: 1,
        treatMissingData: TreatMissingData.NOT_BREACHING,
      })

    warningsAlarm.addAlarmAction(
      new SnsAction(props.alarmsSnsTopic),
      ...props.investigationActions,
    )
    warningsAlarm.addOkAction(new SnsAction(props.alarmsSnsTopic))
  }

  /** Customer-managed key for the application log groups (OPHKOTO-94).
   *
   * Associating a key with an existing group encrypts only newly ingested
   * events; older ones stay under CloudWatch Logs' default encryption until
   * retention expires them. Disabling or deleting the key makes every event
   * encrypted with it unreadable, hence RETAIN.
   *
   * @see https://docs.aws.amazon.com/AmazonCloudWatch/latest/logs/encrypt-log-data-kms.html
   */
  private createLogsKey(): Key {
    const key = new Key(this, "LogsKey", {
      alias: "kitu/logs",
      description: "Kitun sovelluslokiryhmien salausavain",
      enableKeyRotation: true,
      removalPolicy: RemovalPolicy.RETAIN,
    })

    const logsService = `logs.${this.region}.amazonaws.com`
    const actions = [
      "kms:Encrypt",
      "kms:Decrypt",
      "kms:ReEncrypt*",
      "kms:GenerateDataKey*",
      "kms:Describe*",
    ]

    // The ARNs are built from the names rather than the LogGroup constructs,
    // which reference this key: referencing them back would be a cycle. A group
    // missing from this list fails to associate with the key.
    key.addToResourcePolicy(
      new PolicyStatement({
        principals: [new ServicePrincipal(logsService)],
        actions,
        resources: ["*"],
        conditions: {
          ArnEquals: {
            "kms:EncryptionContext:aws:logs:arn": Object.values(
              LOG_GROUP_NAMES,
            ).map(
              (name) =>
                `arn:${this.partition}:logs:${this.region}:${this.account}:log-group:${name}`,
            ),
          },
        },
      }),
    )

    // Writes encrypt as the CloudWatch Logs service (the statement above), but
    // reads such as FilterLogEvents and StartLiveTail make a KMS Decrypt call
    // as the calling role through kms:ViaService (seen in CloudTrail). Without
    // this statement the deploy job's live tail and any role without
    // kms:Decrypt in IAM, such as read-only developer roles, could no longer
    // read the logs. The principal is "*", but the key is usable only through
    // CloudWatch Logs and only from this account — the same scope as an AWS
    // managed key — so which groups a role may read or write is still decided
    // by its logs:* IAM permissions.
    key.addToResourcePolicy(
      new PolicyStatement({
        principals: [new AnyPrincipal()],
        actions,
        resources: ["*"],
        conditions: {
          StringEquals: {
            "kms:CallerAccount": this.account,
            "kms:ViaService": logsService,
          },
        },
      }),
    )

    return key
  }

  /** Enable Transaction Search and build what reads the `aws/spans` log group.
   *
   * Transaction Search enables querying traces older than 30 days. On top of the span
   * log group this also creates the PostYkiSuoritus metric filter and alarm, the
   * YkiSuoritusNotifications Lambda, its email topic and the alarm state-change rule.
   *
   * @see https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/CloudWatch-Transaction-Search.html
   * @see https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/CloudWatch-Transaction-Search-Cloudformation.html
   */
  private enableTransactionSearch(
    infoSnsTopic: ITopic,
    ykiSuoritusEmailRecipients: string[],
  ) {
    // This enables Transaction Search. CDK doesn't have a L2 construct for TS yet.
    const transactionSearchConfig = new CfnTransactionSearchConfig(
      this,
      "TransactionSearch",
      {
        indexingPercentage: 100,
      },
    )

    // The rest is granting XRay write rights to the Transaction Search log groups that store the trace data.
    const xray = new ServicePrincipal("xray", {
      conditions: {
        ArnLike: {
          "aws:SourceArn": `arn:${this.partition}:xray:${this.region}:${this.account}:*`,
        },
        StringEquals: {
          "aws:SourceAccount": this.account,
        },
      },
    })

    const transactionSearchSpans = LogGroup.fromLogGroupName(
      this,
      "TransactionSearchSpans",
      "aws/spans",
    )

    const applicationSignalsData = LogGroup.fromLogGroupName(
      this,
      "ApplicationSignalsData",
      "/aws/application-signals/data",
    )

    new LogRetention(this, "ApplicationSignalsDataetention", {
      logGroupName: "/aws/application-signals/data",
      retention: RetentionDays.THREE_YEARS,
    })

    transactionSearchSpans.grantWrite(xray)
    applicationSignalsData.grantWrite(xray)

    // Transaction Search cannot be enabled until it has write access to the log groups specified above.
    //
    // This means that we need to tell CloudFormation that the transactionSearchConfig resource has to be created *after* the ResourcePolicy resources of these log groups, but CDK does not expose the resource policy resources of L2 log group constructs.
    //
    // Instead, we use the .node property to reach into the L1 resource level and find the appropriate ResourcePolicy resource. AWS has various documentation pages on this:
    //
    // CDK guide: https://docs.aws.amazon.com/cdk/v2/guide/cfn-layer.html
    // Knowledge Center: https://repost.aws/knowledge-center/cdk-retrieve-construct-objects
    //
    // The end result is that the TransactionSearch resource in cdk.out/assembly-${env}/${env}LogGroups.template.json contains this property:
    //
    //   "DependsOn": [
    //     "ApplicationSignalsDataPolicyResourcePolicy921BB654",
    //     "TransactionSearchSpansPolicyResourcePolicy81B12951"
    //    ],

    transactionSearchConfig.addDependency(
      // Note that findChild() only searches *direct* children, so we need to traverse the tree until we reach the resource policy.
      transactionSearchSpans.node
        .findChild("Policy")
        .node.findChild("ResourcePolicy") as CfnResourcePolicy,
    )
    transactionSearchConfig.addDependency(
      // Note that findChild() only searches *direct* children, so we need to traverse the tree until we reach the resource policy.
      applicationSignalsData.node
        .findChild("Policy")
        .node.findChild("ResourcePolicy") as CfnResourcePolicy,
    )

    const postYkiSuoritusAlarm = transactionSearchSpans
      .addMetricFilter("PostYkiSuoritus", {
        metricName: "PostYkiSuoritus",
        metricNamespace: "Kitu",
        // A metric filter sees ONE log record at a time, and the two things we
        // care about live on two DIFFERENT spans:
        //
        //   kitu                  <- SERVER segment: uri, method, status.code
        //     secured request     <- INTERNAL: arvioitu
        //       YkiSuoritusRepository.save
        //
        // Spring Security's "secured request" observation wraps the controller
        // invocation, so Span.current() inside YkiApiController.postHenkilosuoritus
        // is that INTERNAL span — not the HTTP SERVER span. A pattern requiring
        // both uri/method and arvioitu therefore matches nothing, which is what
        // silently flatlined this metric between 4.6.2026 and 16.9.2026.
        //
        // Filtering on arvioitu alone is precise: the attribute is written in
        // exactly one place (YkiApiController, POST /yki/api/suoritus), so its
        // presence already identifies the endpoint. Keep it that way — if a
        // second call site ever sets "arvioitu", this alarm starts double-counting.
        //
        // Do not try to reinstate the endpoint check via $.attributes.http.route:
        // CloudWatch Logs metric filter patterns cannot reference JSON keys that
        // contain periods (no bracket/quote escape works).
        filterPattern: FilterPattern.all(
          FilterPattern.booleanValue("$.attributes.arvioitu", true),
          FilterPattern.stringValue("$.status.code", "!=", "ERROR"),
        ),
      })
      .metric({
        statistic: Stats.SUM,
        period: Duration.minutes(1),
      })
      .createAlarm(this, "YkiSuoritusAlarm", {
        alarmDescription: "YKI-suorituksia arvioitu",
        threshold: 1,
        evaluationPeriods: 1, // 1 minute, matching the metric period above
        comparisonOperator:
          ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
        treatMissingData: TreatMissingData.NOT_BREACHING,
      })

    // The alarm has no actions of its own: Chatbot's stock alarm card is too
    // technical for the #kielitutkintorekisteri audience. Instead its state
    // changes drive a Lambda that posts a short "arriving" notice on ALARM and,
    // on the return to OK, a summary of the batch grouped by tutkintopäivä,
    // kieli, taso and arviointitila (read from the spans with Logs Insights).
    const ykiSuoritusNotifications = new NodejsFunction(
      this,
      "YkiSuoritusNotifications",
      {
        runtime: Runtime.NODEJS_LATEST,
        entry: path.join(__dirname, "yki-suoritus-notifications/handler.ts"),
        timeout: Duration.minutes(5),
        environment: {
          TOPIC_ARN: infoSnsTopic.topicArn,
          LOG_GROUP: transactionSearchSpans.logGroupName,
        },
        initialPolicy: [
          new PolicyStatement({
            actions: ["logs:StartQuery"],
            resources: [transactionSearchSpans.logGroupArn],
          }),
          new PolicyStatement({
            actions: ["logs:GetQueryResults"],
            resources: ["*"],
          }),
          new PolicyStatement({
            actions: ["cloudwatch:DescribeAlarmHistory"],
            resources: [postYkiSuoritusAlarm.alarmArn],
          }),
        ],
      },
    )
    infoSnsTopic.grantPublish(ykiSuoritusNotifications)

    // Email gets only the batch summary, as plain text, on a topic of its own:
    // the info topic carries Chatbot JSON and other notices too.
    if (ykiSuoritusEmailRecipients.length > 0) {
      const emailTopic = new Topic(this, "YkiSuoritusEmailTopic", {
        displayName: "Kielitutkintorekisteri",
      })
      for (const osoite of ykiSuoritusEmailRecipients) {
        emailTopic.addSubscription(new EmailSubscription(osoite))
      }
      emailTopic.grantPublish(ykiSuoritusNotifications)
      ykiSuoritusNotifications.addEnvironment(
        "EMAIL_TOPIC_ARN",
        emailTopic.topicArn,
      )
    }

    new Rule(this, "YkiSuoritusAlarmStateChange", {
      description:
        "YKI-suoritusten vastaanottoilmoitukset Slackiin ja sähköpostiin",
      eventPattern: {
        source: ["aws.cloudwatch"],
        detailType: ["CloudWatch Alarm State Change"],
        resources: [postYkiSuoritusAlarm.alarmArn],
      },
      targets: [new LambdaFunction(ykiSuoritusNotifications)],
    })
  }
}
