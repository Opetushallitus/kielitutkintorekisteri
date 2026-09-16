import { aws_sns, Duration, Stack, StackProps } from "aws-cdk-lib"
import {
  ComparisonOperator,
  IAlarmAction,
  Stats,
  TreatMissingData,
} from "aws-cdk-lib/aws-cloudwatch"
import { SnsAction } from "aws-cdk-lib/aws-cloudwatch-actions"
import { ServicePrincipal } from "aws-cdk-lib/aws-iam"
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
import { SnsTopic } from "aws-cdk-lib/aws-events-targets"
import { ITopic } from "aws-cdk-lib/aws-sns"

export interface LogGroupsStackProps extends StackProps {
  alarmsSnsTopic: aws_sns.ITopic
  infoSnsTopic: aws_sns.ITopic
  investigationActions: IAlarmAction[]
}

export class LogGroupsStack extends Stack {
  readonly serviceLogGroup: LogGroup
  readonly serviceAuditLogGroup: LogGroup

  constructor(scope: Construct, id: string, props: LogGroupsStackProps) {
    super(scope, id, props)

    const dataProtectionAuditLogGroup = new LogGroup(
      this,
      "DataProtectionAudit",
      {
        logGroupName: "KituServiceDataProtectionAudit",
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
      logGroupName: "KituService",
      dataProtectionPolicy,
    })
    this.serviceAuditLogGroup = new LogGroup(this, "ServiceAudit", {
      logGroupName: "KituServiceAudit",
    })

    this.enableTransactionSearch(props.infoSnsTopic)

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

  /** Enable Transaction Search.
   *
   * This enables querying traces older than 30 days.
   *
   * @see https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/CloudWatch-Transaction-Search.html
   * @see https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/CloudWatch-Transaction-Search-Cloudformation.html
   */
  private enableTransactionSearch(infoSnsTopic: ITopic) {
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

    postYkiSuoritusAlarm.addAlarmAction(new SnsAction(infoSnsTopic))
    postYkiSuoritusAlarm.addOkAction(new SnsAction(infoSnsTopic))
  }
}
