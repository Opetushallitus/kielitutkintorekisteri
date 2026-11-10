import { Duration, Stack, StackProps, aws_logs_destinations } from "aws-cdk-lib"
import { Construct } from "constructs"
import {
  FilterPattern,
  LogGroup,
  SubscriptionFilter,
} from "aws-cdk-lib/aws-logs"
import { NodejsFunction } from "aws-cdk-lib/aws-lambda-nodejs"
import { Runtime } from "aws-cdk-lib/aws-lambda"
import path = require("node:path")
import { StringParameter } from "aws-cdk-lib/aws-ssm"
import { Effect, PolicyStatement } from "aws-cdk-lib/aws-iam"
import { ITopic } from "aws-cdk-lib/aws-sns"
import { SnsAction } from "aws-cdk-lib/aws-cloudwatch-actions"
import { Stats, TreatMissingData } from "aws-cdk-lib/aws-cloudwatch"

export interface KoskiAuditLogsIntegrationStackProps extends StackProps {
  serviceAuditLogGroup: LogGroup
  alarmsSnsTopic: ITopic
  koski: {
    region: string
    account: string
  }
}

export class KoskiAuditLogsIntegrationStack extends Stack {
  constructor(
    scope: Construct,
    id: string,
    props: KoskiAuditLogsIntegrationStackProps,
  ) {
    super(scope, id, props)
    const { serviceAuditLogGroup, alarmsSnsTopic, koski } = props

    const koski_role_arn = `arn:aws:iam::${koski.account}:role/kitu-sqs-sender`
    const koski_sqs_queue_url = `https://sqs.${koski.region}.amazonaws.com/${koski.account}/oma-opintopolku-loki-audit-queue`

    const sendAuditLogsToKoskiLambda = new NodejsFunction(this, "function", {
      runtime: Runtime.NODEJS_LATEST,
      entry: path.join(__dirname, "koski-audit-logs-integration/handler.ts"),
      environment: {
        KOSKI_SQS_QUEUE_URL: koski_sqs_queue_url,
        KOSKI_ROLE_ARN: koski_role_arn,
      },
      initialPolicy: [
        new PolicyStatement({
          sid: "AllowSendKoskiSts",
          effect: Effect.ALLOW,
          actions: ["sts:AssumeRole"],
          resources: [koski_role_arn],
        }),
      ],
    })

    new SubscriptionFilter(this, "sendAuditLogsToKoskiSubscriptionFilter", {
      logGroup: serviceAuditLogGroup,
      filterName: "sendAuditLogsToKoski",
      // We only send audit logs to koski, that are in OPH standardized format.
      // The format is checking by this filter - if the event contains word "operation",
      // it will be passed to lambda
      filterPattern: FilterPattern.anyTerm("operation"),
      destination: new aws_logs_destinations.LambdaDestination(
        sendAuditLogsToKoskiLambda,
      ),
    })

    const auditQueueParam = new StringParameter(
      this,
      "KoskiAuditLogsIntegrationParams",
      {
        parameterName:
          "/kitu/koski-integration/oma-opintopolku-loki-audit-queue",
        stringValue: `https://sqs.${koski.region}.amazonaws.com/${koski.account}/oma-opintopolku-loki-audit-queue`,
      },
    )

    auditQueueParam.grantRead(sendAuditLogsToKoskiLambda)

    const errorsAlarm = sendAuditLogsToKoskiLambda
      .metricErrors({ period: Duration.minutes(5), statistic: Stats.SUM })
      .createAlarm(this, "SendAuditLogsToKoskiErrors", {
        alarmDescription:
          "Auditlokien välitys KOSKI-järjestelmän SQS-jonoon epäonnistuu",
        threshold: 1,
        evaluationPeriods: 1,
        treatMissingData: TreatMissingData.NOT_BREACHING,
      })
    errorsAlarm.addAlarmAction(new SnsAction(alarmsSnsTopic))
    errorsAlarm.addOkAction(new SnsAction(alarmsSnsTopic))
  }
}
