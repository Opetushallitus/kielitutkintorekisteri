import { Stage, StageProps } from "aws-cdk-lib"
import { ContainerImage } from "aws-cdk-lib/aws-ecs"
import { Construct } from "constructs"
import { EnvironmentConfig } from "./accounts"
import { AlarmsStack } from "./alarms-stack"
import { ConnectionsStack } from "./connections-stack"
import { DbStack } from "./db-stack"
import { DnsStack } from "./dns-stack"
import { GithubActionsStack } from "./github-actions-stack"
import { LogGroupsStack } from "./log-groups-stack"
import { NetworkStack } from "./network-stack"
import { Route53HealthChecksStack } from "./route53-health-checks-stack"
import { ServiceStack } from "./service-stack"
import { BackupsStack } from "./backups-stack"
import { BackupResource } from "aws-cdk-lib/aws-backup"
import { KoskiAuditLogsIntegrationStack } from "./koski-audit-logs-integration-stack"

interface EnvironmentStageProps extends StageProps {
  environmentConfig: EnvironmentConfig
  serviceImage: ContainerImage
}

export class EnvironmentStage extends Stage {
  constructor(scope: Construct, id: string, props: EnvironmentStageProps) {
    super(scope, id, props)

    const { env, environmentConfig } = props

    new GithubActionsStack(this, "GithubActions", {
      env,
    })

    // us-east-1 must be created first so the primary stack can subscribe its
    // topics to the single shared Slack configuration.
    const usEastAlarmsStack = new AlarmsStack(this, "AlarmsUsEast1", {
      env: { ...env, region: "us-east-1" },
      automaticInvestigations: environmentConfig.automaticInvestigations,
    })
    const alarmsStack = new AlarmsStack(this, "Alarms", {
      env,
      automaticInvestigations: environmentConfig.automaticInvestigations,
      slack: {
        workspaceId: environmentConfig.slackWorkspaceId,
        alarmsChannel: environmentConfig.slackAlarmsChannel,
        infoChannel: environmentConfig.slackInfoChannel,
        additionalAlarmTopics: [usEastAlarmsStack.alarmSnsTopic],
        additionalInfoTopics: [usEastAlarmsStack.infoSnsTopic],
      },
    })

    new DnsStack(this, "Dns", {
      env,
      name: environmentConfig.domainName,
    })

    const logGroupsStack = new LogGroupsStack(this, "LogGroups", {
      env,
      alarmsSnsTopic: alarmsStack.alarmSnsTopic,
      infoSnsTopic: alarmsStack.infoSnsTopic,
      investigationActions: alarmsStack.investigationActions,
      ykiSuoritusEmailRecipients: environmentConfig.ykiSuoritusEmailRecipients,
    })

    const networkStack = new NetworkStack(this, "Network", {
      env,
      cidrBlock: environmentConfig.network.cidr,
      maxAzs: environmentConfig.network.maxAzs,
    })

    const connectionsStack = new ConnectionsStack(this, "Connections", {
      env,
      vpc: networkStack.vpc,
    })

    const dbStack = new DbStack(this, "Database", {
      env,
      vpc: networkStack.vpc,
      databaseName: environmentConfig.databaseName,
      productionQuality: environmentConfig.productionQuality,
    })

    connectionsStack.databaseSG = dbStack.cluster.connections.securityGroups[0]

    new ServiceStack(this, "Service", {
      env,
      name: environmentConfig.name,
      domainName: environmentConfig.domainName,
      serviceSecurityGroup: connectionsStack.serviceSG,
      loadBalancerSecurityGroup: connectionsStack.loadBalancerSG,
      logGroup: logGroupsStack.serviceLogGroup,
      auditLogGroup: logGroupsStack.serviceAuditLogGroup,
      vpc: networkStack.vpc,
      database: dbStack.cluster,
      databaseName: environmentConfig.databaseName,
      image: props.serviceImage,
      alarmSnsTopic: alarmsStack.alarmSnsTopic,
      investigationActions: alarmsStack.investigationActions,
      productionQuality: environmentConfig.productionQuality,
    })

    new Route53HealthChecksStack(this, "Route53HealthChecks", {
      env: { ...env, region: "us-east-1" },
      domainName: environmentConfig.domainName,
      alarmsSnsTopic: usEastAlarmsStack.alarmSnsTopic,
      investigationActions: usEastAlarmsStack.investigationActions,
    })

    new BackupsStack(this, "Backups", {
      env,
      resources: [BackupResource.fromRdsServerlessCluster(dbStack.cluster)],
      notificationTopic: alarmsStack.alarmSnsTopic,
    })

    new KoskiAuditLogsIntegrationStack(this, "KoskiAuditLogsIntegration", {
      env,
      serviceAuditLogGroup: logGroupsStack.serviceAuditLogGroup,
      koski: environmentConfig.koski,
    })

    connectionsStack.createRules()
  }
}
