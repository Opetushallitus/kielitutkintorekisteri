import * as cdk from "aws-cdk-lib"
import {
  aws_ecs,
  aws_secretsmanager,
  CfnOutput,
  Duration,
  Stack,
  StackProps,
} from "aws-cdk-lib"
import {
  Certificate,
  CertificateValidation,
} from "aws-cdk-lib/aws-certificatemanager"
import { IAlarmAction, TreatMissingData } from "aws-cdk-lib/aws-cloudwatch"
import { SnsAction } from "aws-cdk-lib/aws-cloudwatch-actions"
import { IVpc, SecurityGroup } from "aws-cdk-lib/aws-ec2"
import {
  Cluster,
  ContainerDependencyCondition,
  ContainerImage,
  ContainerInsights,
  LogDriver,
  Secret,
} from "aws-cdk-lib/aws-ecs"
import { ApplicationLoadBalancedFargateService } from "aws-cdk-lib/aws-ecs-patterns"
import {
  ApplicationLoadBalancer,
  ApplicationProtocol,
  HttpCodeTarget,
  SslPolicy,
} from "aws-cdk-lib/aws-elasticloadbalancingv2"
import { ILogGroup, LogGroup } from "aws-cdk-lib/aws-logs"
import { DatabaseCluster } from "aws-cdk-lib/aws-rds"
import { HostedZone } from "aws-cdk-lib/aws-route53"
import { ITopic } from "aws-cdk-lib/aws-sns"
import { Construct } from "constructs"
import { ManagedPolicy, PolicyStatement } from "aws-cdk-lib/aws-iam"
import * as s3 from "aws-cdk-lib/aws-s3"
import { StringParameter } from "aws-cdk-lib/aws-ssm"

export interface ServiceStackProps extends StackProps {
  auditLogGroup: ILogGroup
  logGroup: ILogGroup
  image: ContainerImage
  name: string
  domainName: string
  serviceSecurityGroup: SecurityGroup
  loadBalancerSecurityGroup: SecurityGroup
  vpc: IVpc
  database: DatabaseCluster
  databaseName: string
  alarmSnsTopic: ITopic
  investigationActions: IAlarmAction[]
  productionQuality: boolean
}

export class ServiceStack extends Stack {
  readonly service: ApplicationLoadBalancedFargateService

  constructor(scope: Construct, id: string, props: ServiceStackProps) {
    super(scope, id, props)

    // Default Postgres DB user: https://docs.aws.amazon.com/cdk/api/v2/docs/aws-cdk-lib.aws_rds.DatabaseCluster.html#credentials
    const dbUser = "postgres"

    const zone = HostedZone.fromLookup(this, "Zone", {
      domainName: props.domainName,
    })

    const certificate = new Certificate(this, "Certificate", {
      domainName: props.domainName,
      validation: CertificateValidation.fromDns(zone),
    })

    const loadBalancer = new ApplicationLoadBalancer(this, "LoadBalancer", {
      vpc: props.vpc,
      securityGroup: props.loadBalancerSecurityGroup,
      internetFacing: true,
    })

    const tehtavapankkiBucket = new s3.Bucket(this, "Kitu-Bucket", {
      bucketName: `kitu-bucket-${props.name}`,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      enforceSSL: true,
      versioned: true,
      lifecycleRules: [
        {
          noncurrentVersionExpiration: cdk.Duration.days(30),
          expiredObjectDeleteMarker: true,
          abortIncompleteMultipartUploadAfter: cdk.Duration.days(1),
        },
      ],
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    })

    const snsAction = new SnsAction(props.alarmSnsTopic)
    const alarm5xx = loadBalancer.metrics
      .httpCodeTarget(HttpCodeTarget.TARGET_5XX_COUNT)
      .createAlarm(this, "LoadBalancer5xxAlarm", {
        evaluationPeriods: 1,
        threshold: 4,
        treatMissingData: TreatMissingData.NOT_BREACHING,
      })
    alarm5xx.addAlarmAction(snsAction, ...props.investigationActions)
    alarm5xx.addOkAction(snsAction)

    const cluster = new Cluster(this, "Cluster", {
      containerInsightsV2: props.productionQuality
        ? ContainerInsights.ENHANCED
        : ContainerInsights.DISABLED,
      vpc: props.vpc,
    })

    new CfnOutput(this, "ClusterArn", { value: cluster.clusterArn })
    new CfnOutput(this, "ClusterName", { value: cluster.clusterName })

    this.service = new ApplicationLoadBalancedFargateService(this, "Kitu", {
      cluster,
      securityGroups: [props.serviceSecurityGroup],
      loadBalancer: loadBalancer,
      taskImageOptions: {
        image: props.image,
        containerPort: 8080,
        logDriver: LogDriver.awsLogs({
          logGroup: props.logGroup,
          streamPrefix: "kitu",
        }),
        environment: {
          SPRING_PROFILES_ACTIVE: props.name,
          DATABASE_URL: `jdbc:postgresql://${props.database.clusterEndpoint.socketAddress}/${props.databaseName}`,
          DATABASE_USER: dbUser,
          AUDIT_LOG_LOG_GROUP_NAME: props.auditLogGroup.logGroupName,
        },
        secrets: {
          DATABASE_PASSWORD: Secret.fromSecretsManager(
            props.database.secret!,
            "password",
          ),
          KIELITESTI_TOKEN: aws_ecs.Secret.fromSecretsManager(
            aws_secretsmanager.Secret.fromSecretNameV2(
              this,
              "KielitestiToken",
              "kielitesti-token",
            ),
          ),
          PALVELUKAYTTAJA_PASSWORD: aws_ecs.Secret.fromSecretsManager(
            aws_secretsmanager.Secret.fromSecretNameV2(
              this,
              "PalvelukayttajaPassword",
              "palvelukayttaja-password",
            ),
          ),
          YKI_API_PASSWORD: aws_ecs.Secret.fromSecretsManager(
            aws_secretsmanager.Secret.fromSecretNameV2(
              this,
              "YkiApiPassword",
              "yki-api-password",
            ),
          ),
          YKI_API_USER: aws_ecs.Secret.fromSecretsManager(
            aws_secretsmanager.Secret.fromSecretNameV2(
              this,
              "YkiApiUser",
              "yki-api-user",
            ),
          ),
          PALVELUKAYTTAJA_OAUTH2_SECRET: aws_ecs.Secret.fromSecretsManager(
            aws_secretsmanager.Secret.fromSecretNameV2(
              this,
              "PalvelukayttajaOAuthPassword",
              "palvelukayttaja-oauth-password",
            ),
          ),
          // Käännösavainten synkronointi Tolgee Cloudiin ajetaan vain QA:sta.
          ...(props.name === "qa"
            ? {
                TOLGEE_API_KEY: aws_ecs.Secret.fromSecretsManager(
                  aws_secretsmanager.Secret.fromSecretNameV2(
                    this,
                    "TolgeeApiKey",
                    "tolgee-api-key",
                  ),
                ),
              }
            : {}),
        },
      },
      cpu: 2048,
      memoryLimitMiB: 4096,
      healthCheckGracePeriod: Duration.seconds(40),
      circuitBreaker: {
        enable: true,
        rollback: true,
      },
      domainName: props.domainName,
      domainZone: zone,
      protocol: ApplicationProtocol.HTTPS,
      redirectHTTP: true,
      certificate,
      sslPolicy: SslPolicy.RECOMMENDED_TLS,
    })

    // The default target group health check for ALBs is {healthyThresholdCount: 5, interval: Duration.seconds(30)}.
    // This means that a deployment takes at least 5*30 seconds = 2 minutes 30 seconds per container.
    this.service.targetGroup.configureHealthCheck({
      enabled: true,
      healthyThresholdCount: 2,
      interval: Duration.seconds(10),
      path: "/kielitutkinnot/actuator/health",
    })

    // The default load balancer configuration waits 300 seconds (5 minutes) before moving a container to UNUSED state.
    // This means that a deployment can take 300 seconds to wait for a container to shut down.
    // This value should be low enough that deployments are fast, and high enough that any large file transfers succeed.
    // This only affects connections that are open when a new deployment occurs.
    this.service.targetGroup.setAttribute(
      "deregistration_delay.timeout_seconds",
      "5",
    )

    // https://github.com/aws-observability/aws-otel-collector/blob/main/config.yaml
    const otelCollectorConfig = new StringParameter(
      this,
      "OtelCollectorConfig",
      {
        stringValue: `
extensions:
  health_check:

receivers:
  otlp:
    protocols:
      grpc:
        endpoint: 0.0.0.0:4317
      http:
        endpoint: 0.0.0.0:4318
  awsxray:
    endpoint: 0.0.0.0:2000
    transport: udp

processors:
  batch/traces:
    timeout: 1s
    send_batch_size: 50
  batch/metrics:
    timeout: 60s
  groupbytrace:
  tail_sampling:
    policies:
      - name: default
        type: always_sample
      - name: health checks
        type: and
        and:
          and_sub_policy:
            - name: route-health-checks
              type: string_attribute
              string_attribute:
                key: http.url
                values: [ /actuator/health ]
            - name: probabilistic-policy
              type: probabilistic
              probabilistic:
                sampling_percentage: 0.1           

exporters:
  awsxray:
  awsemf:

service:
  pipelines:
    traces:
      receivers: [otlp,awsxray]
      processors: [groupbytrace, tail_sampling, batch/traces]
      exporters: [awsxray]
    metrics:
      receivers: [otlp]
      processors: [batch/metrics]
      exporters: [awsemf]

  extensions: [health_check]
      `,
      },
    )

    const otelCollectorContainer = this.service.taskDefinition.addContainer(
      "AwsOtelCollector",
      {
        image: ContainerImage.fromRegistry(
          // renovate: datasource=docker
          "public.ecr.aws/aws-observability/aws-otel-collector:v0.47.0",
        ),
        secrets: {
          // https://aws-otel.github.io/docs/setup/ecs/config-through-ssm
          AOT_CONFIG_CONTENT: Secret.fromSsmParameter(otelCollectorConfig),
        },
        logging: LogDriver.awsLogs({
          logGroup: props.logGroup,
          streamPrefix: "AwsOtelCollector",
        }),
      },
    )

    // ECS pysäyttää konttiriippuvuudet käänteisessä järjestyksessä, joten kun
    // app-kontti riippuu sidecarista, sidecar pysäytetään vasta app-kontin
    // jälkeen. Näin Spring-shutdownin viimeinen Micrometer-OTLP-flush osuu
    // vielä elossa olevaan localhost:4318:aan, eikä ECS-deploysta synny
    // 'Connection refused' -varoituksia.
    this.service.taskDefinition.defaultContainer!.addContainerDependencies({
      container: otelCollectorContainer,
      condition: ContainerDependencyCondition.START,
    })

    // EMF exporter-created log group.
    // See: https://aws-otel.github.io/docs/getting-started/cloudwatch-metrics#cloudwatch-emf-exporter-awsemf
    // This is configured in the default ADOT config file.
    // Another option would be to disable the EMF exporter.
    const metricsLogGroup = LogGroup.fromLogGroupName(
      this,
      "MetricsLogGroup",
      "/metrics/kitu",
    )
    metricsLogGroup.grant(
      this.service.taskDefinition.taskRole,
      "logs:CreateLogGroup",
    )
    metricsLogGroup.grantWrite(this.service.taskDefinition.taskRole)

    props.auditLogGroup.grantWrite(this.service.taskDefinition.taskRole)

    // Ref: https://docs.aws.amazon.com/aws-managed-policy/latest/reference/AWSXrayWriteOnlyAccess.html
    this.service.taskDefinition.taskRole.addManagedPolicy(
      ManagedPolicy.fromAwsManagedPolicyName("AWSXrayWriteOnlyAccess"),
    )

    const cpuAlarm = this.service.service
      .metricCpuUtilization()
      .createAlarm(this, "CpuUtilization", {
        threshold: 50,
        evaluationPeriods: 1,
      })
    cpuAlarm.addAlarmAction(snsAction, ...props.investigationActions)

    const memoryAlarm = this.service.service
      .metricMemoryUtilization()
      .createAlarm(this, "MemoryUtilization", {
        threshold: 50,
        evaluationPeriods: 1,
      })
    memoryAlarm.addAlarmAction(snsAction, ...props.investigationActions)

    const taskRole = this.service.taskDefinition.taskRole
    tehtavapankkiBucket.grantRead(taskRole)
    tehtavapankkiBucket.grantPut(taskRole)
    taskRole.addToPrincipalPolicy(
      new PolicyStatement({
        actions: ["s3:DeleteObject"],
        resources: [tehtavapankkiBucket.arnForObjects("*")],
      }),
    )
  }
}
