# kitu infrastructure

AWS CDK (TypeScript) app that provisions everything kielitutkintorekisteri needs.
The code under `lib/` defines the stacks; `bin/infra.ts` wires them into stages.

When you change anything in this directory, keep this file accurate — see CLAUDE.md.

## AWS account layout

| Stage  | Account        | Region(s)                 | Purpose                                                                |
| ------ | -------------- | ------------------------- | ---------------------------------------------------------------------- |
| `Util` | `961341524988` | `eu-west-1`               | Shared ECR repo + GitHub Actions OIDC trust + Util-level Slack alarms  |
| `Dev`  | `682033502734` | `eu-west-1` + `us-east-1` | Untuva/dev environment                                                 |
| `Test` | `961341546901` | `eu-west-1` + `us-east-1` | QA environment                                                         |
| `Prod` | `515966535475` | `eu-west-1` + `us-east-1` | Production. Reader replica, deletion protection, performance insights. |

The `us-east-1` footprint per env is small: Route53 health checks (must be global) and
a slim `AlarmsStack` that fronts them. Everything else is in `eu-west-1`.

## Project layout

```
infra/
├── bin/infra.ts                     # CDK app: instantiates Util + Dev/Test/Prod stages
└── lib/
    ├── accounts.ts                  # Account IDs, CIDR blocks, Slack channels, KOSKI accounts
    ├── env.ts                       # getEnv(): fetch required env var or throw
    ├── account-settings-stack.ts    # Account-wide S3 Block Public Access (Util + every env)
    ├── utility-stage.ts             # Util stage (composes the stacks below)
    ├── container-repository-stack.ts  # ECR repo `kitu` + cross-account pull grants
    ├── github-actions-stack.ts      # OIDC provider + deploy role + PR diff role
    ├── environment-stage.ts         # Composes the stacks below per env
    ├── network-stack.ts             # VPC + subnets
    ├── connections-stack.ts         # Security groups (two-phase: createRules() at end)
    ├── db-stack.ts                  # Aurora Postgres ServerlessV2
    ├── service-stack.ts             # ECS Fargate service + ALB + OTel sidecar
    ├── dns-stack.ts                 # Looks up pre-existing Route53 zone
    ├── route53-health-checks-stack.ts  # us-east-1 health check + alarm
    ├── alarms-stack.ts              # SNS topics + Chatbot Slack + AIOps investigation group
    ├── log-groups-stack.ts          # Log groups + LogErrors/LogWarnings filters + alarms,
    │                                #   Transaction Search, PostYkiSuoritus alarm + notifications
    ├── yki-suoritus-notifications/  # Lambda source for the YKI suoritus notifications
    ├── backups-stack.ts             # AWS Backup vault for the Aurora cluster
    ├── koski-audit-logs-integration-stack.ts  # Subscription filter + Lambda → KOSKI SQS
    └── koski-audit-logs-integration/  # Lambda source for the above
```

`infra/scripts/` holds helpers that belong to the CDK app rather than to the
server: `audit-deployed-stacks.sh` + `cdk-app-inventory.mjs` (see _Stack
lifecycle_ below), and `presign-yki-historia-upload.mjs`, which
`scripts/yki_historia_upload_presign.sh` calls to mint presigned PUT URLs for
the retained YKI-historia bucket (see _Prod-only stacks_ below).

## Util stage (`bin/infra.ts`, `lib/utility-stage.ts`)

Created once, in the shared util account. Four things live here:

- **`Util/AccountSettings`** — account-wide S3 Block Public Access, the same
  stack every env account gets (see [Account settings](#account-settings)).
- **`Util/ImageBuilds`** — ECR repo `kitu` with `IMMUTABLE` tags and scan-on-push.
  The repo grants pull to each env account principal and pull+push to the GitHub
  Actions deployment role. CI builds an image, pushes it here, then env deploys
  resolve the image from ECR by `TAG` env var (`bin/infra.ts:23-26`).
- **`Util/Alarms`** — its own `AlarmsStack` for image-scan findings + Util-level
  Slack notifications.
- **`Util/GithubActions`** — the OIDC provider, the deployment role and the PR
  diff role (see [GitHub Actions roles](#github-actions-roles)). In Util the
  deployment role also trusts `ref:refs/heads/main`, because `build_image`
  pushes to ECR from main without an environment.

## Environment stages (`lib/environment-stage.ts`)

Each of `Dev`, `Test`, `Prod` instantiates the same set of stacks in roughly this
order (because of dependencies):

1. **`AccountSettings`** — account-wide S3 Block Public Access, see
   [Account settings](#account-settings). No dependencies on anything else.
2. **`GithubActions`** — per-env CDK deploy role, CloudWatch log-tail permission
   and the read-only `cloudformation:ListStacks` / `lambda:ListFunctions` used by
   the weekly audit, plus the read-only PR diff role. Trusted via OIDC; see
   [GitHub Actions roles](#github-actions-roles).
3. **`AlarmsUsEast1`** (region: `us-east-1`) and **`Alarms`** (env region) — see
   the Monitoring section below. The us-east-1 stack is created first so the
   primary stack can subscribe its SNS topics to the single shared Slack
   `SlackChannelConfiguration` (Chatbot allows only one per Slack channel per
   account).
4. **`Dns`** — `HostedZone.fromLookup(...)` against a **pre-existing** Route53
   zone. The zone is created manually to avoid CDK ever destroying it.
5. **`LogGroups`** — `KituService` and `KituServiceAudit` log groups, the
   `KituServiceDataProtectionAudit` group (with a Finnish-SSN data-protection
   policy applied to the service log group), the customer-managed KMS key
   `alias/kitu/logs` that encrypts all three (see
   [Log group encryption](#log-group-encryption)), `LogErrors`/`LogWarnings` metric
   filters and alarms, CloudWatch Transaction Search wiring for X-Ray spans,
   and the YKI-suoritus Slack notifications (`YkiSuoritusAlarm` + an EventBridge
   rule + the `YkiSuoritusNotifications` Lambda, plus the optional
   `YkiSuoritusEmailTopic` for emailed summaries).
6. **`Network`** — VPC with the per-env CIDR (Dev `10.15.0.0/18`, Test
   `10.15.64.0/18`, Prod `10.15.128.0/18`) and 2 AZs in dev / 3 AZs in test+prod.
7. **`Connections`** — three empty SGs (`serviceSG`, `loadBalancerSG`,
   `databaseSG`). Rules are added in `createRules()` at the very end of the
   stage, after the DB is created and `databaseSG` is populated. This deferred
   wiring avoids a circular dependency between stacks.
8. **`Database`** — Aurora Postgres ServerlessV2. Always one writer; prod
   additionally gets a reader replica with auto-scaling, deletion protection,
   and Performance Insights. The cluster is in private subnets and only the
   Service stack's task security group is granted ingress on `5432`. No
   network path for a developer is wired today: if you need a psql session,
   add a bastion or `aws ecs run-task`-based tunnel as a separate stack.
   For SQL without a network path, `enableDataApi: true` is set, so
   `aws rds-data execute-statement` / `batch-execute-statement` reach the
   cluster over the AWS API (IAM + the cluster secret, no VPC route). That is
   how the YKI historia karanteenitaulu was populated during the 2011–2016
   migration (`scripts/load_yki_historia_siirtymattomat.py`); it is IAM-gated, audited in
   CloudTrail, and deliberately not used for anything in the register itself.
9. **`Service`** — the application Fargate service, see below.
10. **`Route53HealthChecks`** (region: `us-east-1`) — HTTPS check against the
    public domain; alarm wired to `usEastAlarmsStack.investigationActions`.
11. **`Backups`** — AWS Backup plan that protects the Aurora cluster; backup-job
    state changes go to the alarm SNS topic.
12. **`KoskiAuditLogsIntegration`** — a CloudWatch Logs subscription filter on
    the audit log group → Lambda → SQS in the KOSKI account. The Lambda assumes
    a role (`kitu-sqs-sender`) that the KOSKI account creates with a matching
    trust policy.

### Prod-only stacks

There are none. `Prod/YkiHistoriaUpload` (`lib/yki-historia-upload-stack.ts`)
used to be wired directly in `bin/infra.ts` as a child of the `Prod` stage,
providing the S3 bucket `kitu-yki-historia-upload-prod` that an external
organization uploaded the YKI history export to via presigned PUT URLs. It was
removed once the 2011–2016 migration had been run. The presign helper and the
migration scripts were removed with it and restored on 14.10.2026 for the
Solki–kitu count reconciliation; the stack itself was not restored, since the
bucket still exists (see `scripts/yki-historia-migration.md`).

**The bucket had `removalPolicy: RETAIN`, so deleting the CloudFormation stack
leaves it behind — holding the source export, which contains henkilötunnuksia,
names and addresses.** Deleting the stack is therefore not enough: decide
explicitly whether the bucket and its versions should be emptied and deleted,
and do that deliberately rather than leaving an unmanaged bucket of personal
data behind. See _Stack lifecycle_ for why an orphaned stack is a hazard.

**Deleting the stack also deleted the bucket's TLS-only policy.** CDK creates
the `enforceSSL` statement as a separate `AWS::S3::BucketPolicy` whose removal
policy defaults to `DESTROY`, independent of the bucket's own `RETAIN`.
Versioning, encryption, the public access block and the lifecycle rule are
bucket properties and stayed; the policy is its own resource and went with the
stack. The bucket had no policy at all (Security Hub S3.5 failing) until
28.10.2026, when the policy below was put back by hand. If it ever goes missing
again, re-apply it with prod credentials; check `get-bucket-policy` first and
merge rather than overwrite if some other policy has appeared:

```bash
aws s3api get-bucket-policy --bucket kitu-yki-historia-upload-prod
aws s3api put-bucket-policy --bucket kitu-yki-historia-upload-prod --policy '{
  "Version": "2012-10-17",
  "Statement": [{
    "Sid": "AllowSSLRequestsOnly",
    "Effect": "Deny",
    "Principal": "*",
    "Action": "s3:*",
    "Resource": [
      "arn:aws:s3:::kitu-yki-historia-upload-prod",
      "arn:aws:s3:::kitu-yki-historia-upload-prod/*"
    ],
    "Condition": { "Bool": { "aws:SecureTransport": "false" } }
  }]
}'
```

`presign-yki-historia-upload.mjs` mints `https` URLs, so uploads keep working.
The same trap applies to any retained bucket: when a stack that owns one is
deleted, re-check its bucket policy.

## Account settings

`AccountSettings` (`lib/account-settings-stack.ts`) is instantiated once per
account — in Util and in each env stage — in the stage's primary region. It
holds settings that are account-wide rather than regional, so they must never
go into a stack that exists in both `eu-west-1` and `us-east-1` (such as
`AlarmsStack`).

- **S3 Block Public Access at account level** (Security Hub control S3.1,
  `s3-account-level-public-access-blocks-periodic`; OPHKOTO-92): all four
  settings on. CloudFormation has no resource type for this setting, so it is
  an `AwsCustomResource` calling `S3Control.PutPublicAccessBlock`. It has no
  `onDelete`, so removing the stack leaves the protection on. It runs only when
  the resource is created or its parameters change — a manual change in the
  console is not undone by the next deploy, the Config rule is what catches
  that. Presigned URLs keep working: they are signed requests, not public
  access. Check with
  `aws s3control get-public-access-block --account-id <account>`.

The custom resource's singleton Lambda takes its runtime from CDK like every
other CDK-provided Lambda, see _Stack lifecycle_.

## Service stack details (`lib/service-stack.ts`)

- ALB with HTTP→HTTPS redirect; certificate from ACM with DNS validation against
  the looked-up zone.
- Task definition: 2048 CPU, 4096 MiB memory; deployment circuit breaker with
  rollback enabled.
- Container image: resolved by `TAG` env var; image lives in the Util account
  ECR repo and is pulled across accounts via the resource policy granted in
  `container-repository-stack.ts`.
- Secrets pulled at task start from Secrets Manager: `kielitesti-token`,
  `palvelukayttaja-password`, `yki-api-password`, `yki-api-user`,
  `palvelukayttaja-oauth-password`, plus `tolgee-api-key` **in the `qa` env
  only** (`props.name === "qa"`). **These must exist in the env account
  before deploy** — CDK doesn't create them. The DB password is sourced
  from the auto-generated cluster secret.
- `tolgee-api-key` is what scopes the UI-translation key sync to QA: the app
  gates the whole `i18n/tolgee` package on `kitu.tolgee.apiKey` being non-empty,
  so envs without the secret never write to Tolgee. See the main README's
  "Avainten synkronointi Tolgeehen".
- OTel sidecar: collector config stored as an SSM parameter
  (`/kitu/otel-config-<stack-id>`), loaded into the sidecar via
  `AOT_CONFIG_CONTENT`. Tail sampling: 0.1% for the health check route,
  always-sample for everything else. Traces export to X-Ray; metrics export
  to CloudWatch via EMF.
- Health check: 2 successful pings × 10s = 20s to healthy. Deployment drain
  timeout 5s.
- Alarms attached: 5xx count ≥ 4 over the period; CPU > 50%; memory > 50%.
  All three trigger SNS, plus an investigation when `automaticInvestigations`
  is on for the env.
- An S3 bucket for `tehtavapankki` (`kitu-bucket-<env>`) is created and the
  task role gets read, put and `s3:DeleteObject` on it (write for the
  Koealusta-import scheduled task, read for the virkailija-facing listing page
  and signed download URLs). The bucket blocks public access, denies non-TLS
  requests (`enforceSSL`, Security Hub S3.5) and is versioned with a lifecycle
  rule that expires noncurrent versions after 30 days, cleans up expired
  delete markers and aborts incomplete multipart uploads after a day.
- **MFA Delete is deliberately not used (OPHKOTO-92, Security Hub S3.20).**
  Only the account's root user can enable it, only through the CLI, and S3
  doesn't allow it on a bucket that has a lifecycle configuration. The nightly
  import deletes same-content re-downloads (`removeDuplicates`) and overwrites
  assets, so without lifecycle every old version would be kept forever. The
  compensating control: versioning makes every delete and overwrite
  recoverable for 30 days, and the task role is granted `s3:DeleteObject`
  rather than `s3:DeleteObject*`, so the application identity can't
  permanently delete a version — only the lifecycle rule does that. S3.20
  doesn't evaluate buckets that have a lifecycle configuration, by AWS's own
  design. Don't widen the grant back to `grantReadWrite`/`grantDelete`: both
  include `s3:DeleteObjectVersion`.

## Monitoring, alarms, investigations, Slack

How CloudWatch alarms, Amazon Q investigations, log-based metrics, and the Slack
feed fit together.

### End-to-end flow

```
log line → metric filter → CloudWatch metric → CloudWatch alarm
                                                  │
                                                  ├─► SNS topic ─► Chatbot ─► Slack
                                                  │
                                                  └─► AIOps investigation group
                                                                ├─► SNS topic ─► Chatbot ─► Slack
                                                                └─► Q-curated message ─► Chatbot ─► Slack
```

When an alarm transitions to `ALARM`:

1. CloudWatch publishes the alarm payload to the SNS alarm topic. AWS Chatbot is
   subscribed to that topic and forwards a templated message to Slack.
2. The same alarm has the AIOps investigation group ARN as a second action.
   CloudWatch calls `aiops:CreateInvestigation` on the group (allowed by the
   group's resource policy that grants
   `aiops.alarms.cloudwatch.amazonaws.com`). **This step only happens when
   `automaticInvestigations` is enabled — see below.**
3. Q investigates in the background. While doing so, it pushes events both to
   the investigation group's SNS topic and directly to the configured chat
   channels (`chatConfigurationArns`). The latter is what produces the
   Q-curated summary in Slack.

### Feature flag: `automaticInvestigations`

`EnvironmentConfig.automaticInvestigations` (`lib/accounts.ts`) decides whether
alarms auto-start an Amazon Q investigation. **It is `false` in every env
today** — investigations run Logs Insights queries and metric scans that are
billed as CloudWatch usage, and the cost was disproportionate to the value.

What the flag does and does not change:

- **Off** — `AlarmsStack.investigationActions` is an empty array, so no alarm
  gets the investigation group ARN as an action. Every alarm still publishes to
  SNS and still reaches Slack; only the Q auto-start is gone.
- **On** — the array holds the single `InvestigationGroupAlarmAction`, spread
  into each `addAlarmAction(...)` call.
- Either way the investigation group, its role, its resource policy and the SNS
  grants are created as before, so an investigation can still be started **by
  hand** from the CloudWatch console. Flipping the flag back on is a pure alarm
  change — no group is recreated and no investigation history is lost.

To re-enable for one env, set `automaticInvestigations: true` on that env in
`lib/accounts.ts` and deploy; the diff is limited to `AlarmActions` on the
alarms in `LogGroups`, `Service` and `Route53HealthChecks`.

#### Why the investigation-group export is pinned

`AlarmsStack` ends with an explicit
`this.exportValue(this.investigationGroup.attrArn)`. **Leave it there** — it is
what makes the flag safe to flip in both directions, and it is deliberately
permanent.

Without it, turning the flag off removes the investigation-group ARN from every
consumer stack, and with it the CloudFormation export that carried the ARN
across stacks. CloudFormation refuses to delete an export while an
already-deployed stack still imports it, and CDK deploys the producer (`Alarms`
/ `AlarmsUsEast1`) **before** its consumers, so the producer's update rolls
back:

```
Cannot delete export Dev-AlarmsUsEast1:ExportsOutputFnGetAttInvestigationGroupArnECB47902
as it is in use by Dev-Route53HealthChecks
```

`exportValue` pins the export under the exact same name the automatic
cross-stack reference generates, so the export simply always exists:

- **Flag off** — the export is left dangling with no importers. An unused
  CloudFormation export carries no cost.
- **Flag on** — the automatic cross-stack reference and `exportValue` resolve to
  the same single output, so `exportValue` is a no-op and the consumers import
  the export that is already there.

The result is that flipping `automaticInvestigations` is a pure `AlarmActions`
change in either direction, with no export being created or deleted and no
two-step deploy to coordinate.

### AWS Health events

`alarms-stack.ts` also has an EventBridge rule (`AwsHealthScheduledChanges`)
that forwards `aws.health` events of category `scheduledChange` to the alarm SNS
topic, and from there to Slack via Chatbot. These are AWS's advance warnings
about things that will break if ignored: Lambda runtime end-of-support, RDS
engine end-of-life, certificate expiry. Without the rule they only appear in the
AWS Health Dashboard, which nobody reads — that is how six Lambdas ended up
stranded on `nodejs20.x` past its deprecation date.

Two deliberate choices:

- **Target is `alarmSnsTopic`, not `infoSnsTopic`.** Only prod defines a
  `slackInfoChannel` (`accounts.ts`), and the `us-east-1` stack reaches Slack
  only through `additionalAlarmTopics`. The alarm topic is the one topic that is
  subscribed in every instance of this stack.
- **Only `scheduledChange`.** Adding `accountNotification` would pull in things
  like Shield subscription renewal reminders and drown the signal.
- **Noise codes are excluded by `eventTypeCode`.** `OHITETTAVAT_HEALTH_TAPAHTUMAT`
  lists the `scheduledChange` events that need no action and are filtered out
  with `anything-but`. Currently only `AWS_ECS_TASK_PATCHING_RETIREMENT`:
  the ECS service replaces retired Fargate tasks on its own, and every `main`
  deploy replaces them anyway. Add a code there instead of widening or dropping
  the category filter.

Coverage comes for free: `AlarmsStack` is already instantiated in both
`eu-west-1` and `us-east-1` per env, plus once for `Util`. Health events for
global services are only delivered to `us-east-1`; regional ones go to their own
region. Both are covered.

### Stack layout

Per env (`Dev` / `Test` / `Prod`):

- **`<Env>/AlarmsUsEast1`** — SNS topics + investigation group in `us-east-1`.
  Used exclusively by Route53 health checks. It does _not_ own a Chatbot Slack
  channel; instead its SNS topics are subscribed to the primary stack's Slack
  channel via `additionalAlarmTopics` / `additionalInfoTopics`. (Chatbot only
  allows one `SlackChannelConfiguration` per channel per account, so the
  primary stack owns it and us-east-1 piggybacks.) Its investigation group is
  _not_ wired to a Slack chat configuration — Route53 investigations only emit
  SNS notifications. If more alarms are ever added in `us-east-1`, this needs
  to be revisited.
- **`<Env>/Alarms`** — Primary alarm infrastructure in the env's region. Owns
  the Chatbot `SlackChannelConfiguration`, the Investigation group with both
  SNS and Slack notification channels, and exposes
  `alarmSnsTopic` / `infoSnsTopic` / `investigationActions` for other stacks to
  attach to.
- **`<Env>/LogGroups`** — Service log groups + metric filters that turn
  structured log lines into CloudWatch metrics, and the `LogErrors` /
  `LogWarnings` alarms wired to those topics + investigation actions. It also
  enables Transaction Search and owns the informational YKI-suoritus
  notifications on the **info** topic. `YkiSuoritusAlarm`, driven by the
  `PostYkiSuoritus` metric filter over the `aws/spans` log group, only marks
  when a batch of arvioitu suoritukset starts (ALARM) and ends (OK); it has
  **no alarm actions**, because Chatbot's stock alarm card is too technical for
  the channel's audience. Instead the EventBridge rule
  `YkiSuoritusAlarmStateChange` invokes the `YkiSuoritusNotifications` Lambda
  (`lib/yki-suoritus-notifications/`), which publishes Chatbot **custom
  notifications** (`{"version":"1.0","source":"custom",…}`) to the info topic:
  a short "saapuu…" notice on ALARM, and on ALARM→OK a summary with the count
  grouped by tutkintopäivä, kieli, taso and arviointitila. The breakdown comes
  from a Logs Insights query over `aws/spans` for the batch window — the window
  starts at the alarm's previous OK transition (from `DescribeAlarmHistory`) or
  five minutes before the ALARM, whichever is later, so a batch split by a quiet
  minute is not counted twice. Insights is used rather than metric dimensions
  because metric filters allow at most three dimensions and every
  `tutkintopaiva` value would become a new billed custom metric. If the query
  returns nothing it is retried once after a minute, because freshly ingested
  spans can be missing from Insights for a while even after their
  `@ingestionTime` (seen in untuva on 6.10.2026); if it still fails, a summary
  without the breakdown is sent.

  The summary (not the "saapuu…" notice) can also go out by email: when the
  env's `ykiSuoritusEmailRecipients` in `lib/accounts.ts` is non-empty, the
  stack creates `YkiSuoritusEmailTopic` with one `email` subscription per
  address, and the Lambda publishes a plain-text version of the summary there
  (`EMAIL_TOPIC_ARN`). It is a separate topic because the info topic carries
  Chatbot JSON and other notices. **Each address must confirm the SNS
  subscription** from the AWS email it receives after the deploy, otherwise
  nothing is delivered. Mail comes from `no-reply@sns.amazonaws.com`. A failed
  email publish is only logged, so that the Lambda's async retry does not
  repost the Slack summary.

  The filter reads a span attribute (`arvioitu`) that the application sets via
  `Span.current()` in `YkiApiController`, next to `tutkintopaiva`,
  `tutkintokieli`, `tutkintotaso` and `arviointitila` that the Lambda's query
  groups by. Two traps live here, both of which
  have already silently broken this alarm once:

  1. Metric filter patterns cannot reference JSON keys containing periods, so
     OTel semconv attributes (`http.route`, `http.request.method`) are
     unreachable — only flat, dot-free keys work.
  2. A metric filter matches **one log record at a time**, and a single HTTP
     request produces several span records. `Span.current()` inside a controller
     is Spring Security's `secured request` INTERNAL span, _not_ the HTTP SERVER
     span that carries `uri` / `method`. A pattern requiring attributes from
     both spans matches nothing — and fails silently, since a metric that is
     never emitted is indistinguishable from "nothing happened" under
     `treatMissingData: NOT_BREACHING`.

  When changing this filter, validate the pattern against real records before
  deploying: `aws logs test-metric-filter --filter-pattern '<pattern>'
--log-event-messages file://samples.json`, with samples pulled from
  `aws/spans` via CloudWatch Logs Insights.

### Slack channels

Per `lib/accounts.ts`:

| Env  | Alarms channel                           | Info channel             |
| ---- | ---------------------------------------- | ------------------------ |
| dev  | `kielitutkintorekisteri-alerts-dev-test` | —                        |
| test | `kielitutkintorekisteri-alerts-dev-test` | —                        |
| prod | `kielitutkintorekisteri-alerts`          | `kielitutkintorekisteri` |

Workspace `T02C6SZL7KP` for all envs.

### IAM identities and policies involved

Four pieces, easy to confuse:

1. **Investigation group role** (`InvestigationGroupRole` in
   `alarms-stack.ts`). Assumed by `aiops.amazonaws.com`. Has
   `AIOpsAssistantPolicy` (managed). Used by Q itself to read service
   telemetry while running an investigation.
2. **Chatbot Slack channel role** (`SlackBot/ConfigurationRole`, auto-created
   by the CDK construct). Assumed by `chatbot.amazonaws.com`. We attach
   `AIOpsAssistantPolicy` to it explicitly — without that, Q can't call
   `aiops:GetInvestigation*` to fetch the data needed to render the curated
   Slack message, and the channel stays silent even though
   `chatConfigurationArns` is wired correctly.
3. **Investigation group resource policy** (`investigationGroupPolicy` on the
   `CfnInvestigationGroup`). Grants `aiops.alarms.cloudwatch.amazonaws.com`
   the right to call `CreateInvestigation` / `CreateInvestigationEvent` on
   this group, scoped to alarms in the same account/region. Without this,
   alarms silently fail to start investigations.
4. **Alarm SNS topic resource policy** (`alarmSnsTopic` in `alarms-stack.ts`).
   Grants `aiops.amazonaws.com` `sns:Publish` on the topic, with an
   `aws:SourceAccount` condition. Q publishes investigation events directly
   to whichever SNS topic is listed under the investigation group's
   `chatbotNotificationChannels` — without this grant, the publish is
   silently denied, so `CreateInvestigation` succeeds but no
   `CreateInvestigationEvent` ever follows and Slack stays empty even though
   an investigation was created. See
   [Investigations chat integration](https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/Investigations-Integrations.html#Investigations-Integrations-Chat-policy).

### Log-based metric filters (`lib/log-groups-stack.ts`)

The application logs JSON in ECS format
(`logging.structured.format.console=ecs`, level field is `log.level`).

- **`LogErrors`** — counts entries where `$.success == false` _or_
  `$.log.level == "ERROR"`. Threshold: **3**, evaluation period 1. Triggers
  SNS alarm (+ investigation when enabled). Earlier versions also matched on
  the existence
  of `$.stack_trace` / `$.error.type`; that produced false positives because
  Spring Boot includes those fields in any WARN log that carries a
  `Throwable` (SpringDoc and OTel emit such warnings during startup).
- **`LogWarnings`** — counts entries where `$.level == "WARN"` _or_
  `$.log.level == "WARN"` (`FilterPattern.any(...)`, so both the flat and the
  nested level field match). Threshold: **5**.

Add new metric/alarm pairs through the same
`addMetricFilter(...).metric(...).createAlarm(...)` chain and remember to
attach `SnsAction(alarmsSnsTopic)` and spread `...investigationActions` so they
reach Slack.

### Verifying live state

```bash
# Investigation groups (note: aws cli ≥ 2.34 required for `aws aiops`)
aws aiops list-investigation-groups --region eu-west-1 --profile <env>
aws aiops get-investigation-group --identifier <name> --region eu-west-1 --profile <env> \
  --query 'chatbotNotificationChannel'

# Chatbot Slack channel role + attached policies
aws chatbot describe-slack-channel-configurations --region us-east-1 --profile <env> \
  --query 'SlackChannelConfigurations[?ConfigurationName==`<channel-name>`].IamRoleArn'

# Alarm SNS topic policy — must list aiops.amazonaws.com as a Publish principal
aws sns get-topic-attributes --topic-arn <alarm-sns-arn> --region eu-west-1 \
  --profile <env> --query 'Attributes.Policy' --output text | jq .
aws iam list-attached-role-policies --role-name <role-from-above> --profile <env>
```

## Log group encryption

`KituService`, `KituServiceAudit` and `KituServiceDataProtectionAudit` are
encrypted with a customer-managed KMS key per account, `alias/kitu/logs`
(`LogsKey` in `log-groups-stack.ts`, automatic rotation, `RETAIN`). It was
added for OPHKOTO-94 by associating the key with the existing groups. CloudWatch
Logs encrypts only events ingested after the association with the key. Older
events stay in the same groups under the default CloudWatch Logs encryption
until the 731-day retention expires them.

The key policy has two statements besides the default account-root one:

1. **`logs.<region>.amazonaws.com`**, restricted by
   `kms:EncryptionContext:aws:logs:arn` to the three group ARNs. The ARNs are
   built from `LOG_GROUP_NAMES`, not from the `LogGroup` constructs, which
   would be a dependency cycle. A new group encrypted with this key must be
   added to that list, or the association fails.
2. **Any principal in this account, via `kms:ViaService` =
   `logs.<region>.amazonaws.com`**: the same scope as an AWS managed key.
   CloudTrail in dev (27.10.2026) shows that writes (the ECS `awslogs` driver,
   the audit log appender, data-protection findings) call KMS as the
   CloudWatch Logs service and need only statement 1. Reads such as
   `FilterLogEvents` and `StartLiveTail` instead make a KMS `Decrypt` call
   **as the calling role**. Without this statement the deploy job's
   `start-live-tail` and any role without `kms:Decrypt` in IAM (e.g. read-only
   developer roles) could no longer read the logs. Which groups a role may read
   is still decided by its `logs:*` IAM permissions. **Don't remove it.**

Disabling, scheduling deletion of or removing CloudWatch Logs' access to the
key makes every event encrypted with it unreadable. Check the association
with `aws logs describe-log-groups --log-group-name-prefix KituService` (the
`kmsKeyId` field).

`aws/spans` (Transaction Search), `/metrics/kitu` (ADOT EMF) and the Lambdas'
`/aws/lambda/*` groups are created outside CDK by the services themselves and
still use the default encryption.

## KOSKI audit logs integration

The service's audit log group has a CloudWatch Logs subscription filter
(`anyTerm("operation")`) that fires a Lambda. The Lambda parses the log event,
assumes a role in the KOSKI account
(`arn:aws:iam::<koski-account>:role/kitu-sqs-sender`), and pushes to the SQS
queue `oma-opintopolku-loki-audit-queue` in the KOSKI account. KOSKI manages
both the role's trust policy (allowing the kitu Lambda role to assume it) and
the queue itself. Per env:

| Env  | KOSKI account  | KOSKI region |
| ---- | -------------- | ------------ |
| dev  | `500150530292` | `eu-west-1`  |
| test | `692437769085` | `eu-west-1`  |
| prod | `508832528142` | `eu-west-1`  |

## Deploying

CI (`/.github/workflows/build.yml`) auto-deploys main: builds the image, pushes
it to Util ECR, then runs `cdk deploy` against Dev → Test → Prod with the new
`TAG`. A PR that touches `infra/` instead runs `cdk diff --method=template`
against Util, Dev, Test and Prod, and `cdk-notifier` posts one comment per
environment. Changes to the container image tag are suppressed, and nothing is
diffed in the merge queue. PR diffs run **without a GitHub environment**, as
`kitu-github-actions-diff-role`, which can only read CloudFormation templates.
Only main uses the environments and the deploy role; see
[GitHub Actions roles](#github-actions-roles).

For local diffs/deploys, the `infra/package.json` scripts cover dev:

```bash
npm run diff/dev
npm run deploy/dev
```

For other envs, invoke cdk directly with `TAG` set. Narrow with `--exclusively`
when you only want to touch a few stacks:

```bash
TAG=$(git rev-parse main) npx cdk diff   --exclusively 'Prod/Alarms' 'Prod/AlarmsUsEast1' 'Prod/LogGroups'
TAG=$(git rev-parse main) npx cdk deploy --exclusively 'Prod/Alarms' 'Prod/AlarmsUsEast1' 'Prod/LogGroups'
```

CDK bootstrap (`cdk-hnb659fds-*` roles) must already exist in each account; the
GitHub Actions deploy role assumes those roles to publish artifacts.

### GitHub Actions roles

Each account has two roles that GitHub Actions assumes through OIDC
(`lib/github-actions-stack.ts`). Both trust exact `sub` claims with
`StringEquals`, not a wildcard:

| Role                            | Trusted `sub`                                                                    | Can                                                                                                   |
| ------------------------------- | -------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------- |
| `kitu-github-actions-role`      | `repo:Opetushallitus/kielitutkintorekisteri:environment:<Util\|Dev\|Test\|Prod>` | assume the CDK bootstrap roles (deploy), live-tail `KituService`, run the weekly drift audit          |
|                                 | Util only, additionally `…:ref:refs/heads/main`                                  | push to ECR (`build_image` has no environment)                                                        |
| `kitu-github-actions-diff-role` | `repo:Opetushallitus/kielitutkintorekisteri:pull_request`                        | `cloudformation:DescribeStacks`/`GetTemplate`/`GetTemplateSummary`/`ListStackResources`, nothing else |

Things to know before changing these:

- **A job that declares `environment:` gets `…:environment:<name>` whatever
  its trigger.** The `sub` therefore can't tell a main deploy from a PR or a
  manual dispatch on another branch that declares the same environment. That
  separation is the job of the GitHub environments' deployment branch policy
  (Util/Dev/Test/Prod: `main` only), a repository setting outside this code; see
  [Manual prerequisites](#manual-prerequisites-that-dont-live-in-cdk).
- **The environment name in the `sub` is the workflow's spelling** (`Dev`),
  not the stored environment name (`dev`), and IAM compares it
  case-sensitively. `GithubActionsStack` takes it from the stage id.
- **The diff role is deliberately not allowed to assume the CDK lookup role**,
  whose `ReadOnlyAccess` would let any PR, Renovate's included, read every S3
  object in the account. `cdk diff --method=template` only reads the deployed
  templates: context lookups are cached in `cdk.context.json`, and when the
  lookup role can't be assumed the CLI warns and proceeds with the diff role's
  own credentials. This was verified against every Dev stack in both regions
  with exactly these four actions.
- **The deploy role's trust is deployed by CI with that same role.** A wrong
  subject therefore surfaces only on the _next_ run, as an
  `AssumeRoleWithWebIdentity` failure. The fix is a local
  `TAG=$(git rev-parse main) npx cdk deploy --exclusively '<Env>/GithubActions'`
  with an admin SSO profile for that account.
- **To see which subjects really assume the roles**, look at the
  `userName` of `AssumeRoleWithWebIdentity` events in CloudTrail:

```bash
aws cloudtrail lookup-events --profile <env> \
  --lookup-attributes AttributeKey=EventName,AttributeValue=AssumeRoleWithWebIdentity \
  --max-items 500 --query 'Events[].CloudTrailEvent' --output json \
  | jq -r '.[] | fromjson | "\(.userIdentity.userName) \(.requestParameters.roleArn) \(.errorCode // "ok")"' \
  | sort | uniq -c
```

## Stack lifecycle and runtime currency

**Lambda runtimes are never pinned, and must not be.** The two Lambdas this repo
owns (`koski-audit-logs-integration-stack.ts` and the YkiSuoritusNotifications
function in `log-groups-stack.ts`) use `Runtime.NODEJS_LATEST`, and
CDK's own custom-resource Lambdas (`LogRetention`, `VpcRestrictDefaultSG`,
`OpenIdConnectProvider`) use `determineLatestNodeRuntimeName()`. Both are
_variable_ values that move when `aws-cdk-lib` is upgraded. Renovate automerges
those minor bumps and every `main` push redeploys Dev → Test → Prod, so runtimes
stay current with no human action.

**The one thing that breaks this is a stack falling out of the pipeline.** When
a stack is renamed or removed from the app, CloudFormation does _not_ delete the
old stack — it just stops being managed, and its resources freeze at whatever
they were on the last deploy. Every stale Lambda runtime this project has ever
had came from that, not from a stale pin:

| Orphan stack                         | Cause                                                                 |
| ------------------------------------ | --------------------------------------------------------------------- |
| `Prod-AlarmsStack`                   | renamed to `Prod-Alarms`; Slack notifier Lambda deleted in `7f800a99` |
| `CertificateStack` (us-east-1)       | ACM cert moved into `service-stack.ts` in the env's own region        |
| `<Env>-Bastion`, `<Env>-EcsRdsProxy` | removed from the app in `253dc088`                                    |
| `Util-SlackBot`                      | folded into `Util-Alarms`                                             |

**So: when you remove or rename a stack, delete the old CloudFormation stack in
every account in the same change.** Not doing so is what this section exists to
prevent.

### Stacks that are deliberately outside the CDK app

- **`DnsStack`** per env — owns the Route53 hosted zone. Kept out of CDK so the
  app can never destroy it (`dns-stack.ts` looks it up instead). Note the app
  _also_ has a `<Env>/Dns` stack, which only performs that lookup and therefore
  synthesises to an empty stack.
- **`CDKToolkit`** — CDK bootstrap.

### `PhysicalName.GENERATE_IF_NEEDED` is load-bearing

`alarms-stack.ts` creates its SNS topics with
`topicName: PhysicalName.GENERATE_IF_NEEDED`. That makes the physical name — and
therefore the ARN — resolve at synth time, so the `us-east-1` ↔ `eu-west-1`
topic sharing needs no CloudFormation export. Remove it and CDK immediately
falls back to `crossRegionReferences`, which materialises a pair of
`CustomCrossRegionExport{Reader,Writer}` custom-resource Lambdas per stack —
exactly the ones that went stale in `CertificateStack` and `DnsStack`.

### Weekly audit

`.github/workflows/aws-audit.yml` runs `infra/scripts/audit-deployed-stacks.sh`
against each account every Monday at 06:00 UTC (and on `workflow_dispatch`). The
script synthesises the app, reads the expected stacks straight out of the cloud
assembly (`cdk-app-inventory.mjs` — `cdk list` only gives construct paths, not
stack names), and reports:

- CloudFormation stacks that are deployed but no longer in the app
- stacks in the app that are not deployed
- Lambda functions whose runtime is not the current `Runtime.NODEJS_LATEST`

The expected runtime is read from `aws-cdk-lib` at run time, so it tracks
Renovate's bumps by itself. Ignore lists live at the top of the script:
`CDKToolkit`, `DnsStack`, `StackSet-*`, `AWS-QuickSetup-*`,
`awsconfigconforms-*` and `ApplicationInsights-*` for stacks, and
`vaka-pilvi-tietoturva-*` / `aws-controltower-*` / `aws-quicksetup-*` for Lambdas — the last three are OPH's
central Control Tower tooling and AWS's own, not ours. A failing run is reported
to Slack by `slack-notifier.yml`.

Run it locally against one account with:

```bash
TAG=$(git rev-parse main) AWS_PROFILE=oph-ktr-prod infra/scripts/audit-deployed-stacks.sh
```

## Manual prerequisites that don't live in CDK

Listed here because deploys silently or loudly fail when these don't exist:

- **Route53 hosted zone** per env (`kios.<domain>`). Looked up by name in
  `dns-stack.ts`; not created by CDK to prevent accidental destruction.
- **Secrets Manager secrets** per env account, by exact name:
  `kielitesti-token`, `palvelukayttaja-password`, `yki-api-password`,
  `yki-api-user`, `palvelukayttaja-oauth-password` — plus `tolgee-api-key` on
  the Test account only, injected solely into the QA task. The repo's `scripts/`
  directory has helpers (`scripts/ensure_aws_secrets.sh`) for setting these.
  (`oppijanumero-password` was listed here until 17.9.2026 but is **not** read by
  anything — not `service-stack.ts`, not `ensure_aws_secrets.sh`, not the app.
  ONR auth goes through `palvelukayttaja-password`.)
  (`slack-webhook-url` is **no longer needed** — the Lambda that read it was
  replaced by Chatbot in `7f800a99`. The secrets still exist in the accounts and
  can be deleted.)
- **`tolgee-api-key` in the Test account only** (`961341546901`, `eu-west-1`),
  a Tolgee Cloud **Project API Key** (not a Personal Access Token) with scopes
  `keys.view`, `keys.create`, `keys.delete`, `translations.view`. Create it by
  hand before deploying QA; `ensure_aws_secrets.sh` deliberately does not fetch
  it, because local dev never syncs translations. Until it exists the sync is
  simply off, so the change is safe to merge and deploy ahead of the key.
- **"Main only" deployment branch policy on the GitHub environments `Util`,
  `dev`, `test` and `prod`** (Settings → Environments → Deployment branches and
  tags → Selected branches: `main`). It is what keeps PRs and other branches off
  the deploy role, because the OIDC subject of a job that declares
  `environment:` doesn't depend on its trigger (see
  [GitHub Actions roles](#github-actions-roles)). **Unlike everything else
  here, nothing fails when it's missing: the protection silently disappears.**
  Set on 27.10.2026. To restore it:

  ```bash
  for e in Util dev test prod; do
    gh api -X PUT repos/Opetushallitus/kielitutkintorekisteri/environments/$e \
      --input - <<<'{"deployment_branch_policy":{"protected_branches":false,"custom_branch_policies":true}}'
    gh api -X POST repos/Opetushallitus/kielitutkintorekisteri/environments/$e/deployment-branch-policies \
      -f name=main -f type=branch
  done
  ```

  The `PUT` replaces the environment settings it doesn't mention, so check
  `gh api repos/Opetushallitus/kielitutkintorekisteri/environments/<env>` first
  if reviewers or a wait timer have been added since. Verify with the
  read-only `aws-audit.yml`: dispatched from `main` every job must run, and
  dispatched from any other branch every job must fail with "is not allowed to
  deploy to … due to environment protection rules". Use a throwaway branch for
  the latter: a dispatch attaches its failing check runs to that branch's head
  commit, so on a PR branch they show up on the PR.

- **KOSKI account-side resources**: IAM role `kitu-sqs-sender` (with trust
  policy for the kitu Lambda role) and SQS queue
  `oma-opintopolku-loki-audit-queue`. Owned by the KOSKI team.
- **`TAG` env var** set to a tag that exists in the Util ECR repo. CI sets this
  automatically; locally use `TAG=$(git rev-parse main)` to deploy main's
  image, or pin to a specific commit.

There are no Prod-only stacks left, so nothing extra beyond the above.
