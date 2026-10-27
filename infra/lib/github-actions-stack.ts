import { Stack, StackProps } from "aws-cdk-lib"
import {
  OpenIdConnectPrincipal,
  OpenIdConnectProvider,
  PolicyStatement,
  Role,
} from "aws-cdk-lib/aws-iam"
import { Construct } from "constructs"

export const GITHUB_REPOSITORY_SUBJECT =
  "repo:Opetushallitus/kielitutkintorekisteri"

export interface GithubActionsStackProps extends StackProps {
  /**
   * The GitHub environment the deploy jobs use for this account, spelled
   * exactly as in the workflows: the OIDC `sub` claim carries the workflow's
   * spelling (`environment:Dev`), and IAM compares it case-sensitively.
   */
  githubEnvironment: string
  /** Further OIDC subjects allowed to assume the deploy role. */
  additionalDeploySubjects?: string[]
}

export class GithubActionsStack extends Stack {
  public githubActionsRole: Role

  constructor(scope: Construct, id: string, props: GithubActionsStackProps) {
    super(scope, id, props)

    const githubActionsOidcProvider = new OpenIdConnectProvider(
      this,
      "GithubActionsIdentityProvider",
      {
        url: "https://token.actions.githubusercontent.com",
        clientIds: ["sts.amazonaws.com"],
      },
    )

    // A job that declares `environment:` gets the subject
    // `<repo>:environment:<name>` whatever its trigger, so this alone does not
    // tell a main deploy from a PR or a dispatch on another branch: the
    // environments' "main only" deployment branch policy does that. The trust
    // is deployed by CI with this very role, so a wrong subject only surfaces
    // on the next run, and fixing it takes a local deploy of
    // <Env>/GithubActions.
    this.githubActionsRole = new Role(this, "GithubActionsRole", {
      roleName: `kitu-github-actions-role`,
      assumedBy: new OpenIdConnectPrincipal(githubActionsOidcProvider, {
        StringEquals: {
          "token.actions.githubusercontent.com:aud": "sts.amazonaws.com",
          "token.actions.githubusercontent.com:sub": [
            `${GITHUB_REPOSITORY_SUBJECT}:environment:${props.githubEnvironment}`,
            ...(props.additionalDeploySubjects ?? []),
          ],
        },
      }),
    })

    // PR jobs run `cdk diff --method=template` without an environment, so
    // their subject is `<repo>:pull_request`. Reading the deployed templates
    // is all that needs: context lookups are cached in cdk.context.json, and
    // when the CDK lookup role can't be assumed the CLI warns and proceeds
    // with these credentials. Deliberately not the lookup role, whose
    // ReadOnlyAccess would let any PR, Renovate's included, read S3 objects.
    const diffRole = new Role(this, "GithubActionsDiffRole", {
      roleName: "kitu-github-actions-diff-role",
      assumedBy: new OpenIdConnectPrincipal(githubActionsOidcProvider, {
        StringEquals: {
          "token.actions.githubusercontent.com:aud": "sts.amazonaws.com",
          "token.actions.githubusercontent.com:sub": `${GITHUB_REPOSITORY_SUBJECT}:pull_request`,
        },
      }),
    })
    diffRole.addToPolicy(
      new PolicyStatement({
        sid: "AllowReadingStackTemplates",
        actions: [
          "cloudformation:DescribeStacks",
          "cloudformation:GetTemplate",
          "cloudformation:GetTemplateSummary",
          "cloudformation:ListStackResources",
        ],
        // All regions: Dev, Test and Prod have stacks in us-east-1 too.
        resources: [
          `arn:${this.partition}:cloudformation:*:${this.account}:stack/*`,
        ],
      }),
    )

    this.githubActionsRole.addToPolicy(
      new PolicyStatement({
        sid: "AllowAssumingCDKRoles",
        actions: ["sts:AssumeRole", "iam:PassRole"],
        resources: [
          "arn:aws:iam::*:role/cdk-hnb659fds-deploy-role-*",
          "arn:aws:iam::*:role/cdk-hnb659fds-file-publishing-*",
          "arn:aws:iam::*:role/cdk-hnb659fds-image-publishing-*",
          "arn:aws:iam::*:role/cdk-hnb659fds-lookup-*",
        ],
      }),
    )
    this.githubActionsRole.addToPolicy(
      new PolicyStatement({
        sid: "AllowDriftAudit",
        // scripts/audit-deployed-stacks.sh vertaa deployattuja pinoja ja
        // Lambda-ajonaikoja CDK-appiin. Kumpikaan list-rajapinta ei tue
        // resurssitason rajausta, joten resources on pakko olla "*".
        actions: ["cloudformation:ListStacks", "lambda:ListFunctions"],
        resources: ["*"],
      }),
    )
    this.githubActionsRole.addToPolicy(
      new PolicyStatement({
        sid: "AllowLiveTailing",
        actions: ["logs:StartLiveTail"],
        // _deploy-env.yml tails the KituService log group after assuming this
        // role in the target environment account. Scope to that log group only;
        // wildcard resources would also expose audit logs and other apps.
        resources: [
          `arn:aws:logs:${this.region}:${this.account}:log-group:KituService`,
        ],
      }),
    )
  }
}
