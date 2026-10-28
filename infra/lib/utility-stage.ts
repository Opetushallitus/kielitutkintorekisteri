import { Stage, StageProps } from "aws-cdk-lib"
import { PolicyStatement } from "aws-cdk-lib/aws-iam"
import { Construct } from "constructs"
import { AccountSettingsStack } from "./account-settings-stack"
import { ContainerRepositoryStack } from "./container-repository-stack"
import {
  GITHUB_REPOSITORY_SUBJECT,
  GithubActionsStack,
} from "./github-actions-stack"
import { AlarmsStack } from "./alarms-stack"
import { SlackChannel } from "./accounts"

interface UtilityStageProps extends StageProps {
  allowPullsFromAccounts: string[]
  slackChannel: SlackChannel
  slackWorkspaceId: string
}

export class UtilityStage extends Stage {
  readonly imageBuildsStack: ContainerRepositoryStack

  constructor(scope: Construct, id: string, props: UtilityStageProps) {
    super(scope, id, props)

    new AccountSettingsStack(this, "AccountSettings", { env: props.env })

    const githubActionsStack = new GithubActionsStack(this, "GithubActions", {
      env: props.env,
      githubEnvironment: "Util",
      // build_image pushes to ECR from main without an environment.
      additionalDeploySubjects: [
        `${GITHUB_REPOSITORY_SUBJECT}:ref:refs/heads/main`,
      ],
    })

    const alarmsStack = new AlarmsStack(this, "Alarms", {
      env: props.env,
      // Util-tilillä ei ole hälytyksiä, joihin tutkimus voitaisiin kytkeä.
      automaticInvestigations: false,
      slack: {
        workspaceId: props.slackWorkspaceId,
        alarmsChannel: props.slackChannel,
      },
    })

    this.imageBuildsStack = new ContainerRepositoryStack(this, "ImageBuilds", {
      env: props.env,
      allowPullsFromAccounts: props.allowPullsFromAccounts,
      vulnerabilitiesSnsTopic: alarmsStack.alarmSnsTopic,
    })

    githubActionsStack.githubActionsRole.addToPolicy(
      new PolicyStatement({
        actions: ["ecr:GetAuthorizationToken"],
        resources: ["*"],
      }),
    )

    this.imageBuildsStack.repository.grantPullPush(
      githubActionsStack.githubActionsRole,
    )
  }
}
