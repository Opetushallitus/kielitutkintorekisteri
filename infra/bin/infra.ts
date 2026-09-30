#!/usr/bin/env node
import "source-map-support/register"
import { App } from "aws-cdk-lib"
import { EcrImage } from "aws-cdk-lib/aws-ecs"
import { deploymentAccounts, utilityAccount } from "../lib/accounts"
import { getEnv } from "../lib/env"
import { EnvironmentStage } from "../lib/environment-stage"
import { UtilityStage } from "../lib/utility-stage"

const app = new App()

const utilityStage = new UtilityStage(app, "Util", {
  env: utilityAccount,
  slackChannel: utilityAccount.slackChannel,
  slackWorkspaceId: utilityAccount.slackWorkspaceId,
  allowPullsFromAccounts: [
    deploymentAccounts.dev.account,
    deploymentAccounts.test.account,
    deploymentAccounts.prod.account,
  ],
})

const serviceImage = EcrImage.fromEcrRepository(
  utilityStage.imageBuildsStack.repository,
  getEnv("TAG"),
)

new EnvironmentStage(app, "Dev", {
  env: deploymentAccounts.dev,
  environmentConfig: deploymentAccounts.dev,
  serviceImage,
})
new EnvironmentStage(app, "Test", {
  env: deploymentAccounts.test,
  environmentConfig: deploymentAccounts.test,
  serviceImage,
})
new EnvironmentStage(app, "Prod", {
  env: deploymentAccounts.prod,
  environmentConfig: deploymentAccounts.prod,
  serviceImage,
})
