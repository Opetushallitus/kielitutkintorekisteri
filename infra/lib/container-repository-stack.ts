import * as cdk from "aws-cdk-lib"
import { StackProps } from "aws-cdk-lib"
import { Repository, TagMutability } from "aws-cdk-lib/aws-ecr"
import { AccountPrincipal } from "aws-cdk-lib/aws-iam"
import { Construct } from "constructs"
import { Topic } from "aws-cdk-lib/aws-sns"
import { SnsTopic } from "aws-cdk-lib/aws-events-targets"

interface ImageBuildStackProps extends StackProps {
  vulnerabilitiesSnsTopic: Topic
  allowPullsFromAccounts: string[]
}

export class ContainerRepositoryStack extends cdk.Stack {
  readonly repository: Repository

  constructor(scope: Construct, id: string, props: ImageBuildStackProps) {
    super(scope, id, props)

    this.repository = new Repository(this, "Service", {
      repositoryName: "kitu",
      imageTagMutability: TagMutability.IMMUTABLE,
      imageScanOnPush: true,
    })

    this.repository
      .onImageScanCompleted("ReportVulnerabilities", {
        eventPattern: {
          detail: {
            $or: [
              {
                "finding-severity-counts": {
                  CRITICAL: [{ numeric: [">", 0] }],
                },
              },
              {
                "finding-severity-counts": {
                  HIGH: [{ numeric: [">", 0] }],
                },
              },
            ],
          },
        },
      })
      .addTarget(new SnsTopic(props.vulnerabilitiesSnsTopic))

    for (const accountId of props.allowPullsFromAccounts) {
      this.repository.grantPull(new AccountPrincipal(accountId))
    }
  }
}
