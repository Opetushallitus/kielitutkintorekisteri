import { Stack, StackProps } from "aws-cdk-lib"
import { PolicyStatement } from "aws-cdk-lib/aws-iam"
import {
  AwsCustomResource,
  AwsCustomResourcePolicy,
  PhysicalResourceId,
} from "aws-cdk-lib/custom-resources"
import { Construct } from "constructs"

export class AccountSettingsStack extends Stack {
  constructor(scope: Construct, id: string, props: StackProps) {
    super(scope, id, props)

    new AwsCustomResource(this, "S3AccountPublicAccessBlock", {
      onUpdate: {
        service: "S3Control",
        action: "PutPublicAccessBlock",
        parameters: {
          AccountId: this.account,
          PublicAccessBlockConfiguration: {
            BlockPublicAcls: true,
            IgnorePublicAcls: true,
            BlockPublicPolicy: true,
            RestrictPublicBuckets: true,
          },
        },
        physicalResourceId: PhysicalResourceId.of(
          "s3-account-public-access-block",
        ),
      },
      policy: AwsCustomResourcePolicy.fromStatements([
        new PolicyStatement({
          actions: ["s3:PutAccountPublicAccessBlock"],
          resources: ["*"],
        }),
      ]),
    })
  }
}
