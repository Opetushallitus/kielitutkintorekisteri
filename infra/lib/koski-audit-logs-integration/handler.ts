import {
  BatchResultErrorEntry,
  SendMessageBatchCommand,
  SendMessageBatchRequestEntry,
  SQSClient,
} from "@aws-sdk/client-sqs"
import { fromTemporaryCredentials } from "@aws-sdk/credential-providers"
import { CloudWatchLogsEvent } from "aws-lambda"
import { getAuditLogEntry, parse } from "./parser"

const SQS_BATCH_SIZE = 10

export const handler = async (event: CloudWatchLogsEvent) => {
  const QueueUrl = process.env.KOSKI_SQS_QUEUE_URL
  if (!QueueUrl) {
    throw "Cannot proceed, because 'KOSKI_SQS_QUEUE_URL' is missing."
  }

  const roleArn = process.env.KOSKI_ROLE_ARN
  if (!roleArn) {
    throw "Cannot proceed, because 'KOSKI_ROLE_ARN' is missing."
  }

  const data = parse(event)
  const auditLogEntries = getAuditLogEntry(data)

  if (auditLogEntries.length === 0) {
    return { statusCode: 200, message: "no audit log entries to send" }
  }

  const sqs = new SQSClient({
    credentials: fromTemporaryCredentials({
      params: { RoleArn: roleArn },
    }),
  })

  const entries: SendMessageBatchRequestEntry[] = auditLogEntries.map(
    (entry, index) => ({
      Id: String(index),
      MessageBody: JSON.stringify(entry),
    }),
  )

  const failed: BatchResultErrorEntry[] = []
  for (let i = 0; i < entries.length; i += SQS_BATCH_SIZE) {
    const Entries = entries.slice(i, i + SQS_BATCH_SIZE)
    try {
      const result = await sqs.send(
        new SendMessageBatchCommand({ QueueUrl, Entries }),
      )
      if (result.Failed?.length) {
        failed.push(...result.Failed)
      }
    } catch (error) {
      console.log("failed to send batch to sqs", error)
      // CloudWatch Logs invokes Lambda asynchronously, so only a thrown error
      // makes Lambda retry the event; a returned object counts as success.
      throw new Error(`failed to send audit log batch to sqs: ${String(error)}`)
    }
  }

  if (failed.length) {
    // Throw so that Lambda retries the whole event. Audit logs tolerate
    // duplicates better than data loss.
    console.log("partial failure sending audit logs to sqs", failed)
    throw new Error(`failed to send ${failed.length} audit log entries to sqs`)
  }

  console.log(`sent ${auditLogEntries.length} audit log entries to sqs`)
  return {
    statusCode: 200,
    message: "ok",
    sent: auditLogEntries.length,
  }
}
