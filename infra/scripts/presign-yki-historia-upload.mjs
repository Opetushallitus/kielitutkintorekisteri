#!/usr/bin/env node
// Mints a presigned S3 PUT URL. Invoked by scripts/yki_historia_upload_presign.sh.
// Inputs come via env vars (set by the wrapper):
//   BUCKET, KEY, TTL_SECONDS, REGION, AWS_PROFILE
// Output: the presigned URL on stdout. Errors on stderr with non-zero exit.

import { S3Client } from "@aws-sdk/client-s3"
import { PutObjectCommand } from "@aws-sdk/client-s3"
import { getSignedUrl } from "@aws-sdk/s3-request-presigner"
import { fromIni } from "@aws-sdk/credential-providers"

function required(name) {
  const v = process.env[name]
  if (!v) {
    process.stderr.write(`Missing required env var: ${name}\n`)
    process.exit(2)
  }
  return v
}

const bucket = required("BUCKET")
const key = required("KEY")
const ttl = Number.parseInt(required("TTL_SECONDS"), 10)
const region = required("REGION")
const profile = required("AWS_PROFILE")

if (!Number.isInteger(ttl) || ttl < 60 || ttl > 604800) {
  process.stderr.write(
    `TTL_SECONDS must be an integer between 60 and 604800, got: ${process.env.TTL_SECONDS}\n`,
  )
  process.exit(2)
}

const client = new S3Client({
  region,
  credentials: fromIni({ profile }),
  requestChecksumCalculation: "WHEN_REQUIRED",
})

const url = await getSignedUrl(
  client,
  new PutObjectCommand({ Bucket: bucket, Key: key }),
  { expiresIn: ttl },
)

process.stdout.write(url + "\n")
