# sitewise-samples
IoT SiteWise code samples

# Import job from CLI

https://docs.aws.amazon.com/cli/latest/reference/iotsitewise/create-bulk-import-job.html
https://docs.aws.amazon.com/iot-sitewise/latest/userguide/ingest-bulkImport.html


aws iotsitewise create-bulk-import-job --cli-input-json file://jboconfiguration.json

## Check status
aws iotsitewise describe-bulk-import-job --job-id d747fade-f3a1-4758-b435-cf80d773c4d6

