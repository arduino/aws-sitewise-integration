# AWS IoT SiteWise importer

This project provides a way to extract time series samples from Arduino cloud, publishing into AWS IoT SiteWise, creating 
required model/asset hierarchy mapping Arduino Things into SiteWise Assets.
Arduino Things can be filterd by tags, to properly select objects to monitor.

## Architecture

![deployment schema](docs/deployment-schema.png)

Job is based on a Go lambda function triggered by periodic events from AWS EventBridge.
Job is configured to extract samples every 30 minutes (configurable). By default, time series data are aggregated at 5 minute resolution (as AVG aggregation statistic).

## Deployment via Cloud Formation Template

It is possible to deploy required resources via [cloud formation template](deployment/cloud-formation-template/deployment.yaml)

AWS user must have permissions to:
  * create a new CFT stack (policy: AWSCloudFormationFullAccess)
  * S3 buckets (policy: AmazonS3FullAccess)
  * IAM Roles (policy: IAMFullAccess)
  * Lambda functions (policy: AWSLambda_FullAccess)
  * EventBridge rules (policy: AmazonEventBridgeFullAccess)
  * SSM parameters (Parameter store) (policy: AmazonSSMFullAccess)

To see runtime required permissions, see policies defined in [cloud formation template](deployment/cloud-formation-template/deployment.yaml)

Before creating a stack, it is required to create a temporary S3 bucket where storing lambda binaries.

Follow these steps to deploy a new stack:
* download [lambda code binaries](https://github.com/arduino/aws-sitewise-integration/releases) and [Cloud Formation Template](https://github.com/arduino/aws-sitewise-integration/releases). See releases page for latest stable release.
* upload CFT and binary zip file on an S3 bucket accessible by the AWS account. For the CFT yaml file, copy the Object URL (it will be required in next step).

* start creation of a new cloud formation stack

![CFT 1](docs/cft-stack-1.png)

* fill all required parameters.
  <br/>**Mandatory**: Arduino API key and secret, S3 bucket where code has been uploaded
  <br/>**Optional**: tag filter for filtering things, organization identifier and samples resolution

![CFT 2](docs/cft-stack-2.png)

### Configuration parameters

| Parameter | Description |
| --------- | ----------- |
| /arduino/sitewise-importer/{stack-name}/iot/api-key  | IoT API key |
| /arduino/sitewise-importer/{stack-name}/iot/api-secret | IoT API secret |
| /arduino/sitewise-importer/{stack-name}/iot/org-id    | (optional) organization id |
| /arduino/sitewise-importer/{stack-name}/iot/filter/tags    | (optional) tags filtering. Syntax: tag=value,tag2=value2  |
| /arduino/sitewise-importer/{stack-name}/iot/samples-resolution  | (optional) samples resolution (default: 5 minutes) |
| /arduino/sitewise-importer/{stack-name}/iot/scheduling  | function scheduling |

## Import historical data with a batch job

For more info, see [import batch](resources/job/README.md)

