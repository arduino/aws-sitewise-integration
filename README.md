# AWS IoT SiteWise importer

This project provides a way to extract time series samples from Arduino cloud, publishing into AWS IoT SiteWise.
Other than publishing ts samples, job is able to create Models/Assets into SiteWise starting from Arduino defined Things.
Things can be filterd by tags.

## Deployment schema

![deployment schema](docs/deployment-schema.png)

Imported is based on a Go lambda function triggered by periodic events from EventBridge.
Job is configured to extract samples for a 30min time window, so scheduled triggers must be configured accordingly.

### Policies

See policies defined in [cloud formation template](deployment/cloud-formation-template/deployment.yaml)

### Configuration parameters

| Parameter | Description |
| --------- | ----------- |
| /arduino/sitewise-importer/iot/api-key  | IoT API key |
| /arduino/sitewise-importer/iot/api-secret | IoT API secret |
| /arduino/sitewise-importer/iot/org-id    | (optional) organization id |
| /arduino/sitewise-importer/iot/filter/tags    | (optional) tags filtering. Syntax: tag=value,tag2=value2  |
| /iot/samples-resolution-seconds  | (optional) samples resolution (default: 300s) |

## Deployment via Cloud Formation Template

It is possible to deploy required resources via [cloud formation template](deployment/cloud-formation-template/deployment.yaml)
Required steps to deploy project:
* compile lambda
```console
foo@bar:~$ ./compile-lambda.sh
arduino-sitewise-integration-lambda.zip archive created
```
* Save zip file on an S3 bucket accessible by the AWS account
* Start creation of a new cloud formation stack provising the [cloud formation template](deployment/cloud-formation-template/deployment.yaml)
* Fill all required parameters (mandatory: Arduino API key and secret, S3 bucket and key where code has been uploaded. Optionally, tag filter for filtering things, organization identifier and samples resolution)

## Import historical data with a batch job

For more info, see [import batch](resources/job/README.md)

