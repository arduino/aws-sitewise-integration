# Import historical data

One way of importing historical samples into SiteWise is via job. A job can pick samples from csv files saved on a S3 bucket and push them into SiteWise.
To do that, csv files must contain:
* Asset and Property identifier
OR
* Property alias

Asset properties created by integration lambda are configured with a peroperty alias defined as:
 ```
 /<thing name>/<property name>
 ```

Given a thing called 'Compressor Car 1' with 2 properties (pressure, temperature), a possible csv to be used into data import job is:
 ```csv
/Compressor Car 1/pressure,DOUBLE,1714916982,0,GOOD,8.78
/Compressor Car 1/temperature,DOUBLE,1714916982,0,GOOD,49.90
 ```

job can be started with command. You can use [jboconfiguration.json](jboconfiguration.json) file as configuration reference.
```console
aws iotsitewise create-bulk-import-job --cli-input-json file://jboconfiguration.json
```

## Check job status

Given the id returned by the above command, it is possible to monitor job using following command
```console
foo@bar:~$ aws iotsitewise describe-bulk-import-job --job-id <job identifier>
```

## AWS resources
https://docs.aws.amazon.com/cli/latest/reference/iotsitewise/create-bulk-import-job.html
https://docs.aws.amazon.com/iot-sitewise/latest/userguide/ingest-bulkImport.html
