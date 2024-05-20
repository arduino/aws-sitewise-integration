
Il modello deve essere creato.
Il file CSV può definire l'ALIAS, come /<thingName>/<propertyName>

CSV bulk import recursive? Non sembra supportato e deve essere dato il percorso del file.
Quindi, troppi file, problema di import.

Supporta l'auto delete dei files importati, quindi no problem per purging (e cmq il cliente si può fare una policy time based per il purging).

## CREATE MODEL
aws iotsitewise create-asset-model --cli-input-json file://asset-model-payload.json
(https://docs.aws.amazon.com/iot-sitewise/latest/userguide/create-asset-models.html#create-asset-model-cli)


## BULK IMPORT DATA FROM CSV
https://docs.aws.amazon.com/cli/latest/reference/iotsitewise/create-bulk-import-job.html
https://docs.aws.amazon.com/iot-sitewise/latest/userguide/ingest-bulkImport.html

>>>>
aws iotsitewise create-bulk-import-job --cli-input-json file://jboconfiguration.json

## Check status
aws iotsitewise describe-bulk-import-job --job-id d747fade-f3a1-4758-b435-cf80d773c4d6

