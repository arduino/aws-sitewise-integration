#!/bin/bash

GOOS=linux CGO_ENABLED=0 go build -o bootstrap -tags lambda.norpc lambda.go
zip arduino-sitewise-integration-lambda.zip bootstrap
rm bootstrap
echo "arduino-sitewise-integration-lambda.zip archive created"
