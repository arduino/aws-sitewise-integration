#!/bin/bash

GOOS=linux CGO_ENABLED=0 go build -o bootstrap -tags lambda.norpc lambda.go
zip sitewise-aligner.zip bootstrap
rm bootstrap
Create sitewise-aligner.zip archive
