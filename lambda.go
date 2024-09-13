// This file is part of arduino aws-sitewise-integration.
//
// Copyright 2024 ARDUINO SA (http://www.arduino.cc/)
//
// This software is released under the Mozilla Public License Version 2.0,
// which covers the main part of aws-sitewise-integration.
// The terms of this license can be found at:
// https://www.mozilla.org/media/MPL/2.0/index.815ca599c9df.txt
//
// You can be released from the requirements of the above licenses by purchasing
// a commercial license. Buying such a license is mandatory if you want to
// modify or otherwise use the software for commercial activities involving the
// Arduino software without disclosing the source code of your own applications.
// To purchase a commercial license, send an email to license@arduino.cc.

package main

import (
	"context"
	"errors"
	"os"

	"github.com/arduino/aws-sitewise-integration/app/align"
	"github.com/arduino/aws-sitewise-integration/internal/parameters"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/sirupsen/logrus"
)

type SiteWiseImportTrigger struct {
	Dev bool `json:"dev"`
}

const (
	ArduinoPrefix                      = "/arduino/sitewise-importer/" + parameters.StackName
	IoTApiKey                          = ArduinoPrefix + "/iot/api-key"
	IoTApiSecret                       = ArduinoPrefix + "/iot/api-secret"
	IoTApiOrgId                        = ArduinoPrefix + "/iot/org-id"
	IoTApiTags                         = ArduinoPrefix + "/iot/filter/tags"
	SamplesReso                        = ArduinoPrefix + "/iot/samples-resolution"
	Scheduling                         = ArduinoPrefix + "/iot/scheduling"
	SamplesResolutionSeconds           = 300
	DefaultTimeExtractionWindowMinutes = 60
)

func HandleRequest(ctx context.Context, event *SiteWiseImportTrigger) (*string, error) {

	logger := logrus.NewEntry(logrus.New())
	stack := os.Getenv("STACK_NAME")

	var tags *string

	logger.Infoln("------ Reading parameters from SSM")
	paramReader, err := parameters.New()
	if err != nil {
		return nil, err
	}
	apikey, err := paramReader.ReadConfig(IoTApiKey, stack)
	if err != nil {
		logger.Error("Error reading parameter "+paramReader.ResolveParameter(IoTApiKey, stack), err)
	}
	apiSecret, err := paramReader.ReadConfig(IoTApiSecret, stack)
	if err != nil {
		logger.Error("Error reading parameter "+paramReader.ResolveParameter(IoTApiSecret, stack), err)
	}
	origId, _ := paramReader.ReadConfig(IoTApiOrgId, stack)
	organizationId := ""
	if origId != nil {
		organizationId = *origId
	}
	if apikey == nil || apiSecret == nil {
		return nil, errors.New("key and secret are required")
	}
	tagsParam, _ := paramReader.ReadConfig(IoTApiTags, stack)
	if tagsParam != nil {
		tags = tagsParam
	}
	res, err := paramReader.ReadConfig(SamplesReso, stack)
	if err != nil {
		logger.Warn("Error reading parameter "+paramReader.ResolveParameter(SamplesReso, stack)+". Set resolution to default value", err)
	}
	resolution := int(SamplesResolutionSeconds)
	switch *res {
	case "1m":
		resolution = 60
	case "5m":
		resolution = 300
	case "15m":
		resolution = 900
	case "1h":
		resolution = 3600
	}
	if resolution > 3600 {
		logger.Errorf("Resolution %d is invalid", resolution)
		return nil, errors.New("resolution must be between -1 and 3600")
	}
	if resolution < 60 || resolution > 3600 {
		logger.Errorf("Resolution %d is invalid", resolution)
		return nil, errors.New("resolution must be between 60 and 3600")
	}

	logger.Infoln("------ Running import. Stack:", stack)
	if event.Dev || os.Getenv("DEV") == "true" {
		logger.Infoln("Running in dev mode")
		os.Setenv("IOT_API_URL", "https://api2.oniudra.cc")
	}
	logger.Infoln("key:", *apikey)
	logger.Infoln("secret:", "*********")
	if organizationId != "" {
		logger.Infoln("organizationId:", organizationId)
	} else {
		logger.Infoln("organizationId: not set")
	}
	if tags != nil {
		logger.Infoln("tags:", *tags)
	}
	// Resolve scheduling
	extractionWindowMinutes, err := configureDataExtractionTimeWindow(logger, paramReader, stack)
	if err != nil {
		return nil, err
	}

	err = align.StartAlignAndImport(ctx, logger, *apikey, *apiSecret, organizationId, tags, true, resolution, extractionWindowMinutes)
	if err != nil {
		return nil, err
	}

	message := "Data aligned and imported successfully"
	return &message, nil
}

func configureDataExtractionTimeWindow(logger *logrus.Entry, paramReader *parameters.ParametersClient, stack string) (int, error) {
	var schedule *string
	var err error
	schedule, err = paramReader.ReadConfig(Scheduling, stack)
	if err != nil {
		logger.Error("Error reading parameter "+paramReader.ResolveParameter(Scheduling, stack), err)
		return -1, err
	}
	extractionWindowMinutes := DefaultTimeExtractionWindowMinutes
	switch *schedule {
	case "5 minutes":
		extractionWindowMinutes = 5
	case "15 minutes":
		extractionWindowMinutes = 15
	case "1 hour":
		extractionWindowMinutes = 60
	}
	return extractionWindowMinutes, nil
}

func main() {
	lambda.Start(HandleRequest)
}
