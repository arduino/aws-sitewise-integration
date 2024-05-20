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
	IoTApiKey                   = "/sitewise-importer/iot/api-key"
	IoTApiSecret                = "/sitewise-importer/iot/api-secret"
	IoTApiOrgId                 = "/sitewise-importer/iot/org-id"
	IoTApiTags                  = "/sitewise-importer/iot/filter/tags"
	SamplesResolutionSeconds    = 300
	TimeExtractionWindowMinutes = 30
)

func HandleRequest(ctx context.Context, event *SiteWiseImportTrigger) (*string, error) {

	logger := logrus.NewEntry(logrus.New())

	var tags *string

	logger.Infoln("------ Reading parameters from SSM")
	paramReader, err := parameters.New()
	if err != nil {
		return nil, err
	}
	apikey, err := paramReader.ReadConfig(IoTApiKey)
	if err != nil {
		logger.Error("Error reading parameter "+IoTApiKey, err)
	}
	apiSecret, err := paramReader.ReadConfig(IoTApiSecret)
	if err != nil {
		logger.Error("Error reading parameter "+IoTApiSecret, err)
	}
	origId, _ := paramReader.ReadConfig(IoTApiOrgId)
	organizationId := ""
	if origId != nil {
		organizationId = *origId
	}
	if apikey == nil || apiSecret == nil {
		return nil, errors.New("key and secret are required")
	}
	tagsParam, _ := paramReader.ReadConfig(IoTApiTags)
	if tagsParam != nil {
		tags = tagsParam
	}

	logger.Infoln("------ Running import...")
	if event.Dev {
		logger.Infoln("Running in dev mode")
		os.Setenv("IOT_API_URL", "https://api-dev.arduino.cc")
	}
	logger.Infoln("key:", *apikey)
	logger.Infoln("secret:", *apiSecret)
	logger.Infoln("organization-id:", organizationId)
	if tags != nil {
		logger.Infoln("tags:", *tags)
	}

	err = align.StartAlignAndImport(ctx, logger, *apikey, *apiSecret, organizationId, tags, true, SamplesResolutionSeconds, TimeExtractionWindowMinutes)
	if err != nil {
		return nil, err
	}

	message := "Data aligned and imported successfully"
	return &message, nil
}

func main() {
	lambda.Start(HandleRequest)
}
