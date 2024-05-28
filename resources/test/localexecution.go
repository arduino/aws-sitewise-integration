package main

import (
	"context"
	"errors"
	"os"

	"github.com/arduino/aws-sitewise-integration/app/align"
	"github.com/arduino/aws-sitewise-integration/internal/parameters"
	"github.com/sirupsen/logrus"
)

const (
	IoTApiKey                   = "/sitewise-importer/iot/api-key"
	IoTApiSecret                = "/sitewise-importer/iot/api-secret"
	IoTApiOrgId                 = "/sitewise-importer/iot/org-id"
	IoTApiTags                  = "/sitewise-importer/iot/filter/tags"
	SamplesResolutionSeconds    = 300
	TimeExtractionWindowMinutes = 30
)

func HandleRequest(ctx context.Context, dev bool) (*string, error) {

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
	if dev {
		logger.Infoln("Running in dev mode")
		os.Setenv("IOT_API_URL", "https://api2.oniudra.cc")
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
	HandleRequest(context.Background(), true)
}
