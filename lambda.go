package main

import (
	"context"
	"errors"
	"os"

	"github.com/arduino/aws-sitewise-integration/app/align"
	"github.com/arduino/aws-sitewise-integration/internal/parameters"
	"github.com/aws/aws-lambda-go/lambda"
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

	var tags *string

	println("------ Reading parameters from SSM")
	paramReader, err := parameters.New()
	if err != nil {
		return nil, err
	}
	apikey, err := paramReader.ReadConfig(IoTApiKey)
	if err != nil {
		println("Error reading parameter "+IoTApiKey, err)
	}
	apiSecret, err := paramReader.ReadConfig(IoTApiSecret)
	if err != nil {
		println("Error reading parameter "+IoTApiSecret, err)
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

	println("Running import...")
	if event.Dev {
		println("Running in dev mode")
		os.Setenv("IOT_API_URL", "https://api-dev.arduino.cc")
	}
	println("key:", *apikey)
	println("secret:", *apiSecret)
	println("organization-id:", organizationId)
	if tags != nil {
		println("tags:", *tags)
	}

	err = align.StartAlignAndImport(ctx, *apikey, *apiSecret, organizationId, tags, true, SamplesResolutionSeconds, TimeExtractionWindowMinutes)
	if err != nil {
		return nil, err
	}

	message := "Data aligned and imported successfully"
	return &message, nil
}

func main() {
	lambda.Start(HandleRequest)
}
