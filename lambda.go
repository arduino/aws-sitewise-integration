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
	Tags string `json:"tags"`
	Dev  bool   `json:"dev"`
}

func HandleRequest(ctx context.Context, event *SiteWiseImportTrigger) (*string, error) {

	var tags *string
	if event.Tags != "" {
		tags = &event.Tags
	}

	println("------ Reading parameters from SSM")
	paramReader, err := parameters.New()
	if err != nil {
		return nil, err
	}
	apikey, err := paramReader.ReadConfig("/sitewise-importer/iot/api-key")
	if err != nil {
		println("Error reading parameter /sitewise-importer/iot/api-key", err)
	}
	apiSecret, err := paramReader.ReadConfig("/sitewise-importer/iot/api-secret")
	if err != nil {
		println("Error reading parameter /sitewise-importer/iot/api-secret", err)
	}
	origId, _ := paramReader.ReadConfig("/sitewise-importer/iot/org-id")
	organizationId := ""
	if origId != nil {
		organizationId = *origId
	}
	if apikey == nil || apiSecret == nil {
		return nil, errors.New("key and secret are required")
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

	err = align.StartAlignAndImport(ctx, *apikey, *apiSecret, organizationId, tags, true, 300, 30)
	if err != nil {
		return nil, err
	}

	message := "Data aligned and imported successfully"
	return &message, nil
}

func main() {
	lambda.Start(HandleRequest)
}
