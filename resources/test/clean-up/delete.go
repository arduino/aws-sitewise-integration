package main

import (
	"context"
	"errors"
	"os"

	"github.com/arduino/aws-sitewise-integration/internal/parameters"
	"github.com/arduino/aws-sitewise-integration/internal/sitewiseclient"
	"github.com/sirupsen/logrus"
)

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

func HandleRequest(ctx context.Context, dev bool) (*string, error) {

	stack := os.Getenv("STACK_NAME")
	logger := logrus.NewEntry(logrus.New())

	var tags *string

	logger.Infoln("------ Reading parameters from SSM")
	paramReader, err := parameters.New()
	if err != nil {
		return nil, err
	}
	apikey, err := paramReader.ReadConfig(IoTApiKey, stack)
	if err != nil {
		logger.Error("Error reading parameter "+IoTApiKey, err)
	}
	apiSecret, err := paramReader.ReadConfig(IoTApiSecret, stack)
	if err != nil {
		logger.Error("Error reading parameter "+IoTApiSecret, err)
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

	sitewisecl, err := sitewiseclient.New(logger)
	if err != nil {
		return nil, err
	}

	out, err := sitewisecl.ListAssetModels(ctx, nil)
	if err != nil {
		return nil, err
	}
	for _, model := range out.AssetModelSummaries {
		logger.Infoln("Model: ", *model.Name)
		sitewisecl.DeleteAssetModel(ctx, model.Id)
	}

	message := "Data aligned and imported successfully"
	return &message, nil
}

func main() {
	_, err := HandleRequest(context.Background(), true)
	if err != nil {
		panic(err)
	}
}

