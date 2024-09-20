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

package entityalign

import (
	"context"
	"testing"

	sitewiseMocks "github.com/arduino/aws-sitewise-integration/internal/sitewiseclient/mocks"
	iotclient "github.com/arduino/iot-client-go/v2"
	"github.com/aws/aws-sdk-go-v2/service/iotsitewise"
	"github.com/aws/aws-sdk-go-v2/service/iotsitewise/types"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestAlign_AlignAlreadyCreatedModelsIfNeeded(t *testing.T) {

	ctx := context.Background()
	logger := logrus.NewEntry(logrus.New())

	// Static id definitions
	thingId := "bb831f04-0940-4ea6-9c24-83668e372919"
	modelId := "03ba45c2-eab3-44ed-a68f-94a26d41df4c"

	// Mocks
	swclient := sitewiseMocks.NewAPI(t)

	// Define thing
	thingsMap := make(map[string]iotclient.ArduinoThing)
	thingsMap[thingId] = iotclient.ArduinoThing{
		Id: thingId,
		Properties: []iotclient.ArduinoProperty{
			{
				Name: "temperature",
				Type: "INT",
			},
			{
				Name: "pressure",
				Type: "INT",
			},
		},
	}

	modelDefinitions := make(map[string]*iotsitewise.DescribeAssetModelOutput)
	modelDefinitions[modelId] = &iotsitewise.DescribeAssetModelOutput{
		AssetModelId: &modelId,
		AssetModelProperties: []types.AssetModelProperty{
			{
				DataType: types.PropertyDataTypeDouble,
				Name:     toPtr("temperature"),
				Type: &types.PropertyType{
					Measurement: &types.Measurement{},
				},
			},
		},
	}

	// Define asset
	assets := make(map[string]assetDefintion)
	assets[thingId] = assetDefintion{
		assetId: "e9e11559-ceca-4c2f-875d-76c1068a45f4",
		modelId: modelId,
		thingId: thingId,
	}

	swclient.On("UpdateAssetModelProperties", ctx, mock.Anything, thingPropertiesMap(thingsMap[thingId]), mock.Anything).Return(nil)
	swclient.On("PollForModelActiveStatus", ctx, modelId, mock.Anything).Return(true)

	models := make(map[string]*string)
	models["temperature"] = toPtr(modelId)
	uomMap := make(map[string][]string)

	aligner := New(swclient, logger)
	_, errs := aligner.alignAlreadyCreatedModels(ctx, thingsMap, models, modelDefinitions, assets, uomMap)
	assert.Nil(t, errs)
	assert.Equal(t, 2, len(models))
	assert.Equal(t, modelId, *models["pressure,temperature"])
}

func TestAlign_IfAlignedNoCallsToSW(t *testing.T) {

	ctx := context.Background()
	logger := logrus.NewEntry(logrus.New())

	// Static id definitions
	thingId := "bb831f04-0940-4ea6-9c24-83668e372919"
	modelId := "03ba45c2-eab3-44ed-a68f-94a26d41df4c"

	// Mocks
	swclient := sitewiseMocks.NewAPI(t)

	// Define thing
	thingsMap := make(map[string]iotclient.ArduinoThing)
	thingsMap[thingId] = iotclient.ArduinoThing{
		Id: thingId,
		Properties: []iotclient.ArduinoProperty{
			{
				Name: "temperature",
				Type: "INT",
			},
		},
	}

	modelDefinitions := make(map[string]*iotsitewise.DescribeAssetModelOutput)
	modelDefinitions[modelId] = &iotsitewise.DescribeAssetModelOutput{
		AssetModelId: &modelId,
		AssetModelProperties: []types.AssetModelProperty{
			{
				DataType: types.PropertyDataTypeDouble,
				Name:     toPtr("temperature"),
				Type: &types.PropertyType{
					Measurement: &types.Measurement{},
				},
			},
		},
	}

	// Define asset
	assets := make(map[string]assetDefintion)
	assets[thingId] = assetDefintion{
		assetId: "e9e11559-ceca-4c2f-875d-76c1068a45f4",
		modelId: modelId,
		thingId: thingId,
	}

	// No calls to SW

	models := make(map[string]*string)
	models["temperature"] = &modelId
	uomMap := make(map[string][]string)

	aligner := New(swclient, logger)
	_, errs := aligner.alignAlreadyCreatedModels(ctx, thingsMap, models, modelDefinitions, assets, uomMap)
	assert.Nil(t, errs)
	assert.Equal(t, 1, len(models))
}

func toPtr(val string) *string {
	return &val
}
