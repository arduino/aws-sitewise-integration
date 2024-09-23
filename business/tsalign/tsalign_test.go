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

package tsalign

import (
	"context"
	"fmt"
	"testing"
	"time"

	iotapiMocks "github.com/arduino/aws-sitewise-integration/internal/iot/mocks"
	sitewiseMocks "github.com/arduino/aws-sitewise-integration/internal/sitewiseclient/mocks"
	iotclient "github.com/arduino/iot-client-go/v2"
	"github.com/aws/aws-sdk-go-v2/service/iotsitewise"
	"github.com/aws/aws-sdk-go-v2/service/iotsitewise/types"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestPartitionData(t *testing.T) {

	response := generateSamples(10)
	partitions := partitionResults(response)
	assert.Equal(t, 1, len(partitions))
	assert.Equal(t, 10, len(partitions[0].ts))
	assert.Equal(t, 10, len(partitions[0].values))

	response = generateSamples(35)
	partitions = partitionResults(response)
	assert.Equal(t, 4, len(partitions))
	assert.Equal(t, 10, len(partitions[0].ts))
	assert.Equal(t, 10, len(partitions[0].values))
	assert.Equal(t, 5, len(partitions[3].ts))
	assert.Equal(t, 5, len(partitions[3].values))

	response = generateSamples(60)
	partitions = partitionResults(response)
	assert.Equal(t, 6, len(partitions))
	for _, p := range partitions {
		assert.Equal(t, 10, len(p.ts))
		assert.Equal(t, 10, len(p.values))
	}
}

func generateSamples(howMany int) iotclient.ArduinoSeriesResponse {
	values := []float64{}
	ts := []time.Time{}
	now := time.Now()
	for i := 0; i < howMany; i++ {
		values = append(values, float64(i))
		ts = append(ts, now.Add(time.Duration(-i)*time.Second))
	}
	return iotclient.ArduinoSeriesResponse{
		Values: values,
		Times:  ts,
	}
}

func TestTSExtraction_extractSamplesForDefinedThings(t *testing.T) {
	ctx := context.Background()
	logger := logrus.NewEntry(logrus.New())

	// Static id definitions
	thingId := "bb831f04-0940-4ea6-9c24-83668e372919"
	modelId := "03ba45c2-eab3-44ed-a68f-94a26d41df4c"
	assetId := "e9e11559-ceca-4c2f-875d-76c1068a45f4"
	propertyId := "c86f4ed9-7f52-4bd3-bdc6-b2936bec68ac"
	propertyIdString := "a86f4ed9-7f52-4bd3-bdc6-b2936bec67de"

	// Mocks
	swclient := sitewiseMocks.NewAPI(t)
	arclient := iotapiMocks.NewAPI(t)

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
			{
				Name: "msg",
				Type: "CHARSTRING",
			},
		},
	}

	// API mocks
	swclient.On("ListAssetModels", ctx).Return(&iotsitewise.ListAssetModelsOutput{
		AssetModelSummaries: []types.AssetModelSummary{
			{
				Id: &modelId,
			},
		},
	}, nil).Once()
	swclient.On("ListAssets", ctx, &modelId).Return(&iotsitewise.ListAssetsOutput{
		AssetSummaries: []types.AssetSummary{
			{
				Id:         &assetId,
				Name:       toPtr("test"),
				ExternalId: &thingId,
			},
		},
	}, nil).Once()
	swclient.On("DescribeAsset", ctx, assetId).Return(&iotsitewise.DescribeAssetOutput{
		AssetId:         &assetId,
		AssetName:       toPtr("test"),
		AssetExternalId: toPtr(thingId),
		AssetProperties: []types.AssetProperty{
			{
				Name:     toPtr("temperature"),
				DataType: types.PropertyDataTypeDouble,
			},
			{
				Name:     toPtr("pressure"),
				DataType: types.PropertyDataTypeDouble,
			},
			{
				Name:     toPtr("msg"),
				DataType: types.PropertyDataTypeString,
			},
		},
	}, nil)

	// Mock data for iot-api
	now := time.Now()
	responses := []iotclient.ArduinoSeriesResponse{
		{
			Aggregation: toPtr("AVG"),
			Query:       fmt.Sprintf("property.%s", propertyId),
			Times:       []time.Time{now.Add(-time.Minute * 1), now},
			Values:      []float64{1.0, 2.0},
			CountValues: 2,
		},
	}
	samples := iotclient.ArduinoSeriesBatch{
		Responses: responses,
	}
	arclient.On("GetTimeSeriesByThing", ctx, thingId, mock.Anything, mock.Anything, int64(300)).Return(&samples, false, nil)
	arclient.On("GetTimeSeriesSampling", ctx, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&iotclient.ArduinoSeriesBatchSampled{
		Responses: []iotclient.ArduinoSeriesSampledResponse{
			{
				Query:       fmt.Sprintf("property.%s", propertyIdString),
				Times:       []time.Time{now.Add(-time.Minute * 1), now},
				Values:      []any{"msg1", "msg2"},
				CountValues: 2,
			},
		},
	}, false, nil)

	tsAligner := New(swclient, arclient, logger)
	errs := tsAligner.AlignTimeSeriesSamplesIntoSiteWise(ctx, 60, thingsMap, 300)
	assert.Nil(t, errs)
}

func toPtr(val string) *string {
	return &val
}
