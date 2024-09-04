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
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/arduino/aws-sitewise-integration/internal/iot"
	"github.com/arduino/aws-sitewise-integration/internal/sitewiseclient"
	iotclient "github.com/arduino/iot-client-go/v2"
	"github.com/aws/aws-sdk-go-v2/service/iotsitewise"
	"github.com/sirupsen/logrus"
)

const importConcurrency = 10

type TsAligner struct {
	sitewisecl *sitewiseclient.IotSiteWiseClient
	iotcl      *iot.Client
	logger     *logrus.Entry
}

func New(sitewisecl *sitewiseclient.IotSiteWiseClient, iotcl *iot.Client, logger *logrus.Entry) *TsAligner {
	return &TsAligner{sitewisecl: sitewisecl, iotcl: iotcl, logger: logger}
}

func (a *TsAligner) AlignTimeSeriesSamplesIntoSiteWise(
	ctx context.Context,
	timeWindowInMinutes int,
	thingsMap map[string]iotclient.ArduinoThing,
	resolution int) error {

	var wg sync.WaitGroup
	tokens := make(chan struct{}, importConcurrency)

	a.logger.Infoln("=====> Align perf data - last ", timeWindowInMinutes, " minutes")
	models, err := a.sitewisecl.ListAssetModels(ctx, nil)
	if err != nil {
		return err
	}
	for _, model := range models.AssetModelSummaries {
		continueimport := true
		var nextToken *string
		for continueimport {
			assets, err := a.sitewisecl.ListAssets(ctx, model.Id, nextToken)
			if err != nil {
				return err
			}

			for _, asset := range assets.AssetSummaries {
				if asset.ExternalId == nil {
					continue
				}
				// Asset external id is mapped on Thing ID
				thing, ok := thingsMap[*asset.ExternalId]
				if !ok {
					a.logger.Warn("Thing not found: ", *asset.ExternalId)
					continue
				}

				wg.Add(1)
				tokens <- struct{}{}

				go func(assetId string, assetName string) {
					defer func() { <-tokens }()
					defer wg.Done()

					describedAsset, err := a.sitewisecl.DescribeAsset(ctx, assetId)
					if err != nil {
						a.logger.Error("Error describing asset: ", assetId, err)
						return
					}

					propertiesToImport, propertiesToImportAliases := a.mapPropertiesToImport(describedAsset, thing, assetName)

					err = a.populateTSDataIntoSiteWise(ctx, timeWindowInMinutes, *asset.ExternalId, propertiesToImport, propertiesToImportAliases, resolution)
					if err != nil {
						a.logger.Error("Error populating time series data: ", err)
						return
					}
				}(*asset.Id, *asset.Name)
			}

			nextToken = assets.NextToken
			if nextToken == nil {
				continueimport = false
			}
		}
	}

	// Wait for all routines termination
	wg.Wait()

	return nil
}

func (a *TsAligner) mapPropertiesToImport(describedAsset *iotsitewise.DescribeAssetOutput, thing iotclient.ArduinoThing, assetName string) ([]string, map[string]string) {
	propertiesToImport := make([]string, 0, len(describedAsset.AssetProperties))
	propertiesToImportAliases := make(map[string]string, len(describedAsset.AssetProperties))
	for _, prop := range describedAsset.AssetProperties {
		for _, thingProperty := range thing.Properties {
			if *prop.Name == thingProperty.Name {
				a.logger.Infoln("  Importing TS for: ", assetName, *prop.Name, " thingPropertyId: ", thingProperty.Id)
				propertiesToImport = append(propertiesToImport, thingProperty.Id)
				propertiesToImportAliases[thingProperty.Id] = fmt.Sprintf("/%s/%s", thing.Name, *prop.Name)
			}
		}
	}
	return propertiesToImport, propertiesToImportAliases
}

func (a *TsAligner) populateTSDataIntoSiteWise(
	ctx context.Context,
	loopMinutes int,
	thingID string,
	propertiesToImport []string,
	propertiesToImportAliases map[string]string,
	resolution int) error {

	to := time.Now().Truncate(time.Hour).UTC()
	from := to.Add(-time.Duration(loopMinutes) * time.Minute)
	var batched *iotclient.ArduinoSeriesBatch
	var err error
	var retry bool
	for i := 0; i < 3; i++ {
		if thingID != "" {
			batched, retry, err = a.iotcl.GetTimeSeriesByThing(ctx, thingID, from, to, int64(resolution))
		} else {
			batched, retry, err = a.iotcl.GetTimeSeries(ctx, propertiesToImport, from, to, int64(resolution))
		}
		if !retry {
			break
		} else {
			// This is due to a rate limit on the IoT API, we need to wait a bit before retrying
			a.logger.Infof("Rate limit reached for thing %s. Waiting 1 second before retrying.\n", thingID)
			time.Sleep(1 * time.Second)
		}
	}
	if err != nil {
		return err
	}
	for _, response := range batched.Responses {
		if response.CountValues == 0 {
			continue
		}

		propertyID := strings.Replace(response.Query, "property.", "", 1)
		if !slices.Contains(propertiesToImport, propertyID) {
			a.logger.Infof("Not mapped property %s. Skipping import.\n", propertyID)
			continue
		}
		alias := propertiesToImportAliases[propertyID]
		if alias == "" {
			a.logger.Warn("Alias not found. Skipping import.")
			continue
		}

		chunks := partitionResults(response)
		for _, c := range chunks {
			a.logger.Infoln("  Importing ", len(c.ts), " data points for: ", alias, " - ts:", joinTs(c.ts))
			erri := a.sitewisecl.PopulateTimeSeriesByAlias(ctx, alias, c.ts, c.values)
			if erri != nil {
				return err
			}
		}
	}
	return nil
}

type chunk struct {
	ts     []int64
	values []float64
}

// To be coherent with SiteWise API, we need to partition the results in chunks of 10 elements
func partitionResults(response iotclient.ArduinoSeriesResponse) []chunk {
	chunks := []chunk{}
	for i := 0; i < len(response.Times); i += 10 {
		end := i + 10
		if end > len(response.Times) {
			end = len(response.Times)
		}

		times := response.Times[i:end]
		unixTimes := make([]int64, len(times))
		for j := 0; j < len(times); j++ {
			unixTimes[j] = times[j].Unix()
		}
		c := chunk{
			ts:     unixTimes,
			values: response.Values[i:end],
		}
		chunks = append(chunks, c)
	}
	return chunks
}

func joinTs(ts []int64) string {
	tsarr := []string{}
	for _, v := range ts {
		tsarr = append(tsarr, fmt.Sprintf("%d", v))
	}
	return strings.Join(tsarr, ",")
}
