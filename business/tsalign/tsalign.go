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

	"crypto/rand"
	"math/big"

	"github.com/arduino/aws-sitewise-integration/business/entityalign"
	"github.com/arduino/aws-sitewise-integration/internal/iot"
	"github.com/arduino/aws-sitewise-integration/internal/sitewiseclient"
	iotclient "github.com/arduino/iot-client-go/v2"
	"github.com/aws/aws-sdk-go-v2/service/iotsitewise"
	"github.com/sirupsen/logrus"
)

const importConcurrency = 10
const retryCount = 5

type TsAligner struct {
	sitewisecl sitewiseclient.API
	iotcl      iot.API
	logger     *logrus.Entry
}

func New(sitewisecl sitewiseclient.API, iotcl iot.API, logger *logrus.Entry) *TsAligner {
	return &TsAligner{sitewisecl: sitewisecl, iotcl: iotcl, logger: logger}
}

func (a *TsAligner) getAllModels(ctx context.Context) ([]*iotsitewise.ListAssetModelsOutput, error) {
	results := []*iotsitewise.ListAssetModelsOutput{}
	var nextToken *string
	models, err := a.sitewisecl.ListAssetModels(ctx)
	if err != nil {
		return nil, err
	}
	results = append(results, models)
	for models.NextToken != nil {
		models, err = a.sitewisecl.ListAssetModelsNext(ctx, nextToken)
		if err != nil {
			return nil, err
		}
		results = append(results, models)
	}
	return results, nil
}

func (a *TsAligner) AlignTimeSeriesSamplesIntoSiteWise(
	ctx context.Context,
	timeWindowInMinutes int,
	thingsMap map[string]iotclient.ArduinoThing,
	resolution int) []error {

	var wg sync.WaitGroup
	tokens := make(chan struct{}, importConcurrency)
	errorChannel := make(chan error, len(thingsMap))

	from, to := computeTimeAlignment(resolution, timeWindowInMinutes)

	a.logger.Infoln("=====> Align perf data - time window ", timeWindowInMinutes, " minutes - from ", from, " to ", to, " - resolution ", resolution, " seconds")
	allModels, err := a.getAllModels(ctx)
	if err != nil {
		return []error{err}
	}

	for _, models := range allModels {
		for _, model := range models.AssetModelSummaries {
			continueimport := true
			var nextToken *string
			for continueimport {
				var assets *iotsitewise.ListAssetsOutput
				if nextToken != nil {
					assets, err = a.sitewisecl.ListAssetsNext(ctx, model.Id, nextToken)
				} else {
					assets, err = a.sitewisecl.ListAssets(ctx, model.Id)
				}
				if err != nil {
					return []error{err}
				}

				for _, asset := range assets.AssetSummaries {
					if asset.ExternalId == nil {
						a.logger.Warn("Asset external id not found, skipping it: ", *asset.Name)
						continue
					}
					// Asset external id is mapped on Thing ID
					thing, ok := thingsMap[*asset.ExternalId]
					if !ok {
						a.logger.Debug("Thing not found, not detected by import filters: ", *asset.ExternalId)
						continue
					}
					propertiesMap := make(map[string]iotclient.ArduinoProperty, len(thing.Properties))
					for _, p := range thing.Properties {
						propertiesMap[p.Id] = p
					}

					tokens <- struct{}{}
					wg.Add(1)

					go func(assetId, assetName, externalId string, propertiesMap map[string]iotclient.ArduinoProperty) {
						defer func() { <-tokens }()
						defer wg.Done()

						describedAsset, err := a.sitewisecl.DescribeAsset(ctx, assetId)
						if err != nil {
							a.logger.Error("Error describing asset: ", assetId, err)
							return
						}

						mappedProperties := a.mapPropertiesToImport(describedAsset, thing, assetName)

						importedProperties := []string{}
						if len(mappedProperties.PropertiesToImport) > 0 {
							p, err := a.populateTSDataIntoSiteWise(ctx, externalId, mappedProperties, resolution, from, to)
							if err != nil {
								a.logger.Error("Error populating time series data: ", err)
								errorChannel <- err
								return
							}
							if len(p) > 0 {
								importedProperties = append(importedProperties, p...)
							}
						}

						if len(mappedProperties.CharPropertiesToImport) > 0 {
							p, err := a.populateCharTSDataIntoSiteWise(ctx, externalId, mappedProperties, resolution, from, to)
							if err != nil {
								a.logger.Error("Error populating string based time series data: ", err)
								errorChannel <- err
								return
							}
							if len(p) > 0 {
								importedProperties = append(importedProperties, p...)
							}
						}

						// Check if there are properties that have been imported (on_change - import last value)
						err = a.populateLastValueForOnChangeProperties(ctx, propertiesMap, importedProperties, mappedProperties.PropertiesToImportAliases)
						if err != nil {
							a.logger.Error("Error populating last values time series data: ", err)
							errorChannel <- err
							return
						}

					}(*asset.Id, *asset.Name, *asset.ExternalId, propertiesMap)
				}

				nextToken = assets.NextToken
				if nextToken == nil {
					continueimport = false
				}
			}
		}
	}

	// Wait for all routines termination
	wg.Wait()
	close(errorChannel)

	// Check if there were errors
	errorsToReturn := []error{}
	for err := range errorChannel {
		if err != nil {
			errorsToReturn = append(errorsToReturn, err)
		}
	}
	if len(errorsToReturn) > 0 {
		a.logger.Warnln("=====> Detected execution errors...")
		return errorsToReturn
	}

	return nil
}

type mappedProperties struct {
	PropertiesToImport        []string
	CharPropertiesToImport    []string
	PropertiesToImportAliases map[string]string
}

func (a *TsAligner) mapPropertiesToImport(describedAsset *iotsitewise.DescribeAssetOutput, thing iotclient.ArduinoThing, assetName string) *mappedProperties {
	propertiesToImport := []string{}
	charPropertiesToImport := []string{}
	propertiesToImportAliases := make(map[string]string, len(describedAsset.AssetProperties))
	for _, prop := range describedAsset.AssetProperties {
		for _, thingProperty := range thing.Properties {
			if *prop.Name == thingProperty.Name {
				a.logger.Debugln("  Importing TS for: ", assetName, *prop.Name, " thingPropertyId: ", thingProperty.Id)
				if iot.IsPropertyString(thingProperty.Type) || iot.IsPropertyLocation(thingProperty.Type) {
					charPropertiesToImport = append(charPropertiesToImport, thingProperty.Id)
				} else {
					propertiesToImport = append(propertiesToImport, thingProperty.Id)
				}
				propertiesToImportAliases[thingProperty.Id] = entityalign.PropertyAlias(thing.Id, *prop.Name)
			}
		}
	}
	return &mappedProperties{
		PropertiesToImport:        propertiesToImport,
		CharPropertiesToImport:    charPropertiesToImport,
		PropertiesToImportAliases: propertiesToImportAliases,
	}
}

func computeTimeAlignment(resolutionSeconds, timeWindowInMinutes int) (time.Time, time.Time) {
	// Compute time alignment
	if resolutionSeconds <= 60 {
		resolutionSeconds = 300 // Align to 5 minutes
	}
	to := time.Now().Truncate(time.Duration(resolutionSeconds) * time.Second).UTC()
	if resolutionSeconds <= 900 {
		// Shift time window to avoid missing data
		to = to.Add(-time.Duration(300) * time.Second)
	}
	from := to.Add(-time.Duration(timeWindowInMinutes) * time.Minute)
	return from, to
}

func randomRateLimitingSleep() {
	// Random sleep to avoid rate limiting (1s + random(0-500ms))
	n, err := rand.Int(rand.Reader, big.NewInt(500))
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	randomSleep := n.Int64() + 1000
	time.Sleep(time.Duration(randomSleep) * time.Millisecond)
}

func (a *TsAligner) populateTSDataIntoSiteWise(
	ctx context.Context,
	thingID string,
	mappedProperties *mappedProperties,
	resolution int,
	from, to time.Time) ([]string, error) {

	propertiesImported := []string{}
	var batched *iotclient.ArduinoSeriesBatch
	var err error
	var retry bool
	for i := 0; i < retryCount; i++ {
		batched, retry, err = a.iotcl.GetTimeSeriesByThing(ctx, thingID, from, to, int64(resolution))
		if !retry {
			break
		} else {
			// This is due to a rate limit on the IoT API, we need to wait a bit before retrying
			a.logger.Infof("Rate limit reached for thing %s. Waiting before retrying.\n", thingID)
			randomRateLimitingSleep()
		}
	}
	if err != nil {
		return nil, err
	}
	for _, response := range batched.Responses {
		if response.CountValues == 0 {
			continue
		}

		propertyID := strings.Replace(response.Query, "property.", "", 1)
		if !slices.Contains(mappedProperties.PropertiesToImport, propertyID) {
			propertiesImported = append(propertiesImported, propertyID)
		}
		if !slices.Contains(mappedProperties.PropertiesToImport, propertyID) {
			a.logger.Debugf("Not mapped property %s. Skipping import.\n", propertyID)
			continue
		}
		alias := mappedProperties.PropertiesToImportAliases[propertyID]
		if alias == "" {
			a.logger.Warn("Alias not found. Skipping import.")
			continue
		}

		chunks := partitionResults(response)
		for _, c := range chunks {
			a.logger.Debugln("  Importing ", len(c.ts), " data points for: ", alias, " - ts:", joinTs(c.ts))
			erri := a.sitewisecl.PopulateTimeSeriesByAlias(ctx, alias, c.ts, c.values)
			if erri != nil {
				return nil, err
			}
		}
	}
	return propertiesImported, nil
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

func (a *TsAligner) populateCharTSDataIntoSiteWise(
	ctx context.Context,
	thingID string,
	mappedProperties *mappedProperties,
	resolution int,
	from, to time.Time) ([]string, error) {

	propertiesImported := []string{}
	var batched *iotclient.ArduinoSeriesBatchSampled
	var err error
	var retry bool
	for i := 0; i < retryCount; i++ {
		// ctx context.Context, propertiesToImport []string, from, to time.Time, interval int32
		batched, retry, err = a.iotcl.GetTimeSeriesSampling(ctx, mappedProperties.CharPropertiesToImport, from, to, int32(resolution))
		if !retry {
			break
		} else {
			// This is due to a rate limit on the IoT API, we need to wait a bit before retrying
			a.logger.Infof("Rate limit reached for thing %s. Waiting before retrying.\n", thingID)
			randomRateLimitingSleep()
		}
	}
	if err != nil {
		return nil, err
	}
	for _, response := range batched.Responses {
		if response.CountValues == 0 {
			continue
		}

		propertyID := strings.Replace(response.Query, "property.", "", 1)
		if !slices.Contains(mappedProperties.CharPropertiesToImport, propertyID) {
			propertiesImported = append(propertiesImported, propertyID)
		}
		if !slices.Contains(mappedProperties.CharPropertiesToImport, propertyID) {
			a.logger.Debugf("Not mapped property %s. Skipping import.\n", propertyID)
			continue
		}
		alias := mappedProperties.PropertiesToImportAliases[propertyID]
		if alias == "" {
			a.logger.Warn("Alias not found. Skipping import.")
			continue
		}

		chunks := partitionSampledResults(response)
		for _, c := range chunks {
			a.logger.Debugln("  Importing ", len(c.ts), " data points for: ", alias, " - ts:", joinTs(c.ts))
			erri := a.sitewisecl.PopulateSampledSamplesTimeSeriesByAlias(ctx, alias, c.ts, c.values)
			if erri != nil {
				return nil, err
			}
		}
	}
	return propertiesImported, nil
}

type chunkAnyValue struct {
	ts     []int64
	values []any
}

// To be coherent with SiteWise API, we need to partition the results in chunks of 10 elements
func partitionSampledResults(response iotclient.ArduinoSeriesSampledResponse) []chunkAnyValue {
	chunks := []chunkAnyValue{}
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
		c := chunkAnyValue{
			ts:     unixTimes,
			values: response.Values[i:end],
		}
		chunks = append(chunks, c)
	}
	return chunks
}

func isLastValueAllowedPropertyType(pType string) bool {
	return iot.IsPropertyString(pType) || iot.IsPropertyNumberType(pType) || iot.IsPropertyBool(pType) || iot.IsPropertyLocation(pType)
}

func (a *TsAligner) populateLastValueForOnChangeProperties(
	ctx context.Context,
	propertiesMap map[string]iotclient.ArduinoProperty,
	importedProperties []string,
	propertiesToImportAliases map[string]string) error {

	lastValuesToImport := []sitewiseclient.DataPoint{}
	now := time.Now().UTC()
	for propertyId, alias := range propertiesToImportAliases {
		if !slices.Contains(importedProperties, propertyId) {
			property, ok := propertiesMap[propertyId]
			if !ok || property.LastValue == nil || property.UpdateStrategy != "ON_CHANGE" {
				continue
			}

			if isLastValueAllowedPropertyType(property.Type) {
				a.logger.Debugln("  + Importing last value for: ", alias, " - name ", property.Name, " - last value: ", property.UpdateStrategy, " - ", property.LastValue)
				lastValuesToImport = append(lastValuesToImport, sitewiseclient.DataPoint{
					PropertyAlias: alias,
					Ts:            now.Unix(),
					Value:         property.LastValue,
				})
			}
		}
	}
	if len(lastValuesToImport) > 0 {
		err := a.sitewisecl.PopulateArbitrarySamplesByAlias(ctx, lastValuesToImport)
		if err != nil {
			a.logger.Error("Error populating last values time series data: ", err)
			return err
		}
	}

	return nil
}
