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
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"

	"github.com/arduino/aws-sitewise-integration/internal/sitewiseclient"
	iotclient "github.com/arduino/iot-client-go/v2"
	"github.com/aws/aws-sdk-go-v2/service/iotsitewise"
	"github.com/aws/aws-sdk-go-v2/service/iotsitewise/types"
	"github.com/sirupsen/logrus"
)

const (
	alignParallelism = 6
	keySeparator     = ","
)

type aligner struct {
	sitewisecl sitewiseclient.API
	logger     *logrus.Entry
}

func New(sitewisecl sitewiseclient.API, logger *logrus.Entry) *aligner {
	return &aligner{
		sitewisecl: sitewisecl,
		logger:     logger,
	}
}

func (a *aligner) Align(ctx context.Context, things []iotclient.ArduinoThing, propertyDefinitions map[string]iotclient.ArduinoPropertytype) []error {
	a.logger.Infoln("=====> Aligning entities")
	thingsMap := toThingMap(things)
	uomMap := extractUomMap(propertyDefinitions)
	models, modelDefinitions, err := a.getSiteWiseModels(ctx)
	if err != nil {
		return []error{err}
	}
	assets, err := a.getSiteWiseAssets(ctx, models)
	if err != nil {
		return []error{err}
	}

	a.logger.Infoln("=====> Discovered models:")
	for k, v := range models {
		a.logger.Infoln("  Model ["+*v+"] - key:", k)
	}

	// Align model assests with things for new properties added
	a.logger.Infoln("=====> Aligning already created models with things")
	models, errs := a.alignAlreadyCreatedModels(ctx, thingsMap, models, modelDefinitions, assets, uomMap)
	if len(errs) > 0 {
		return errs
	}

	// Align not discovered models
	a.logger.Infoln("=====> Create newly discovred models")
	models, errs = a.alignModels(ctx, things, models, uomMap)
	if len(errs) > 0 {
		return errs
	}

	// All models are created, now create assets. These can be done in parallel.
	a.logger.Infoln("=====> Aligning and create assets")
	return a.alignAssets(ctx, things, models, assets)
}

func (a *aligner) alignAlreadyCreatedModels(
	ctx context.Context,
	thingsMap map[string]iotclient.ArduinoThing,
	models map[string]*string,
	modelDefinitions map[string]*iotsitewise.DescribeAssetModelOutput,
	assets map[string]assetDefintion,
	uomMap map[string][]string) (map[string]*string, []error) {

	for _, asset := range assets {
		a.logger.Debugln("Asset: ", asset.assetId, " - model: ", asset.modelId, " - thing: ", asset.thingId)
		// Get associated thing
		thing, ok := thingsMap[asset.thingId]
		if !ok {
			a.logger.Debugln("Thing not found for asset, not detected by import filters: ", asset.assetId, ". Skipping.")
			continue
		}
		thingKey := buildModelKeyFromThing(thing)

		// Get model key from associated model
		descModel, ok := modelDefinitions[asset.modelId]
		if !ok {
			a.logger.Debugln("Model not found for asset: ", asset.assetId, ". Skipping.")
			continue
		}
		if len(descModel.AssetModelProperties) > 0 {
			modelKey, ok := buildModelKeyFromModel(descModel)
			if !ok {
				continue
			}
			// Check if model key is the same as thing key
			if modelKey != thingKey && thingKey != "" && modelKey != "" {
				if isThingContainedInModel(modelKey, thingKey) {
					a.logger.Infoln("Thing is contained into given model, skipping model update. Model: ", descModel.AssetModelId, " - key: ", modelKey, " - thing: ", thing.Id)
				} else {
					a.logger.Warnln("Model and thing are not aligned. Model(key): ", modelKey, " - Thing(key): ", thingKey)
					err := a.sitewisecl.UpdateAssetModelProperties(ctx, descModel, thingPropertiesMap(thing), uomMap)
					if err != nil {
						a.logger.Errorln("Error updating model properties for asset: ", asset.assetId, err)
						return models, []error{err}
					}
					a.logger.Infoln("Model properties updated for model: ", *descModel.AssetModelId, " - key: ", modelKey, " - thing: ", thing.Id, " - wait for model to be active...")
					a.sitewisecl.PollForModelActiveStatus(ctx, *descModel.AssetModelId, 5)
				}

				models[thingKey] = descModel.AssetModelId
			}
			continue
		} else {
			a.logger.Warnln("Model has no properties, skipping.")
			continue
		}
	}

	return models, nil
}

func (a *aligner) alignModels(ctx context.Context, things []iotclient.ArduinoThing, models map[string]*string, uomMap map[string][]string) (map[string]*string, []error) {
	// Understand if there are models to create
	for _, thing := range things {
		propsTypeMap := make(map[string]string, len(thing.Properties))
		for _, prop := range thing.Properties {
			propsTypeMap[prop.Name] = prop.Type
		}

		key := buildModelKeyFromMap(propsTypeMap)
		a.logger.Debugln("Searching for model with key: ", key)

		// Discover thing properties
		_, ok := models[key]
		if !ok {
			a.logger.Infoln("Model not found for thing: ", thing.Id, thing.Name, ". Creating it.")
			var createdModel *iotsitewise.CreateAssetModelOutput
			var err error
			var modelName string
			for i := 0; i < 100; i++ {
				modelName = composeModelName(thing.Name, i)
				createdModel, err = a.sitewisecl.CreateAssetModel(ctx, modelName, propsTypeMap, uomMap)
				if err != nil {
					var errConflicc *types.ResourceAlreadyExistsException
					if errors.As(err, &errConflicc) {
						a.logger.Infoln("  Model already exists with the same name, retry")
						continue
					}
					return models, []error{err}
				}
				// If model is created, exit the loop
				break
			}

			a.logger.Infof("Wait for model [%s] to be active...\n", modelName)
			a.sitewisecl.PollForModelActiveStatus(ctx, *createdModel.AssetModelId, 5)
			models[key] = createdModel.AssetModelId
		}

	}

	return models, nil
}

func (a *aligner) alignAssets(ctx context.Context, things []iotclient.ArduinoThing, models map[string]*string, assets map[string]assetDefintion) []error {
	var wg sync.WaitGroup
	tokens := make(chan struct{}, alignParallelism)
	errorChannel := make(chan error, len(things))

	for _, thing := range things {
		propsAliasMap := make(map[string]string, len(thing.Properties))
		propsTypeMap := make(map[string]string, len(thing.Properties))
		for _, prop := range thing.Properties {
			propsAliasMap[prop.Name] = PropertyAlias(thing.Id, prop.Name)
			propsTypeMap[prop.Name] = prop.Type
		}

		key := buildModelKeyFromMap(propsTypeMap)
		a.logger.Infoln("=====> Aligning thing: ", thing.Id, " - name: ", thing.Name, " - model key: ", key)

		// Discover thing properties
		model, ok := models[key]
		var modelId *string
		if !ok {
			a.logger.Errorln("Model not found for thing: ", thing.Id, thing.Name, ". Skipping.")
			continue
		} else {
			modelId = model
		}

		tokens <- struct{}{}
		wg.Add(1)

		go func(modelIdentifier string) {
			defer func() { <-tokens }()
			defer wg.Done()

			var assetId *string
			asset, ok := assets[thing.Id]
			if ok {
				a.logger.Debugln("Thing is already aligned, skipping creation. ID: ", thing.Id)
				assetId = &asset.assetId
			} else {
				// Create asset
				a.logger.Infoln("Creating asset for thing: ", thing.Id)
				assetObj, err := a.sitewisecl.CreateAsset(ctx, thing.Name, modelIdentifier, thing.Id)
				if err != nil {
					a.logger.Errorln("Error creating asset for thing: ", thing.Id, thing.Name, err)
					errorChannel <- err
					return
				}
				assetId = assetObj.AssetId

				// Wait for asset to be active before updating properties...
				a.sitewisecl.PollForAssetActiveStatus(ctx, *assetId, 10)
			}

			err := a.sitewisecl.UpdateAssetProperties(ctx, *assetId, propsAliasMap)
			if err != nil {
				a.logger.Errorln("Error updating asset properties for thing: ", thing.Id, thing.Name, err)
				errorChannel <- err
			}
		}(*modelId)
	}

	a.logger.Infoln("=====> Wait for tasks completion...")
	// Wait for all assets to be created
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

func composeModelName(thingName string, increment int) string {
	if increment == 0 {
		return fmt.Sprintf("Thing Model from (%s)", thingName)
	} else {
		return fmt.Sprintf("Thing Model from (%s) - %d", thingName, increment)
	}
}

func PropertyAlias(thingId, propertyName string) string {
	return fmt.Sprintf("/%s/%s", thingId, propertyName)
}

type assetDefintion struct {
	assetId string
	modelId string
	thingId string
}

func (a *aligner) getSiteWiseAssets(ctx context.Context, models map[string]*string) (map[string]assetDefintion, error) {
	discoveredAssets := make(map[string]assetDefintion)
	a.logger.Infoln("=====> Get SiteWise assets")
	for _, modelId := range models {
		next := true
		var token *string
		for next {
			assets, err := a.sitewisecl.ListAssets(ctx, modelId, token)
			if err != nil {
				return nil, err
			}
			if assets.NextToken == nil {
				next = false
			} else {
				token = assets.NextToken
			}

			// Discover assets. Keep only the one with externalId. ExternalId is mapped to thingId
			for _, asset := range assets.AssetSummaries {
				if asset.ExternalId != nil {
					discoveredAssets[*asset.ExternalId] = assetDefintion{
						assetId: *asset.Id,
						modelId: *modelId,
						thingId: *asset.ExternalId,
					}
				}
			}
		}
	}
	return discoveredAssets, nil
}

func (a *aligner) getSiteWiseModels(ctx context.Context) (map[string]*string, map[string]*iotsitewise.DescribeAssetModelOutput, error) {
	discoveredModels := make(map[string]*string)
	modelDefinitions := make(map[string]*iotsitewise.DescribeAssetModelOutput)
	a.logger.Infoln("=====> Get SiteWise models")
	next := true
	var token *string
	for next {
		models, err := a.sitewisecl.ListAssetModels(ctx, token)
		if err != nil {
			return nil, nil, err
		}
		if models.NextToken == nil {
			next = false
		} else {
			token = models.NextToken
		}

		// Discover models
		for _, model := range models.AssetModelSummaries {
			descModel, err := a.sitewisecl.DescribeAssetModel(ctx, model.Id)
			if err != nil {
				return nil, nil, err
			}
			modelDefinitions[*model.Id] = descModel

			if len(descModel.AssetModelProperties) > 0 {
				key, ok := buildModelKeyFromModel(descModel)
				if ok {
					discoveredModels[key] = model.Id
				}
			}
		}
	}
	return discoveredModels, modelDefinitions, nil
}

func buildModelKeyFromModel(descModel *iotsitewise.DescribeAssetModelOutput) (string, bool) {
	props := make([]string, 0, len(descModel.AssetModelProperties))
	for _, prop := range descModel.AssetModelProperties {
		if prop.Type != nil && *prop.Name != "" && prop.Type.Measurement != nil { // Check if property is a measurement, not an aggregate
			props = append(props, *prop.Name)
		}
	}
	if len(props) > 0 {
		return buildKey(props), true
	}
	return "", false
}

func buildKey(props []string) string {
	slices.Sort(props)
	return strings.Join(props, keySeparator)
}

func splitKey(key string) []string {
	return strings.Split(key, keySeparator)
}

func buildModelKeyFromMap(propMap map[string]string) string {
	props := make([]string, 0, len(propMap))
	for k := range propMap {
		if propMap[k] != "" {
			props = append(props, k)
		}
	}
	return buildKey(props)
}

func buildModelKeyFromThing(thing iotclient.ArduinoThing) string {
	propsTypeMap := thingPropertiesMap(thing)
	return buildModelKeyFromMap(propsTypeMap)
}

func toThingMap(things []iotclient.ArduinoThing) map[string]iotclient.ArduinoThing {
	thingMap := make(map[string]iotclient.ArduinoThing, len(things))
	for _, thing := range things {
		thingMap[thing.Id] = thing
	}
	return thingMap
}

func thingPropertiesMap(thing iotclient.ArduinoThing) map[string]string {
	props := make(map[string]string, len(thing.Properties))
	for _, prop := range thing.Properties {
		props[prop.Name] = prop.Type
	}
	return props
}

func isThingContainedInModel(modelKey, thingKey string) bool {
	modelProps := splitKey(modelKey)
	thingProps := splitKey(thingKey)
	for _, prop := range thingProps {
		if !slices.Contains(modelProps, prop) {
			return false
		}
	}
	return true
}

func extractUomMap(types map[string]iotclient.ArduinoPropertytype) map[string][]string {
	uomMap := make(map[string][]string)
	for _, prop := range types {
		if len(prop.Units) > 0 {
			uomMap[prop.Name] = prop.Units
		}
	}
	return uomMap
}
