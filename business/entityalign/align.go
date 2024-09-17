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

func Align(ctx context.Context, logger *logrus.Entry, things []iotclient.ArduinoThing, sitewisecl *sitewiseclient.IotSiteWiseClient) []error {
	logger.Infoln("=====> Aligning entities")
	thingsMap := toThingMap(things)
	models, modelDefinitions, err := getSiteWiseModels(ctx, logger, sitewisecl)
	if err != nil {
		return []error{err}
	}
	assets, err := getSiteWiseAssets(ctx, logger, sitewisecl, models)
	if err != nil {
		return []error{err}
	}

	logger.Infoln("Discovered models:")
	for k, v := range models {
		logger.Infoln("  Model ["+*v+"] - key:", k)
	}

	// Align model assests with things for new properties added
	logger.Infoln("=====> Aligning already created models with things")
	for _, asset := range assets {
		logger.Debugln("Asset: ", asset.assetId, " - model: ", asset.modelId, " - thing: ", asset.thingId)
		// Get associated thing
		thing, ok := thingsMap[asset.thingId]
		if !ok {
			logger.Debugln("Thing not found for asset: ", asset.assetId, ". Skipping.")
			continue
		}
		thingKey := buildModelKeyFromThing(thing)

		// Get model key from associated model
		descModel, ok := modelDefinitions[asset.modelId]
		if !ok {
			logger.Debugln("Model not found for asset: ", asset.assetId, ". Skipping.")
			continue
		}
		if len(descModel.AssetModelProperties) > 0 {
			key, ok := buildModelKeyFromModel(descModel)
			if !ok {
				continue
			}
			// Check if model key is the same as thing key
			if key != thingKey && thingKey != "" && key != "" {
				logger.Warnln("Model and thing are not aligned. Model(key): ", key, " - Thing(key): ", thingKey)
				err := sitewisecl.UpdateAssetModelProperties(ctx, descModel, thingPropertiesMap(thing))
				if err != nil {
					logger.Errorln("Error updating model properties for asset: ", asset.assetId, err)
					return []error{err}
				}
				logger.Infoln("Model properties updated for model: ", descModel.AssetModelId, " - key: ", key, " - thing: ", thing.Id, " - wait for model to be active...")
				sitewisecl.PollForModelActiveStatus(ctx, *descModel.AssetModelId, 10)
				models[thingKey] = descModel.AssetModelId
			}
			continue
		} else {
			logger.Warnln("Model has no properties, skipping.")
			continue
		}
	}

	// Align not discovered models
	logger.Infoln("=====> Create newly discovred models")
	models, errs := alignModels(ctx, sitewisecl, logger, things, models)
	if len(errs) > 0 {
		return errs
	}

	// All models are created, now create assets. These can be done in parallel.
	var wg sync.WaitGroup
	tokens := make(chan struct{}, 5)
	errorChannel := make(chan error, len(things))

	logger.Infoln("=====> Aligning and create assets")
	for _, thing := range things {
		propsAliasMap := make(map[string]string, len(thing.Properties))
		propsTypeMap := make(map[string]string, len(thing.Properties))
		for _, prop := range thing.Properties {
			propsAliasMap[prop.Name] = PropertyAlias(thing.Id, prop.Name)
			propsTypeMap[prop.Name] = prop.Type
		}

		key := buildModelKeyFromMap(propsTypeMap)
		logger.Infoln("=====> Aligning thing: ", thing.Id, " - name: ", thing.Name, " - model key: ", key)

		// Discover thing properties
		model, ok := models[key]
		var modelId *string
		if !ok {
			logger.Errorln("Model not found for thing: ", thing.Id, thing.Name, ". Skipping.")
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
				logger.Debugln("Thing is already aligned, skipping creation. ID: ", thing.Id)
				assetId = &asset.assetId
			} else {
				// Create asset
				logger.Infoln("Creating asset for thing: ", thing.Id)
				assetObj, err := sitewisecl.CreateAsset(ctx, thing.Name, modelIdentifier, thing.Id)
				if err != nil {
					logger.Errorln("Error creating asset for thing: ", thing.Id, thing.Name, err)
					errorChannel <- err
					return
				}
				assetId = assetObj.AssetId

				// Wait for asset to be active before updating properties...
				sitewisecl.PollForAssetActiveStatus(ctx, *assetId, 10)
			}

			err = sitewisecl.UpdateAssetProperties(ctx, *assetId, propsAliasMap)
			if err != nil {
				logger.Errorln("Error updating asset properties for thing: ", thing.Id, thing.Name, err)
				errorChannel <- err
			}
		}(*modelId)
	}

	logger.Infoln("=====> Wait for tasks completion...")
	// Wait for all assets to be created
	wg.Wait()
	close(errorChannel)

	logger.Infoln("=====> Check for errors...")
	// Check if there were errors
	errorsToReturn := []error{}
	for err := range errorChannel {
		if err != nil {
			errorsToReturn = append(errorsToReturn, err)
		}
	}
	if len(errorsToReturn) > 0 {
		return errorsToReturn
	}

	return nil
}

func alignModels(ctx context.Context, sitewisecl *sitewiseclient.IotSiteWiseClient, logger *logrus.Entry, things []iotclient.ArduinoThing, models map[string]*string) (map[string]*string, []error) {
	// Understand if there are models to create
	for _, thing := range things {
		propsTypeMap := make(map[string]string, len(thing.Properties))
		for _, prop := range thing.Properties {
			propsTypeMap[prop.Name] = prop.Type
		}

		key := buildModelKeyFromMap(propsTypeMap)
		logger.Debugln("Searching for model with key: ", key)

		// Discover thing properties
		_, ok := models[key]
		if !ok {
			logger.Infoln("Model not found for thing: ", thing.Id, thing.Name, ". Creating it.")
			var createdModel *iotsitewise.CreateAssetModelOutput
			var err error
			var modelName string
			for i := 0; i < 100; i++ {
				modelName = composeModelName(thing.Name, i)
				createdModel, err = sitewisecl.CreateAssetModel(ctx, modelName, propsTypeMap)
				if err != nil {
					var errConflicc *types.ResourceAlreadyExistsException
					if errors.As(err, &errConflicc) {
						logger.Infoln("  Model already exists with the same name, retry")
						continue
					}
					return models, []error{err}
				}
				// If model is created, exit the loop
				break
			}

			logger.Infof("Wait for model [%s] to be active...\n", modelName)
			sitewisecl.PollForModelActiveStatus(ctx, *createdModel.AssetModelId, 10)
			models[key] = createdModel.AssetModelId
		}

	}

	return models, nil
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

func getSiteWiseAssets(ctx context.Context, logger *logrus.Entry, sitewisecl *sitewiseclient.IotSiteWiseClient, models map[string]*string) (map[string]assetDefintion, error) {
	discoveredAssets := make(map[string]assetDefintion)
	logger.Infoln("=====> Get SiteWise assets")
	for _, modelId := range models {
		next := true
		var token *string
		for next {
			assets, err := sitewisecl.ListAssets(ctx, modelId, token)
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

func getSiteWiseModels(ctx context.Context, logger *logrus.Entry, sitewisecl *sitewiseclient.IotSiteWiseClient) (map[string]*string, map[string]*iotsitewise.DescribeAssetModelOutput, error) {
	discoveredModels := make(map[string]*string)
	modelDefinitions := make(map[string]*iotsitewise.DescribeAssetModelOutput)
	logger.Infoln("=====> Get SiteWise models")
	next := true
	var token *string
	for next {
		models, err := sitewisecl.ListAssetModels(ctx, token)
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
			descModel, err := sitewisecl.DescribeAssetModel(ctx, model.Id)
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
		return buildModelKey(props), true
	}
	return "", false
}

func buildModelKey(props []string) string {
	slices.Sort(props)
	return strings.Join(props, ",")
}

func buildModelKeyFromMap(propMap map[string]string) string {
	props := make([]string, 0, len(propMap))
	for k := range propMap {
		if propMap[k] != "" {
			props = append(props, k)
		}
	}
	slices.Sort(props)
	return strings.Join(props, ",")
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
