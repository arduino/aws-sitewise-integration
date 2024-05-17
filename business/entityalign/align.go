package entityalign

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/arduino/aws-sitewise-integration/internal/sitewiseclient"
	iotclient "github.com/arduino/iot-client-go"
	"github.com/aws/aws-sdk-go-v2/service/iotsitewise/types"
)

func Align(ctx context.Context, things []iotclient.ArduinoThing, sitewisecl *sitewiseclient.IotSiteWiseClient) error {
	println("=====> Aligning entities")
	models, err := getSiteWiseModels(ctx, sitewisecl)
	if err != nil {
		return err
	}
	assets, err := getSiteWiseAssets(ctx, sitewisecl, models)
	if err != nil {
		return err
	}

	for _, thing := range things {
		println("=====> Aligning thing: ", thing.Id, thing.Name)
		propsAliasMap := make(map[string]string, len(thing.Properties))
		propsTypeMap := make(map[string]string, len(thing.Properties))
		for _, prop := range thing.Properties {
			propsAliasMap[prop.Name] = propertyAlias(thing.Name, prop.Name)
			propsTypeMap[prop.Name] = prop.Type
			println("  Property: ", prop.Name, prop.Type, " -> ", propsAliasMap[prop.Name])
		}

		_, ok := assets[thing.Id]
		if ok {
			println("Thing already aligned, skipping. Thing: ", thing.Id)
			continue
		}

		// Discover thing properties
		key := buildModelKeyFromMap(propsAliasMap)
		model, ok := models[key]
		var modelId *string
		if !ok {
			println("Model not found for thing: ", thing.Id, thing.Name, ". Creating it.")
			createdModel, err := sitewisecl.CreateAssetModel(ctx, composeModelName(thing.Name), propsTypeMap)
			if err != nil {
				return err
			}
			sitewisecl.PollForModelActiveStatus(ctx, *createdModel.AssetModelId, 10)
			modelId = createdModel.AssetModelId
		} else {
			modelId = model.Id
		}

		// Create asset
		println("Creating asset for thing: ", thing.Id)
		asset, err := sitewisecl.CreateAsset(ctx, thing.Name, *modelId, thing.Id)
		if err != nil {
			return err
		}

		// Wait for asset to be active before updating properties...
		sitewisecl.PollForAssetActiveStatus(ctx, *asset.AssetId, 10)

		err = sitewisecl.UpdateAssetProperty(ctx, *asset.AssetId, propsAliasMap)
		if err != nil {
			return err
		}

	}

	return nil
}

func composeModelName(thingName string) string {
	return fmt.Sprintf("Thing Model from (%s)", thingName)
}

func propertyAlias(thingName, propertyName string) string {
	return fmt.Sprintf("/%s/%s", thingName, propertyName)
}

func getSiteWiseAssets(ctx context.Context, sitewisecl *sitewiseclient.IotSiteWiseClient, models map[string]*types.AssetModelSummary) (map[string]string, error) {
	discoveredAssets := make(map[string]string)
	println("=====> Get SiteWise assets")
	for _, model := range models {
		next := true
		var token *string
		for next {
			assets, err := sitewisecl.ListAssets(ctx, model.Id, token)
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
					discoveredAssets[*asset.ExternalId] = *asset.Id
				}
			}
		}
	}
	return discoveredAssets, nil
}

func getSiteWiseModels(ctx context.Context, sitewisecl *sitewiseclient.IotSiteWiseClient) (map[string]*types.AssetModelSummary, error) {
	discoveredModels := make(map[string]*types.AssetModelSummary)
	println("=====> Get SiteWise models")
	next := true
	var token *string
	for next {
		models, err := sitewisecl.ListAssetModels(ctx, token)
		if err != nil {
			return nil, err
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
				return nil, err
			}

			if len(descModel.AssetModelProperties) > 0 {
				props := make([]string, len(descModel.AssetModelProperties))
				for _, prop := range descModel.AssetModelProperties {
					if prop.Type != nil && prop.Type.Measurement != nil { // Check if property is a measurement, not an aggregate
						props = append(props, *prop.Name)
					}
				}
				discoveredModels[buildModelKey(props)] = &model
			}
		}
	}
	return discoveredModels, nil
}

func buildModelKey(props []string) string {
	slices.Sort(props)
	return strings.Join(props, ",")
}

func buildModelKeyFromMap(propMap map[string]string) string {
	props := make([]string, len(propMap))
	for k := range propMap {
		props = append(props, k)
	}
	slices.Sort(props)
	return strings.Join(props, ",")
}
