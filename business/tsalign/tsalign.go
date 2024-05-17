package tsalign

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/arduino/aws-sitewise-integration/internal/iot"
	"github.com/arduino/aws-sitewise-integration/internal/sitewiseclient"
	iotclient "github.com/arduino/iot-client-go"
	"github.com/aws/aws-sdk-go-v2/service/iotsitewise"
)

const importConcurrency = 5

type TsAligner struct {
	sitewisecl *sitewiseclient.IotSiteWiseClient
	iotcl      *iot.Client
}

func New(sitewisecl *sitewiseclient.IotSiteWiseClient, iotcl *iot.Client) *TsAligner {
	return &TsAligner{sitewisecl: sitewisecl, iotcl: iotcl}
}

func (a *TsAligner) AlignTimeSeriesSamplesIntoSiteWise(
	ctx context.Context,
	timeWindowInMinutes int,
	thingsMap map[string]iotclient.ArduinoThing,
	resolution int) error {

	var wg sync.WaitGroup
	tokens := make(chan struct{}, importConcurrency)

	println("=====> Align perf data - last ", timeWindowInMinutes, " minutes")
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
				thing, ok := thingsMap[*asset.ExternalId]
				if !ok {
					println("Thing not found: ", *asset.ExternalId)
					continue
				}

				wg.Add(1)
				tokens <- struct{}{}

				go func(assetId string, assetName string) {
					defer func() { <-tokens }()
					defer wg.Done()

					describedAsset, err := a.sitewisecl.DescribeAsset(ctx, assetId)
					if err != nil {
						println("Error describing asset: ", assetId, err)
						return
					}

					propertiesToImport, propertiesToImportAliases := mapPropertiesToImport(describedAsset, thing, assetName)

					err = a.populateTSDataIntoSiteWise(ctx, timeWindowInMinutes, propertiesToImport, propertiesToImportAliases, resolution)
					if err != nil {
						println("Error populating time series data: ", err)
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

func mapPropertiesToImport(describedAsset *iotsitewise.DescribeAssetOutput, thing iotclient.ArduinoThing, assetName string) ([]string, map[string]string) {
	propertiesToImport := make([]string, 0, len(describedAsset.AssetProperties))
	propertiesToImportAliases := make(map[string]string, len(describedAsset.AssetProperties))
	for _, prop := range describedAsset.AssetProperties {
		for _, thingProperty := range thing.Properties {
			if *prop.Name == thingProperty.Name {
				println("  Importing TS for: ", assetName, *prop.Name, " thingPropertyId: ", thingProperty.Id)
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
	propertiesToImport []string,
	propertiesToImportAliases map[string]string,
	resolution int) error {

	to := time.Now().UTC()
	from := to.Add(-time.Duration(loopMinutes) * time.Minute)
	batched, err := a.iotcl.GetTimeSeries(ctx, propertiesToImport, from, to, int64(resolution))
	if err != nil {
		return err
	}
	for _, response := range batched.Responses {
		if response.CountValues == 0 {
			continue
		}
		ts := make([]int64, 0, response.CountValues)
		for _, t := range response.Times {
			ts = append(ts, t.Unix())
		}
		property := strings.Replace(response.Query, "property.", "", 1)
		alias := propertiesToImportAliases[property]
		println("  Importing ", len(ts), " data points for: ", alias, " - ts:", joinTs(ts))
		if alias == "" {
			println("Alias not found. Skipping import.")
			continue
		}
		erri := a.sitewisecl.PopulateTimeSeriesByAlias(ctx, alias, ts, response.Values)
		if erri != nil {
			return err
		}
	}
	return nil
}

func joinTs(ts []int64) string {
	tsarr := []string{}
	for _, v := range ts {
		tsarr = append(tsarr, fmt.Sprintf("%d", v))
	}
	return strings.Join(tsarr, ",")
}
