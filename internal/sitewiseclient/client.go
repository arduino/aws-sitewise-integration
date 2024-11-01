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

package sitewiseclient

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/arduino/aws-sitewise-integration/internal/iot"
	"github.com/arduino/aws-sitewise-integration/internal/utils"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/iotsitewise"
	"github.com/aws/aws-sdk-go-v2/service/iotsitewise/types"
	"github.com/sirupsen/logrus"
)

type IotSiteWiseClient struct {
	svc    *iotsitewise.Client
	logger *logrus.Entry
}

//go:generate mockery --name API --filename sitewise_api.go
type API interface {
	ListAssetModels(ctx context.Context) (*iotsitewise.ListAssetModelsOutput, error)
	ListAssetModelsNext(ctx context.Context, nextToken *string) (*iotsitewise.ListAssetModelsOutput, error)
	ListAssets(ctx context.Context, assetModelId *string) (*iotsitewise.ListAssetsOutput, error)
	ListAssetsNext(ctx context.Context, assetModelId *string, nextToken *string) (*iotsitewise.ListAssetsOutput, error)
	DescribeAssetModel(ctx context.Context, assetModelId *string) (*iotsitewise.DescribeAssetModelOutput, error)
	DeleteAssetModel(ctx context.Context, assetModelId *string) (*iotsitewise.DeleteAssetModelOutput, error)
	CreateDataBulkImportJob(ctx context.Context, jobNumber int, bucket string, filesToImport []string, roleArn string) (*iotsitewise.CreateBulkImportJobOutput, error)
	ListBulkImportJobs(ctx context.Context, nextToken *string) (*iotsitewise.ListBulkImportJobsOutput, error)
	GetBulkImportJobStatus(ctx context.Context, jobId *string) (*iotsitewise.DescribeBulkImportJobOutput, error)
	CreateAssetModel(ctx context.Context, name string, properties map[string]string, uomMap map[string][]string) (*iotsitewise.CreateAssetModelOutput, error)
	CreateAsset(ctx context.Context, name string, assetModelId string, thingId string) (*iotsitewise.CreateAssetOutput, error)
	DescribeModel(ctx context.Context, assetModelId string) (*iotsitewise.DescribeAssetModelOutput, error)
	PollForModelActiveStatus(ctx context.Context, modelId string, maxRetry int) bool
	IsModelActive(ctx context.Context, model *iotsitewise.DescribeAssetModelOutput) bool
	DescribeAsset(ctx context.Context, assetId string) (*iotsitewise.DescribeAssetOutput, error)
	IsAssetActive(ctx context.Context, asset *iotsitewise.DescribeAssetOutput) bool
	PollForAssetActiveStatus(ctx context.Context, assetId string, maxRetry int) bool
	UpdateAssetModelProperties(ctx context.Context, assetModel *iotsitewise.DescribeAssetModelOutput, thingProperties map[string]string, uomMap map[string][]string) error
	UpdateAssetProperties(ctx context.Context, assetId string, thingProperties map[string]string) error
	PopulateTimeSeriesByAlias(ctx context.Context, propertyAlias string, ts []int64, values []float64) error
	PopulateSampledSamplesTimeSeriesByAlias(ctx context.Context, propertyAlias string, ts []int64, values []any) error
	PopulateArbitrarySamplesByAlias(ctx context.Context, points []DataPoint) error
}

func New(logger *logrus.Entry) (*IotSiteWiseClient, error) {
	awsOpts := []func(*config.LoadOptions) error{}

	config.WithRetryer(func() aws.Retryer {
		return retry.NewStandard(func(o *retry.StandardOptions) {
			o.MaxAttempts = 5
		})
	})

	cfg, err := config.LoadDefaultConfig(
		context.Background(),
		awsOpts...,
	)
	if err != nil {
		return nil, err
	}
	svc := iotsitewise.NewFromConfig(cfg)

	return &IotSiteWiseClient{
		svc:    svc,
		logger: logger,
	}, nil
}

func (c *IotSiteWiseClient) ListAssetModels(ctx context.Context) (*iotsitewise.ListAssetModelsOutput, error) {
	maxRes := int32(100)
	return c.svc.ListAssetModels(ctx, &iotsitewise.ListAssetModelsInput{
		MaxResults: &maxRes,
	})
}

func (c *IotSiteWiseClient) ListAssetModelsNext(ctx context.Context, nextToken *string) (*iotsitewise.ListAssetModelsOutput, error) {
	maxRes := int32(100)
	return c.svc.ListAssetModels(ctx, &iotsitewise.ListAssetModelsInput{
		MaxResults: &maxRes,
		NextToken:  nextToken,
	})
}

func (c *IotSiteWiseClient) DescribeAssetModel(ctx context.Context, assetModelId *string) (*iotsitewise.DescribeAssetModelOutput, error) {
	return c.svc.DescribeAssetModel(ctx, &iotsitewise.DescribeAssetModelInput{
		AssetModelId: assetModelId,
	})
}

func (c *IotSiteWiseClient) DeleteAssetModel(ctx context.Context, assetModelId *string) (*iotsitewise.DeleteAssetModelOutput, error) {
	return c.svc.DeleteAssetModel(ctx, &iotsitewise.DeleteAssetModelInput{
		AssetModelId: assetModelId,
	})
}

func (c *IotSiteWiseClient) ListAssets(ctx context.Context, assetModelId *string) (*iotsitewise.ListAssetsOutput, error) {
	maxRes := int32(100)
	return c.svc.ListAssets(ctx, &iotsitewise.ListAssetsInput{
		MaxResults:   &maxRes,
		AssetModelId: assetModelId,
	})
}

func (c *IotSiteWiseClient) ListAssetsNext(ctx context.Context, assetModelId *string, nextToken *string) (*iotsitewise.ListAssetsOutput, error) {
	maxRes := int32(100)
	return c.svc.ListAssets(ctx, &iotsitewise.ListAssetsInput{
		MaxResults:   &maxRes,
		NextToken:    nextToken,
		AssetModelId: assetModelId,
	})
}

func (c *IotSiteWiseClient) CreateDataBulkImportJob(ctx context.Context, jobNumber int, bucket string, filesToImport []string, roleArn string) (*iotsitewise.CreateBulkImportJobOutput, error) {

	if len(filesToImport) == 0 {
		return nil, fmt.Errorf("no files to import")
	}

	files := make([]types.File, len(filesToImport))
	for i, file := range filesToImport {
		files[i] = types.File{
			Bucket: &bucket,
			Key:    &file,
		}
	}

	return c.svc.CreateBulkImportJob(ctx, &iotsitewise.CreateBulkImportJobInput{
		ErrorReportLocation: &types.ErrorReportLocation{
			Bucket: &bucket,
			Prefix: utils.StringPointer("error-reports"),
		},
		Files:             files,
		JobName:           utils.StringPointer(fmt.Sprintf("bulk-import-job-%d", jobNumber)),
		JobRoleArn:        &roleArn,
		AdaptiveIngestion: utils.BoolPointer(true),
		JobConfiguration: &types.JobConfiguration{
			FileFormat: &types.FileFormat{
				Csv: &types.Csv{
					ColumnNames: []types.ColumnName{
						"ALIAS",
						"DATA_TYPE",
						"TIMESTAMP_SECONDS",
						"TIMESTAMP_NANO_OFFSET",
						"QUALITY",
						"VALUE",
					},
				},
			},
		},
	})
}

func (c *IotSiteWiseClient) ListBulkImportJobs(ctx context.Context, nextToken *string) (*iotsitewise.ListBulkImportJobsOutput, error) {
	maxRes := int32(100)
	return c.svc.ListBulkImportJobs(ctx, &iotsitewise.ListBulkImportJobsInput{
		MaxResults: &maxRes,
		NextToken:  nextToken,
	})
}

func (c *IotSiteWiseClient) GetBulkImportJobStatus(ctx context.Context, jobId *string) (*iotsitewise.DescribeBulkImportJobOutput, error) {
	return c.svc.DescribeBulkImportJob(ctx, &iotsitewise.DescribeBulkImportJobInput{
		JobId: jobId,
	})
}

func mapType(ptype string) types.PropertyDataType {
	ptype = strings.ToUpper(ptype)

	if iot.IsPropertyNumberType(ptype) || iot.IsPropertyBool(ptype) {
		return types.PropertyDataTypeDouble
	} else if iot.IsPropertyString(ptype) || iot.IsPropertyLocation(ptype) {
		return types.PropertyDataTypeString
	}

	return types.PropertyDataTypeString
}

func (c *IotSiteWiseClient) CreateAssetModel(ctx context.Context, name string, properties map[string]string, uomMap map[string][]string) (*iotsitewise.CreateAssetModelOutput, error) {
	var modelProperties []types.AssetModelPropertyDefinition
	for property, ptype := range properties {
		mappedType := mapType(ptype)
		var uom *string
		if u, ok := uomMap[ptype]; ok {
			if len(u) > 0 {
				uom = &u[0]
			}
		}
		modelProperties = append(modelProperties, types.AssetModelPropertyDefinition{
			Name:     &property,
			DataType: mappedType,
			Type: &types.PropertyType{
				Measurement: &types.Measurement{},
			},
			Unit: uom,
		})
	}
	return c.svc.CreateAssetModel(ctx, &iotsitewise.CreateAssetModelInput{
		AssetModelName:       &name,
		AssetModelProperties: modelProperties,
	})
}

func (c *IotSiteWiseClient) CreateAsset(ctx context.Context, name string, assetModelId string, thingId string) (*iotsitewise.CreateAssetOutput, error) {
	return c.svc.CreateAsset(ctx, &iotsitewise.CreateAssetInput{
		AssetModelId:    &assetModelId,
		AssetName:       &name,
		AssetExternalId: &thingId,
	})
}

func (c *IotSiteWiseClient) DescribeModel(ctx context.Context, assetModelId string) (*iotsitewise.DescribeAssetModelOutput, error) {
	return c.svc.DescribeAssetModel(ctx, &iotsitewise.DescribeAssetModelInput{
		AssetModelId: &assetModelId,
	})
}

func (c *IotSiteWiseClient) PollForModelActiveStatus(ctx context.Context, modelId string, maxRetry int) bool {
	for i := 0; i < maxRetry; i++ {
		model, err := c.DescribeModel(ctx, modelId)
		if err != nil {
			return false
		}
		if c.IsModelActive(ctx, model) {
			return true
		}
		time.Sleep(1 * time.Second)
	}
	return false
}

func (c *IotSiteWiseClient) IsModelActive(ctx context.Context, model *iotsitewise.DescribeAssetModelOutput) bool {
	return model != nil && model.AssetModelStatus.State == types.AssetModelStateActive
}

func (c *IotSiteWiseClient) DescribeAsset(ctx context.Context, assetId string) (*iotsitewise.DescribeAssetOutput, error) {
	return c.svc.DescribeAsset(ctx, &iotsitewise.DescribeAssetInput{
		AssetId:           &assetId,
		ExcludeProperties: false,
	})
}

func (c *IotSiteWiseClient) IsAssetActive(ctx context.Context, asset *iotsitewise.DescribeAssetOutput) bool {
	return asset != nil && asset.AssetStatus.State == types.AssetStateActive
}

func (c *IotSiteWiseClient) PollForAssetActiveStatus(ctx context.Context, assetId string, maxRetry int) bool {
	for i := 0; i < maxRetry; i++ {
		asset, err := c.DescribeAsset(ctx, assetId)
		if err != nil {
			return false
		}
		if c.IsAssetActive(ctx, asset) {
			return true
		}
		time.Sleep(1 * time.Second)
	}
	return false
}

func (c *IotSiteWiseClient) UpdateAssetModelProperties(ctx context.Context, assetModel *iotsitewise.DescribeAssetModelOutput, thingProperties map[string]string, uomMap map[string][]string) error {
	assetModelInput := iotsitewise.UpdateAssetModelInput{
		AssetModelId:              assetModel.AssetModelId,
		AssetModelName:            assetModel.AssetModelName,
		AssetModelDescription:     assetModel.AssetModelDescription,
		AssetModelHierarchies:     assetModel.AssetModelHierarchies,
		AssetModelProperties:      assetModel.AssetModelProperties,
		AssetModelExternalId:      assetModel.AssetModelExternalId,
		AssetModelCompositeModels: assetModel.AssetModelCompositeModels,
	}

	assetModelProperties := make(map[string]string, len(assetModel.AssetModelProperties))
	for _, prop := range assetModel.AssetModelProperties {
		assetModelProperties[*prop.Name] = *prop.Id
	}

	modified := false
	for propertyName, ptype := range thingProperties {
		_, ok := assetModelProperties[propertyName]
		if !ok {
			modified = true
			if assetModelInput.AssetModelProperties == nil {
				assetModelInput.AssetModelProperties = []types.AssetModelProperty{}
			}
			mappedType := mapType(ptype)
			var uom *string
			if u, ok := uomMap[ptype]; ok {
				if len(u) > 0 {
					uom = &u[0]
				}
			}
			assetModelInput.AssetModelProperties = append(assetModelInput.AssetModelProperties, types.AssetModelProperty{
				Name:     &propertyName,
				DataType: mappedType,
				Type: &types.PropertyType{
					Measurement: &types.Measurement{},
				},
				Unit: uom,
			})
		}
	}

	if modified {
		_, err := c.svc.UpdateAssetModel(ctx, &assetModelInput)
		if err != nil {
			return err
		}
	}

	return nil
}

type propertyDefinition struct {
	ArduinoPropertyId string
	AssetProperty     *types.AssetProperty
}

// property is map with key as SiteWise property id and as value the alias of the property to be updated
func (c *IotSiteWiseClient) UpdateAssetProperties(ctx context.Context, assetId string, thingProperties map[string]string) error {
	assetDescribed, err := c.DescribeAsset(context.Background(), assetId)
	if err != nil {
		return err
	}
	assetPropertiesMap := make(map[string]propertyDefinition, len(thingProperties))
	for _, prop := range assetDescribed.AssetProperties {
		assetPropertiesMap[*prop.Name] = propertyDefinition{
			ArduinoPropertyId: *prop.Id,
			AssetProperty:     &prop,
		}
	}

	for property, alias := range thingProperties {
		sitewisePropertyId, ok := assetPropertiesMap[property]
		if !ok {
			c.logger.Info("Property not found in asset: ", property)
			continue
		}

		// Check if property is already updated
		if sitewisePropertyId.AssetProperty.Alias != nil && *sitewisePropertyId.AssetProperty.Alias == alias {
			continue
		}

		_, err := c.svc.UpdateAssetProperty(ctx, &iotsitewise.UpdateAssetPropertyInput{
			AssetId:       &assetId,
			PropertyId:    &sitewisePropertyId.ArduinoPropertyId,
			PropertyAlias: &alias,
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *IotSiteWiseClient) PopulateTimeSeriesByAlias(ctx context.Context, propertyAlias string, ts []int64, values []float64) error {
	if len(ts) != len(values) {
		return fmt.Errorf("timestamps and values must have the same length")
	}
	if len(ts) == 0 {
		return fmt.Errorf("no data to populate")
	}
	var data []types.PutAssetPropertyValueEntry
	var pvalues []types.AssetPropertyValue
	entry := "1"

	for i := 0; i < len(ts); i++ {
		pvalues = append(pvalues, types.AssetPropertyValue{
			Timestamp: &types.TimeInNanos{
				TimeInSeconds: &ts[i],
			},
			Value: &types.Variant{
				DoubleValue: &values[i],
			},
			Quality: types.QualityGood,
		})
	}

	data = append(data, types.PutAssetPropertyValueEntry{
		EntryId:        &entry,
		PropertyAlias:  &propertyAlias,
		PropertyValues: pvalues,
	})

	out, err := c.svc.BatchPutAssetPropertyValue(ctx, &iotsitewise.BatchPutAssetPropertyValueInput{
		Entries: data,
	})
	if err != nil {
		return err
	}
	if out.ErrorEntries != nil {
		for _, entry := range out.ErrorEntries {
			c.logger.Error("Error on entry: ", *entry.EntryId)
			if entry.Errors != nil {
				for _, err := range entry.Errors {
					c.logger.Error("		[Error] ", err.ErrorCode, *err.ErrorMessage)
				}
			}
		}
	}
	return nil
}

func interfaceToString(value interface{}) string {
	switch v := value.(type) {
	case string:
		return v
	case int:
		return strconv.Itoa(v)
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	case bool:
		return strconv.FormatBool(v)
	case map[string]any:
		encoded, err := json.Marshal(v)
		if err != nil {
			return fmt.Sprintf("%v", v)
		}
		return string(encoded)
	default:
		return fmt.Sprintf("%v", v)
	}
}

func (c *IotSiteWiseClient) PopulateSampledSamplesTimeSeriesByAlias(ctx context.Context, propertyAlias string, ts []int64, values []any) error {
	if len(ts) != len(values) {
		return fmt.Errorf("timestamps and values must have the same length")
	}
	if len(ts) == 0 {
		return fmt.Errorf("no data to populate")
	}
	var data []types.PutAssetPropertyValueEntry
	var pvalues []types.AssetPropertyValue
	entry := "1"

	for i := 0; i < len(ts); i++ {
		variant := types.Variant{}

		switch v := values[i].(type) {
		case string:
			variant.StringValue = &v
		case int:
			valInt32 := int32(v)
			variant.IntegerValue = &valInt32
		case float64:
			variant.DoubleValue = &v
		case map[string]any:
			encoded := interfaceToString(v)
			variant.StringValue = &encoded
		default:
			c.logger.Warn("Unsupported type: ", reflect.TypeOf(v))
			continue
		}

		pvalues = append(pvalues, types.AssetPropertyValue{
			Timestamp: &types.TimeInNanos{
				TimeInSeconds: &ts[i],
			},
			Value:   &variant,
			Quality: types.QualityGood,
		})
	}

	data = append(data, types.PutAssetPropertyValueEntry{
		EntryId:        &entry,
		PropertyAlias:  &propertyAlias,
		PropertyValues: pvalues,
	})

	out, err := c.svc.BatchPutAssetPropertyValue(ctx, &iotsitewise.BatchPutAssetPropertyValueInput{
		Entries: data,
	})
	if err != nil {
		return err
	}
	if out.ErrorEntries != nil {
		for _, entry := range out.ErrorEntries {
			c.logger.Error("Error on entry: ", *entry.EntryId)
			if entry.Errors != nil {
				for _, err := range entry.Errors {
					c.logger.Error("		[Error sampling] ", err.ErrorCode, *err.ErrorMessage)
				}
			}
		}
	}
	return nil
}

type DataPoint struct {
	PropertyAlias string
	Ts            int64
	Value         any
}

func (c *IotSiteWiseClient) PopulateArbitrarySamplesByAlias(ctx context.Context, points []DataPoint) error {
	if len(points) == 0 {
		return fmt.Errorf("no data to populate")
	}

	var data []types.PutAssetPropertyValueEntry
	entry := 1

	for i := 0; i < len(points); i++ {
		variant := types.Variant{}

		switch v := points[i].Value.(type) {
		case bool:
			vBool := 0.0
			if v {
				vBool = 1.0
			}
			variant.DoubleValue = &vBool
		case string:
			variant.StringValue = &v
		case int32:
			valInt32 := v
			variant.IntegerValue = &valInt32
		case int64:
			valInt32 := int32(v)
			variant.IntegerValue = &valInt32
		case int:
			valInt32 := int32(v)
			variant.IntegerValue = &valInt32
		case float32:
			valFloat := float64(v)
			variant.DoubleValue = &valFloat
		case float64:
			variant.DoubleValue = &v
		case map[string]any:
			encoded := interfaceToString(v)
			variant.StringValue = &encoded
		default:
			c.logger.Warn("Unsupported type: ", reflect.TypeOf(v))
			continue
		}

		entryIdStringValue := strconv.Itoa(entry)
		data = append(data, types.PutAssetPropertyValueEntry{
			EntryId:       &entryIdStringValue,
			PropertyAlias: &points[i].PropertyAlias,
			PropertyValues: []types.AssetPropertyValue{
				{
					Timestamp: &types.TimeInNanos{
						TimeInSeconds: &points[i].Ts,
					},
					Value:   &variant,
					Quality: types.QualityGood,
				},
			},
		})

		entry++

		if len(data) == 10 {
			out, err := c.svc.BatchPutAssetPropertyValue(ctx, &iotsitewise.BatchPutAssetPropertyValueInput{
				Entries: data,
			})
			if err != nil {
				return err
			}
			if out.ErrorEntries != nil {
				for _, entry := range out.ErrorEntries {
					c.logger.Error("Error on entry: ", *entry.EntryId)
					if entry.Errors != nil {
						for _, err := range entry.Errors {
							c.logger.Error("		[Error last value] ", err.ErrorCode, *err.ErrorMessage)
						}
					}
				}
			}
			data = []types.PutAssetPropertyValueEntry{}
			entry = 1
		}
	}

	if len(data) > 0 {
		out, err := c.svc.BatchPutAssetPropertyValue(ctx, &iotsitewise.BatchPutAssetPropertyValueInput{
			Entries: data,
		})
		if err != nil {
			return err
		}
		if out.ErrorEntries != nil {
			for _, entry := range out.ErrorEntries {
				c.logger.Error("Error on entry: ", *entry.EntryId)
				if entry.Errors != nil {
					for _, err := range entry.Errors {
						c.logger.Error("		[Error sampling] ", err.ErrorCode, *err.ErrorMessage)
					}
				}
			}
		}
	}

	return nil
}
