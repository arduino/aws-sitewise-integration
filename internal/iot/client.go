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
package iot

import (
	"context"
	"fmt"
	"time"

	iotclient "github.com/arduino/iot-client-go/v2"
	"golang.org/x/oauth2"
)

var ErrOtaAlreadyInProgress = fmt.Errorf("ota already in progress")

//go:generate mockery --name API --filename iot_api.go
type API interface {
	ThingList(ctx context.Context, ids []string, device *string, props bool, tags map[string]string) ([]iotclient.ArduinoThing, error)
	GetTimeSeriesByThing(ctx context.Context, thingID string, from, to time.Time, interval int64) (*iotclient.ArduinoSeriesBatch, bool, error)
	GetTimeSeriesSampling(ctx context.Context, propertiesToImport []string, from, to time.Time, interval int32) (*iotclient.ArduinoSeriesBatchSampled, bool, error)
	PropertiesDefinition(ctx context.Context) (map[string]iotclient.ArduinoPropertytype, error)
}

// Client can perform actions on Arduino IoT Cloud.
type Client struct {
	api   *iotclient.APIClient
	token oauth2.TokenSource
}

// NewClient returns a new client implementing the Client interface.
// It needs client Credentials for cloud authentication.
func NewClient(key, secret, organization string) (*Client, error) {
	cl := &Client{}
	err := cl.setup(key, secret, organization)
	if err != nil {
		err = fmt.Errorf("instantiate new iot client: %w", err)
		return nil, err
	}
	return cl, nil
}

// ThingList returns a list of things on Arduino IoT Cloud.
func (cl *Client) ThingList(ctx context.Context, ids []string, device *string, extractProperties bool, tags map[string]string) ([]iotclient.ArduinoThing, error) {
	ctx, err := ctxWithToken(ctx, cl.token)
	if err != nil {
		return nil, err
	}

	request := cl.api.ThingsV2Api.ThingsV2List(ctx)
	request = request.ShowProperties(extractProperties)

	if ids != nil {
		request = request.Ids(ids)
	}

	if device != nil {
		request = request.DeviceId(*device)
	}

	if tags != nil {
		t := make([]string, 0, len(tags))
		for key, val := range tags {
			// Use the 'key:value' format required from the backend
			t = append(t, key+":"+val)
		}
		request = request.Tags(t)
	}

	things, _, err := cl.api.ThingsV2Api.ThingsV2ListExecute(request)
	if err != nil {
		err = fmt.Errorf("retrieving things, %w", errorDetail(err))
		return nil, err
	}
	return things, nil
}

func (cl *Client) GetTimeSeriesByThing(ctx context.Context, thingID string, from, to time.Time, interval int64) (*iotclient.ArduinoSeriesBatch, bool, error) {
	if thingID == "" {
		return nil, false, fmt.Errorf("no thing provided")
	}

	ctx, err := ctxWithToken(ctx, cl.token)
	if err != nil {
		return nil, false, err
	}

	requests := []iotclient.BatchQueryRequestMediaV1{
		{
			From:     from,
			Interval: &interval,
			Q:        fmt.Sprintf("thing.%s", thingID),
			To:       to,
		},
	}

	if len(requests) == 0 {
		return nil, false, fmt.Errorf("no valid properties provided")
	}

	batchQueryRequestsMediaV1 := iotclient.BatchQueryRequestsMediaV1{
		Requests: requests,
	}

	request := cl.api.SeriesV2Api.SeriesV2BatchQuery(ctx)
	request = request.BatchQueryRequestsMediaV1(batchQueryRequestsMediaV1)
	ts, httpResponse, err := cl.api.SeriesV2Api.SeriesV2BatchQueryExecute(request)
	if err != nil {
		err = fmt.Errorf("retrieving time series: %w", errorDetail(err))
		if httpResponse != nil && httpResponse.StatusCode == 429 { // Retry if rate limited
			return nil, true, err
		}
		return nil, false, err
	}
	return ts, false, nil
}

func (cl *Client) GetTimeSeriesSampling(ctx context.Context, propertiesToImport []string, from, to time.Time, interval int32) (*iotclient.ArduinoSeriesBatchSampled, bool, error) {

	ctx, err := ctxWithToken(ctx, cl.token)
	if err != nil {
		return nil, false, err
	}

	requests := []iotclient.BatchQuerySampledRequestMediaV1{}

	for _, propId := range propertiesToImport {
		requests = append(requests, iotclient.BatchQuerySampledRequestMediaV1{
			From:     &from,
			Interval: &interval,
			Q:        fmt.Sprintf("property.%s", propId),
			To:       &to,
		})
	}

	if len(requests) == 0 {
		return nil, false, fmt.Errorf("no valid properties provided")
	}

	batchQueryRequestsMediaV1 := iotclient.BatchQuerySampledRequestsMediaV1{
		Requests: requests,
	}

	request := cl.api.SeriesV2Api.SeriesV2BatchQuerySampling(ctx)
	request = request.BatchQuerySampledRequestsMediaV1(batchQueryRequestsMediaV1)
	ts, httpResponse, err := cl.api.SeriesV2Api.SeriesV2BatchQuerySamplingExecute(request)
	if err != nil {
		err = fmt.Errorf("retrieving time series: %w", errorDetail(err))
		if httpResponse != nil && httpResponse.StatusCode == 429 { // Retry if rate limited
			return nil, true, err
		}
		return nil, false, err
	}
	return ts, false, nil
}

func (cl *Client) setup(client, secret, organizationId string) error {
	baseURL := GetArduinoAPIBaseURL()

	// Configure a token source given the user's credentials.
	cl.token = NewUserTokenSource(client, secret, baseURL, organizationId)

	config := iotclient.NewConfiguration()
	if organizationId != "" {
		config.AddDefaultHeader("X-Organization", organizationId)
	}
	config.Servers = iotclient.ServerConfigurations{
		{
			URL:         fmt.Sprintf("%s/iot", baseURL),
			Description: "IoT API endpoint",
		},
	}
	cl.api = iotclient.NewAPIClient(config)

	return nil
}

// PropertiesDefinition returns properties definition from Arduino IoT Cloud.
func (cl *Client) PropertiesDefinition(ctx context.Context) (map[string]iotclient.ArduinoPropertytype, error) {
	ctx, err := ctxWithToken(ctx, cl.token)
	if err != nil {
		return nil, err
	}

	request := cl.api.PropertyTypesV1Api.PropertyTypesV1ListTypes(ctx)
	types, _, err := cl.api.PropertyTypesV1Api.PropertyTypesV1ListTypesExecute(request)
	if err != nil {
		err = fmt.Errorf("retrieving things, %w", errorDetail(err))
		return nil, err
	}

	pTypes := make(map[string]iotclient.ArduinoPropertytype)
	for _, t := range types {
		pTypes[t.Type] = t
	}
	return pTypes, nil
}
