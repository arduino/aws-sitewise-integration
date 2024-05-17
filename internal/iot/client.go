package iot

import (
	"context"
	"fmt"
	"time"

	"github.com/antihax/optional"
	iotclient "github.com/arduino/iot-client-go"
	"golang.org/x/oauth2"
)

var ErrOtaAlreadyInProgress = fmt.Errorf("ota already in progress")

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

// DeviceList retrieves and returns a list of all Arduino IoT Cloud devices
// belonging to the user performing the request.
func (cl *Client) DeviceList(ctx context.Context, tags map[string]string) ([]iotclient.ArduinoDevicev2, error) {
	ctx, err := ctxWithToken(ctx, cl.token)
	if err != nil {
		return nil, err
	}

	opts := &iotclient.DevicesV2ListOpts{}
	if tags != nil {
		t := make([]string, 0, len(tags))
		for key, val := range tags {
			// Use the 'key:value' format required from the backend
			t = append(t, key+":"+val)
		}
		opts.Tags = optional.NewInterface(t)
	}

	devices, _, err := cl.api.DevicesV2Api.DevicesV2List(ctx, opts)
	if err != nil {
		err = fmt.Errorf("listing devices: %w", errorDetail(err))
		return nil, err
	}
	return devices, nil
}

// DeviceShow allows to retrieve a specific device, given its id,
// from Arduino IoT Cloud.
func (cl *Client) DeviceShow(ctx context.Context, id string) (*iotclient.ArduinoDevicev2, error) {
	ctx, err := ctxWithToken(ctx, cl.token)
	if err != nil {
		return nil, err
	}

	dev, _, err := cl.api.DevicesV2Api.DevicesV2Show(ctx, id, nil)
	if err != nil {
		err = fmt.Errorf("retrieving device, %w", errorDetail(err))
		return nil, err
	}
	return &dev, nil
}

// DeviceTagsCreate allows to create or overwrite tags on a device of Arduino IoT Cloud.
func (cl *Client) DeviceTagsCreate(ctx context.Context, id string, tags map[string]string) error {
	ctx, err := ctxWithToken(ctx, cl.token)
	if err != nil {
		return err
	}

	for key, val := range tags {
		t := iotclient.Tag{Key: key, Value: val}
		_, err := cl.api.DevicesV2TagsApi.DevicesV2TagsUpsert(ctx, id, t)
		if err != nil {
			err = fmt.Errorf("cannot create tag %s: %w", key, errorDetail(err))
			return err
		}
	}
	return nil
}

// DeviceTagsDelete deletes the tags of a device of Arduino IoT Cloud,
// given the device id and the keys of the tags.
func (cl *Client) DeviceTagsDelete(ctx context.Context, id string, keys []string) error {
	ctx, err := ctxWithToken(ctx, cl.token)
	if err != nil {
		return err
	}

	for _, key := range keys {
		_, err := cl.api.DevicesV2TagsApi.DevicesV2TagsDelete(ctx, id, key)
		if err != nil {
			err = fmt.Errorf("cannot delete tag %s: %w", key, errorDetail(err))
			return err
		}
	}
	return nil
}

// ThingShow allows to retrieve a specific thing, given its id,
// from Arduino IoT Cloud.
func (cl *Client) ThingShow(ctx context.Context, id string) (*iotclient.ArduinoThing, error) {
	ctx, err := ctxWithToken(ctx, cl.token)
	if err != nil {
		return nil, err
	}

	thing, _, err := cl.api.ThingsV2Api.ThingsV2Show(ctx, id, nil)
	if err != nil {
		err = fmt.Errorf("retrieving thing, %w", errorDetail(err))
		return nil, err
	}
	return &thing, nil
}

// ThingList returns a list of things on Arduino IoT Cloud.
func (cl *Client) ThingList(ctx context.Context, ids []string, device *string, props bool, tags map[string]string) ([]iotclient.ArduinoThing, error) {
	ctx, err := ctxWithToken(ctx, cl.token)
	if err != nil {
		return nil, err
	}

	opts := &iotclient.ThingsV2ListOpts{}
	opts.ShowProperties = optional.NewBool(props)

	if ids != nil {
		opts.Ids = optional.NewInterface(ids)
	}

	if device != nil {
		opts.DeviceId = optional.NewString(*device)
	}

	if tags != nil {
		t := make([]string, 0, len(tags))
		for key, val := range tags {
			// Use the 'key:value' format required from the backend
			t = append(t, key+":"+val)
		}
		opts.Tags = optional.NewInterface(t)
	}

	things, _, err := cl.api.ThingsV2Api.ThingsV2List(ctx, opts)
	if err != nil {
		err = fmt.Errorf("retrieving things, %w", errorDetail(err))
		return nil, err
	}
	return things, nil
}

// ThingTagsCreate allows to create or overwrite tags on a thing of Arduino IoT Cloud.
func (cl *Client) ThingTagsCreate(ctx context.Context, id string, tags map[string]string) error {
	ctx, err := ctxWithToken(ctx, cl.token)
	if err != nil {
		return err
	}

	for key, val := range tags {
		t := iotclient.Tag{Key: key, Value: val}
		_, err := cl.api.ThingsV2TagsApi.ThingsV2TagsUpsert(ctx, id, t)
		if err != nil {
			err = fmt.Errorf("cannot create tag %s: %w", key, errorDetail(err))
			return err
		}
	}
	return nil
}

// ThingTagsDelete deletes the tags of a thing of Arduino IoT Cloud,
// given the thing id and the keys of the tags.
func (cl *Client) ThingTagsDelete(ctx context.Context, id string, keys []string) error {
	ctx, err := ctxWithToken(ctx, cl.token)
	if err != nil {
		return err
	}

	for _, key := range keys {
		_, err := cl.api.ThingsV2TagsApi.ThingsV2TagsDelete(ctx, id, key)
		if err != nil {
			err = fmt.Errorf("cannot delete tag %s: %w", key, errorDetail(err))
			return err
		}
	}
	return nil
}

// DashboardShow allows to retrieve a specific dashboard, given its id,
// from Arduino IoT Cloud.
func (cl *Client) DashboardShow(ctx context.Context, id string) (*iotclient.ArduinoDashboardv2, error) {
	ctx, err := ctxWithToken(ctx, cl.token)
	if err != nil {
		return nil, err
	}

	dashboard, _, err := cl.api.DashboardsV2Api.DashboardsV2Show(ctx, id, nil)
	if err != nil {
		err = fmt.Errorf("retrieving dashboard, %w", errorDetail(err))
		return nil, err
	}
	return &dashboard, nil
}

// DashboardList returns a list of dashboards on Arduino IoT Cloud.
func (cl *Client) DashboardList(ctx context.Context) ([]iotclient.ArduinoDashboardv2, error) {
	ctx, err := ctxWithToken(ctx, cl.token)
	if err != nil {
		return nil, err
	}

	dashboards, _, err := cl.api.DashboardsV2Api.DashboardsV2List(ctx, nil)
	if err != nil {
		err = fmt.Errorf("listing dashboards: %w", errorDetail(err))
		return nil, err
	}
	return dashboards, nil
}

func (cl *Client) GetTimeSeries(ctx context.Context, properties []string, from, to time.Time, interval int64) (*iotclient.ArduinoSeriesBatch, error) {
	if len(properties) == 0 {
		return nil, fmt.Errorf("no properties provided")
	}

	ctx, err := ctxWithToken(ctx, cl.token)
	if err != nil {
		return nil, err
	}

	requests := make([]iotclient.BatchQueryRequestMediaV1, 0, len(properties))
	for _, prop := range properties {
		if prop == "" {
			continue
		}
		requests = append(requests, iotclient.BatchQueryRequestMediaV1{
			From:     from,
			Interval: interval,
			Q:        fmt.Sprintf("property.%s", prop),
			To:       to,
		})
	}

	if len(requests) == 0 {
		return nil, fmt.Errorf("no valid properties provided")
	}

	batchQueryRequestsMediaV1 := iotclient.BatchQueryRequestsMediaV1{
		Requests: requests,
	}

	ts, _, err := cl.api.SeriesV2Api.SeriesV2BatchQuery(ctx, batchQueryRequestsMediaV1)
	if err != nil {
		err = fmt.Errorf("retrieving time series: %w", errorDetail(err))
		return nil, err
	}
	return &ts, nil
}

func (cl *Client) setup(client, secret, organization string) error {
	baseURL := GetArduinoAPIBaseURL()

	// Configure a token source given the user's credentials.
	cl.token = NewUserTokenSource(client, secret, baseURL)

	config := iotclient.NewConfiguration()
	if organization != "" {
		config.DefaultHeader = map[string]string{"X-Organization": organization}
	}
	config.BasePath = baseURL + "/iot"
	cl.api = iotclient.NewAPIClient(config)

	return nil
}
