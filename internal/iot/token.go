package iot

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"

	iotclient "github.com/arduino/iot-client-go"
	"golang.org/x/oauth2"
	cc "golang.org/x/oauth2/clientcredentials"
)

func GetArduinoAPIBaseURL() string {
	baseURL := "https://api2.arduino.cc"
	if url := os.Getenv("IOT_API_URL"); url != "" {
		baseURL = url
	}
	return baseURL
}

// Build a new token source to forge api JWT tokens based on provided credentials
func NewUserTokenSource(client, secret, baseURL string) oauth2.TokenSource {
	// We need to pass the additional "audience" var to request an access token.
	additionalValues := url.Values{}
	additionalValues.Add("audience", "https://api2.arduino.cc/iot")
	// Set up OAuth2 configuration.
	config := cc.Config{
		ClientID:       client,
		ClientSecret:   secret,
		TokenURL:       baseURL + "/iot/v1/clients/token",
		EndpointParams: additionalValues,
	}

	// Retrieve a token source that allows to retrieve tokens
	// with an automatic refresh mechanism.
	return config.TokenSource(context.Background())
}

func ctxWithToken(ctx context.Context, src oauth2.TokenSource) (context.Context, error) {
	// Retrieve a valid token from the src.
	tok, err := src.Token()
	if err != nil {
		if strings.Contains(err.Error(), "401") {
			return nil, errors.New("wrong credentials")
		}
		return nil, fmt.Errorf("cannot retrieve a valid token: %w", err)
	}
	return context.WithValue(ctx, iotclient.ContextAccessToken, tok.AccessToken), nil
}
