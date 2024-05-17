package parameters

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/ssm"
)

type ParametersClient struct {
	ssmcl *ssm.Client
}

func New() (*ParametersClient, error) {

	awsOpts := []func(*config.LoadOptions) error{}

	cfg, err := config.LoadDefaultConfig(
		context.Background(),
		awsOpts...,
	)
	if err != nil {
		return nil, err
	}

	cl := ssm.NewFromConfig(cfg)

	return &ParametersClient{
		ssmcl: cl,
	}, nil
}

func (c *ParametersClient) ReadConfig(param string) (*string, error) {
	value, err := c.ssmcl.GetParameter(context.Background(), &ssm.GetParameterInput{
		Name:           aws.String(param),
		WithDecryption: aws.Bool(true),
	})
	if err != nil {
		return nil, err
	}
	return value.Parameter.Value, nil
}
