package provider

import (
	"errors"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	"github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

type GSIProvider struct {
	c          *dynamodb.DynamoDB
	autoImport bool
}

func providerWithConfigure(cfgFn schema.ConfigureFunc) *schema.Provider {
	return &schema.Provider{
		Schema: map[string]*schema.Schema{
			"access_key": {
				Type:        schema.TypeString,
				Optional:    true,
				DefaultFunc: schema.EnvDefaultFunc("AWS_ACCESS_KEY_ID", nil),
				Description: "AWS access key ID",
			},

			"secret_key": {
				Type:        schema.TypeString,
				Optional:    true,
				DefaultFunc: schema.EnvDefaultFunc("AWS_SECRET_ACCESS_KEY", nil),
				Description: "AWS secret key ID",
			},

			"token": {
				Type:        schema.TypeString,
				Optional:    true,
				DefaultFunc: schema.EnvDefaultFunc("AWS_SESSION_TOKEN", nil),
				Description: "AWS session token",
			},

			"profile": {
				Type:        schema.TypeString,
				Optional:    true,
				DefaultFunc: schema.EnvDefaultFunc("AWS_PROFILE", nil),
				Description: "AWS profile",
			},

			"auto_import": {
				Type:        schema.TypeBool,
				Optional:    true,
				Default:     false,
				Description: "Automatically import on create, not recommended unless transitioning away from GSI created with the AWS resource",
			},

			"region": {
				Type:     schema.TypeString,
				Optional: true,
				DefaultFunc: schema.MultiEnvDefaultFunc([]string{
					"AWS_REGION",
					"AWS_DEFAULT_REGION",
				}, "us-east-1"),
				Description: "AWS region",
			},

			"dynamodb_endpoint": {
				Type:        schema.TypeString,
				Optional:    true,
				DefaultFunc: schema.EnvDefaultFunc("AWS_DYNAMODB_ENDPOINT", nil),
				Description: "AWS dynamodb endpoint",
			},

			"assume_role": &schema.Schema{
				Type:     schema.TypeList,
				Optional: true,
				MaxItems: 1,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"role_arn": {
							Type:        schema.TypeString,
							Optional:    true,
							Description: "Amazon Resource Name (ARN) of an IAM Role to assume prior to making API calls.",
						},
					},
				},
			},
		},
		ResourcesMap: map[string]*schema.Resource{
			"gsi_global_secondary_index": dynamoDBGSIResource(),
		},
		ConfigureFunc: cfgFn,
	}
}

func Provider() *schema.Provider {
	return providerWithConfigure(providerConfigure)
}

func newClient(region string, accessKey string, secretKey string, token string, profile string, endpoint string, role_arn string) (*dynamodb.DynamoDB, error) {
	options := session.Options{}
	options.Config = *aws.NewConfig().WithRegion(region)
	if accessKey != "" && secretKey != "" {
		options.Config.Credentials = credentials.NewStaticCredentials(accessKey, secretKey, token)
	} else if profile != "" {
		options.SharedConfigState = session.SharedConfigEnable
		options.Profile = profile
	} else {
		return nil, errors.New("no credentials for AWS")
	}

	if endpoint != "" {
		options.Config.EndpointResolver = endpoints.ResolverFunc(func(service, region string, optFns ...func(*endpoints.Options)) (endpoints.ResolvedEndpoint, error) {
			if service == endpoints.DynamodbServiceID {
				return endpoints.ResolvedEndpoint{
					URL: endpoint,
				}, nil
			}

			return endpoints.DefaultResolver().EndpointFor(service, region, optFns...)
		})
	}

	sess, err := session.NewSessionWithOptions(options)
	if err != nil {
		return nil, err
	}

	if role_arn != "" {
		// Assume the role and use the resulting credentials.
		options.Config.Credentials = stscreds.NewCredentials(sess, role_arn)
		sess, err = session.NewSessionWithOptions(options)
		if err != nil {
			return nil, err
		}
	}

	return dynamodb.New(sess), nil
}

func providerConfigure(d *schema.ResourceData) (interface{}, error) {
	accessKey := d.Get("access_key").(string)
	secretKey := d.Get("secret_key").(string)
	token := d.Get("token").(string)
	profile := d.Get("profile").(string)
	region := d.Get("region").(string)
	endpoint := d.Get("dynamodb_endpoint").(string)
	assume_role_config := d.Get("assume_role").([]interface{})

	role_arn := ""
	if len(assume_role_config) > 0 {
		configmap := assume_role_config[0].(map[string]interface{})
		if v, ok := configmap["role_arn"].(string); ok && v != "" {
			role_arn = v
		}
	}

	c, err := newClient(region, accessKey, secretKey, token, profile, endpoint, role_arn)
	if err != nil {
		return nil, err
	}

	return &GSIProvider{
		c:          c,
		autoImport: d.Get("auto_import").(bool),
	}, nil
}
