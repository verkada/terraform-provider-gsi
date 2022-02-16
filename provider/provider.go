package provider

import (
	"errors"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

type GSIProvider struct {
	c          *dynamodb.DynamoDB
	autoImport bool
}

func Provider() *schema.Provider {
	return &schema.Provider{
		Schema: map[string]*schema.Schema{
			"access_key": {
				Type:     schema.TypeString,
				Optional: true,
				DefaultFunc: schema.EnvDefaultFunc("AWS_ACCESS_KEY_ID", nil),
			},

			"secret_key": {
				Type:     schema.TypeString,
				Optional: true,
				DefaultFunc: schema.EnvDefaultFunc("AWS_SECRET_ACCESS_KEY", nil),
			},

			"token": {
				Type:     schema.TypeString,
				Optional: true,
				DefaultFunc: schema.EnvDefaultFunc("AWS_SESSION_TOKEN", nil),
			},

			"profile": {
				Type:     schema.TypeString,
				Optional: true,
				DefaultFunc: schema.EnvDefaultFunc("AWS_PROFILE", nil),
			},

			"auto_import": {
				Type:     schema.TypeBool,
				Optional: true,
				Default:  false,
			},

			"region": {
				Type:     schema.TypeString,
				Required: true,
				DefaultFunc: schema.MultiEnvDefaultFunc([]string{
					"AWS_REGION",
					"AWS_DEFAULT_REGION",
				}, nil),
				InputDefault: "us-east-1", // lintignore:AWSAT003
			},
		},
		ResourcesMap: map[string]*schema.Resource{
			"gsi_global_secondary_index": dynamoDBGSIResource(),
		},
		ConfigureFunc: providerConfigure,
	}
}

func providerConfigure(d *schema.ResourceData) (interface{}, error) {
	accessKey := d.Get("access_key").(string)
	secretKey := d.Get("secret_key").(string)
	token := d.Get("token").(string)
	profile := d.Get("profile").(string)

	options := session.Options{}
	config := aws.Config{
		Region: aws.String(d.Get("region").(string)),
	}
	if accessKey != "" && secretKey != "" {
		config.Credentials = credentials.NewStaticCredentials(accessKey, secretKey, token)
	} else if profile != "" {
		options.SharedConfigState = session.SharedConfigEnable
		options.Profile = profile
	} else {
		return nil, errors.New("no credentials for AWS")
	}

	sess, err := session.NewSessionWithOptions(options)
	if err != nil {
		return nil, err
	}

	return &GSIProvider{
		c:          dynamodb.New(sess),
		autoImport: d.Get("auto_import").(bool),
	}, nil
}
