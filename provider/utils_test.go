package provider

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

func createTable(c *dynamodb.DynamoDB, tn string, attributes map[string]string, keys map[string]string) error {
	return createTableWithMode(c, tn, attributes, keys, dynamodb.BillingModeProvisioned)
}

func createTableWithMode(c *dynamodb.DynamoDB, tn string, attributes map[string]string, keys map[string]string, mode string) error {
	c.DeleteTable(&dynamodb.DeleteTableInput{TableName: aws.String(tn)})

	keySchema := make([]*dynamodb.KeySchemaElement, len(keys))
	i := 0
	for k, v := range keys {
		keySchema[i] = &dynamodb.KeySchemaElement{
			AttributeName: aws.String(k),
			KeyType:       aws.String(v),
		}
		i++
	}

	attributeDefinitions := make([]*dynamodb.AttributeDefinition, len(attributes))
	i = 0
	for k, v := range attributes {
		attributeDefinitions[i] = &dynamodb.AttributeDefinition{
			AttributeName: aws.String(k),
			AttributeType: aws.String(v),
		}
		i++
	}

	args := dynamodb.CreateTableInput{
		TableName:            aws.String(tn),
		AttributeDefinitions: attributeDefinitions,
		KeySchema:            keySchema,
		BillingMode:          &mode,
	}

	if mode == dynamodb.BillingModeProvisioned {
		args.ProvisionedThroughput = &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(10),
			WriteCapacityUnits: aws.Int64(10),
		}
	}

	if _, err := c.CreateTable(&args); err != nil {
		return err
	}

	if err := waitDynamoDBTableActive(c, tn); err != nil {
		return err
	}

	return nil
}

func testProviderConfigure(autoImport bool) schema.ConfigureFunc {
	return func(d *schema.ResourceData) (interface{}, error) {
		accessKey := d.Get("access_key").(string)
		secretKey := d.Get("secret_key").(string)
		token := d.Get("token").(string)
		profile := d.Get("profile").(string)
		region := d.Get("region").(string)
		endpoint := d.Get("dynamodb_endpoint").(string)

		c, err := newClient(region, accessKey, secretKey, token, profile, endpoint, "")
		if err != nil {
			return nil, err
		}

		return &GSIProvider{
			c:          c,
			autoImport: autoImport,
		}, nil
	}
}
