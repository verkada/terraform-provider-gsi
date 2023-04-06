package provider

import (
	"errors"
	"fmt"
	"log"
	"os"
	"regexp"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/terraform"
)

func newTestClient() (*dynamodb.DynamoDB, error) {
	accessKey := os.Getenv("AWS_ACCESS_KEY_ID")
	secretKey := os.Getenv("AWS_SECRET_ACCESS_KEY")
	token := os.Getenv("AWS_TOKEN")
	profile := os.Getenv("AWS_PROFILE")
	region := os.Getenv("AWS_REGION")
	if region == "" {
		region = "us-east-1"
	}
	endpoint := os.Getenv("AWS_DYNAMODB_ENDPOINT")

	return newClient(region, accessKey, secretKey, token, profile, endpoint, "")
}

func statusDynamoDBTable(c *dynamodb.DynamoDB, tn string) resource.StateRefreshFunc {
	return func() (interface{}, string, error) {
		t, err := c.DescribeTable(&dynamodb.DescribeTableInput{TableName: aws.String(tn)})
		if err != nil {
			if aerr, ok := err.(awserr.Error); ok && aerr.Code() == dynamodb.ErrCodeResourceNotFoundException {
				return nil, "", nil
			}
			return nil, "", err
		}

		return t.Table, aws.StringValue(t.Table.TableStatus), nil
	}
}

func waitDynamoDBTableActive(c *dynamodb.DynamoDB, tn string) error {
	stateConf := &resource.StateChangeConf{
		Pending: []string{
			dynamodb.TableStatusCreating,
			dynamodb.TableStatusUpdating,
		},
		Target: []string{
			dynamodb.TableStatusActive,
		},
		Timeout: 10 * time.Second,
		Refresh: statusDynamoDBTable(c, tn),
	}

	_, err := stateConf.WaitForState()

	return err
}

func testAccPreCheck(t *testing.T, c *dynamodb.DynamoDB, tn string, attributes map[string]string, keys map[string]string) {
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
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(10),
			WriteCapacityUnits: aws.Int64(10),
		},
	}
	if _, err := c.CreateTable(&args); err != nil {
		t.Fatal("Could not create test table", err)
	}

	if err := waitDynamoDBTableActive(c, tn); err != nil {
		t.Fatal("Could not create test table", err)
	}
}

func TestAccCreateProvisionedBasic(t *testing.T) {
	c, err := newTestClient()
	if err != nil {
		t.Fatal("Could not create dynamodb client", err)
		return
	}

	if err := createTable(c, "test_table", map[string]string{"p": "S"}, map[string]string{"p": "HASH"}); err != nil {
		t.Fatal("Failed to create test table", err)
	}

	resource.Test(t, resource.TestCase{
		Providers: map[string]*schema.Provider{
			"gsi": providerWithConfigure(testProviderConfigure(false)),
		},
		Steps: []resource.TestStep{
			{
				Config: `
resource "gsi_global_secondary_index" "gsi" {
	name            = "basic_index"
	table_name      = "test_table"
	read_capacity   = 5
	write_capacity  = 5
	hash_key        = "p"
	hash_key_type   = "S"
	range_key       = "r"
	range_key_type  = "N"
	projection_type = "KEYS_ONLY"
}`,
				Check: resource.ComposeTestCheckFunc(
					waitDynamoGSIActiveCheck(c, "test_table", "basic_index"),
					testAccCheckGSIGlobalSecondaryIndexExists("gsi", "test_table", "basic_index"),
					testAccCheckGSIGlobalSecondaryIndexValues(c, "test_table", "basic_index", "p", "r", "KEYS_ONLY"),
				),
			},
		},
	})
}

func TestAccCreatePayPerRequestBasic(t *testing.T) {
	c, err := newTestClient()
	if err != nil {
		t.Fatal("Could not create dynamodb client", err)
		return
	}

	if err := createTableWithMode(c, "test_table", map[string]string{"p": "S"}, map[string]string{"p": "HASH"}, dynamodb.BillingModePayPerRequest); err != nil {
		t.Fatal("Failed to create test table", err)
	}

	resource.Test(t, resource.TestCase{
		Providers: map[string]*schema.Provider{
			"gsi": providerWithConfigure(testProviderConfigure(false)),
		},
		Steps: []resource.TestStep{
			{
				Config: `
resource "gsi_global_secondary_index" "gsi" {
	name            = "basic_index"
	table_name      = "test_table"
	hash_key        = "p"
	hash_key_type   = "S"
	range_key       = "r"
	range_key_type  = "N"
	billing_mode    = "PAY_PER_REQUEST"
	projection_type = "KEYS_ONLY"
}`,
				Check: resource.ComposeTestCheckFunc(
					waitDynamoGSIActiveCheck(c, "test_table", "basic_index"),
					testAccCheckGSIGlobalSecondaryIndexExists("gsi", "test_table", "basic_index"),
					testAccCheckGSIGlobalSecondaryIndexValues(c, "test_table", "basic_index", "p", "r", "KEYS_ONLY"),
				),
			},
		},
	})
}

func TestAccInvalidBillingModeScalingParams(t *testing.T) {
	c, err := newTestClient()
	if err != nil {
		t.Fatal("Could not create dynamodb client", err)
		return
	}

	if err := createTableWithMode(c, "test_table", map[string]string{"p": "S"}, map[string]string{"p": "HASH"}, dynamodb.BillingModePayPerRequest); err != nil {
		t.Fatal("Failed to create test table", err)
	}

	resource.Test(t, resource.TestCase{
		Providers: map[string]*schema.Provider{
			"gsi": providerWithConfigure(testProviderConfigure(false)),
		},
		Steps: []resource.TestStep{
			{
				Config: `
resource "gsi_global_secondary_index" "gsi" {
	name            = "basic_index"
	table_name      = "test_table"
	hash_key        = "p"
	hash_key_type   = "S"
	range_key       = "r"
	range_key_type  = "N"
	read_capacity   = 10
	billing_mode    = "PAY_PER_REQUEST"
	projection_type = "KEYS_ONLY"
}`,
				ExpectError: regexp.MustCompile("read_capacity / write_capacity must not be set for billing_mode = PAY_PER_REQUEST"),
			},
			{
				Config: `
resource "gsi_global_secondary_index" "gsi" {
	name                = "basic_index"
	table_name          = "test_table"
	hash_key            = "p"
	hash_key_type       = "S"
	range_key           = "r"
	range_key_type      = "N"
	autoscaling_enabled = true
	billing_mode        = "PAY_PER_REQUEST"
	projection_type     = "KEYS_ONLY"
}`,
				ExpectError: regexp.MustCompile("autoscaling cannot be enabled with billing_mode = PAY_PER_REQUEST"),
			},
			{
				Config: `
resource "gsi_global_secondary_index" "gsi" {
	name                = "basic_index"
	table_name          = "test_table"
	hash_key            = "p"
	hash_key_type       = "S"
	range_key           = "r"
	range_key_type      = "N"
	billing_mode        = "PROVISIONED"
	projection_type     = "KEYS_ONLY"
}`,
				ExpectError: regexp.MustCompile("read_capacity / write_capacity must be set to a value >= 1 for billing_mode = PROVISIONED"),
			},
		},
	})
}

func TestAccCreateBasicAutoscaling(t *testing.T) {
	c, err := newTestClient()
	if err != nil {
		t.Fatal("Could not create dynamodb client", err)
		return
	}

	if err := createTable(c, "test_table", map[string]string{"p": "S"}, map[string]string{"p": "HASH"}); err != nil {
		t.Fatal("Failed to create test table", err)
	}

	resource.Test(t, resource.TestCase{
		Providers: map[string]*schema.Provider{
			"gsi": providerWithConfigure(testProviderConfigure(false)),
		},
		Steps: []resource.TestStep{
			{
				Config: `
resource "gsi_global_secondary_index" "gsi" {
	name                = "basic_index"
	table_name          = "test_table"
	read_capacity       = 5
	write_capacity      = 5
	hash_key            = "p"
	hash_key_type       = "S"
	range_key           = "r"
	range_key_type      = "N"
	projection_type     = "KEYS_ONLY"
	autoscaling_enabled = true
}`,
				Check: resource.ComposeTestCheckFunc(
					waitDynamoGSIActiveCheck(c, "test_table", "basic_index"),
					testAccCheckGSIGlobalSecondaryIndexExists("gsi", "test_table", "basic_index"),
					testAccCheckGSIGlobalSecondaryIndexValues(c, "test_table", "basic_index", "p", "r", "KEYS_ONLY"),
				),
			},
		},
	})
}

func TestAccSwitchAutoscaling(t *testing.T) {
	c, err := newTestClient()
	if err != nil {
		t.Fatal("Could not create dynamodb client", err)
		return
	}

	if err := createTable(c, "test_table", map[string]string{"p": "S"}, map[string]string{"p": "HASH"}); err != nil {
		t.Fatal("Failed to create test table", err)
	}

	resource.Test(t, resource.TestCase{
		Providers: map[string]*schema.Provider{
			"gsi": providerWithConfigure(testProviderConfigure(false)),
		},
		Steps: []resource.TestStep{
			{
				Config: `
resource "gsi_global_secondary_index" "gsi" {
	name                = "basic_index"
	table_name          = "test_table"
	read_capacity       = 5
	write_capacity      = 5
	hash_key            = "p"
	hash_key_type       = "S"
	range_key           = "r"
	range_key_type      = "N"
	projection_type     = "KEYS_ONLY"
	autoscaling_enabled = false
}`,
				Check: resource.ComposeTestCheckFunc(
					testAccCheckGSIGlobalSecondaryIndexExists("gsi", "test_table", "basic_index"),
					testAccCheckGSIGlobalSecondaryIndexValues(c, "test_table", "basic_index", "p", "r", "KEYS_ONLY"),
				),
			},
			{
				PreConfig: simulateAutoscaling(c, "test_table", "basic_index", 10, 10),
				Config: `
resource "gsi_global_secondary_index" "gsi" {
	name                = "basic_index"
	table_name          = "test_table"
	read_capacity       = 5
	write_capacity      = 5
	hash_key            = "p"
	hash_key_type       = "S"
	range_key           = "r"
	range_key_type      = "N"
	projection_type     = "KEYS_ONLY"
	autoscaling_enabled = true
}`,
				Check: resource.ComposeTestCheckFunc(
					testAccCheckGSIGlobalSecondaryIndexExists("gsi", "test_table", "basic_index"),
					testAccCheckGSIGlobalSecondaryIndexValues(c, "test_table", "basic_index", "p", "r", "KEYS_ONLY"),
				),
			},
		},
	})
}

func TestAccAutoImport(t *testing.T) {
	c, err := newTestClient()
	if err != nil {
		t.Fatal("Could not create dynamodb client", err)
		return
	}

	if err := createTable(c, "test_table", map[string]string{"p": "S"}, map[string]string{"p": "HASH"}); err != nil {
		t.Fatal("Failed to create test table", err)
	}

	input := dynamodb.UpdateTableInput{
		TableName: aws.String("test_table"),
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String("p"),
				AttributeType: aws.String("S"),
			},
			{
				AttributeName: aws.String("r"),
				AttributeType: aws.String("S"),
			},
		},
		GlobalSecondaryIndexUpdates: []*dynamodb.GlobalSecondaryIndexUpdate{
			&dynamodb.GlobalSecondaryIndexUpdate{
				Create: &dynamodb.CreateGlobalSecondaryIndexAction{
					IndexName: aws.String("basic_index"),
					KeySchema: []*dynamodb.KeySchemaElement{
						{
							AttributeName: aws.String("p"),
							KeyType:       aws.String(dynamodb.KeyTypeHash),
						},
						{
							AttributeName: aws.String("r"),
							KeyType:       aws.String(dynamodb.KeyTypeRange),
						},
					},
					Projection: &dynamodb.Projection{
						NonKeyAttributes: nil,
						ProjectionType:   aws.String("KEYS_ONLY"),
					},
					ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
						ReadCapacityUnits:  aws.Int64(10),
						WriteCapacityUnits: aws.Int64(10),
					},
				},
			},
		},
	}

	_, err = c.UpdateTable(&input)
	if err != nil {
		log.Fatal("Failed to update table", err)
	}

	if err = waitDynamoDBGSIActive(c, "test_table", "basic_index"); err != nil {
		log.Fatal("Failed to update table", err)
	}

	resource.Test(t, resource.TestCase{
		Providers: map[string]*schema.Provider{
			"gsi": providerWithConfigure(testProviderConfigure(true)),
		},
		Steps: []resource.TestStep{
			{
				Config: `
resource "gsi_global_secondary_index" "gsi" {
	name                = "basic_index"
	table_name          = "test_table"
	read_capacity       = 5
	write_capacity      = 5
	hash_key            = "p"
	hash_key_type       = "S"
	range_key           = "r"
	range_key_type      = "S"
	projection_type     = "KEYS_ONLY"
	autoscaling_enabled = true
}`,
				Check: resource.ComposeTestCheckFunc(
					testAccCheckGSIGlobalSecondaryIndexExists("gsi", "test_table", "basic_index"),
					testAccCheckGSIGlobalSecondaryIndexValues(c, "test_table", "basic_index", "p", "r", "KEYS_ONLY"),
				),
			},
		},
	})
}

func simulateAutoscaling(c *dynamodb.DynamoDB, tn, in string, rc, wc int64) func() {
	return func() {
		input := dynamodb.UpdateTableInput{
			TableName: aws.String(tn),
			GlobalSecondaryIndexUpdates: []*dynamodb.GlobalSecondaryIndexUpdate{
				&dynamodb.GlobalSecondaryIndexUpdate{
					Update: &dynamodb.UpdateGlobalSecondaryIndexAction{
						IndexName: aws.String(in),
						ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
							ReadCapacityUnits:  aws.Int64(rc),
							WriteCapacityUnits: aws.Int64(wc),
						},
					},
				},
			},
		}

		_, err := c.UpdateTable(&input)
		if err != nil {
			log.Fatal("Failed to update table", err)
		}

		if err = waitDynamoDBGSIActive(c, tn, in); err != nil {
			log.Fatal("Failed to update table", err)
		}
	}
}

func testAccCheckGSIGlobalSecondaryIndexValues(c *dynamodb.DynamoDB, tn, in string, hashKey, rangeKey string, projection string) resource.TestCheckFunc {
	return func(_ *terraform.State) error {
		_, gsi, err := describeGSI(c, tn, in)
		if err != nil {
			return err
		}

		if gsi == nil {
			return fmt.Errorf("GSI %s not found on table %s", in, tn)
		}

		rHashKey := ""
		rRangeKey := ""
		for _, k := range gsi.KeySchema {
			if aws.StringValue(k.KeyType) == dynamodb.KeyTypeHash {
				rHashKey = aws.StringValue(k.AttributeName)
			} else if aws.StringValue(k.KeyType) == dynamodb.KeyTypeRange {
				rRangeKey = aws.StringValue(k.AttributeName)
			}
		}

		if rHashKey != hashKey {
			return errors.New("Invalid hash key")
		}

		if rRangeKey != rangeKey {
			return errors.New("Invalid range key")
		}

		if *gsi.Projection.ProjectionType != projection {
			return errors.New("Invalid projection")
		}

		return nil
	}
}

func testAccCheckGSIGlobalSecondaryIndexExists(rn, tn, in string) resource.TestCheckFunc {
	return func(s *terraform.State) error {
		id := tn + ":" + in
		rs, ok := s.RootModule().Resources["gsi_global_secondary_index."+rn]

		if !ok {
			return fmt.Errorf("Not found: %s", rn)
		}

		if rs.Primary.ID == "" {
			return fmt.Errorf("No ID set")
		}

		if rs.Primary.ID != id {
			return fmt.Errorf("Invalid ID")
		}

		return nil
	}
}

// Waits for the index to be ready before running validation.
// Test cleanup could happen before the index is fully created
func waitDynamoGSIActiveCheck(c *dynamodb.DynamoDB, tn, in string) resource.TestCheckFunc {
	return func(_ *terraform.State) error {
		stateConf := &resource.StateChangeConf{
			Pending: []string{
				dynamodb.IndexStatusCreating,
				dynamodb.IndexStatusUpdating,
			},
			Target: []string{
				dynamodb.IndexStatusActive,
			},
			Timeout: createGSITimeout,
			Refresh: statusDynamoDBGSI(c, tn, in),
		}

		_, err := stateConf.WaitForState()

		return err
	}
}
