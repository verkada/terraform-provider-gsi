package provider

import (
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

const (
	createGSITimeout = 30 * time.Minute
	updateGSITimeout = 20 * time.Minute
	deleteGSITimeout = 10 * time.Minute
)

func dynamoDBGSIResource() *schema.Resource {
	return &schema.Resource{
		Schema: map[string]*schema.Schema{
			"arn": {
				Type:     schema.TypeString,
				Computed: true,
			},
			"table_name": {
				Type:     schema.TypeString,
				Required: true,
				ForceNew: true,
			},
			"hash_key": {
				Type:     schema.TypeString,
				Required: true,
				ForceNew: true,
			},
			"name": {
				Type:     schema.TypeString,
				Required: true,
				ForceNew: true,
			},
			"non_key_attributes": {
				Type:     schema.TypeSet,
				Optional: true,
				Elem:     &schema.Schema{Type: schema.TypeString},
				ForceNew: true,
			},
			"projection_type": {
				Type:         schema.TypeString,
				Required:     true,
				ValidateFunc: stringInSlice(dynamodb.ProjectionType_Values(), false),
				ForceNew:     true,
			},
			"range_key": {
				Type:     schema.TypeString,
				Optional: true,
				ForceNew: true,
			},
			// An autoscaler must be set, these values are only used on creation.
			"initial_read_capacity": {
				Type:     schema.TypeInt,
				Required: true,
			},
			"initial_write_capacity": {
				Type:     schema.TypeInt,
				Required: true,
			},
		},
		Create: dynamoDBGSICreate,
		Read:   dynamoDBGSIRead,
		Update: dynamoDBGSIUpdate,
		Delete: dynamoDBGSIDelete,
		Importer: &schema.ResourceImporter{
			State: schema.ImportStatePassthrough,
		},
	}
}

func dynamoDBGSICreate(d *schema.ResourceData, m interface{}) error {
	p := m.(*GSIProvider)
	tn := d.Get("table_name").(string)
	in := d.Get("name").(string)

	if d.IsNewResource() && p.autoImport {
		found, err := readGSI(d, p.c, tn, in)
		if err != nil {
			return err
		}

		if found {
			d.SetId(fmt.Sprintf("%s:%s", tn, in))
			log.Printf("[INFO] Dynamodb Table GSI (%s) automatically imported", d.Get("name").(string))
			return nil
		}
	}

	keySchema := []*dynamodb.KeySchemaElement{
		&dynamodb.KeySchemaElement{
			AttributeName: aws.String(d.Get("hash_key").(string)),
			KeyType:       aws.String(dynamodb.KeyTypeHash),
		},
	}

	if r, ok := d.GetOk("range_key"); ok {
		keySchema = append(keySchema, &dynamodb.KeySchemaElement{
			AttributeName: aws.String(r.(string)),
			KeyType:       aws.String(dynamodb.KeyTypeRange),
		})
	}

	projection := &dynamodb.Projection{
		NonKeyAttributes: nil,
		ProjectionType:   aws.String(d.Get("projection_type").(string)),
	}
	if v, ok := d.GetOk("non_key_attributes"); ok {
		nka := v.(*schema.Set).List()
		projection.NonKeyAttributes = make([]*string, 0, len(nka))
		for _, a := range nka {
			projection.NonKeyAttributes = append(projection.NonKeyAttributes, aws.String(a.(string)))
		}
	}

	ad, err := getAttributeDefinition(p.c, tn)
	if err != nil {
		return err
	}

	input := dynamodb.UpdateTableInput{
		TableName: aws.String(tn),
		AttributeDefinitions: ad,
		GlobalSecondaryIndexUpdates: []*dynamodb.GlobalSecondaryIndexUpdate{
			&dynamodb.GlobalSecondaryIndexUpdate{
				Create: &dynamodb.CreateGlobalSecondaryIndexAction{
					IndexName:  aws.String(in),
					KeySchema:  keySchema,
					Projection: projection,
					ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
						ReadCapacityUnits:  aws.Int64(int64(d.Get("initial_read_capacity").(int))),
						WriteCapacityUnits: aws.Int64(int64(d.Get("initial_write_capacity").(int))),
					},
				},
			},
		},
	}

	_, err = p.c.UpdateTable(&input)
	if err != nil {
		return fmt.Errorf("error creating DynamoDB GSI (%s) on table %s: %w", in, tn, err)
	}

	if err = waitDynamoDBGSIActive(p.c, tn, in); err != nil {
		return err
	}

	d.SetId(fmt.Sprintf("%s:%s", tn, in))

	return dynamoDBGSIRead(d, m)
}

func getAttributeDefinition(c *dynamodb.DynamoDB, tn string) ([]*dynamodb.AttributeDefinition, error) {
	t, err := c.DescribeTable(&dynamodb.DescribeTableInput{
		TableName: aws.String(tn),
	})
	if err != nil {
		return nil, err
	}

	return t.Table.AttributeDefinitions, nil
}

func idToNames(id string) (string, string, error) {
	splits := strings.SplitN(id, ":", 2)
	if len(splits) != 2 {
		return "", "", fmt.Errorf("Invalid DynamoDB GSI ID (%s)", id)
	}
	return splits[0], splits[1], nil
}

func dynamoDBGSIRead(d *schema.ResourceData, m interface{}) error {
	c := m.(*GSIProvider).c
	tn, in, err := idToNames(d.Id())

	if err != nil {
		return err
	}

	found, err := readGSI(d, c, tn, in)
	if !found {
		if !d.IsNewResource() {
			log.Printf("[WARN] Dynamodb Table GSI (%s) not found, removing from state", d.Id())
			d.SetId("")
			return nil
		}

		return fmt.Errorf("dynamodb table (%s) or GSI not found (%s)", tn, in)

	}

	return err
}

func readGSI(d *schema.ResourceData, c *dynamodb.DynamoDB, tn string, in string) (bool, error) {
	i, err := describeGSI(c, tn, in)
	if err != nil {
		return false, err
	}

	if i == nil {
		return false, nil
	}

	d.Set("arn", i.IndexArn)

	d.Set("range_key", nil)
	for _, attribute := range i.KeySchema {
		if aws.StringValue(attribute.KeyType) == dynamodb.KeyTypeHash {
			d.Set("hash_key", attribute.AttributeName)
		}

		if aws.StringValue(attribute.KeyType) == dynamodb.KeyTypeRange {
			d.Set("range_key", attribute.AttributeName)
		}
	}

	d.Set("non_key_attributes", nil)
	d.Set("projection_type", nil)
	if i.Projection != nil {
		d.Set("projection_type", aws.StringValue(i.Projection.ProjectionType))
		d.Set("non_key_attributes", aws.StringValueSlice(i.Projection.NonKeyAttributes))
	}

	return true, nil
}

func dynamoDBGSIUpdate(d *schema.ResourceData, m interface{}) error {
	// Non of the fields are updatable.
	return nil
}

func dynamoDBGSIDelete(d *schema.ResourceData, m interface{}) error {
	c := m.(*GSIProvider).c
	tn, in, err := idToNames(d.Id())
	if err != nil {
		return err
	}

	_, err = c.UpdateTable(&dynamodb.UpdateTableInput{
		TableName: aws.String(tn),
		GlobalSecondaryIndexUpdates: []*dynamodb.GlobalSecondaryIndexUpdate{
			&dynamodb.GlobalSecondaryIndexUpdate{
				Delete: &dynamodb.DeleteGlobalSecondaryIndexAction{
					IndexName: aws.String(in),
				},
			},
		},
	})

	if err != nil {
		if aerr, ok := err.(awserr.Error); ok && aerr.Code() == dynamodb.ErrCodeResourceNotFoundException {
			return fmt.Errorf("dynamodb table %s or index %s does not exist", tn, in)
		}
		return fmt.Errorf("failed to delete GSI %s", in)
	}

	if err := waitDynamoDBGSIDeleted(c, tn, in); err != nil {
		return fmt.Errorf("error waiting for DynamoDB GSI (%s) deletion on table %s: %w", in, tn, err)
	}

	return nil
}

func describeGSI(c *dynamodb.DynamoDB, tn string, in string) (*dynamodb.GlobalSecondaryIndexDescription, error) {
	t, err := c.DescribeTable(&dynamodb.DescribeTableInput{
		TableName: aws.String(tn),
	})
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok && aerr.Code() == dynamodb.ErrCodeResourceNotFoundException {
			return nil, nil
		}

		return nil, fmt.Errorf("error reading Dynamodb Table (%s): %w", tn, err)
	}

	for _, i := range t.Table.GlobalSecondaryIndexes {
		if *i.IndexName == in {
			return i, nil
		}
	}

	return nil, nil
}

func statusDynamoDBGSI(c *dynamodb.DynamoDB, tn string, in string) StateRefreshFunc {
	return func() (interface{}, string, error) {
		i, err := describeGSI(c, tn, in)
		if err != nil {
			return nil, "", err
		}
		if i == nil {
			return nil, "", nil
		}

		return i, aws.StringValue(i.IndexStatus), nil
	}
}

func waitDynamoDBGSIDeleted(c *dynamodb.DynamoDB, tn string, in string) error {
	stateConf := &StateChangeConf{
		Pending: []string{
			dynamodb.IndexStatusDeleting,
			dynamodb.IndexStatusActive,
		},
		Target:  []string{},
		Timeout: deleteGSITimeout,
		Refresh: statusDynamoDBGSI(c, tn, in),
	}

	_, err := stateConf.WaitForState()

	return err
}

func waitDynamoDBGSIActive(c *dynamodb.DynamoDB, tn string, in string) error {
	stateConf := &StateChangeConf{
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
