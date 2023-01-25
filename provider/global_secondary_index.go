package provider

import (
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/validation"
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
				Type:        schema.TypeString,
				Computed:    true,
				Description: "ARN of the Global Secondary Index.",
			},
			"table_name": {
				Type:        schema.TypeString,
				Required:    true,
				ForceNew:    true,
				Description: "Name of the DynamoDB table to which the GSI is associated..",
			},
			"name": {
				Type:        schema.TypeString,
				Required:    true,
				ForceNew:    true,
				Description: "Name of the index.",
			},
			"non_key_attributes": {
				Type:        schema.TypeSet,
				Optional:    true,
				Elem:        &schema.Schema{Type: schema.TypeString},
				ForceNew:    true,
				Description: "Additional attributes to include based in the projection.",
			},
			"projection_type": {
				Type:         schema.TypeString,
				Required:     true,
				ValidateFunc: stringInSlice(dynamodb.ProjectionType_Values(), false),
				ForceNew:     true,
				Description:  "Projection type.",
			},
			"hash_key": {
				Type:        schema.TypeString,
				Required:    true,
				ForceNew:    true,
				Description: "Hash key of the index.",
			},
			"range_key": {
				Type:        schema.TypeString,
				Optional:    true,
				ForceNew:    true,
				Description: "Range key of the index.",
			},
			"hash_key_type": {
				Type:        schema.TypeString,
				Required:    true,
				ForceNew:    true,
				Description: "Type of the hash key.",
			},
			"range_key_type": {
				Type:        schema.TypeString,
				Optional:    true,
				ForceNew:    true,
				Description: "Type of the range key.",
			},
			"billing_mode": {
				Type:         schema.TypeString,
				Optional:     true,
				ValidateFunc: stringInSlice(dynamodb.BillingMode_Values(), false),
				Default:      dynamodb.BillingModeProvisioned,
				Description:  "The billing mode to apply to this index. Should match the associated table",
			},
			"read_capacity": {
				Type:        schema.TypeInt,
				Optional:    true,
				Description: "Read capacity for the index, untracked after creation if autoscaling is enabled.",
				DiffSuppressFunc: func(k, old, new string, d *schema.ResourceData) bool {
					return old != "" && d.Get("autoscaling_enabled").(bool)
				},
				ValidateFunc: validation.IntAtLeast(0),
			},
			"write_capacity": {
				Type:        schema.TypeInt,
				Optional:    true,
				Description: "Write capacity for the table, untracked after creation if autoscaling is enabled.",
				DiffSuppressFunc: func(k, old, new string, d *schema.ResourceData) bool {
					return old != "" && d.Get("autoscaling_enabled").(bool)
				},
				ValidateFunc: validation.IntAtLeast(0),
			},
			"autoscaling_enabled": {
				Type:        schema.TypeBool,
				Optional:    true,
				Description: "Whether capacity is controlled by an autoscaler.",
				Default:     false,
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
		// If auto-import is enabled, we just capture the current state and a drift should be
		// expected of the next plan if the imported state is different from that of this GSI.
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

	ad, err := getAttributeDefinition(p.c, tn)
	if err != nil {
		return err
	}

	hType := d.Get("hash_key_type")
	rhType := getAttributeType(ad, aws.String(d.Get("hash_key").(string)))
	if rhType == "" {
		ad = append(ad, &dynamodb.AttributeDefinition{
			AttributeName: aws.String(d.Get("hash_key").(string)),
			AttributeType: aws.String(hType.(string)),
		})
	} else if rhType != hType {
		return errors.New("Hash key type does not match the existing definition on the table")
	}

	keySchema := []*dynamodb.KeySchemaElement{
		&dynamodb.KeySchemaElement{
			AttributeName: aws.String(d.Get("hash_key").(string)),
			KeyType:       aws.String(dynamodb.KeyTypeHash),
		},
	}

	if r, ok := d.GetOk("range_key"); ok {
		rType, e := d.GetOkExists("range_key_type")
		if !e {
			return errors.New("Missing range_key_type")
		}
		rrType := getAttributeType(ad, aws.String(r.(string)))
		if rrType == "" {
			ad = append(ad, &dynamodb.AttributeDefinition{
				AttributeName: aws.String(r.(string)),
				AttributeType: aws.String(rType.(string)),
			})
		} else if rType != rrType {
			return errors.New("Range key type does not match the existing definition on the table")
		}

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

	if err = validateBillingMode(d); err != nil {
		return err
	}

	input := dynamodb.UpdateTableInput{
		TableName:            aws.String(tn),
		AttributeDefinitions: ad,
		GlobalSecondaryIndexUpdates: []*dynamodb.GlobalSecondaryIndexUpdate{
			&dynamodb.GlobalSecondaryIndexUpdate{
				Create: &dynamodb.CreateGlobalSecondaryIndexAction{
					IndexName:  aws.String(in),
					KeySchema:  keySchema,
					Projection: projection,
				},
			},
		},
	}

	if d.Get("billing_mode") == dynamodb.BillingModeProvisioned {
		input.GlobalSecondaryIndexUpdates[0].Create.ProvisionedThroughput = &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(int64(d.Get("read_capacity").(int))),
			WriteCapacityUnits: aws.Int64(int64(d.Get("write_capacity").(int))),
		}
	}

	_, err = p.c.UpdateTable(&input)
	if err != nil {
		return fmt.Errorf("error creating DynamoDB GSI (%s) on table %s: %w", in, tn, err)
	}

	if err = waitDynamoDBGSIActive(p.c, tn, in); err != nil {
		return err
	}

	if d.Get("autoscaling_enabled").(bool) {
		// Don't persist the capacity in the state if it is managed by an autoscaler.
		d.Set("read_capacity", nil)
		d.Set("write_capacity", nil)
	}

	d.SetId(fmt.Sprintf("%s:%s", tn, in))

	return dynamoDBGSIRead(d, m)
}

func validateBillingMode(d *schema.ResourceData) error {
	readCapacity := d.Get("read_capacity").(int)
	writCapacity := d.Get("write_capacity").(int)
	switch d.Get("billing_mode") {
	case dynamodb.BillingModePayPerRequest:
		if readCapacity != 0 || writCapacity != 0 {
			return errors.New("read_capacity / write_capacity must not be set for billing_mode = PAY_PER_REQUEST")
		} else if d.Get("autoscaling_enabled").(bool) {
			return errors.New("autoscaling cannot be enabled with billing_mode = PAY_PER_REQUEST")
		}
	case dynamodb.BillingModeProvisioned:
		if readCapacity == 0 || writCapacity == 0 {
			return errors.New("read_capacity / write_capacity must be set to a value >= 1 for billing_mode = PROVISIONED")
		}
	}
	return nil
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
	// Convert the GSI name to (table_name, index_name).
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

func getAttributeType(ad []*dynamodb.AttributeDefinition, n *string) string {
	for _, attr := range ad {
		if *attr.AttributeName == *n {
			return *attr.AttributeType
		}
	}
	return ""
}

func readGSI(d *schema.ResourceData, c *dynamodb.DynamoDB, tn string, in string) (bool, error) {
	t, i, err := describeGSI(c, tn, in)
	if err != nil {
		return false, err
	}

	if i == nil {
		return false, nil
	}

	d.Set("arn", i.IndexArn)

	// Since readGSI can be used on an import on create, we need to erase the optional values from the
	// state or we will end up with writing a state that is the expected one rather than the applied one
	// if the applied one does not have the values set.
	d.Set("non_key_attributes", []string{})
	for _, attr := range []string{"range_key", "projection_type"} {
		d.Set(attr, nil)
	}

	for _, attribute := range i.KeySchema {
		attrType := getAttributeType(t.AttributeDefinitions, attribute.AttributeName)
		if attrType == "" {
			return true, fmt.Errorf("Attribute %s not defined on table", *attribute.AttributeName)
		}

		if aws.StringValue(attribute.KeyType) == dynamodb.KeyTypeHash {
			d.Set("hash_key", attribute.AttributeName)
			d.Set("hash_key_type", attrType)
		}

		if aws.StringValue(attribute.KeyType) == dynamodb.KeyTypeRange {
			d.Set("range_key", attribute.AttributeName)
			d.Set("range_key_type", attrType)
		}
	}

	if i.Projection != nil {
		d.Set("projection_type", aws.StringValue(i.Projection.ProjectionType))
		d.Set("non_key_attributes", aws.StringValueSlice(i.Projection.NonKeyAttributes))
	}

	if i.ProvisionedThroughput != nil {
		d.Set("read_capacity", i.ProvisionedThroughput.ReadCapacityUnits)
		d.Set("write_capacity", i.ProvisionedThroughput.WriteCapacityUnits)
	}

	return true, nil
}

func dynamoDBGSIUpdate(d *schema.ResourceData, m interface{}) error {
	c := m.(*GSIProvider).c
	tn, in, err := idToNames(d.Id())

	if err != nil {
		return err
	}

	if err = validateBillingMode(d); err != nil {
		return err
	}

	if !d.Get("autoscaling_enabled").(bool) && d.Get("billing_mode") == dynamodb.BillingModeProvisioned {
		update := &dynamodb.UpdateGlobalSecondaryIndexAction{
			IndexName:             aws.String(in),
			ProvisionedThroughput: &dynamodb.ProvisionedThroughput{},
		}

		changed := false
		if d.HasChange("read_capaciity") {
			changed = true
			update.ProvisionedThroughput.ReadCapacityUnits = aws.Int64(int64(d.Get("read_capacity").(int)))
		}
		if d.HasChange("write_capacity") {
			changed = true
			update.ProvisionedThroughput.WriteCapacityUnits = aws.Int64(int64(d.Get("write_capacity").(int)))
		}

		if changed {
			if _, err := c.UpdateTable(&dynamodb.UpdateTableInput{
				TableName: aws.String(tn),
				GlobalSecondaryIndexUpdates: []*dynamodb.GlobalSecondaryIndexUpdate{
					&dynamodb.GlobalSecondaryIndexUpdate{
						Update: update,
					},
				},
			}); err != nil {
				return err
			}

			if err := waitDynamoDBGSIActive(c, tn, in); err != nil {
				return fmt.Errorf("error waiting for DynamoDB GSI (%s) update on table %s: %w", in, tn, err)
			}
		}
	}

	return dynamoDBGSIRead(d, m)
}

func dynamoDBGSIDelete(d *schema.ResourceData, m interface{}) error {
	c := m.(*GSIProvider).c
	tn, in, err := idToNames(d.Id())
	if err != nil {
		return err
	}

	log.Printf("[DEBUG] Deleting Dynamodb Table GSI %s on table %s", in, tn)

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

func describeGSI(c *dynamodb.DynamoDB, tn string, in string) (*dynamodb.TableDescription, *dynamodb.GlobalSecondaryIndexDescription, error) {
	t, err := c.DescribeTable(&dynamodb.DescribeTableInput{
		TableName: aws.String(tn),
	})
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok && aerr.Code() == dynamodb.ErrCodeResourceNotFoundException {
			return nil, nil, nil
		}

		return nil, nil, fmt.Errorf("error reading Dynamodb Table (%s): %w", tn, err)
	}

	for _, i := range t.Table.GlobalSecondaryIndexes {
		if *i.IndexName == in {
			return t.Table, i, nil
		}
	}

	return nil, nil, nil
}

func statusDynamoDBGSI(c *dynamodb.DynamoDB, tn string, in string) resource.StateRefreshFunc {
	return func() (interface{}, string, error) {
		_, i, err := describeGSI(c, tn, in)
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
	stateConf := &resource.StateChangeConf{
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
	stateConf := &resource.StateChangeConf{
		Pending: []string{
			dynamodb.IndexStatusUpdating,
		},
		Target: []string{
			dynamodb.IndexStatusCreating,
			dynamodb.IndexStatusActive,
		},
		Timeout: createGSITimeout,
		Refresh: statusDynamoDBGSI(c, tn, in),
	}

	_, err := stateConf.WaitForState()

	return err
}
