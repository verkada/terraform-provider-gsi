---
# generated by https://github.com/hashicorp/terraform-plugin-docs
page_title: "gsi_global_secondary_index Resource - terraform-provider-gsi"
subcategory: ""
description: |-
  
---

# gsi_global_secondary_index (Resource)





<!-- schema generated by tfplugindocs -->
## Schema

### Required

- **hash_key** (String) Hash key of the index.
- **hash_key_type** (String) Type of the hash key.
- **name** (String) Name of the index.
- **projection_type** (String) Projection type.
- **read_capacity** (Number) Read capacity for the index, untracked after creation if autoscaling is enabled.
- **table_name** (String) Name of the DynamoDB table to which the GSI is associated..
- **write_capacity** (Number) Write capacity for the table, untracked after creation if autoscaling is enabled.

### Optional

- **autoscaling_enabled** (Boolean) Whether capacity is controlled by an autoscaler.
- **id** (String) The ID of this resource.
- **non_key_attributes** (Set of String) Additional attributes to include based in the projection.
- **range_key** (String) Range key of the index.
- **range_key_type** (String) Type of the range key.

### Read-Only

- **arn** (String) ARN of the Global Secondary Index.


