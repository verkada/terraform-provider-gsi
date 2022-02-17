# Terraform Provider GSI

## Overview

This provider is a workaround to a limitation of the AWS provider (and TF language) which prevents the management of global secondary indexes associated with an autoscaler.

The approach is to decouple the table resource (still owned by the AWS provider) from the GSI resource whilegi ignoring the GSIs on the table.

```terraform

provider "gsi" {}

resource "aws_dynamodb_table" "test_table" {
  name           = "test_table"
  read_capacity  = 5
  write_capacity = 5
  hash_key       = "UserId"

  attribute {
    name = "UserId"
    type = "S"
  }

  lifecycle {
    ignore_changes = [global_secondary_index]
  }
}

resource "gsi_global_secondary_index" "test_index" {
  name       = "test_index"
  table_name = aws_dynamodb_table.test_table.name

  hash_key       = "UserId"
  hash_key_type  = "S"
  range_key      = "OrderId"
  range_key_type = "S"

  read_capacity   = 5
  write_capacity  = 5
  projection_type = "KEYS_ONLY"
}
```

Since you might have a lot of existing GSIs already, you can use `auto_import = true` in the provider configuration. When set, the first create will automatically import the GSI if one with the same name exists. Note that it will not attempt to correct drift so it might be a two step process to get to a clean plan.

## Build

Run the following command to build the provider

```shell
go build -o terraform-provider-gsi
```

## Run acceptance tests

```shell
(cd docker-compose; docker-compose up -d)
make testacc
```

## Run unit tests

```shell
make test
```