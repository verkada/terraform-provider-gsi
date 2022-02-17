terraform {
    required_providers {
        gsi = {
            source  = "verkada/gsi"
        }
    }
}

provider "aws" {
}

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
    ignore_changes = [global_secondary_index, attribute]
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

    depends_on = [
      aws_dynamodb_table.test_table
    ]
}
