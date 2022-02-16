terraform {
  required_providers {
    vinfra = {
      version = "0.1"
      source = "verkada/vinfra"
    }
  }
}

provider "vinfra" {
    region = "us-west-2"
    profile = "hiring"
    auto_import = true
}

resource "vinfra_gsi" "sample" {
    table_name = "sample-table"
    name            = "effectiveEntityId-creation-index-2"
    hash_key        = "effectiveEntityId"
    non_key_attributes = [
    "tokenHash",
    ]
    range_key       = "creation"
    projection_type = "INCLUDE"
    initial_read_capacity   = 5
    initial_write_capacity  = 5
}